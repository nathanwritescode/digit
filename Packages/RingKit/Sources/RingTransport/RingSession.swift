import Foundation
import RingProtocol

/// What `RingSession` needs from a connection to a ring. `RingConnection` is the Bluetooth
/// implementation; tests supply a scripted one.
public protocol RingLink: Sendable {
    /// Conformers must also refuse frames where `erasesRingData` is true, by throwing `RingError.refusedDestructive`.
    ///
    /// Deadline contract: `send` must return or throw within `writeTimeout`, and after it throws
    /// the link must accept the next `send`. The link owns the write deadline because only it can
    /// clear its own in-flight state. `RingSession` keeps a backstop that is always later than
    /// `writeTimeout`, for a link that breaks this, but a link relying on it may be left unusable.
    /// The session never has two `send` calls in progress at once.
    func send(_ frame: Frame) async throws
    /// The longest `send` may take before it throws.
    var writeTimeout: TimeInterval { get }
    /// The next frame from the ring, in arrival order, or `nil` if none arrives within `timeout`.
    ///
    /// `RingSession` calls this from a single reader task, one call at a time, for as long as the
    /// session runs. With a `timeout` of zero it must return a frame that is already queued, or
    /// `nil` promptly: the session takes that `nil` to mean nothing that arrived before the call
    /// is still waiting to be read.
    func nextFrame(timeout: TimeInterval) async -> Frame?
}

extension RingConnection: RingLink {}

/// Request/response conversations with a connected ring, and the ring's own uploads.
///
/// **Reading.** While the session runs, one reader task takes every frame from the link in
/// arrival order and routes it:
/// - a ring-initiated event (group `0x04`) is acknowledged with the same group and key and
///   payload `[0x00]`, exactly once, whether or not a conversation is in flight, and is then
///   delivered to `uploads()` subscribers;
/// - a live value (group `0x06`) is delivered to subscribers only;
/// - anything else goes to the conversation in flight, or is dropped if there is none, so it can
///   never be taken for the reply to a later request.
///
/// **Conversations.** One runs at a time: a call made while another is in progress waits its
/// turn, so conversations cannot steal each other's frames. Actor methods can interleave at every
/// `await`, so the actor alone is not enough; `exclusively` is the queue. Before a conversation
/// writes anything, the reader drains the link (a zero-timeout read that comes back empty), and
/// only frames read after that are offered to it. Frames that arrived before it are therefore
/// never returned as its reply.
///
/// **Writes.** The link takes one write at a time, so every write (a conversation's command, a
/// history acknowledgement, an event acknowledgement) takes a turn on a second queue, in the
/// order the writes were asked for. Event acknowledgements are written in arrival order by one
/// task. Before a conversation writes, every event already read is acknowledged; an event that
/// arrives during a conversation's write is acknowledged after that write finishes, and before
/// any write the conversation asks for later. A conversation does not return until the
/// acknowledgements for events read before it ended have been written. A failed acknowledgement
/// does not fail the conversation and is not retried.
///
/// **Lifetime.** The reader starts on `start()`, or on the first request or subscription, and
/// ends on `stop()`. Between conversations it waits for frames in slices of at most 0.1 s, so a
/// new conversation can start up to that long after it is asked for.
public actor RingSession {
    private nonisolated let link: any RingLink
    /// How long to wait for each frame, and for each write to be confirmed.
    public nonisolated let replyTimeout: TimeInterval
    /// The most time one conversation (a request, or a whole history transfer) may take receiving,
    /// however many frames keep arriving. It does not cover the caller's `persist` step.
    public nonisolated let conversationTimeout: TimeInterval
    // Injected so tests can pin the time and zone. The zone is read on each use, not once, because
    // a long-lived session can outlast a change of time zone. The autoupdating zone is meant to
    // follow system changes; not verified on a device.
    private nonisolated let clock: @Sendable () -> Date
    private nonisolated let timeZone: @Sendable () -> TimeZone
    // Monotonic, and injected so tests can move time without waiting.
    private nonisolated let now: @Sendable () -> ContinuousClock.Instant
    /// The longest single wait the reader makes. A conversation waits for the reader's current
    /// wait to end before it starts, so this bounds that delay; it is well inside `replyTimeout`
    /// so the drain fits in one reply slice.
    private nonisolated let pollInterval: TimeInterval

    private static let eventGroup: UInt8 = 0x04
    private static let liveGroup: UInt8 = 0x06

    // Conversation turns.
    private var busy = false
    private var turnWaiters: [CheckedContinuation<Void, Never>] = []

    // Write turns.
    private var writing = false
    private var writeWaiters: [CheckedContinuation<Void, Never>] = []

    // The reader.
    private enum ReaderState { case notStarted, running, stopped }
    private var readerState = ReaderState.notStarted
    private var readerGeneration = 0
    private var readerTask: Task<Void, Never>?
    /// Counts the reader's waits, so the drain can insist on an empty read that began after it was asked for.
    private var waitsBegun = 0
    /// Frames the reader took after `stop`; routed first if the session is started again.
    private var carriedOver: [Frame] = []

    // The conversation in flight.
    /// Set while a conversation waits for the link to drain: the first wait numbered at least
    /// this that comes back empty opens the conversation.
    private var drainFromWait: Int?
    private var conversationOpen = false
    private var mailbox: [Frame] = []
    private enum Wake: Sendable { case frame(Frame), drained, timedOut }
    private var waiter: (id: Int, continuation: CheckedContinuation<Wake, Error>, timer: Task<Void, Never>)?
    private var lastWaiterID = 0

    // Event acknowledgements.
    private var pendingAcks: [Frame] = []
    private var acksQueued = 0
    private var acksFinished = 0
    private var ackWriterRunning = false
    private var flushWaiters: [(target: Int, continuation: CheckedContinuation<Void, Never>)] = []

    // Subscribers.
    private var subscribers: [Int: AsyncStream<RingUpload>.Continuation] = [:]
    private var lastSubscriberID = 0

    public init(
        link: any RingLink,
        replyTimeout: TimeInterval = 5,
        conversationTimeout: TimeInterval = 60,
        clock: @escaping @Sendable () -> Date = { Date() },
        timeZone: @escaping @Sendable () -> TimeZone = { TimeZone.autoupdatingCurrent }
    ) {
        self.init(link: link, replyTimeout: replyTimeout, conversationTimeout: conversationTimeout,
                  monotonicNow: { ContinuousClock.now }, clock: clock, timeZone: timeZone)
    }

    /// Test hook: also injects the monotonic clock that the conversation deadline is measured on.
    init(
        link: any RingLink,
        replyTimeout: TimeInterval,
        conversationTimeout: TimeInterval,
        monotonicNow: @escaping @Sendable () -> ContinuousClock.Instant,
        clock: @escaping @Sendable () -> Date = { Date() },
        timeZone: @escaping @Sendable () -> TimeZone = { TimeZone.autoupdatingCurrent }
    ) {
        self.link = link
        self.replyTimeout = replyTimeout
        self.conversationTimeout = conversationTimeout
        self.pollInterval = min(replyTimeout / 4, 0.1)
        self.now = monotonicNow
        self.clock = clock
        self.timeZone = timeZone
    }

    deinit {
        for subscriber in subscribers.values { subscriber.finish() }
    }

    // MARK: Lifetime

    /// Starts reading frames from the link. Requests and subscriptions start the session
    /// themselves if it has never run, so calling this is needed only to acknowledge events
    /// before anything else is asked for, or to resume after `stop()`.
    public func start() {
        guard readerState != .running else { return }
        readerState = .running
        readerGeneration += 1
        let generation = readerGeneration
        let previous = readerTask
        let link = self.link
        // Holds the session weakly, so a session nobody references ends its reader.
        readerTask = Task { [weak self] in
            // A reader left over from before `stop` must have ended, or two would compete for frames.
            await previous?.value
            await self?.routeCarriedOver(generation: generation)
            while let wait = await self?.beginWait(generation: generation) {
                let frame = await link.nextFrame(timeout: wait.timeout)
                await self?.route(frame, wait: wait.number)
            }
        }
    }

    /// Stops reading. The conversation in flight fails with `RingError.notConnected`, as do any
    /// queued behind it and any later ones until `start()` is called again; subscribers' streams
    /// end. Acknowledgements already queued are still written. Returns once the reader has
    /// ended (at most one reader wait later) and those acknowledgements have been written.
    public func stop() async {
        switch readerState {
        case .notStarted:
            readerState = .stopped
            return
        case .stopped:
            return
        case .running:
            break
        }
        readerState = .stopped
        wake(with: .failure(RingError.notConnected))
        for subscriber in subscribers.values { subscriber.finish() }
        subscribers = [:]
        await readerTask?.value
        await flushAcknowledgements()
    }

    /// Ring-initiated events and live values, in arrival order, from now until `stop()`. Each
    /// call returns a new stream; a stream that is no longer iterated should be cancelled or
    /// dropped, because it buffers without limit. After `stop()` the stream is already finished.
    public func uploads() -> AsyncStream<RingUpload> {
        let (stream, continuation) = AsyncStream.makeStream(of: RingUpload.self)
        if readerState == .notStarted { start() }
        guard readerState == .running else {
            continuation.finish()
            return stream
        }
        lastSubscriberID += 1
        let id = lastSubscriberID
        subscribers[id] = continuation
        continuation.onTermination = { [weak self] _ in
            Task { await self?.unsubscribe(id) }
        }
        return stream
    }

    private func unsubscribe(_ id: Int) { subscribers[id] = nil }

    private func publish(_ upload: RingUpload) {
        for subscriber in subscribers.values { subscriber.yield(upload) }
    }

    // MARK: Reader

    private func beginWait(generation: Int) -> (number: Int, timeout: TimeInterval)? {
        guard readerState == .running, generation == readerGeneration else { return nil }
        waitsBegun += 1
        // While a conversation waits for the drain, read without waiting so it starts promptly.
        return (waitsBegun, drainFromWait == nil ? pollInterval : 0)
    }

    private func routeCarriedOver(generation: Int) {
        guard readerState == .running, generation == readerGeneration else { return }
        let frames = carriedOver
        carriedOver = []
        for frame in frames { dispatch(frame) }
    }

    private func route(_ frame: Frame?, wait number: Int) {
        guard let frame else {
            // An empty read that began after the conversation asked for the drain: everything that
            // arrived before then has been routed, so the conversation may start.
            if let from = drainFromWait, number >= from {
                drainFromWait = nil
                conversationOpen = true
                wake(with: .success(.drained))
            }
            return
        }
        guard readerState == .running else {
            carriedOver.append(frame)
            return
        }
        dispatch(frame)
    }

    private func dispatch(_ frame: Frame) {
        switch frame.group {
        case Self.eventGroup:
            queueAcknowledgement(of: frame)
            publish(.event(frame))
        case Self.liveGroup:
            publish(.live(frame))
        default:
            // Outside a conversation this can only be stale: a late reply or the tail of a transfer.
            guard conversationOpen else { return }
            if waiter != nil {
                wake(with: .success(.frame(frame)))
            } else {
                mailbox.append(frame)
            }
        }
    }

    /// Waits for the reader to hand the conversation a frame or report the drain, for at most `timeout`.
    private func wait(timeout: TimeInterval) async throws -> Wake {
        guard readerState == .running else { throw RingError.notConnected }
        lastWaiterID += 1
        let id = lastWaiterID
        return try await withCheckedThrowingContinuation { continuation in
            let timer = Task { [weak self] in
                try? await Task.sleep(for: .seconds(timeout))
                guard !Task.isCancelled else { return }
                await self?.timeOut(waiter: id)
            }
            waiter = (id, continuation, timer)
        }
    }

    private func timeOut(waiter id: Int) {
        guard waiter?.id == id else { return }
        wake(with: .success(.timedOut))
    }

    private func wake(with result: Result<Wake, Error>) {
        guard let current = waiter else { return }
        waiter = nil
        current.timer.cancel()
        current.continuation.resume(with: result)
    }

    // MARK: Conversations

    /// Runs `body` once every earlier conversation has finished. Turns are granted in call order.
    /// The turn is released when `body` returns or throws, so a conversation that times out
    /// does not block the next; every wait inside a conversation is bounded.
    private func exclusively<T: Sendable>(_ body: () async throws -> T) async throws -> T {
        if busy {
            await withCheckedContinuation { turnWaiters.append($0) }
        } else {
            busy = true
        }
        defer {
            // Hand the turn straight to the next waiter so a newcomer cannot jump the queue.
            if turnWaiters.isEmpty { busy = false } else { turnWaiters.removeFirst().resume() }
        }
        return try await body()
    }

    /// One conversation: takes the turn, waits for the link to drain, runs `body` with the
    /// conversation's deadline, and before giving up the turn writes the acknowledgements for
    /// every event read so far.
    private func conversation<T: Sendable>(_ body: (ContinuousClock.Instant) async throws -> T) async throws -> T {
        if readerState == .notStarted { start() }
        return try await exclusively {
            let deadline = deadline()
            let result: Result<T, Error>
            do {
                try await openConversation(until: deadline)
                result = .success(try await body(deadline))
            } catch {
                result = .failure(error)
            }
            conversationOpen = false
            drainFromWait = nil
            mailbox = []
            await flushAcknowledgements()
            return try result.get()
        }
    }

    private func openConversation(until deadline: ContinuousClock.Instant) async throws {
        guard readerState == .running else { throw RingError.notConnected }
        mailbox = []
        drainFromWait = waitsBegun + 1
        while !conversationOpen {
            _ = try await wait(timeout: try waitSlice(until: deadline, "the link to drain"))
        }
    }

    /// The next frame for the conversation, or `nil` if none arrives within one wait slice.
    private func nextFrame(until deadline: ContinuousClock.Instant, _ what: String) async throws -> Frame? {
        // The deadline is checked first, so frames that keep arriving cannot hold a conversation open.
        let slice = try waitSlice(until: deadline, what)
        if !mailbox.isEmpty { return mailbox.removeFirst() }
        guard case .frame(let frame) = try await wait(timeout: slice) else { return nil }
        return frame
    }

    /// Sends a command and returns the ring's reply to it. Ring-initiated events are acknowledged and skipped.
    public func request(_ command: Frame) async throws -> Frame {
        // Also refused in `transmit`; checked here too so a refusal does not wait for a turn.
        guard !command.erasesRingData else { throw RingError.refusedDestructive(command) }
        return try await conversation { try await converse(command, deadline: $0) }
    }

    private func deadline() -> ContinuousClock.Instant { now() + .seconds(conversationTimeout) }

    /// How long to wait for the next frame: `replyTimeout`, but never past `deadline`.
    private func waitSlice(until deadline: ContinuousClock.Instant, _ what: String) throws -> TimeInterval {
        let left = deadline - now()
        guard left > .zero else { throw RingError.timeout(what) }
        return min(replyTimeout, left / .seconds(1))
    }

    private func converse(_ command: Frame, deadline: ContinuousClock.Instant) async throws -> Frame {
        let what = String(format: "reply to %04X", command.type)
        try await transmit(command)
        // Events never reach the conversation: the reader acknowledges them.
        while let frame = try await nextFrame(until: deadline, what) {
            if frame.type == command.type {
                if frame.isErrorReply { throw RingError.rejected(frame) }
                return frame
            }
        }
        throw RingError.timeout(what)
    }

    public func deviceInfo() async throws -> DeviceInfo? {
        DeviceInfo(payload: try await request(Command.deviceInfo).payload)
    }

    public func currentActivity() async throws -> CurrentActivity? {
        CurrentActivity(payload: try await request(Command.currentActivity).payload)
    }

    /// Sets the ring's clock to `date` (default: now by the session's clock) in the session's current time zone.
    public func setTime(_ date: Date? = nil) async throws {
        _ = try await request(Command.setTime(date ?? clock(), timeZone: timeZone()))
    }

    /// Fetches one kind of stored history with the context needed to decode it later. When nothing
    /// is stored the transfer has empty data and `persist` is not called (nothing was received, so
    /// nothing is acknowledged).
    ///
    /// The ring answers with a header frame, then data frames, then an end frame carrying a
    /// checksum of the concatenated data. `persist` then runs with the verified transfer, and only
    /// if it succeeds is the transfer acknowledged, so the data is durable before the ring is
    /// told it was received. No delete command is sent.
    ///
    /// `persist` runs while this session's turn is held, so it must not call back into the
    /// session (any method), or the session deadlocks. Events that arrive meanwhile are still
    /// acknowledged.
    ///
    /// Verified on an R99 with single-record transfers, where the ring kept its history after
    /// the acknowledgement. Transfers spanning several data frames have not been seen yet.
    public func fetchHistory(
        _ kind: HistoryKind,
        persist: @Sendable (HistoryTransfer) async throws -> Void
    ) async throws -> HistoryTransfer {
        try await conversation { try await transferHistory(kind, deadline: $0, persist: persist) }
    }

    private func transferHistory(
        _ kind: HistoryKind,
        deadline: ContinuousClock.Instant,
        persist: @Sendable (HistoryTransfer) async throws -> Void
    ) async throws -> HistoryTransfer {
        let header = try await converse(Command.history(kind), deadline: deadline)
        // A short header means there are no records of this kind.
        guard header.payload.count > 9 else { return makeTransfer(kind, header: header, data: []) }

        var data: [UInt8] = []
        let what = "\(kind.name) history transfer"
        while let frame = try await nextFrame(until: deadline, what) {
            guard frame.group == Command.healthGroup else { continue }
            if frame.key == Command.historyEndKey {
                let p = frame.payload
                let expected = p.count >= 6 ? UInt16(p[4]) | UInt16(p[5]) << 8 : nil
                guard let expected, CRC16.ccittFalse(data) == expected else {
                    // Not acknowledged and no resend requested: the caller starts over with a fresh request.
                    throw RingError.checksumMismatch
                }
                let transfer = makeTransfer(kind, header: header, data: data)
                try await persist(transfer)
                try await transmit(Command.historyTransferOK)
                return transfer
            }
            data += frame.payload
        }
        throw RingError.timeout(what)
    }

    private func makeTransfer(_ kind: HistoryKind, header: Frame, data: [UInt8]) -> HistoryTransfer {
        HistoryTransfer(kind: kind, header: header.payload, data: data,
                        fetchedAt: HistoryTransfer.truncatedToMilliseconds(clock()), timeZoneIdentifier: timeZone().identifier)
    }

    // MARK: Writes

    /// Runs `body` once every earlier write has finished, in call order, so two writes never
    /// reach the link at once.
    private func withWriteTurn<T: Sendable>(_ body: () async throws -> T) async throws -> T {
        if writing {
            await withCheckedContinuation { writeWaiters.append($0) }
        } else {
            writing = true
        }
        defer {
            if writeWaiters.isEmpty { writing = false } else { writeWaiters.removeFirst().resume() }
        }
        return try await body()
    }

    /// Test hook: how many writes are waiting for the write in progress.
    var queuedWrites: Int { writeWaiters.count }

    /// Every write goes through here so the delete-range refusal holds even for a link that forgets it.
    private func transmit(_ frame: Frame, acknowledgement: Bool = false) async throws {
        guard !frame.erasesRingData else { throw RingError.refusedDestructive(frame) }
        // Events already read are acknowledged before a conversation's write, so the order on the
        // link follows the order of arrival. The acknowledgement writer does not wait for itself.
        if !acknowledgement { await flushAcknowledgements() }
        try await withWriteTurn {
            // After `stop`, only event acknowledgements already queued are still written.
            guard acknowledgement || readerState == .running else { throw RingError.notConnected }
            // Backstop only, for a link that breaks the `RingLink.send` contract. A compliant link
            // throws by `writeTimeout` and clears its own in-flight state first; this deadline is
            // always `replyTimeout` later than that, so it fires first only if the link is
            // non-compliant. It is derived from the link's bound so the two cannot drift apart. A
            // final write can therefore overrun the conversation deadline by up to
            // `writeTimeout + replyTimeout`, plus the time spent waiting for queued writes.
            let link = self.link
            try await withDeadline(link.writeTimeout + replyTimeout, "write confirmation") { try await link.send(frame) }
        }
    }

    // MARK: Event acknowledgements

    /// Events the ring starts on its own (group `0x04`) must be echoed back with payload `[0x00]`.
    /// Queued here in arrival order and written by one task, through the write queue.
    private func queueAcknowledgement(of event: Frame) {
        pendingAcks.append(Frame(group: event.group, key: event.key, payload: [0x00]))
        acksQueued += 1
        guard !ackWriterRunning else { return }
        ackWriterRunning = true
        Task { await writeAcknowledgements() }
    }

    private func writeAcknowledgements() async {
        while !pendingAcks.isEmpty {
            let ack = pendingAcks.removeFirst()
            // Not retried: a write that failed may still have reached the ring, and a second
            // acknowledgement of the same event is not known to be harmless. Whether the ring
            // repeats an event that was never acknowledged is unverified.
            try? await transmit(ack, acknowledgement: true)
            acksFinished += 1
            let ready = flushWaiters.filter { $0.target <= acksFinished }
            flushWaiters.removeAll { $0.target <= acksFinished }
            for waiter in ready { waiter.continuation.resume() }
        }
        ackWriterRunning = false
    }

    /// Returns once every acknowledgement queued before the call has been written (or has failed).
    private func flushAcknowledgements() async {
        let target = acksQueued
        guard acksFinished < target else { return }
        await withCheckedContinuation { flushWaiters.append((target, $0)) }
    }
}
