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
    /// Drops bytes of frames that have only partly arrived on the reply channel, so that the tail
    /// of a frame that never completed cannot be joined to the start of the next reply. Must not
    /// drop complete frames that are queued for `nextFrame`, and must not drop anything from a
    /// channel that carries no replies.
    func discardPartialFrames() async
}

extension RingConnection: RingLink {}

/// Request/response conversations with a connected ring, and the ring's own uploads.
///
/// **Reading.** While the session runs, one reader task takes every frame from the link in
/// arrival order and routes it:
/// - a ring-initiated event (group `0x04`) is acknowledged with the same group and key and
///   payload `[0x00]`, exactly once, whether or not a conversation is in flight, and is then
///   delivered to `uploads()` subscribers; a group `0x04` error reply (`isErrorReply`) is
///   neither acknowledged nor delivered (see `dispatch`);
/// - a live value (group `0x06`) is delivered to subscribers only;
/// - anything else goes to the conversation in flight, or is dropped if there is none, so it can
///   never be taken for the reply to a later request.
///
/// **Conversations.** One runs at a time: a call made while another is in progress waits its
/// turn, so conversations cannot steal each other's frames. Actor methods can interleave at every
/// `await`, so the actor alone is not enough; `exclusively` is the queue. Before a conversation
/// writes anything, the reader drains the link (a zero-timeout read that comes back empty), the
/// link drops partial frames on its reply channel, and only frames read after the drain are
/// offered to it. Frames that arrived before it are therefore never returned as its reply.
///
/// **Deadlines.** Every wait for a frame is capped by the conversation's deadline. The deadline is
/// checked on every frame the reader routes while the conversation waits, not only by a timer, so
/// frames that keep arriving cannot hold a conversation open, even when the link always has one
/// ready and the thread pool is saturated.
///
/// **Writes.** The link takes one write at a time, so every write (a conversation's command, a
/// history acknowledgement, an event acknowledgement) takes a turn on a second queue, in the
/// order the writes were asked for. Event acknowledgements are written in arrival order by one
/// task. Before a conversation writes, every event already read is acknowledged (a wait capped
/// by the conversation's deadline, or by one write backstop once that has passed); an event that
/// arrives during a conversation's write is acknowledged after that write finishes, and before
/// any write the conversation asks for later. When a conversation's own work ends it waits, for at
/// most one write backstop, for the acknowledgements of events read before then; acknowledgements
/// still unwritten after that are written later by the acknowledgement task, just not waited for.
/// A failed acknowledgement does not fail the conversation and is not retried.
///
/// **Lifetime.** A session lives for one connection. The reader starts on `start()`, or on the
/// first request or subscription, and ends on `stop()`, which is final: a stopped session cannot
/// be started again. After reconnecting, the owner creates a new session, and must not do so
/// until the old session's `stop()` has returned, or two readers would compete for frames.
/// Between conversations the reader waits for frames in slices of at most 0.1 s, so a new
/// conversation can start up to that long after it is asked for, and an idle session wakes about
/// ten times a second. The session cannot tell a quiet ring from a dead link: a reader on a
/// disconnected link keeps polling until `stop()`, so the owner of the link must call `stop()`
/// when it disconnects.
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

    /// Most frames held for an open conversation that is not waiting at that moment (it is writing,
    /// or between two waits). A conversation reads every frame as soon as it can, so this is only
    /// reached when the ring floods the link or the conversation is starved of time; one
    /// observed starved run held about 2 GB before this limit existed. Overflow fails the
    /// conversation with `RingError.tooManyFrames` rather than dropping a frame and carrying on,
    /// because a dropped data frame would corrupt a history transfer (the checksum would catch
    /// it, but only after the whole transfer).
    static let maximumHeldFrames = 1024

    /// Most values an `uploads()` stream buffers for a subscriber that is not keeping up; beyond
    /// it the oldest are dropped. Events are acknowledged by the session whether or not anyone
    /// reads the stream.
    public static let uploadBufferLimit = 256

    // Conversation turns.
    private var busy = false
    private var turnWaiters: [CheckedContinuation<Void, Never>] = []

    // Write turns.
    private var writing = false
    private var writeWaiters: [CheckedContinuation<Void, Never>] = []

    // The reader.
    private enum ReaderState { case notStarted, running, stopped }
    private var readerState = ReaderState.notStarted
    private var readerTask: Task<Void, Never>?
    /// The work of the first `stop()`, which later calls wait for too.
    private var stopping: Task<Void, Never>?
    /// Counts the reader's waits, so the drain can insist on an empty read that began after it was asked for.
    private var waitsBegun = 0

    // The conversation in flight.
    /// The deadline of the conversation holding the turn, if any.
    private var conversationDeadline: ContinuousClock.Instant?
    /// Set while a conversation waits for the link to drain: the first wait numbered at least
    /// this that comes back empty opens the conversation.
    private var drainFromWait: Int?
    private var conversationOpen = false
    /// Frames for the open conversation that arrived while it was not waiting. Emptied when a
    /// conversation opens, so what an earlier conversation left unread never reaches a later one.
    private var mailbox: [Frame] = []
    private var mailboxOverflowed = false
    private enum Wake: Sendable { case frame(Frame), drained, timedOut }
    private var waiter: (id: Int, continuation: CheckedContinuation<Wake, Error>, timer: Task<Void, Never>)?
    private var lastWaiterID = 0

    // Event acknowledgements.
    /// Acknowledgements not yet written start at `pendingAckHead`; the array is compacted now and
    /// then rather than shifted on every write, because under a flood of events it can grow large.
    private var pendingAcks: [Frame] = []
    private var pendingAckHead = 0
    private var acksQueued = 0
    private var acksFinished = 0
    private var ackWriterRunning = false
    private var flushWaiters: [Int: (target: Int, continuation: CheckedContinuation<Bool, Never>, timer: Task<Void, Never>)] = [:]
    private var lastFlushID = 0

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

    /// The write backstop: the link's own write deadline plus one reply slice (see `transmit`).
    /// Also the longest a conversation waits for acknowledgements once its own work is done.
    private nonisolated var writeBackstop: TimeInterval { link.writeTimeout + replyTimeout }

    // MARK: Lifetime

    /// Starts reading frames from the link. Requests and subscriptions start the session
    /// themselves, so calling this is needed only to acknowledge events before anything else is
    /// asked for. Does nothing once the session has been stopped.
    ///
    /// The reader runs until `stop()`, whatever state the link is in: on a disconnected link it
    /// keeps polling, because an empty read looks the same as a quiet ring.
    public func start() {
        guard readerState == .notStarted else { return }
        readerState = .running
        let link = self.link
        // Holds the session weakly, so a session nobody references ends its reader.
        readerTask = Task { [weak self] in
            while let wait = await self?.beginWait() {
                let frame = await link.nextFrame(timeout: wait.timeout)
                await self?.route(frame, wait: wait.number)
                // `nextFrame` returns without suspending when a frame is already queued, so without
                // this a link that always has a frame ready would keep this thread for ever. The
                // conversation, the acknowledgement writer and every timer need a thread too; with
                // a one-thread pool nothing else would ever run.
                await Task.yield()
            }
        }
    }

    /// Stops the session for good: it cannot be started again. The conversation waiting for a
    /// frame fails with `RingError.stopped`; one that is writing or inside `persist` fails at its
    /// next wait or write (so a history transfer stopped inside `persist` is not acknowledged,
    /// which is safe because the ring keeps its data). Conversations queued behind it and any
    /// later ones fail too, and subscribers' streams end. A frame the reader takes while stopping
    /// is dropped; if it is an event, it goes unacknowledged.
    ///
    /// Acknowledgements already queued are still written. Returns once the reader has ended (at
    /// most one reader wait later) and those acknowledgements have been written, or after one
    /// write backstop (`link.writeTimeout + replyTimeout`) of waiting for them, after which the
    /// rest are still written but not waited for. A second call made while the first is in
    /// progress waits for the same thing.
    ///
    /// Call this when the link disconnects: the session cannot tell, and its reader would keep
    /// polling the dead link until stopped.
    public func stop() async {
        if let stopping {
            await stopping.value
            return
        }
        guard readerState == .running else {
            readerState = .stopped
            return
        }
        readerState = .stopped
        wake(with: .failure(RingError.stopped))
        for subscriber in subscribers.values { subscriber.finish() }
        subscribers = [:]
        let reader = readerTask
        let work = Task {
            await reader?.value
            _ = await flushAcknowledgements(waitingAtMost: writeBackstop)
        }
        stopping = work
        await work.value
    }

    /// Ring-initiated events and live values, in arrival order, from now until `stop()`. Each
    /// call returns a new stream. A stream buffers at most `uploadBufferLimit` values: a
    /// subscriber that falls further behind loses the oldest. After `stop()` the stream is
    /// already finished.
    public func uploads() -> AsyncStream<RingUpload> {
        let (stream, continuation) = AsyncStream.makeStream(
            of: RingUpload.self, bufferingPolicy: .bufferingNewest(Self.uploadBufferLimit))
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

    private func beginWait() -> (number: Int, timeout: TimeInterval)? {
        guard readerState == .running else { return nil }
        waitsBegun += 1
        // While a conversation waits for the drain, read without waiting so it starts promptly.
        return (waitsBegun, drainFromWait == nil ? pollInterval : 0)
    }

    private func route(_ frame: Frame?, wait number: Int) {
        guard let frame else {
            // An empty read that began after the conversation asked for the drain: everything that
            // arrived before then has been routed, so the conversation may start. This rests on
            // the link's zero-timeout contract (see `RingLink.nextFrame`), which for the real
            // `FrameInbox` is unverified on hardware. An empty read also says nothing about bytes
            // still in transit: a reply the ring is sending at this moment arrives after the drain
            // and is offered to the conversation (the known gap of a late reply being accepted by
            // a retry of the same request).
            if let from = drainFromWait, number >= from {
                drainFromWait = nil
                conversationOpen = true
                mailbox = []
                mailboxOverflowed = false
                wake(with: .success(.drained))
            }
            return
        }
        // Taken while stopping: dropped, so an event among such frames goes unacknowledged.
        guard readerState == .running else { return }
        // Checked on every frame, not only by the waiter's timer: a link that always has a frame
        // ready must not be able to keep a conversation past its deadline, even if the timer's
        // task cannot get a thread.
        if waiter != nil, let deadline = conversationDeadline, now() >= deadline {
            wake(with: .success(.timedOut))
        }
        dispatch(frame)
    }

    private func dispatch(_ frame: Frame) {
        switch frame.group {
        case Self.eventGroup:
            // An error reply in the event group can only be the ring answering something the app
            // sent in that group, which is an acknowledgement (requests in this group are refused).
            // Acknowledging it could start the two sides acknowledging each other for ever, and it
            // is not an event, so it is dropped.
            guard !frame.isErrorReply else { return }
            queueAcknowledgement(of: frame)
            publish(.event(frame))
        case Self.liveGroup:
            publish(.live(frame))
        default:
            // Outside a conversation this can only be stale: a late reply or the tail of a transfer.
            guard conversationOpen else { return }
            if waiter != nil {
                wake(with: .success(.frame(frame)))
            } else if mailbox.count < Self.maximumHeldFrames {
                mailbox.append(frame)
            } else {
                // The conversation fails at its next read (see `maximumHeldFrames`).
                mailboxOverflowed = true
            }
        }
    }

    /// Waits for the reader to hand the conversation a frame or report the drain, for at most `timeout`.
    private func wait(timeout: TimeInterval) async throws -> Wake {
        guard readerState == .running else { throw RingError.stopped }
        lastWaiterID += 1
        let id = lastWaiterID
        return try await withCheckedThrowingContinuation { continuation in
            // One conversation at a time, and it waits for one thing at a time.
            precondition(waiter == nil, "a conversation is already waiting")
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

    /// One conversation: takes the turn, waits for the link to drain, has the link drop partial
    /// frames, runs `body` with the conversation's deadline, and before giving up the turn waits
    /// (for at most one write backstop) for the acknowledgements of every event read so far.
    private func conversation<T: Sendable>(_ body: (ContinuousClock.Instant) async throws -> T) async throws -> T {
        if readerState == .notStarted { start() }
        return try await exclusively {
            let deadline = deadline()
            conversationDeadline = deadline
            let result: Result<T, Error>
            do {
                try await openConversation(until: deadline)
                // The drain has delivered every complete frame that arrived before this point; the
                // bytes of a frame that only partly arrived are still in the link's assembler, and
                // the next reply would be appended to them. A frame legitimately mid-assembly on the
                // reply channel right now (an event split across notifications, say) is lost: its
                // tail can itself be misread as the start of a frame and be dropped too, so that
                // event goes unacknowledged. That is the better failure: without the reset the stale
                // bytes shift every later frame boundary on that channel, and a misread length can
                // hold every frame after it until the link disconnects. With it, damage lasts until
                // the next conversation at most. A frame that completes between the drain and this
                // reset is offered to the conversation; it can only be stale, as with any late reply.
                await link.discardPartialFrames()
                result = .success(try await body(deadline))
            } catch {
                result = .failure(error)
            }
            conversationOpen = false
            conversationDeadline = nil
            drainFromWait = nil
            // The set waited for is fixed here, so events that keep arriving cannot extend the
            // wait, and the wait is capped, so a stuck acknowledgement costs one backstop at most
            // instead of one per queued event.
            _ = await flushAcknowledgements(waitingAtMost: writeBackstop)
            return try result.get()
        }
    }

    private func openConversation(until deadline: ContinuousClock.Instant) async throws {
        guard readerState == .running else { throw RingError.stopped }
        drainFromWait = waitsBegun + 1
        while !conversationOpen {
            _ = try await wait(timeout: try waitSlice(until: deadline, "the link to drain"))
        }
    }

    /// The next frame for the conversation, or `nil` if none arrives within one wait slice.
    private func nextFrame(until deadline: ContinuousClock.Instant, _ what: String) async throws -> Frame? {
        guard !mailboxOverflowed else { throw RingError.tooManyFrames(Self.maximumHeldFrames) }
        // The deadline is checked first, so frames that keep arriving cannot hold a conversation open.
        let slice = try waitSlice(until: deadline, what)
        if !mailbox.isEmpty { return mailbox.removeFirst() }
        guard case .frame(let frame) = try await wait(timeout: slice) else { return nil }
        return frame
    }

    /// Sends a command and returns the ring's reply to it. Ring-initiated events are acknowledged and skipped.
    ///
    /// Commands in the event (`0x04`) and live-value (`0x06`) groups are refused with
    /// `RingError.refusedUploadGroup`: the reader routes every frame in those groups to
    /// subscribers, so their replies could never reach the request.
    public func request(_ command: Frame) async throws -> Frame {
        // Also refused in `transmit`; checked here too so a refusal does not wait for a turn.
        guard !command.erasesRingData else { throw RingError.refusedDestructive(command) }
        guard command.group != Self.eventGroup, command.group != Self.liveGroup else {
            throw RingError.refusedUploadGroup(command)
        }
        return try await conversation { try await converse(command, deadline: $0) }
    }

    /// Tests only: a request without `request`'s own checks, so tests can reach the refusal in
    /// `transmit` and see where a reply in an upload group would go. Never call it elsewhere.
    func requestSkippingChecks(_ command: Frame) async throws -> Frame {
        try await conversation { try await converse(command, deadline: $0) }
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
        // A command is never written after the deadline: its reply could no longer be waited for,
        // and the write and the waits after it would only add to the overrun.
        guard now() < deadline else { throw RingError.timeout(what) }
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
    /// Verified on an R99, including a transfer spanning three data frames; the ring kept its
    /// history after the acknowledgement in the one case tested.
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

    /// Every write goes through here so the delete-range refusal holds even for a link that forgets it.
    private func transmit(_ frame: Frame, acknowledgement: Bool = false) async throws {
        guard !frame.erasesRingData else { throw RingError.refusedDestructive(frame) }
        // After `stop`, only event acknowledgements are still written; checked again at the write
        // turn, since `stop` can come while this waits.
        guard acknowledgement || readerState == .running else { throw RingError.stopped }
        // Events already read are acknowledged before a conversation's write, so the order on the
        // link follows the order of arrival. The acknowledgement writer does not wait for itself.
        // The wait is part of the conversation, so it ends at the conversation's deadline; if that
        // has already passed (only the history acknowledgement after a `persist` that outran it)
        // it gets one write backstop. If the acknowledgements are not all written by then, the
        // link is not completing writes and the conversation fails rather than jump ahead of them.
        // So a conversation that reaches its write with little time left and an acknowledgement
        // still unwritten fails with this timeout.
        if !acknowledgement, acksFinished < acksQueued {
            var limit = writeBackstop
            if let deadline = conversationDeadline, case let left = deadline - now(), left > .zero {
                limit = left / .seconds(1)
            }
            guard await flushAcknowledgements(waitingAtMost: limit) else {
                throw RingError.timeout("event acknowledgements to be written")
            }
        }
        try await withWriteTurn {
            guard acknowledgement || readerState == .running else { throw RingError.stopped }
            // Backstop only, for a link that breaks the `RingLink.send` contract. A compliant link
            // throws by `writeTimeout` and clears its own in-flight state first; this deadline is
            // always `replyTimeout` later than that, so it fires first only if the link is
            // non-compliant. It is derived from the link's bound so the two cannot drift apart.
            //
            // Overrun past the conversation deadline, with B this backstop: a command is never
            // written after the deadline (`converse`), and the wait above ends by it, but a write
            // that starts just before it can wait up to B for its turn (at most one acknowledgement
            // write is ahead) and take up to B itself, and the conversation then waits up to B for
            // acknowledgements: about 3B in all. The history acknowledgement after `persist` can
            // end up to 4B after the later of the deadline and the end of `persist` (one more B
            // waiting for acknowledgements when the deadline has passed).
            let link = self.link
            try await withDeadline(writeBackstop, "write confirmation") { try await link.send(frame) }
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
        while pendingAckHead < pendingAcks.count {
            let ack = pendingAcks[pendingAckHead]
            pendingAckHead += 1
            if pendingAckHead == pendingAcks.count {
                pendingAcks.removeAll()
                pendingAckHead = 0
            } else if pendingAckHead >= 1024, pendingAckHead * 2 >= pendingAcks.count {
                // Drop the written prefix in one go: amortised O(1) per acknowledgement.
                pendingAcks.removeFirst(pendingAckHead)
                pendingAckHead = 0
            }
            // Not retried: a write that failed may still have reached the ring, and a second
            // acknowledgement of the same event is not known to be harmless. Whether the ring
            // repeats an event that was never acknowledged is unverified.
            try? await transmit(ack, acknowledgement: true)
            acksFinished += 1
            let ready = flushWaiters.filter { $0.value.target <= acksFinished }
            for (id, waiter) in ready {
                flushWaiters[id] = nil
                waiter.timer.cancel()
                waiter.continuation.resume(returning: true)
            }
        }
        ackWriterRunning = false
    }

    /// Waits until every acknowledgement queued before the call has been written (or has failed),
    /// for at most `limit` seconds. Returns whether they all were.
    private func flushAcknowledgements(waitingAtMost limit: TimeInterval) async -> Bool {
        let target = acksQueued
        guard acksFinished < target else { return true }
        lastFlushID += 1
        let id = lastFlushID
        return await withCheckedContinuation { continuation in
            let timer = Task { [weak self] in
                try? await Task.sleep(for: .seconds(limit))
                guard !Task.isCancelled else { return }
                await self?.expireFlush(id)
            }
            flushWaiters[id] = (target, continuation, timer)
        }
    }

    private func expireFlush(_ id: Int) {
        flushWaiters.removeValue(forKey: id)?.continuation.resume(returning: false)
    }

    // MARK: Test hooks (tests only)

    // Internal, for `@testable` tests to observe state that has no signal of its own. Nothing
    // outside the tests may use these.

    /// Tests only: how many writes are waiting for the write in progress.
    var queuedWrites: Int { writeWaiters.count }
    /// Tests only: how many callers are waiting for acknowledgements to be written.
    var pendingFlushes: Int { flushWaiters.count }
    /// Tests only: acknowledgements queued and not yet written (or failed).
    var queuedAcknowledgements: Int { acksQueued - acksFinished }
    /// Tests only: frames held for the open conversation.
    var heldFrames: Int { mailbox.count }
    /// Tests only: whether a conversation is waiting for the link to drain.
    var isDraining: Bool { drainFromWait != nil }
    /// Tests only: whether an open conversation is waiting for a frame.
    var conversationIsWaiting: Bool { conversationOpen && waiter != nil }
    /// Tests only: whether `stop()` has been called.
    var isStopped: Bool { readerState == .stopped }
}
