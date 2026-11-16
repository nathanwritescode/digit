import Foundation
import Testing
import RingProtocol
@testable import RingTransport

/// A scripted stand-in for the ring. `onSend` returns the frames the "ring" delivers in answer to
/// each frame the session writes, and `deliver` sends frames unprompted. Frames are handed to the
/// session's reader strictly in order.
///
/// `nextFrame` really suspends before returning, as the real `FrameInbox` can: even a frame that
/// is already queued is handed over from another thread. The one exception is `setAlwaysReady`,
/// which returns a frame without suspending at all; it exists only for the tests that check the
/// session cannot be starved by such a link.
///
/// Every wait the fake offers ends after a time limit and on task cancellation, so a regression
/// fails a test instead of hanging it.
/// `@unchecked Sendable` because all state is guarded by `lock`.
private final class FakeLink: RingLink, @unchecked Sendable {
    private let lock = NSLock()
    private var inbox: [Frame]
    private var log: [String] = []
    private var sentFrames: [Frame] = []
    private let onSend: @Sendable (Frame) -> [Frame]
    private var reader: (id: Int, continuation: CheckedContinuation<Frame?, Never>)?
    private var lastReaderID = 0

    init(stale: [Frame] = [], guardWrites: Bool = false, writeTimeout: TimeInterval = 0.02, onSend: @escaping @Sendable (Frame) -> [Frame] = { _ in [] }) {
        inbox = stale
        self.guardWrites = guardWrites
        self.writeTimeout = writeTimeout
        self.onSend = onSend
    }

    var sent: [Frame] { lock.withLock { sentFrames } }
    var events: [String] { lock.withLock { log } }
    var queuedFrames: Int { lock.withLock { inbox.count } }

    func record(_ event: String) { mutate { log.append(event) } }

    // Scripted faults for `send`. `parkNextSend` models a link that ignores cancellation and
    // never returns (the real CheckedContinuation behaviour): the test must call `releaseParked()`
    // at the end. `parkSends(ofGroup:)` does the same for every send in a group, and
    // `parkAllSends()` for every send, until `releaseAll()`. After `close()` every send, including
    // one parked, throws at once, as a link that has gone away does. `timeOutNextSend` models a compliant link like the real connection: it holds
    // the write in flight for its own `writeTimeout`, ignoring cancellation, then clears the
    // in-flight state and throws.
    // With `guardWrites`, a send while an earlier one is still in flight throws `busy` (and is
    // logged), as the real connection does. `trickle` is returned by `nextFrame` whenever nothing
    // else is queued; `setTrickleOnNextSend` turns it on once the next frame has been written.
    private enum SendFault { case park, ownTimeout }
    let writeTimeout: TimeInterval
    private var faults: [SendFault] = []
    private var parkedGroup: UInt8?
    private var parkEverything = false
    private var closed = false
    private var parked: [CheckedContinuation<Void, Never>] = []
    private var inFlight = false
    private let guardWrites: Bool
    private var trickleFrame: Frame?
    private var trickleAfterSend: Frame?
    // Scripted reads. `alwaysReady`: returned at once, without suspending. `holdNextEmpty`: the next
    // read that times out keeps its empty result until `releaseHeldEmptyRead()`. `waitsForever`:
    // reads never time out, so the reader stays inside `nextFrame` until a frame is delivered.
    private var alwaysReady: Frame?
    private var holdNextEmpty = false
    private var heldEmpty: CheckedContinuation<Frame?, Never>?
    private var waitsForever = false
    private var partialResets = 0
    private var partialResetDelay: TimeInterval = 0

    func parkNextSend() { mutate { faults.append(.park) } }
    func parkSends(ofGroup group: UInt8) { mutate { parkedGroup = group } }
    func parkAllSends() { mutate { parkEverything = true } }
    /// Every send from now on throws at once, and parked sends are released to throw.
    func close() {
        mutate {
            closed = true
            parkedGroup = nil
            parkEverything = false
        }
        releaseParked()
    }
    func timeOutNextSend() { mutate { faults.append(.ownTimeout) } }
    func setTrickle(_ frame: Frame?) { mutate { trickleFrame = frame } }
    func setTrickleOnNextSend(_ frame: Frame) { mutate { trickleAfterSend = frame } }
    func setAlwaysReady(_ frame: Frame?) { mutate { alwaysReady = frame } }
    func holdNextEmptyRead() { mutate { holdNextEmpty = true } }
    func setWaitsForever(_ value: Bool) { mutate { waitsForever = value } }
    /// `discardPartialFrames` takes this long, as a slow Bluetooth queue might.
    func delayPartialReset(by seconds: TimeInterval) { mutate { partialResetDelay = seconds } }
    var resets: Int { lock.withLock { partialResets } }

    func releaseParked() {
        let all = mutate { () -> [CheckedContinuation<Void, Never>] in
            defer { parked = [] }
            return parked
        }
        for continuation in all { continuation.resume() }
    }

    func releaseHeldEmptyRead() {
        mutate { heldEmpty.take() }?.resume(returning: nil)
    }

    /// Ends every scripted hold: parked writes, held reads, reads that never time out, and the
    /// always-ready and trickle frames. Tests call it when they are cancelled (their time limit).
    func releaseAll() {
        let reading = mutate { () -> [CheckedContinuation<Frame?, Never>] in
            parkedGroup = nil
            parkEverything = false
            alwaysReady = nil
            trickleFrame = nil
            trickleAfterSend = nil
            holdNextEmpty = false
            var woken = [heldEmpty.take()].compactMap { $0 }
            if waitsForever, let current = reader {
                reader = nil
                woken.append(current.continuation)
            }
            waitsForever = false
            return woken
        }
        releaseParked()
        for continuation in reading { continuation.resume(returning: nil) }
    }

    func send(_ frame: Frame) async throws {
        let fault = try mutate { () -> SendFault? in
            if closed { throw RingError.notConnected }
            log.append("attempt \(String(format: "%04X", frame.type))")
            if guardWrites && inFlight {
                log.append("busy \(String(format: "%04X", frame.type))")
                throw RingError.busy("another write is in progress")
            }
            inFlight = true
            if parkEverything { return .park }
            if let group = parkedGroup, frame.group == group { return .park }
            return faults.isEmpty ? nil : faults.removeFirst()
        }
        switch fault {
        case .ownTimeout:
            await withCheckedContinuation { continuation in
                DispatchQueue.global().asyncAfter(deadline: .now() + writeTimeout) {
                    // The link clears its own in-flight state before reporting, so the next send is
                    // accepted. Done here, not after resuming, so task scheduling delay cannot let
                    // the session's backstop observe a write that has already timed out.
                    self.mutate { self.inFlight = false }
                    continuation.resume()
                }
            }
            throw RingError.timeout("write confirmation")
        case .park:
            await withCheckedContinuation { continuation in mutate { parked.append(continuation) } }
            if lock.withLock({ closed }) {
                mutate { inFlight = false }
                throw RingError.notConnected
            }
        case nil:
            break
        }
        let replies = onSend(frame)
        mutate {
            sentFrames.append(frame)
            log.append("send \(String(format: "%04X", frame.type))")
            inFlight = false
            if let next = trickleAfterSend {
                trickleFrame = next
                trickleAfterSend = nil
            }
        }
        deliver(replies)
    }

    /// The ring sends `frames`, as if unprompted.
    func deliver(_ frames: [Frame]) {
        guard !frames.isEmpty else { return }
        let woken = mutate { () -> (CheckedContinuation<Frame?, Never>, Frame)? in
            inbox += frames
            guard let current = reader else { return nil }
            reader = nil
            return (current.continuation, take())
        }
        if let (continuation, frame) = woken { continuation.resume(returning: frame) }
    }

    func discardPartialFrames() async {
        let delay = mutate { () -> TimeInterval in
            partialResets += 1
            log.append("reset")
            return partialResetDelay
        }
        guard delay > 0 else { return }
        await withCheckedContinuation { continuation in
            DispatchQueue.global().asyncAfter(deadline: .now() + delay) { continuation.resume() }
        }
    }

    private enum Read { case now(Frame?), wait(id: Int, forever: Bool) }

    func nextFrame(timeout: TimeInterval) async -> Frame? {
        // Deliberately does not suspend: see `setAlwaysReady`. Not logged, as it is read in a tight loop.
        if let ready = lock.withLock({ alwaysReady }) { return ready }
        return await withCheckedContinuation { (continuation: CheckedContinuation<Frame?, Never>) in
            let read = mutate { () -> Read in
                if !inbox.isEmpty { return .now(take()) }
                if let trickleFrame { return .now(trickleFrame) }
                if timeout <= 0 { return .now(nil) }
                lastReaderID += 1
                reader = (lastReaderID, continuation)
                return .wait(id: lastReaderID, forever: waitsForever)
            }
            switch read {
            case .now(let frame):
                // Handed over from another thread, so the reader really gives up its thread.
                DispatchQueue.global().async { continuation.resume(returning: frame) }
            case .wait(let id, let forever):
                guard !forever else { return }
                DispatchQueue.global().asyncAfter(deadline: .now() + timeout) { self.expire(read: id) }
            }
        }
    }

    private func expire(read id: Int) {
        let expired = mutate { () -> CheckedContinuation<Frame?, Never>? in
            guard let current = reader, current.id == id, !waitsForever else { return nil }
            reader = nil
            guard !holdNextEmpty else {
                holdNextEmpty = false
                heldEmpty = current.continuation
                return nil
            }
            return current.continuation
        }
        expired?.resume(returning: nil)
    }

    /// Takes the first queued frame. Call with the lock held.
    private func take() -> Frame {
        let frame = inbox.removeFirst()
        log.append("take \(String(format: "%04X", frame.type))")
        return frame
    }

    // MARK: Waiting for the fake's state

    private var conditionWaiters: [Int: (condition: () -> Bool, continuation: CheckedContinuation<Bool, Never>)] = [:]
    private var lastConditionID = 0

    /// Runs `body` under the lock, then wakes every waiter whose condition now holds.
    @discardableResult
    private func mutate<T>(_ body: () throws -> T) rethrows -> T {
        let (value, ready) = try lock.withLock { () -> (T, [CheckedContinuation<Bool, Never>]) in
            let value = try body()
            let ready = conditionWaiters.filter { $0.value.condition() }
            for id in ready.keys { conditionWaiters[id] = nil }
            return (value, ready.values.map(\.continuation))
        }
        for continuation in ready { continuation.resume(returning: true) }
        return value
    }

    /// Returns true once `condition` (evaluated under the lock) holds; false after `seconds`, or
    /// when the calling task is cancelled.
    private func waitFor(within seconds: Double, _ condition: @escaping () -> Bool) async -> Bool {
        let id = lock.withLock { () -> Int in
            lastConditionID += 1
            return lastConditionID
        }
        return await withTaskCancellationHandler {
            await withCheckedContinuation { (continuation: CheckedContinuation<Bool, Never>) in
                let immediate = lock.withLock { () -> Bool? in
                    if condition() { return true }
                    if Task.isCancelled { return false }
                    conditionWaiters[id] = (condition, continuation)
                    return nil
                }
                if let immediate {
                    continuation.resume(returning: immediate)
                    return
                }
                DispatchQueue.global().asyncAfter(deadline: .now() + seconds) { self.giveUp(id) }
            }
        } onCancel: {
            giveUp(id)
        }
    }

    private func giveUp(_ id: Int) {
        lock.withLock { conditionWaiters.removeValue(forKey: id) }?.continuation.resume(returning: false)
    }

    /// Whether at least `count` frames were written within `seconds`.
    func waitUntilSent(_ count: Int, within seconds: Double = 10) async -> Bool {
        await waitFor(within: seconds) { self.sentFrames.count >= count }
    }

    func waitUntilSendParked(within seconds: Double = 10) async -> Bool {
        await waitFor(within: seconds) { !self.parked.isEmpty }
    }

    func waitUntilEmptyReadHeld(within seconds: Double = 10) async -> Bool {
        await waitFor(within: seconds) { self.heldEmpty != nil }
    }

    func waitUntilReaderWaits(within seconds: Double = 10) async -> Bool {
        await waitFor(within: seconds) { self.reader != nil }
    }
}

// Synthetic frames: the layout matches the protocol notes, the contents are made up.
private func session(_ link: FakeLink) -> RingSession { RingSession(link: link, replyTimeout: 0.05) }

/// Runs `body` with `s`, then stops the session, whether `body` returned or threw, so no reader
/// outlives its test. If `body` threw or the test is cancelled (its time limit), the fake's holds
/// are released first, so `stop()` cannot wait on a reader the fake is holding and the session's
/// own deadlines can end what is still running. The cancellation handler covers `stop()` too.
@discardableResult
private func using<T: Sendable>(
    _ s: RingSession,
    _ link: FakeLink,
    alsoOnCancel: @escaping @Sendable () -> Void = {},
    _ body: (RingSession) async throws -> T
) async throws -> T {
    try await withTaskCancellationHandler {
        let result: Result<T, Error>
        do {
            result = .success(try await body(s))
        } catch {
            result = .failure(error)
            link.releaseAll()
            alsoOnCancel()
        }
        await s.stop()
        return try result.get()
    } onCancel: {
        link.releaseAll()
        alsoOnCancel()
    }
}

private let kind = HistoryKind.heartRate

// Non-zero bytes, so a test can tell the header payload was really captured.
private func headerBytes(_ length: Int) -> [UInt8] { (0..<length).map { UInt8($0 + 1) } }
private func header(_ length: Int) -> Frame {
    Frame(group: Command.healthGroup, key: kind.rawValue, payload: headerBytes(length))
}
private func dataFrame(_ bytes: [UInt8]) -> Frame {
    Frame(group: Command.healthGroup, key: kind.rawValue, payload: bytes)
}
private func endFrame(checksum: UInt16) -> Frame {
    Frame(group: Command.healthGroup, key: Command.historyEndKey,
          payload: [0, 0, 0, 0, UInt8(checksum & 0xFF), UInt8(checksum >> 8)])
}

/// A link that plays a complete history transfer when the history request arrives.
private func historyLink(chunks: [[UInt8]], checksum: UInt16? = nil, headerLength: Int = 12) -> FakeLink {
    let all = chunks.flatMap { $0 }
    return FakeLink { frame in
        guard frame.type == Command.history(kind).type else { return [] }
        return [header(headerLength)] + chunks.map(dataFrame) + [endFrame(checksum: checksum ?? CRC16.ccittFalse(all))]
    }
}

private actor PersistRecorder {
    var calls: [HistoryTransfer] = []
    func add(_ transfer: HistoryTransfer) { calls.append(transfer) }
}

private struct PersistFailure: Error {}

@Test(.timeLimit(.minutes(1))) func replyIsMatchedByFrameType() async throws {
    let reply = Frame(group: 0x02, key: 0x00, payload: [1, 2, 3])
    let link = FakeLink { $0.type == 0x0200 ? [Frame(group: 0x02, key: 0x0C, payload: [9]), reply] : [] }
    // The unrelated 0x020C frame ahead of the reply must be skipped, not returned.
    try await using(session(link), link) { s async throws in
        #expect(try await s.request(Command.deviceInfo) == reply)
    }
}

@Test(.timeLimit(.minutes(1))) func errorReplyThrowsRejected() async throws {
    let link = FakeLink { _ in [Frame(group: 0x02, key: 0x20, payload: [0xFC])] }
    try await using(session(link), link) { s async throws in
        do {
            _ = try await s.request(Frame(group: 0x02, key: 0x20))
            Issue.record("expected an error")
        } catch RingError.rejected(let frame) {
            #expect(frame.payload == [0xFC])
        }
    }
}

@Test(.timeLimit(.minutes(1))) func eventWhileWaitingIsAcknowledgedAndSkipped() async throws {
    let event = Frame(group: 0x04, key: 0x0A, payload: [1, 2])
    let reply = Frame(group: 0x02, key: 0x00, payload: [5])
    let link = FakeLink { $0.type == 0x0200 ? [event, reply] : [] }
    try await using(session(link), link) { s async throws in
        #expect(try await s.request(Command.deviceInfo) == reply)
        #expect(link.sent.last == Frame(group: 0x04, key: 0x0A, payload: [0x00]))
    }
}

@Test(.timeLimit(.minutes(1))) func noReplyTimesOut() async throws {
    let link = FakeLink()
    try await using(session(link), link) { s async throws in
        do {
            _ = try await s.request(Command.deviceInfo)
            Issue.record("expected a timeout")
        } catch RingError.timeout {
        }
    }
}

@Test(.timeLimit(.minutes(1))) func shortHeaderMeansEmptyHistoryAndNoAcknowledgement() async throws {
    let link = historyLink(chunks: [], headerLength: 9)
    let persisted = PersistRecorder()
    try await using(session(link), link) { s async throws in
        let transfer = try await s.fetchHistory(kind) { await persisted.add($0) }
        #expect(transfer.data.isEmpty)
        #expect(transfer.kind == kind)
        #expect(transfer.header == headerBytes(9))
        #expect(await persisted.calls.isEmpty)
        #expect(link.sent == [Command.history(kind)])
    }
}

@Test(.timeLimit(.minutes(1))) func multiFrameHistoryReturnsConcatenatedBytesAndAcknowledges() async throws {
    let link = historyLink(chunks: [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10]])
    try await using(session(link), link) { s async throws in
        let transfer = try await s.fetchHistory(kind) { _ in }
        #expect(transfer.data == Array(1...10))
        #expect(link.sent == [Command.history(kind), Command.historyTransferOK])
    }
}

@Test(.timeLimit(.minutes(1))) func checksumMismatchThrowsAndSendsNothing() async throws {
    let link = historyLink(chunks: [[1, 2, 3]], checksum: 0xBEEF)
    let persisted = PersistRecorder()
    try await using(session(link), link) { s async throws in
        do {
            _ = try await s.fetchHistory(kind) { await persisted.add($0) }
            Issue.record("expected a checksum error")
        } catch RingError.checksumMismatch {
        }
        #expect(link.sent == [Command.history(kind)])
        #expect(await persisted.calls.isEmpty)
    }
}

@Test(.timeLimit(.minutes(1))) func persistRunsWithVerifiedBytesBeforeAcknowledgement() async throws {
    let link = historyLink(chunks: [[1, 2], [3]])
    try await using(session(link), link) { s async throws in
        _ = try await s.fetchHistory(kind) { transfer in
            link.record("persist \(transfer.data)")
        }
        let events = link.events
        let persist = try #require(events.firstIndex(of: "persist [1, 2, 3]"))
        let ack = try #require(events.firstIndex { $0 == "send 0580" })
        #expect(persist < ack)
    }
}

@Test(.timeLimit(.minutes(1))) func failedPersistSendsNoAcknowledgement() async throws {
    let link = historyLink(chunks: [[1, 2, 3]])
    try await using(session(link), link) { s async throws in
        do {
            _ = try await s.fetchHistory(kind) { _ in throw PersistFailure() }
            Issue.record("expected persist to fail the transfer")
        } catch is PersistFailure {
        }
        #expect(link.sent == [Command.history(kind)])
    }
}

@Test(.timeLimit(.minutes(1))) func transferCarriesHeaderFetchTimeAndTimeZone() async throws {
    let link = historyLink(chunks: [[1, 2], [3]], headerLength: 12)
    // The clock reads finer than a millisecond; the transfer keeps whole milliseconds.
    let now = Date(timeIntervalSince1970: 1_800_000_000.1234567)
    let zone = try #require(TimeZone(identifier: "Pacific/Auckland"))
    let persisted = PersistRecorder()
    try await using(RingSession(link: link, replyTimeout: 0.05, clock: { now }, timeZone: { zone }), link) { s async throws in
        let returned = try await s.fetchHistory(kind) { await persisted.add($0) }
        let expected = HistoryTransfer(
            kind: kind, header: headerBytes(12), data: [1, 2, 3],
            fetchedAt: Date(timeIntervalSince1970: 1_800_000_000.123), timeZoneIdentifier: "Pacific/Auckland")
        #expect(returned == expected)
        // And it survives the file format unchanged.
        #expect(try JSONDecoder().decode(HistoryTransfer.self, from: JSONEncoder().encode(returned)) == returned)
        // What is saved before the acknowledgement is the same full transfer that is returned.
        #expect(await persisted.calls == [expected])
    }
}

@Test(.timeLimit(.minutes(1))) func timeZoneIsReadForEachTransfer() async throws {
    let link = historyLink(chunks: [[1]])
    let zones = ZoneBox(["Asia/Tokyo", "America/Denver"])
    try await using(RingSession(link: link, replyTimeout: 0.05, timeZone: { zones.next() }), link) { s async throws in
        let first = try await s.fetchHistory(kind) { _ in }
        let second = try await s.fetchHistory(kind) { _ in }
        #expect(first.timeZoneIdentifier == "Asia/Tokyo")
        #expect(second.timeZoneIdentifier == "America/Denver")
    }
}

// @unchecked Sendable: all mutable state is guarded by `lock`.
private final class ZoneBox: @unchecked Sendable {
    private let lock = NSLock()
    private var identifiers: [String]
    init(_ identifiers: [String]) { self.identifiers = identifiers }
    func next() -> TimeZone {
        TimeZone(identifier: lock.withLock { identifiers.removeFirst() })!
    }
}

@Test(.timeLimit(.minutes(1))) func setTimeUsesTheSessionsClockAndZone() async throws {
    let link = FakeLink { $0.type == 0x0100 ? [Frame(group: 0x01, key: 0x00, payload: [0])] : [] }
    // 2027-01-15 08:00:00 UTC is 21:00 on the same day in Auckland (UTC+13 in January).
    let now = Date(timeIntervalSince1970: 1_800_000_000)
    let zone = try #require(TimeZone(identifier: "Pacific/Auckland"))
    try await using(RingSession(link: link, replyTimeout: 0.05, clock: { now }, timeZone: { zone }), link) { s async throws in
        try await s.setTime()
        #expect(link.sent == [Command.setTime(now, timeZone: zone)])
        #expect(link.sent.first?.payload[4] == 21)
    }
}

@Test(.timeLimit(.minutes(1))) func staleFramesAreDiscardedBeforeANewRequest() async throws {
    // A leftover frame with the same type as the reply must not be mistaken for the new reply.
    // The session no longer asks the link to discard; its reader drains the link before the
    // request is written, so the leftover is read and dropped first.
    let stale = Frame(group: 0x02, key: 0x00, payload: [0xAA])
    let fresh = Frame(group: 0x02, key: 0x00, payload: [0xBB])
    let link = FakeLink(stale: [stale]) { $0.type == 0x0200 ? [fresh] : [] }
    try await using(session(link), link) { s async throws in
        #expect(try await s.request(Command.deviceInfo) == fresh)
        #expect(link.sent == [Command.deviceInfo])
    }
}

// The drain only removes complete frames; the bytes of a frame whose tail never arrived sit in
// the link's assembler. So each conversation also has the link drop partial frames: after the
// drain (here: after the stale frame was read) and before its command is written.
@Test(.timeLimit(.minutes(1))) func partialFramesAreDroppedOncePerConversationAfterTheDrainAndBeforeTheCommand() async throws {
    let stale = Frame(group: 0x02, key: 0x00, payload: [0xAA])
    let link = FakeLink(stale: [stale]) { $0.type == 0x0200 ? [deviceReply] : [] }
    try await using(session(link), link) { s async throws in
        #expect(try await s.request(Command.deviceInfo) == deviceReply)
        #expect(try await s.request(Command.deviceInfo) == deviceReply)
    }
    let order = link.events.filter { $0 == "reset" || $0.hasPrefix("send") || $0.hasPrefix("take") }
    #expect(order == ["take 0200", "reset", "send 0200", "take 0200", "reset", "send 0200", "take 0200"])
    #expect(link.resets == 2)
}

// The time limit turns a future regression that never reaches the gate into a failure, not a hung CI job.
@Test(.timeLimit(.minutes(1))) func overlappingConversationsRunOneAtATime() async throws {
    let link = FakeLink { frame in
        if frame.type == Command.history(kind).type {
            return [header(12), dataFrame([1]), endFrame(checksum: CRC16.ccittFalse([1]))]
        }
        return [Frame(group: frame.group, key: frame.key, payload: [frame.key])]
    }
    let gate = Gate()
    try await using(session(link), link, alsoOnCancel: { gate.release() }) { s async throws in
        // Hold the first conversation mid-flight, inside its persist step (the turn is held there).
        let first = Task { try await s.fetchHistory(kind) { _ in await gate.park() } }
        #expect(await gate.parked.wait())
        let second = Task { try await s.request(Frame(group: 0x02, key: 0x0C)) }
        // Give the second request ample time to (wrongly) start. If conversations could interleave
        // it would send now.
        try await Task.sleep(for: .milliseconds(100))
        #expect(link.sent == [Command.history(kind)])
        gate.release()
        let (ra, rb) = try await (first.value, second.value)
        #expect(ra.data == [1])
        #expect(rb.payload == [0x0C])
        #expect(link.sent == [Command.history(kind), Command.historyTransferOK, Frame(group: 0x02, key: 0x0C)])
    }
}

@Test(.timeLimit(.minutes(1))) func sessionRefusesDeleteRangeFramesEvenIfTheLinkWouldSendThem() async throws {
    let link = FakeLink()
    try await using(session(link), link) { s async throws in
        do {
            _ = try await s.request(Frame(group: 0x05, key: 0x40))
            Issue.record("expected a refusal")
        } catch RingError.refusedDestructive {
        }
        #expect(link.sent.isEmpty)
    }
}

// `request` refuses delete-range frames before taking a turn; this reaches the refusal every
// write goes through, in `transmit`, by skipping `request`'s own checks. The fake would send it.
@Test(.timeLimit(.minutes(1))) func everyWriteRefusesDeleteRangeFrames() async throws {
    let link = FakeLink()
    try await using(session(link), link) { s async throws in
        for key in [0x40, 0x46, 0x52] as [UInt8] {
            do {
                _ = try await s.requestSkippingChecks(Frame(group: 0x05, key: key))
                Issue.record("expected a refusal")
            } catch RingError.refusedDestructive {
            }
        }
        #expect(link.sent.isEmpty)
    }
}

// RingConnection.send repeats the delete-range refusal, but reaching it needs a CBCentralManager
// (Bluetooth permission). The session-level refusals above and Frame.erasesRingData's own test
// in RingProtocolTests cover the check.

// The reader hands every frame in these groups to subscribers, so a reply to such a request
// could never reach it.
@Test(.timeLimit(.minutes(1))) func requestsInTheUploadGroupsAreRefused() async throws {
    let link = FakeLink { [Frame(group: $0.group, key: $0.key, payload: [0])] }
    try await using(session(link), link) { s async throws in
        for command in [Frame(group: 0x04, key: 0x0E), Frame(group: 0x06, key: 0x01)] {
            do {
                _ = try await s.request(command)
                Issue.record("expected a refusal")
            } catch RingError.refusedUploadGroup(let refused) {
                #expect(refused == command)
            }
        }
        #expect(link.sent.isEmpty)
    }
}

// MARK: Deadlines

/// A monotonic clock that advances one second every time it is read, so a test crosses a
/// conversation deadline after a fixed number of reads instead of after real time.
private final class SteppingClock: @unchecked Sendable {
    private let lock = NSLock()
    private var reads = 0
    private let start = ContinuousClock.now
    func now() -> ContinuousClock.Instant {
        lock.withLock {
            reads += 1
            return start + .seconds(reads)
        }
    }
}

private final class CallCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var count = 0
    func next() -> Int { lock.withLock { count += 1; return count } }
}

private func expectTimeout(_ body: () async throws -> Void) async {
    do {
        try await body()
        Issue.record("expected a timeout")
    } catch RingError.timeout {
    } catch {
        Issue.record("expected a timeout, got \(error)")
    }
}

/// Like `expectTimeout`, but also checks what timed out, so a test can tell which wait ended it.
private func expectTimeout(waitingFor expected: String, _ body: () async throws -> Void) async {
    do {
        try await body()
        Issue.record("expected a timeout")
    } catch RingError.timeout(let what) {
        #expect(what == expected)
    } catch {
        Issue.record("expected a timeout, got \(error)")
    }
}

private let deviceReply = Frame(group: 0x02, key: 0x00, payload: [7])

/// A session whose conversation deadline is five reads of `clock`. Every wait slice reads the
/// clock, so the reply timeout is long enough (1 s of real time) that a slice does not run out
/// while the reader waits for a thread on a loaded machine and use up the budget early.
private func steppingSession(_ link: FakeLink, _ clock: SteppingClock) -> RingSession {
    RingSession(link: link, replyTimeout: 1, conversationTimeout: 5, monotonicNow: { clock.now() })
}

// The write deadline has one owner, the link. If the session's own deadline on a write could fire
// before the link's, the link would still have the write in flight and refuse the next one. The
// combinations include a reply timeout much shorter than the link's write timeout.
//
// The session's backstop fires `replyTimeout` after the link's own deadline, and the two run on
// different timers (a task sleep and a dispatch queue). With a margin of tens of milliseconds a
// loaded machine could delay the link's timer past the backstop, and the test failed on CI with
// `busy` although the code was right (#37). The reply timeouts are therefore large enough to
// absorb scheduling delay. This is still a race between real timers, not a proof: the test fails
// reliably if the backstop is earlier than the link's deadline, and usually (two runs in three
// when tried) if the two are equal. A deterministic version is tracked in #39.
@Test(.timeLimit(.minutes(1)), arguments: [0.5, 1.0], [0.05, 0.5, 2.0])
func linkThatTimesOutItsOwnWriteLeavesTheSessionUsable(replyTimeout: Double, writeTimeout: Double) async throws {
    let link = FakeLink(guardWrites: true, writeTimeout: writeTimeout) { $0.type == 0x0200 ? [deviceReply] : [] }
    try await using(RingSession(link: link, replyTimeout: replyTimeout), link) { s async throws in
        link.timeOutNextSend()
        await expectTimeout { _ = try await s.request(Command.deviceInfo) }
        #expect(try await s.request(Command.deviceInfo) == deviceReply)
    }
}

@Test(.timeLimit(.minutes(1))) func backstopEndsAWriteOnALinkThatNeverReturns() async throws {
    // A non-compliant link: the write ignores cancellation and never returns. The session must
    // still report a timeout instead of hanging (the link itself stays stuck, which is its fault).
    let link = FakeLink(guardWrites: true) { $0.type == 0x0200 ? [deviceReply] : [] }
    try await using(RingSession(link: link, replyTimeout: 0.05), link) { s async throws in
        link.parkNextSend()
        // Independent watchdog: without the backstop the parked write would keep this test alive past
        // any time limit, so release it and fail instead of hanging.
        let watchdogFired = Latch()
        let watchdog = Task {
            try? await Task.sleep(for: .seconds(3))
            guard !Task.isCancelled else { return }
            watchdogFired.open()
            link.releaseParked()
        }
        await expectTimeout { _ = try await s.request(Command.deviceInfo) }
        watchdog.cancel()
        #expect(!watchdogFired.isOpen, "the session did not give up on the write by itself")
        link.releaseParked()
    }
}

@Test(.timeLimit(.minutes(1))) func linkGoingSilentMidTransferTimesOutAndTheNextRequestWorks() async throws {
    let calls = CallCounter()
    let link = FakeLink { frame in
        if frame.type == Command.history(kind).type {
            // First attempt: header and one data frame, then silence (no end frame).
            return calls.next() == 1 ? [header(12), dataFrame([1, 2])] : [header(12), dataFrame([1, 2]), endFrame(checksum: CRC16.ccittFalse([1, 2]))]
        }
        return frame.type == 0x0200 ? [deviceReply] : []
    }
    try await using(session(link), link) { s async throws in
        await expectTimeout { _ = try await s.fetchHistory(kind) { _ in } }
        #expect(try await s.request(Command.deviceInfo) == deviceReply)
        let transfer = try await s.fetchHistory(kind) { _ in }
        #expect(transfer.data == [1, 2])
    }
}

@Test(.timeLimit(.minutes(1))) func transferThatKeepsTricklingPastTheOverallDeadlineTimesOut() async throws {
    let link = FakeLink { frame in
        frame.type == 0x0200 ? [deviceReply] : [header(12)]
    }
    // Every wait is satisfied, so only the overall deadline can end this transfer.
    link.setTrickle(dataFrame([1]))
    let clock = SteppingClock()
    try await using(steppingSession(link, clock), link) { s async throws in
        await expectTimeout { _ = try await s.fetchHistory(kind) { _ in } }
        link.setTrickle(nil)
        #expect(try await s.request(Command.deviceInfo) == deviceReply)
    }
}

// The trickle in the test above is already running before the request, so the link never drains
// and the deadline ends the conversation before it writes. These two start the trickle once the
// command is written, so the deadline has to end the conversation while frames keep coming.
@Test(.timeLimit(.minutes(1))) func transferThatKeepsTricklingAfterItsHeaderTimesOut() async throws {
    let link = FakeLink { frame in
        frame.type == 0x0200 ? [deviceReply] : [header(12)]
    }
    link.setTrickleOnNextSend(dataFrame([1]))
    let clock = SteppingClock()
    try await using(steppingSession(link, clock), link) { s async throws in
        await expectTimeout(waitingFor: "\(kind.name) history transfer") { _ = try await s.fetchHistory(kind) { _ in } }
        #expect(link.sent == [Command.history(kind)])
        link.setTrickle(nil)
        #expect(try await s.request(Command.deviceInfo) == deviceReply)
    }
}

@Test(.timeLimit(.minutes(1))) func unrelatedFramesAfterTheCommandCannotKeepARequestWaiting() async throws {
    let link = FakeLink()
    link.setTrickleOnNextSend(Frame(group: 0x02, key: 0x0C, payload: [1]))
    let clock = SteppingClock()
    try await using(steppingSession(link, clock), link) { s async throws in
        await expectTimeout(waitingFor: "reply to 0200") { _ = try await s.request(Command.deviceInfo) }
        link.setTrickle(nil)
    }
}

@Test(.timeLimit(.minutes(1))) func unrelatedFramesCannotKeepARequestWaitingPastTheDeadline() async throws {
    let link = FakeLink()
    link.setTrickle(Frame(group: 0x02, key: 0x0C, payload: [1]))
    let clock = SteppingClock()
    try await using(steppingSession(link, clock), link) { s async throws in
        await expectTimeout { _ = try await s.request(Command.deviceInfo) }
    }
}

// A link whose `nextFrame` always has a frame ready and never suspends. The reader must still give
// up its thread, and the deadline must end the conversation even if no timer gets to run. CI runs
// the transport suite, these two included, a second time with a one-thread cooperative pool
// (LIBDISPATCH_COOPERATIVE_POOL_STRICT=1), where without those fixes nothing else could run at all.
private func expectTimeoutAtTheDeadline(alwaysReady frame: Frame) async throws {
    let link = FakeLink()
    link.setAlwaysReady(frame)
    // With an event always ready the session queues acknowledgements as fast as it can read, and a
    // fake that completed each write at once would let it start two tasks per write, hundreds of
    // thousands a second, which slows every other test in the process. Parked, at most one write
    // is in flight and the session's backstop ends it every `writeTimeout + replyTimeout`, as on a
    // link where each write takes a round trip. That leaves a queue of hundreds of thousands of
    // acknowledgements, which the session keeps writing after it stops; at one per backstop that
    // would go on for hours in the background of the rest of the run. Closing the link afterwards
    // makes each of those writes fail at once, so the queue is gone within a second or so.
    link.parkSends(ofGroup: 0x04)
    defer { link.close() }
    let conversationTimeout = 0.5
    let s = RingSession(link: link, replyTimeout: 0.05, conversationTimeout: conversationTimeout, monotonicNow: { ContinuousClock.now })
    try await using(s, link) { s async throws in
        let start = ContinuousClock.now
        let pending = Task { try await s.request(Command.deviceInfo) }
        // Generous for a loaded machine; without the fixes this never returned. Bounded here so a
        // regression fails the test instead of keeping it waiting.
        let result = await outcome(of: pending, within: 10)
        let elapsed = ContinuousClock.now - start
        // Never empty, so the drain never completes and nothing is written.
        switch result {
        case .failure(RingError.timeout(let what))?: #expect(what == "the link to drain")
        case let other: Issue.record("expected a timeout, got \(String(describing: other))")
        }
        #expect(elapsed >= .seconds(conversationTimeout))
        #expect(!link.sent.contains(Command.deviceInfo))
        link.setAlwaysReady(nil)
    }
}

@Test(.timeLimit(.minutes(1))) func requestOnALinkThatAlwaysHasAFrameReadyEndsAtItsDeadline() async throws {
    try await expectTimeoutAtTheDeadline(alwaysReady: Frame(group: 0x02, key: 0x0C, payload: [1]))
}

@Test(.timeLimit(.minutes(1))) func requestOnALinkThatAlwaysHasAnEventReadyEndsAtItsDeadline() async throws {
    try await expectTimeoutAtTheDeadline(alwaysReady: Frame(group: 0x04, key: 0x0E, payload: [0x00, 0x01]))
}

// The deadline is checked on every frame routed while the conversation waits, not only when the
// wait's timer fires: here each wait's timer is seconds away in real time, while the stepping
// clock crosses the deadline after a few frames. Relying on the timers alone would take about
// ten seconds (slices of 4, 3, 2 and 1 s); checking on each frame ends it at once.
@Test(.timeLimit(.minutes(1))) func deadlineIsCheckedOnEveryFrameNotOnlyWhenATimerFires() async throws {
    let link = FakeLink()
    link.setAlwaysReady(Frame(group: 0x02, key: 0x0C, payload: [1]))
    let clock = SteppingClock()
    let s = RingSession(link: link, replyTimeout: 600, conversationTimeout: 5, monotonicNow: { clock.now() })
    try await using(s, link) { s async throws in
        let start = ContinuousClock.now
        let pending = Task { try await s.request(Command.deviceInfo) }
        switch await outcome(of: pending, within: 20) {
        case .failure(RingError.timeout)?: break
        case let other: Issue.record("expected a timeout, got \(String(describing: other))")
        }
        #expect(ContinuousClock.now - start < .seconds(3))
        link.setAlwaysReady(nil)
    }
}

// Frames held for a conversation that is not waiting (here: its command write is in progress) are
// capped. Past the cap the conversation fails, rather than dropping a frame and carrying on.
@Test(.timeLimit(.minutes(1))) func tooManyHeldFramesFailTheConversation() async throws {
    let link = FakeLink()
    // Long enough that the backstop cannot cut off the parked write during the test.
    try await using(RingSession(link: link, replyTimeout: 30), link) { s async throws in
        link.parkNextSend()
        let pending = Task { try await s.request(Command.deviceInfo) }
        #expect(await link.waitUntilSendParked())
        let flood = Array(repeating: Frame(group: 0x02, key: 0x0C, payload: [1]), count: RingSession.maximumHeldFrames + 1)
        // The event after the flood shows every frame of the flood has been routed.
        link.deliver(flood + [event])
        await waitUntil("the flood is read") { await s.queuedAcknowledgements == 1 }
        #expect(await s.heldFrames == RingSession.maximumHeldFrames)
        link.releaseParked()
        switch await outcome(of: pending, within: 10) {
        case .failure(RingError.tooManyFrames(let limit))?: #expect(limit == RingSession.maximumHeldFrames)
        case let other: Issue.record("expected tooManyFrames, got \(String(describing: other))")
        }
    }
}

// A conversation waits for the acknowledgements of events read before it ended, but only for one
// write backstop in all: here every acknowledgement write hangs until the session's backstop cuts
// it off, and waiting for each in turn would take twenty backstops.
@Test(.timeLimit(.minutes(1))) func waitForAcknowledgementsAtTheEndOfAConversationIsBounded() async throws {
    let events = (0..<20).map { Frame(group: 0x04, key: 0x0E, payload: [UInt8($0)]) }
    let link = FakeLink { $0.type == 0x0200 ? events + [deviceReply] : [] }
    link.parkSends(ofGroup: 0x04)
    let replyTimeout = 0.5
    try await using(RingSession(link: link, replyTimeout: replyTimeout), link) { s async throws in
        let start = ContinuousClock.now
        #expect(try await s.request(Command.deviceInfo) == deviceReply)
        let elapsed = ContinuousClock.now - start
        // One backstop is writeTimeout + replyTimeout = 0.52 s; twenty would be over 10 s.
        #expect(elapsed < .seconds(5))
        link.releaseAll()
    }
}

// Pins the overrun past the deadline at about 3B, with B the write backstop. Every write hangs
// until the session's backstop ends it. An acknowledgement is in progress when the request starts,
// so its command is written shortly before the deadline; the command write runs B past that, an
// acknowledgement queued during it runs B more, and the conversation waits for it at the end.
@Test(.timeLimit(.minutes(1))) func requestEndsWithinThreeBackstopsOfItsDeadline() async throws {
    let link = FakeLink { $0.type == 0x0200 ? [deviceReply] : [] }
    defer { link.close() }
    link.parkAllSends()
    let replyTimeout = 0.3
    let backstop = link.writeTimeout + replyTimeout
    let conversationTimeout = backstop + 0.15
    let s = RingSession(link: link, replyTimeout: replyTimeout, conversationTimeout: conversationTimeout, monotonicNow: { ContinuousClock.now })
    try await using(s, link) { s async throws in
        await s.start()
        link.deliver([event])
        #expect(await link.waitUntilSendParked())
        let start = ContinuousClock.now
        let pending = Task { try await s.request(Command.deviceInfo) }
        await waitUntil("the command write has started") { link.events.contains("attempt 0200") }
        link.deliver([otherEvent])
        let result = await outcome(of: pending, within: 20)
        let overrun = ContinuousClock.now - start - .seconds(conversationTimeout)
        switch result {
        case .failure(RingError.timeout)?: break
        case let other: Issue.record("expected a timeout, got \(String(describing: other))")
        }
        // A little slack for timers on a loaded machine; the old bound was about 4B.
        #expect(overrun < .seconds(3 * backstop + 0.2))
    }
}

// A command is never written after the conversation's deadline: here the deadline passes while the
// link is dropping partial frames, before the command is written.
@Test(.timeLimit(.minutes(1))) func aCommandIsNeverWrittenAfterTheDeadline() async throws {
    let link = FakeLink { $0.type == 0x0200 ? [deviceReply] : [] }
    link.delayPartialReset(by: 0.5)
    let s = RingSession(link: link, replyTimeout: 0.05, conversationTimeout: 0.1, monotonicNow: { ContinuousClock.now })
    try await using(s, link) { s async throws in
        await expectTimeout(waitingFor: "reply to 0200") { _ = try await s.request(Command.deviceInfo) }
        #expect(!link.events.contains("attempt 0200"))
    }
}

// The wait for acknowledgements before a conversation's write ends at the conversation's deadline,
// not one write backstop later: a conversation that reaches its write with little time left and
// an acknowledgement still unwritten fails with that timeout, and does not write its command.
@Test(.timeLimit(.minutes(1))) func waitForAcknowledgementsBeforeAWriteEndsAtTheDeadline() async throws {
    let link = FakeLink { $0.type == 0x0200 ? [deviceReply] : [] }
    defer { link.close() }
    link.parkSends(ofGroup: 0x04)
    // The acknowledgement's backstop (about 1 s) is far beyond the deadline (0.2 s).
    let s = RingSession(link: link, replyTimeout: 1, conversationTimeout: 0.2, monotonicNow: { ContinuousClock.now })
    try await using(s, link) { s async throws in
        await s.start()
        link.deliver([event])
        #expect(await link.waitUntilSendParked())
        await expectTimeout(waitingFor: "event acknowledgements to be written") {
            _ = try await s.request(Command.deviceInfo)
        }
        #expect(!link.events.contains("attempt 0200"))
    }
}

// MARK: Continuous reader

/// Polls `condition` (session state that has no signal of its own) every millisecond until it
/// holds; records an issue and returns false after `seconds` or when the test is cancelled (its
/// time limit), so a regression fails instead of waiting for ever. It sleeps between polls rather
/// than yielding: a dozen tests spinning on `Task.yield()` at once kept every thread of the pool
/// busy and starved the other tests running in parallel.
@discardableResult
private func waitUntil(_ what: String, within seconds: Double = 10, _ condition: () async -> Bool) async -> Bool {
    let end = ContinuousClock.now + .seconds(seconds)
    while await !condition() {
        guard !Task.isCancelled, ContinuousClock.now < end else {
            Issue.record("gave up waiting until \(what)")
            return false
        }
        try? await Task.sleep(for: .milliseconds(1))
    }
    return true
}

/// The outcome of `task`, or nil if it has not finished within `seconds`.
private func outcome<T: Sendable>(of task: Task<T, Error>, within seconds: Double) async -> Result<T, Error>? {
    let once = OnceResult<T>()
    return await withCheckedContinuation { continuation in
        once.arm(continuation)
        Task {
            do { once.finish(.success(try await task.value)) } catch { once.finish(.failure(error)) }
        }
        Task {
            try? await Task.sleep(for: .seconds(seconds))
            once.finish(nil)
        }
    }
}

// @unchecked Sendable: all state is guarded by `lock`.
private final class OnceResult<T: Sendable>: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<Result<T, Error>?, Never>?
    func arm(_ continuation: CheckedContinuation<Result<T, Error>?, Never>) { lock.withLock { self.continuation = continuation } }
    func finish(_ result: Result<T, Error>?) { lock.withLock { continuation.take() }?.resume(returning: result) }
}

private let event = Frame(group: 0x04, key: 0x0E, payload: [0x00, 0x01])
private let otherEvent = Frame(group: 0x04, key: 0x0A, payload: [0x02])
private func acknowledgement(of event: Frame) -> Frame { Frame(group: 0x04, key: event.key, payload: [0x00]) }

@Test(.timeLimit(.minutes(1))) func eventWhileIdleIsAcknowledged() async throws {
    let link = FakeLink()
    try await using(session(link), link) { s async throws in
        await s.start()
        link.deliver([event])
        #expect(await link.waitUntilSent(1))
        #expect(link.sent == [acknowledgement(of: event)])
    }
}

@Test(.timeLimit(.minutes(1))) func eventBetweenTwoRequestsIsAcknowledgedOnceAndTheSecondGetsItsReply() async throws {
    let replies = CallCounter()
    let link = FakeLink { frame in
        frame.type == 0x0200 ? [Frame(group: 0x02, key: 0x00, payload: [UInt8(replies.next())])] : []
    }
    try await using(session(link), link) { s async throws in
        #expect(try await s.request(Command.deviceInfo).payload == [1])
        // Not waited for: the event may be read while idle or while the second request drains the
        // link. Either way it is acknowledged before the second request is written.
        link.deliver([event])
        #expect(try await s.request(Command.deviceInfo).payload == [2])
        #expect(link.sent == [Command.deviceInfo, acknowledgement(of: event), Command.deviceInfo])
    }
}

@Test(.timeLimit(.minutes(1))) func eventDuringAHistoryTransferLeavesTheDataAndChecksumIntact() async throws {
    let link = FakeLink { frame in
        guard frame.type == Command.history(kind).type else { return [] }
        return [header(12), dataFrame([1, 2]), event, dataFrame([3]), endFrame(checksum: CRC16.ccittFalse([1, 2, 3]))]
    }
    let persisted = PersistRecorder()
    try await using(session(link), link) { s async throws in
        let transfer = try await s.fetchHistory(kind) { await persisted.add($0) }
        #expect(transfer.data == [1, 2, 3])
        #expect(await persisted.calls.map(\.data) == [[1, 2, 3]])
        // The event was read before the end frame, so it is acknowledged before the transfer is.
        #expect(link.sent == [Command.history(kind), acknowledgement(of: event), Command.historyTransferOK])
    }
}

// An event read before a conversation writes is acknowledged first, even when its acknowledgement
// is still queued behind another one that is in progress when the conversation is ready to write.
@Test(.timeLimit(.minutes(1))) func acknowledgementsQueuedBeforeAConversationsWriteAreWrittenFirst() async throws {
    let link = FakeLink { $0.type == 0x0200 ? [deviceReply] : [] }
    // Long enough that the backstop cannot cut off the parked write during the test.
    try await using(RingSession(link: link, replyTimeout: 30), link) { s async throws in
        await s.start()
        link.parkNextSend()
        link.deliver([event, otherEvent])
        #expect(await link.waitUntilSendParked())
        await waitUntil("both events are read") { await s.queuedAcknowledgements == 2 }
        let pending = Task { try await s.request(Command.deviceInfo) }
        // Either waiting for the acknowledgements (correct) or queued for the write turn behind
        // the parked one (if the conversation did not wait for them).
        await waitUntil("the request is ready to write") { await (s.pendingFlushes + s.queuedWrites) > 0 }
        link.releaseParked()
        #expect(try await pending.value == deviceReply)
        #expect(link.sent == [acknowledgement(of: event), acknowledgement(of: otherEvent), Command.deviceInfo])
    }
}

@Test(.timeLimit(.minutes(1))) func staleReplyFromATimedOutRequestIsNotAcceptedByTheNextConversation() async throws {
    let late = Frame(group: 0x02, key: 0x00, payload: [0xAA])
    let activity = Frame(group: 0x02, key: 0x0C, payload: [1])
    let deviceCalls = CallCounter()
    let link = FakeLink { frame in
        switch frame.type {
        // The first device-info request gets no reply in time.
        case 0x0200: return deviceCalls.next() == 1 ? [] : [deviceReply]
        // The late reply turns up again inside the next conversation, ahead of its own reply.
        case 0x020C: return [late, activity]
        case Command.history(kind).type:
            return [late, header(12), dataFrame([1]), late, endFrame(checksum: CRC16.ccittFalse([1]))]
        default: return []
        }
    }
    try await using(session(link), link) { s async throws in
        await expectTimeout { _ = try await s.request(Command.deviceInfo) }
        link.deliver([late])
        #expect(try await s.request(Command.currentActivity) == activity)
        #expect(try await s.fetchHistory(kind) { _ in }.data == [1])
        // Delivered just before a request of the same kind: the drain drops it first.
        link.deliver([late])
        #expect(try await s.request(Command.deviceInfo) == deviceReply)
    }
}

// The drain needs an empty read that *began* after it was asked for. An empty read that began
// earlier says nothing about a frame that arrived since: here the stale frame arrives after the
// reader's read has timed out but before that empty result is routed.
@Test(.timeLimit(.minutes(1))) func emptyReadThatBeganBeforeTheDrainDoesNotOpenTheConversation() async throws {
    let stale = Frame(group: 0x02, key: 0x00, payload: [0xAA])
    let fresh = Frame(group: 0x02, key: 0x00, payload: [0xBB])
    let link = FakeLink { $0.type == 0x0200 ? [fresh] : [] }
    link.holdNextEmptyRead()
    try await using(session(link), link) { s async throws in
        await s.start()
        #expect(await link.waitUntilEmptyReadHeld())
        link.deliver([stale])
        let pending = Task { try await s.request(Command.deviceInfo) }
        await waitUntil("the request waits for the drain") { await s.isDraining }
        link.releaseHeldEmptyRead()
        #expect(try await pending.value == fresh)
    }
}

// Outside a conversation a reply-group frame can only be stale; it is dropped, not kept.
@Test(.timeLimit(.minutes(1))) func framesWithNoConversationOpenAreDroppedNotHeld() async throws {
    let link = FakeLink()
    try await using(session(link), link) { s async throws in
        await s.start()
        link.deliver([Frame(group: 0x02, key: 0x00, payload: [1]), event])
        // The event's acknowledgement shows the frame before it has been routed.
        #expect(await link.waitUntilSent(1))
        #expect(await s.heldFrames == 0)
    }
}

// Frames an earlier conversation left unread (they arrived while it was writing, after the
// frame it was waiting for) never reach a later conversation.
@Test(.timeLimit(.minutes(1))) func framesLeftByOneConversationDoNotReachTheNext() async throws {
    let leftover = Frame(group: 0x02, key: 0x0C, payload: [0xAA])
    let activity = Frame(group: 0x02, key: 0x0C, payload: [0xBB])
    let link = FakeLink { $0.type == 0x020C ? [activity] : [] }
    // Long enough that the backstop cannot cut off the parked write during the test.
    try await using(RingSession(link: link, replyTimeout: 30), link) { s async throws in
        link.parkNextSend()
        let first = Task { try await s.request(Command.deviceInfo) }
        #expect(await link.waitUntilSendParked())
        // Both arrive while the first conversation's write is in progress, so both are held for it.
        link.deliver([deviceReply, leftover])
        await waitUntil("both frames are held") { await s.heldFrames == 2 }
        link.releaseParked()
        #expect(try await first.value == deviceReply)
        #expect(try await s.request(Command.currentActivity) == activity)
    }
}

@Test(.timeLimit(.minutes(1))) func subscribersReceiveEventsAndLiveValuesInOrder() async throws {
    let link = FakeLink()
    try await using(session(link), link) { s async throws in
        let first = await s.uploads()
        let second = await s.uploads()
        let live1 = Frame(group: 0x06, key: 0x01, payload: [60])
        let live2 = Frame(group: 0x06, key: 0x01, payload: [61])
        // The 0x0200 frame has no conversation to go to, so it is dropped.
        link.deliver([event, live1, Frame(group: 0x02, key: 0x00, payload: [1]), otherEvent, live2])
        let expected: [RingUpload] = [.event(event), .live(live1), .event(otherEvent), .live(live2)]
        for stream in [first, second] {
            var received: [RingUpload] = []
            for await upload in stream {
                received.append(upload)
                if received.count == expected.count { break }
            }
            #expect(received == expected)
        }
        #expect(await link.waitUntilSent(2))
        #expect(link.sent == [acknowledgement(of: event), acknowledgement(of: otherEvent)])
    }
}

// An error reply in the event group could only answer one of the session's acknowledgements;
// acknowledging it back could make the two sides answer each other for ever.
@Test(.timeLimit(.minutes(1))) func errorReplyInTheEventGroupIsNeitherAcknowledgedNorPublished() async throws {
    let link = FakeLink()
    try await using(session(link), link) { s async throws in
        let stream = await s.uploads()
        link.deliver([Frame(group: 0x04, key: 0x0E, payload: [0xFC]), otherEvent])
        var iterator = stream.makeAsyncIterator()
        #expect(await iterator.next() == .event(otherEvent))
        #expect(await link.waitUntilSent(1))
        #expect(link.sent == [acknowledgement(of: otherEvent)])
    }
}

@Test(.timeLimit(.minutes(1))) func liveValuesDuringARequestGoToSubscribersNotTheConversation() async throws {
    let live = Frame(group: 0x06, key: 0x01, payload: [60])
    let link = FakeLink { $0.type == 0x0200 ? [live, deviceReply] : [] }
    try await using(session(link), link) { s async throws in
        let stream = await s.uploads()
        #expect(try await s.request(Command.deviceInfo) == deviceReply)
        var iterator = stream.makeAsyncIterator()
        #expect(await iterator.next() == .live(live))
    }
}

// The same, for a request whose type matches the live values (which `request` refuses, so the
// checks are skipped here): a live frame must not be taken as its reply, whether it arrives while
// the command is being written or while the request waits.
@Test(.timeLimit(.minutes(1))) func liveValueOfTheRequestsOwnTypeIsNotTakenAsItsReply() async throws {
    let liveCommand = Frame(group: 0x06, key: 0x01)
    let duringWrite = Frame(group: 0x06, key: 0x01, payload: [60])
    let whileWaiting = Frame(group: 0x06, key: 0x01, payload: [61])
    let link = FakeLink { $0.type == 0x0601 ? [duringWrite] : [] }
    let s = RingSession(link: link, replyTimeout: 5, conversationTimeout: 1, monotonicNow: { ContinuousClock.now })
    try await using(s, link) { s async throws in
        let stream = await s.uploads()
        let pending = Task { try await s.requestSkippingChecks(liveCommand) }
        await waitUntil("the request waits for its reply") { await s.conversationIsWaiting }
        link.deliver([whileWaiting])
        do {
            _ = try await pending.value
            Issue.record("expected a timeout")
        } catch RingError.timeout {
        }
        var iterator = stream.makeAsyncIterator()
        #expect(await iterator.next() == .live(duringWrite))
        #expect(await iterator.next() == .live(whileWaiting))
    }
}

@Test(.timeLimit(.minutes(1))) func stopEndsTheStreamAndFailsAWaitingRequest() async throws {
    let link = FakeLink()
    // A reply timeout far beyond what the test waits: only `stop` can end this request in time.
    try await using(RingSession(link: link, replyTimeout: 30), link) { s async throws in
        let stream = await s.uploads()
        let pending = Task { try await s.request(Command.deviceInfo) }
        // Really waiting for its reply, not merely past its write.
        await waitUntil("the request waits for its reply") { await s.conversationIsWaiting }
        await s.stop()
        switch await outcome(of: pending, within: 5) {
        case .failure(RingError.stopped)?: break
        case let other: Issue.record("expected the request to fail with stopped, got \(String(describing: other))")
        }
        var iterator = stream.makeAsyncIterator()
        #expect(await iterator.next() == nil)
        // Stays stopped: later requests fail at once, and new streams are already finished.
        do {
            _ = try await s.request(Command.deviceInfo)
            Issue.record("expected the request to fail")
        } catch RingError.stopped {
        }
        var late = await s.uploads().makeAsyncIterator()
        #expect(await late.next() == nil)
        #expect(link.sent == [Command.deviceInfo])
    }
}

// A session lives for one connection: once stopped it stays stopped. `start()` does nothing, so
// an event that arrives afterwards is left in the link, and every request, history fetch and
// subscription behaves as stopped.
@Test(.timeLimit(.minutes(1))) func stopIsFinal() async throws {
    let link = FakeLink { $0.type == 0x0200 ? [deviceReply] : [] }
    try await using(session(link), link) { s async throws in
        await s.start()
        await s.stop()
        await s.start()
        link.deliver([event])
        // A running reader would take the event within one poll interval (12.5 ms here).
        try await Task.sleep(for: .milliseconds(100))
        #expect(link.queuedFrames == 1)
        #expect(link.sent.isEmpty)
        do {
            _ = try await s.request(Command.deviceInfo)
            Issue.record("expected the request to fail")
        } catch RingError.stopped {
        }
        do {
            _ = try await s.fetchHistory(kind) { _ in }
            Issue.record("expected the fetch to fail")
        } catch RingError.stopped {
        }
        var iterator = await s.uploads().makeAsyncIterator()
        #expect(await iterator.next() == nil)
        #expect(link.sent.isEmpty)
    }
}

// A second `stop()` made while the first is still waiting for the reader waits for it too. The
// frame the reader takes while stopping is dropped: an event among such frames is not acknowledged.
@Test(.timeLimit(.minutes(1))) func secondStopWaitsForTheReaderAndAFrameTakenWhileStoppingIsDropped() async throws {
    let link = FakeLink()
    link.setWaitsForever(true)
    try await using(session(link), link) { s async throws in
        await s.start()
        #expect(await link.waitUntilReaderWaits())
        let first = Task { await s.stop() }
        await waitUntil("the first stop has begun") { await s.isStopped }
        let secondReturned = Latch()
        let second = Task {
            await s.stop()
            secondReturned.open()
        }
        // The reader is held inside `nextFrame`, so neither stop can have finished.
        try await Task.sleep(for: .milliseconds(100))
        #expect(!secondReturned.isOpen, "a second stop returned before the reader ended")
        link.deliver([event])
        await first.value
        await second.value
        #expect(link.queuedFrames == 0)
        #expect(link.sent.isEmpty, "an event taken while stopping was acknowledged")
    }
}

// A conversation that is writing when `stop()` comes fails at its next wait.
@Test(.timeLimit(.minutes(1))) func stopDuringACommandWriteFailsTheRequest() async throws {
    let link = FakeLink { $0.type == 0x0200 ? [deviceReply] : [] }
    // Long enough that the backstop cannot cut off the parked write during the test.
    try await using(RingSession(link: link, replyTimeout: 30), link) { s async throws in
        link.parkNextSend()
        let pending = Task { try await s.request(Command.deviceInfo) }
        #expect(await link.waitUntilSendParked())
        await s.stop()
        link.releaseParked()
        switch await outcome(of: pending, within: 5) {
        case .failure(RingError.stopped)?: break
        case let other: Issue.record("expected the request to fail with stopped, got \(String(describing: other))")
        }
    }
}

// A history transfer stopped inside `persist` fails, and its acknowledgement is not written: the
// ring keeps its data, so the next session fetches it again.
@Test(.timeLimit(.minutes(1))) func stopDuringPersistFailsTheTransferWithoutAcknowledgingIt() async throws {
    let link = historyLink(chunks: [[1, 2, 3]])
    let gate = Gate()
    try await using(session(link), link, alsoOnCancel: { gate.release() }) { s async throws in
        let pending = Task { try await s.fetchHistory(kind) { _ in await gate.park() } }
        #expect(await gate.parked.wait())
        await s.stop()
        gate.release()
        switch await outcome(of: pending, within: 5) {
        case .failure(RingError.stopped)?: break
        case let other: Issue.record("expected the fetch to fail with stopped, got \(String(describing: other))")
        }
        #expect(link.sent == [Command.history(kind)])
    }
}

// `stop` waits for acknowledgements already queued, and they are still written after the session
// has stopped: here the second is queued behind a write that is in progress when `stop` is called.
@Test(.timeLimit(.minutes(1))) func stopWritesQueuedAcknowledgementsBeforeReturning() async throws {
    let link = FakeLink()
    // Long enough that the backstop cannot cut off the parked write during the test.
    try await using(RingSession(link: link, replyTimeout: 30), link) { s async throws in
        await s.start()
        link.parkNextSend()
        link.deliver([event, otherEvent])
        #expect(await link.waitUntilSendParked())
        await waitUntil("both events are read") { await s.queuedAcknowledgements == 2 }
        let stopped = Latch()
        let stopping = Task {
            await s.stop()
            stopped.open()
        }
        await waitUntil("stop waits for the acknowledgements or returns") { await s.pendingFlushes > 0 || stopped.isOpen }
        #expect(!stopped.isOpen, "stop returned without waiting for queued acknowledgements")
        link.releaseParked()
        await stopping.value
        #expect(link.sent == [acknowledgement(of: event), acknowledgement(of: otherEvent)])
    }
}

@Test(.timeLimit(.minutes(1))) func acknowledgementWaitsForTheConversationsWriteInFlight() async throws {
    // The link refuses a second write while one is in flight, as the real connection does.
    let link = FakeLink(guardWrites: true) { $0.type == 0x0200 ? [deviceReply] : [] }
    // Long enough that the backstop cannot cut off the parked write during the test.
    try await using(RingSession(link: link, replyTimeout: 30), link) { s async throws in
        link.parkNextSend()
        let pending = Task { try await s.request(Command.deviceInfo) }
        #expect(await link.waitUntilSendParked())
        // The event arrives while the request's own write is still in progress.
        link.deliver([event])
        // Ends early on a refused write, so a session that does not queue the acknowledgement fails
        // the assertions below instead of spinning here.
        await waitUntil("the acknowledgement is queued") {
            await s.queuedWrites > 0 || link.events.contains { $0.hasPrefix("busy") }
        }
        link.releaseParked()
        #expect(try await pending.value == deviceReply)
        #expect(link.sent == [Command.deviceInfo, acknowledgement(of: event)])
        #expect(!link.events.contains { $0.hasPrefix("busy") })
    }
}

@Test(.timeLimit(.minutes(1))) func backstopEndsAStuckAcknowledgementAndTheNextRequestWorks() async throws {
    // The write backstop covers acknowledgements too: a stuck one must not hold up the next request.
    let link = FakeLink { $0.type == 0x0200 ? [deviceReply] : [] }
    try await using(session(link), link) { s async throws in
        await s.start()
        link.parkNextSend()
        link.deliver([event])
        #expect(await link.waitUntilSendParked())
        // Independent watchdog, as in `backstopEndsAWriteOnALinkThatNeverReturns`: without the
        // backstop the request would wait on the parked acknowledgement for ever.
        let watchdogFired = Latch()
        let watchdog = Task {
            try? await Task.sleep(for: .seconds(3))
            guard !Task.isCancelled else { return }
            watchdogFired.open()
            link.releaseParked()
        }
        #expect(try await s.request(Command.deviceInfo) == deviceReply)
        watchdog.cancel()
        #expect(!watchdogFired.isOpen, "the session did not give up on the acknowledgement by itself")
        #expect(link.sent == [Command.deviceInfo])
        link.releaseParked()
    }
}

// MARK: withDeadline

@Test(.timeLimit(.minutes(1))) func deadlineHelperReturnsTheOperationsResultWhenItWins() async throws {
    // The long deadline must not keep the call waiting: the timer is cancelled when the operation wins.
    let value = try await withDeadline(3600, "never") { 42 }
    #expect(value == 42)
}

@Test(.timeLimit(.minutes(1))) func deadlineHelperGivesUpOnAnOperationThatIgnoresCancellation() async throws {
    // The operation must ignore its own cancellation, or a helper that waited for it after
    // cancelling it would pass. It is released if the test itself is cancelled (its time limit).
    let parked = ParkedOperation()
    await withTaskCancellationHandler {
        await expectTimeout {
            try await withDeadline(0.05, "stuck") { await parked.park() }
        }
    } onCancel: {
        parked.release()
    }
    parked.release()
}

@Test(.timeLimit(.minutes(1))) func deadlineHelperPropagatesTheOperationsError() async throws {
    do {
        try await withDeadline(3600, "never") { throw PersistFailure() }
        Issue.record("expected an error")
    } catch is PersistFailure {
    }
}

/// Parks its caller until `release()`, ignoring the caller's cancellation on purpose (see the test
/// above). A release that comes first lets the caller straight through.
// @unchecked Sendable: all state is guarded by `lock`.
private final class ParkedOperation: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<Void, Never>?
    private var released = false
    func park() async {
        await withCheckedContinuation { c in
            let open = lock.withLock { () -> Bool in
                if !released { continuation = c }
                return released
            }
            if open { c.resume() }
        }
    }
    func release() {
        let waiting = lock.withLock { () -> CheckedContinuation<Void, Never>? in
            released = true
            return continuation.take()
        }
        waiting?.resume()
    }
}

/// Opens once; waiters are woken then, or give up after a time limit or on cancellation.
// @unchecked Sendable: all state is guarded by `lock`.
private final class Latch: @unchecked Sendable {
    private let lock = NSLock()
    private var opened = false
    private var waiters: [Int: CheckedContinuation<Bool, Never>] = [:]
    private var lastID = 0

    var isOpen: Bool { lock.withLock { opened } }

    func open() {
        let all = lock.withLock { () -> [CheckedContinuation<Bool, Never>] in
            opened = true
            defer { waiters = [:] }
            return Array(waiters.values)
        }
        for waiter in all { waiter.resume(returning: true) }
    }

    /// Whether the latch opened within `seconds` (false also when the caller is cancelled).
    func wait(within seconds: Double = 10) async -> Bool {
        let id = lock.withLock { () -> Int in
            lastID += 1
            return lastID
        }
        return await withTaskCancellationHandler {
            await withCheckedContinuation { (continuation: CheckedContinuation<Bool, Never>) in
                let immediate = lock.withLock { () -> Bool? in
                    if opened { return true }
                    if Task.isCancelled { return false }
                    waiters[id] = continuation
                    return nil
                }
                if let immediate {
                    continuation.resume(returning: immediate)
                    return
                }
                DispatchQueue.global().asyncAfter(deadline: .now() + seconds) { self.giveUp(id) }
            }
        } onCancel: {
            giveUp(id)
        }
    }

    private func giveUp(_ id: Int) {
        lock.withLock { waiters.removeValue(forKey: id) }?.resume(returning: false)
    }
}

/// Holds a caller until `release()`, or until the caller is cancelled; a release that comes first
/// lets the caller straight through. `parked` opens once a caller is held.
// @unchecked Sendable: all state is guarded by `lock`.
private final class Gate: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<Void, Never>?
    private var released = false
    let parked = Latch()
    func park() async {
        await withTaskCancellationHandler {
            await withCheckedContinuation { c in
                let open = lock.withLock { () -> Bool in
                    if !released && !Task.isCancelled { continuation = c }
                    return released || Task.isCancelled
                }
                if open { c.resume() } else { parked.open() }
            }
        } onCancel: {
            release()
        }
    }
    func release() {
        let waiting = lock.withLock { () -> CheckedContinuation<Void, Never>? in
            released = true
            return continuation.take()
        }
        waiting?.resume()
    }
}

// MARK: Monitoring interval

// Synthetic ring replies: `00` is the ack seen on the R99, `FC` the documented unsupported code.
@Test(.timeLimit(.minutes(1)), arguments: MonitoringInterval.allCases)
func monitoringIntervalIsSentAndAckAccepted(interval: MonitoringInterval) async throws {
    let link = FakeLink { $0.type == 0x010C ? [Frame(group: 0x01, key: 0x0C, payload: [0x00])] : [] }
    try await using(session(link), link) { s async throws in
        try await s.setMonitoringInterval(interval)
        #expect(link.sent == [Command.setMonitoringInterval(interval)])
    }
}

@Test(.timeLimit(.minutes(1))) func monitoringIntervalUnsupportedReplyMapsToUnsupported() async throws {
    let link = FakeLink { _ in [Frame(group: 0x01, key: 0x0C, payload: [0xFC])] }
    try await using(session(link), link) { s async throws in
        await #expect(throws: MonitoringSettingError.unsupported) {
            try await s.setMonitoringInterval(.tenMinutes)
        }
    }
}

private func monitoringReply(_ payload: [UInt8]) -> FakeLink {
    FakeLink { _ in [Frame(group: 0x01, key: 0x0C, payload: payload)] }
}

/// Sets the monitoring interval through a session on `link` and stops the session afterwards.
private func setMonitoringInterval(_ interval: MonitoringInterval, on link: FakeLink) async throws {
    try await using(session(link), link) { s async throws in try await s.setMonitoringInterval(interval) }
}

@Test(.timeLimit(.minutes(1)), arguments: [0xFB, 0xFD, 0xFE, 0xFF] as [UInt8])
func monitoringIntervalOtherErrorCodesMapToRejected(code: UInt8) async throws {
    await #expect(throws: MonitoringSettingError.rejected(code)) {
        try await setMonitoringInterval(.fiveMinutes, on: monitoringReply([code]))
    }
}

@Test(.timeLimit(.minutes(1))) func monitoringIntervalNonAckOneByteReplyIsUnexpected() async throws {
    await #expect(throws: MonitoringSettingError.unexpectedReply(0x01)) {
        try await setMonitoringInterval(.fiveMinutes, on: monitoringReply([0x01]))
    }
}

@Test(.timeLimit(.minutes(1))) func monitoringIntervalEmptyReplyIsUnexpected() async throws {
    await #expect(throws: MonitoringSettingError.unexpectedReply(nil)) {
        try await setMonitoringInterval(.fiveMinutes, on: monitoringReply([]))
    }
}

// A reply that merely starts with 00 is not the ack.
@Test(.timeLimit(.minutes(1))) func monitoringIntervalLongerReplyStartingWithZeroIsUnexpected() async throws {
    await #expect(throws: MonitoringSettingError.unexpectedReply(nil)) {
        try await setMonitoringInterval(.fiveMinutes, on: monitoringReply([0x00, 0x01]))
    }
}

@Test(.timeLimit(.minutes(1))) func monitoringIntervalWithNoReplyTimesOut() async throws {
    do {
        try await setMonitoringInterval(.off, on: FakeLink())
        Issue.record("expected an error")
    } catch RingError.timeout {}
}
