import Foundation
import Testing
import RingProtocol
@testable import RingTransport

/// A scripted stand-in for the ring. `onSend` returns the frames the "ring" delivers in answer to
/// each frame the session writes, and `deliver` sends frames unprompted. Frames are handed to the
/// session's reader strictly in order; a reader waiting for a frame is woken as soon as one is
/// delivered, and with a zero timeout `nextFrame` returns at once.
/// `@unchecked Sendable` because all state is guarded by `lock`.
private final class FakeLink: RingLink, @unchecked Sendable {
    private let lock = NSLock()
    private var inbox: [Frame]
    private var log: [String] = []
    private var sentFrames: [Frame] = []
    private let onSend: @Sendable (Frame) -> [Frame]
    private var reader: (id: Int, continuation: CheckedContinuation<Frame?, Never>)?
    private var lastReaderID = 0
    private var sentWaiters: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []

    init(stale: [Frame] = [], guardWrites: Bool = false, writeTimeout: TimeInterval = 0.02, onSend: @escaping @Sendable (Frame) -> [Frame] = { _ in [] }) {
        inbox = stale
        self.guardWrites = guardWrites
        self.writeTimeout = writeTimeout
        self.onSend = onSend
    }

    var sent: [Frame] { lock.withLock { sentFrames } }
    var events: [String] { lock.withLock { log } }

    func record(_ event: String) { lock.withLock { log.append(event) } }

    // Scripted faults for `send`. `parkNextSend` models a link that ignores cancellation and
    // never returns (the real CheckedContinuation behaviour): the test must call `releaseParked()`
    // at the end. `timeOutNextSend` models a compliant link like the real connection: it holds the
    // write in flight for its own `writeTimeout`, ignoring cancellation, then clears the in-flight
    // state and throws.
    // With `guardWrites`, a send while an earlier one is still in flight throws `busy` (and is
    // logged), as the real connection does. `trickle` is returned by `nextFrame` whenever nothing
    // else is queued; `setTrickleOnNextSend` turns it on once the next frame has been written.
    private enum SendFault { case park, ownTimeout }
    let writeTimeout: TimeInterval
    private var faults: [SendFault] = []
    private var parked: [CheckedContinuation<Void, Never>] = []
    private var inFlight = false
    private let guardWrites: Bool
    private var trickleFrame: Frame?
    private var trickleAfterSend: Frame?
    func parkNextSend() { lock.withLock { faults.append(.park) } }
    func timeOutNextSend() { lock.withLock { faults.append(.ownTimeout) } }
    func setTrickle(_ frame: Frame?) { lock.withLock { trickleFrame = frame } }
    func setTrickleOnNextSend(_ frame: Frame) { lock.withLock { trickleAfterSend = frame } }
    var hasParkedSend: Bool { lock.withLock { !parked.isEmpty } }
    func releaseParked() {
        let all = lock.withLock { () -> [CheckedContinuation<Void, Never>] in
            defer { parked = [] }
            return parked
        }
        for continuation in all { continuation.resume() }
    }

    func send(_ frame: Frame) async throws {
        let fault = try lock.withLock { () -> SendFault? in
            if guardWrites && inFlight {
                log.append("busy \(String(format: "%04X", frame.type))")
                throw RingError.busy("another write is in progress")
            }
            inFlight = true
            return faults.isEmpty ? nil : faults.removeFirst()
        }
        switch fault {
        case .ownTimeout:
            await withCheckedContinuation { continuation in
                DispatchQueue.global().asyncAfter(deadline: .now() + writeTimeout) {
                    // The link clears its own in-flight state before reporting, so the next send is
                    // accepted. Done here, not after resuming, so task scheduling delay cannot let
                    // the session's backstop observe a write that has already timed out.
                    self.lock.withLock { self.inFlight = false }
                    continuation.resume()
                }
            }
            throw RingError.timeout("write confirmation")
        case .park:
            await withCheckedContinuation { continuation in lock.withLock { parked.append(continuation) } }
        case nil:
            break
        }
        let replies = onSend(frame)
        let ready = lock.withLock { () -> [CheckedContinuation<Void, Never>] in
            sentFrames.append(frame)
            log.append("send \(String(format: "%04X", frame.type))")
            inFlight = false
            if let next = trickleAfterSend {
                trickleFrame = next
                trickleAfterSend = nil
            }
            let count = sentFrames.count
            defer { sentWaiters.removeAll { $0.count <= count } }
            return sentWaiters.filter { $0.count <= count }.map(\.continuation)
        }
        deliver(replies)
        for continuation in ready { continuation.resume() }
    }

    /// The ring sends `frames`, as if unprompted.
    func deliver(_ frames: [Frame]) {
        guard !frames.isEmpty else { return }
        let woken = lock.withLock { () -> (CheckedContinuation<Frame?, Never>, Frame)? in
            inbox += frames
            guard let current = reader else { return nil }
            reader = nil
            return (current.continuation, inbox.removeFirst())
        }
        if let (continuation, frame) = woken { continuation.resume(returning: frame) }
    }

    /// Returns once at least `count` frames have been written.
    func waitUntilSent(_ count: Int) async {
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            let done = lock.withLock { () -> Bool in
                if sentFrames.count >= count { return true }
                sentWaiters.append((count, continuation))
                return false
            }
            if done { continuation.resume() }
        }
    }

    func nextFrame(timeout: TimeInterval) async -> Frame? {
        await withCheckedContinuation { (continuation: CheckedContinuation<Frame?, Never>) in
            let waitID = lock.withLock { () -> Int? in
                if !inbox.isEmpty || trickleFrame != nil || timeout <= 0 { return nil }
                lastReaderID += 1
                reader = (lastReaderID, continuation)
                return lastReaderID
            }
            guard let waitID else {
                continuation.resume(returning: lock.withLock { inbox.isEmpty ? trickleFrame : inbox.removeFirst() })
                return
            }
            DispatchQueue.global().asyncAfter(deadline: .now() + timeout) {
                let expired = self.lock.withLock { () -> CheckedContinuation<Frame?, Never>? in
                    guard let current = self.reader, current.id == waitID else { return nil }
                    self.reader = nil
                    return current.continuation
                }
                expired?.resume(returning: nil)
            }
        }
    }
}

// Synthetic frames: the layout matches the protocol notes, the contents are made up.
private func session(_ link: FakeLink) -> RingSession { RingSession(link: link, replyTimeout: 0.05) }

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

@Test func replyIsMatchedByFrameType() async throws {
    let reply = Frame(group: 0x02, key: 0x00, payload: [1, 2, 3])
    let link = FakeLink { $0.type == 0x0200 ? [Frame(group: 0x02, key: 0x0C, payload: [9]), reply] : [] }
    // The unrelated 0x020C frame ahead of the reply must be skipped, not returned.
    #expect(try await session(link).request(Command.deviceInfo) == reply)
}

@Test func errorReplyThrowsRejected() async throws {
    let link = FakeLink { _ in [Frame(group: 0x02, key: 0x20, payload: [0xFC])] }
    do {
        _ = try await session(link).request(Frame(group: 0x02, key: 0x20))
        Issue.record("expected an error")
    } catch RingError.rejected(let frame) {
        #expect(frame.payload == [0xFC])
    }
}

@Test func eventWhileWaitingIsAcknowledgedAndSkipped() async throws {
    let event = Frame(group: 0x04, key: 0x0A, payload: [1, 2])
    let reply = Frame(group: 0x02, key: 0x00, payload: [5])
    let link = FakeLink { $0.type == 0x0200 ? [event, reply] : [] }
    #expect(try await session(link).request(Command.deviceInfo) == reply)
    #expect(link.sent.last == Frame(group: 0x04, key: 0x0A, payload: [0x00]))
}

@Test func noReplyTimesOut() async throws {
    let link = FakeLink()
    do {
        _ = try await session(link).request(Command.deviceInfo)
        Issue.record("expected a timeout")
    } catch RingError.timeout {
    }
}

@Test func shortHeaderMeansEmptyHistoryAndNoAcknowledgement() async throws {
    let link = historyLink(chunks: [], headerLength: 9)
    let persisted = PersistRecorder()
    let transfer = try await session(link).fetchHistory(kind) { await persisted.add($0) }
    #expect(transfer.data.isEmpty)
    #expect(transfer.kind == kind)
    #expect(transfer.header == headerBytes(9))
    #expect(await persisted.calls.isEmpty)
    #expect(link.sent == [Command.history(kind)])
}

@Test func multiFrameHistoryReturnsConcatenatedBytesAndAcknowledges() async throws {
    let link = historyLink(chunks: [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10]])
    let transfer = try await session(link).fetchHistory(kind) { _ in }
    #expect(transfer.data == Array(1...10))
    #expect(link.sent == [Command.history(kind), Command.historyTransferOK])
}

@Test func checksumMismatchThrowsAndSendsNothing() async throws {
    let link = historyLink(chunks: [[1, 2, 3]], checksum: 0xBEEF)
    let persisted = PersistRecorder()
    do {
        _ = try await session(link).fetchHistory(kind) { await persisted.add($0) }
        Issue.record("expected a checksum error")
    } catch RingError.checksumMismatch {
    }
    #expect(link.sent == [Command.history(kind)])
    #expect(await persisted.calls.isEmpty)
}

@Test func persistRunsWithVerifiedBytesBeforeAcknowledgement() async throws {
    let link = historyLink(chunks: [[1, 2], [3]])
    _ = try await session(link).fetchHistory(kind) { transfer in
        link.record("persist \(transfer.data)")
    }
    let events = link.events
    let persist = try #require(events.firstIndex(of: "persist [1, 2, 3]"))
    let ack = try #require(events.firstIndex { $0 == "send 0580" })
    #expect(persist < ack)
}

@Test func failedPersistSendsNoAcknowledgement() async throws {
    let link = historyLink(chunks: [[1, 2, 3]])
    do {
        _ = try await session(link).fetchHistory(kind) { _ in throw PersistFailure() }
        Issue.record("expected persist to fail the transfer")
    } catch is PersistFailure {
    }
    #expect(link.sent == [Command.history(kind)])
}

@Test func transferCarriesHeaderFetchTimeAndTimeZone() async throws {
    let link = historyLink(chunks: [[1, 2], [3]], headerLength: 12)
    // The clock reads finer than a millisecond; the transfer keeps whole milliseconds.
    let now = Date(timeIntervalSince1970: 1_800_000_000.1234567)
    let zone = try #require(TimeZone(identifier: "Pacific/Auckland"))
    let s = RingSession(link: link, replyTimeout: 0.05, clock: { now }, timeZone: { zone })
    let persisted = PersistRecorder()
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

@Test func timeZoneIsReadForEachTransfer() async throws {
    let link = historyLink(chunks: [[1]])
    let zones = ZoneBox(["Asia/Tokyo", "America/Denver"])
    let s = RingSession(link: link, replyTimeout: 0.05, timeZone: { zones.next() })
    let first = try await s.fetchHistory(kind) { _ in }
    let second = try await s.fetchHistory(kind) { _ in }
    #expect(first.timeZoneIdentifier == "Asia/Tokyo")
    #expect(second.timeZoneIdentifier == "America/Denver")
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

@Test func setTimeUsesTheSessionsClockAndZone() async throws {
    let link = FakeLink { $0.type == 0x0100 ? [Frame(group: 0x01, key: 0x00, payload: [0])] : [] }
    // 2027-01-15 08:00:00 UTC is 21:00 on the same day in Auckland (UTC+13 in January).
    let now = Date(timeIntervalSince1970: 1_800_000_000)
    let zone = try #require(TimeZone(identifier: "Pacific/Auckland"))
    let s = RingSession(link: link, replyTimeout: 0.05, clock: { now }, timeZone: { zone })
    try await s.setTime()
    #expect(link.sent == [Command.setTime(now, timeZone: zone)])
    #expect(link.sent.first?.payload[4] == 21)
}

@Test func staleFramesAreDiscardedBeforeANewRequest() async throws {
    // A leftover frame with the same type as the reply must not be mistaken for the new reply.
    // The session no longer asks the link to discard; its reader drains the link before the
    // request is written, so the leftover is read and dropped first.
    let stale = Frame(group: 0x02, key: 0x00, payload: [0xAA])
    let fresh = Frame(group: 0x02, key: 0x00, payload: [0xBB])
    let link = FakeLink(stale: [stale]) { $0.type == 0x0200 ? [fresh] : [] }
    #expect(try await session(link).request(Command.deviceInfo) == fresh)
    #expect(link.sent == [Command.deviceInfo])
}

// The time limit turns a future regression that never reaches the gate into a failure, not a hung CI job.
@Test(.timeLimit(.minutes(1))) func overlappingConversationsRunOneAtATime() async throws {
    let link = FakeLink { frame in
        if frame.type == Command.history(kind).type {
            return [header(12), dataFrame([1]), endFrame(checksum: CRC16.ccittFalse([1]))]
        }
        return [Frame(group: frame.group, key: frame.key, payload: [frame.key])]
    }
    let s = session(link)
    // Hold the first conversation mid-flight, inside its persist step (the turn is held there).
    let gate = Gate()
    let first = Task { try await s.fetchHistory(kind) { _ in await gate.park() } }
    await waitUntil { gate.isParked }
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

@Test func sessionRefusesDeleteRangeFramesEvenIfTheLinkWouldSendThem() async throws {
    let link = FakeLink()
    do {
        _ = try await session(link).request(Frame(group: 0x05, key: 0x40))
        Issue.record("expected a refusal")
    } catch RingError.refusedDestructive {
    }
    #expect(link.sent.isEmpty)
}

// RingConnection.send repeats the delete-range refusal, but reaching it needs a CBCentralManager
// (Bluetooth permission). The session-level refusal above and Frame.erasesRingData's own test
// in RingProtocolTests cover the check.

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
    let s = RingSession(link: link, replyTimeout: replyTimeout)
    link.timeOutNextSend()
    await expectTimeout { _ = try await s.request(Command.deviceInfo) }
    #expect(try await s.request(Command.deviceInfo) == deviceReply)
}

@Test(.timeLimit(.minutes(1))) func backstopEndsAWriteOnALinkThatNeverReturns() async throws {
    // A non-compliant link: the write ignores cancellation and never returns. The session must
    // still report a timeout instead of hanging (the link itself stays stuck, which is its fault).
    let link = FakeLink(guardWrites: true) { $0.type == 0x0200 ? [deviceReply] : [] }
    let s = RingSession(link: link, replyTimeout: 0.05)
    link.parkNextSend()
    // Independent watchdog: without the backstop the parked write would keep this test alive past
    // any time limit, so release it and fail instead of hanging.
    let watchdogFired = Flag()
    let watchdog = Task {
        try? await Task.sleep(for: .seconds(3))
        guard !Task.isCancelled else { return }
        watchdogFired.set()
        link.releaseParked()
    }
    await expectTimeout { _ = try await s.request(Command.deviceInfo) }
    watchdog.cancel()
    #expect(!watchdogFired.isSet, "the session did not give up on the write by itself")
    link.releaseParked()
}

// @unchecked Sendable: all state is guarded by `lock`.
private final class Flag: @unchecked Sendable {
    private let lock = NSLock()
    private var value = false
    func set() { lock.withLock { value = true } }
    var isSet: Bool { lock.withLock { value } }
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
    let s = session(link)
    await expectTimeout { _ = try await s.fetchHistory(kind) { _ in } }
    #expect(try await s.request(Command.deviceInfo) == deviceReply)
    let transfer = try await s.fetchHistory(kind) { _ in }
    #expect(transfer.data == [1, 2])
}

@Test(.timeLimit(.minutes(1))) func transferThatKeepsTricklingPastTheOverallDeadlineTimesOut() async throws {
    let link = FakeLink { frame in
        frame.type == 0x0200 ? [deviceReply] : [header(12)]
    }
    // Every wait is satisfied, so only the overall deadline can end this transfer.
    link.setTrickle(dataFrame([1]))
    let clock = SteppingClock()
    let s = RingSession(link: link, replyTimeout: 0.05, conversationTimeout: 5, monotonicNow: { clock.now() })
    await expectTimeout { _ = try await s.fetchHistory(kind) { _ in } }
    link.setTrickle(nil)
    #expect(try await s.request(Command.deviceInfo) == deviceReply)
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
    let s = RingSession(link: link, replyTimeout: 0.05, conversationTimeout: 5, monotonicNow: { clock.now() })
    await expectTimeout(waitingFor: "\(kind.name) history transfer") { _ = try await s.fetchHistory(kind) { _ in } }
    #expect(link.sent == [Command.history(kind)])
    link.setTrickle(nil)
    #expect(try await s.request(Command.deviceInfo) == deviceReply)
}

@Test(.timeLimit(.minutes(1))) func unrelatedFramesAfterTheCommandCannotKeepARequestWaiting() async throws {
    let link = FakeLink()
    link.setTrickleOnNextSend(Frame(group: 0x02, key: 0x0C, payload: [1]))
    let clock = SteppingClock()
    let s = RingSession(link: link, replyTimeout: 0.05, conversationTimeout: 5, monotonicNow: { clock.now() })
    await expectTimeout(waitingFor: "reply to 0200") { _ = try await s.request(Command.deviceInfo) }
    link.setTrickle(nil)
}

@Test(.timeLimit(.minutes(1))) func unrelatedFramesCannotKeepARequestWaitingPastTheDeadline() async throws {
    let link = FakeLink()
    link.setTrickle(Frame(group: 0x02, key: 0x0C, payload: [1]))
    let clock = SteppingClock()
    let s = RingSession(link: link, replyTimeout: 0.05, conversationTimeout: 5, monotonicNow: { clock.now() })
    await expectTimeout { _ = try await s.request(Command.deviceInfo) }
}

// MARK: Continuous reader

/// Yields until `condition` holds. Stops when the test is cancelled (its time limit), so a
/// regression fails instead of spinning forever.
private func waitUntil(_ condition: () async -> Bool) async {
    while await !condition(), !Task.isCancelled { await Task.yield() }
}

private let event = Frame(group: 0x04, key: 0x0E, payload: [0x00, 0x01])
private let otherEvent = Frame(group: 0x04, key: 0x0A, payload: [0x02])
private func acknowledgement(of event: Frame) -> Frame { Frame(group: 0x04, key: event.key, payload: [0x00]) }

@Test(.timeLimit(.minutes(1))) func eventWhileIdleIsAcknowledged() async throws {
    let link = FakeLink()
    let s = session(link)
    await s.start()
    link.deliver([event])
    await link.waitUntilSent(1)
    #expect(link.sent == [acknowledgement(of: event)])
}

@Test(.timeLimit(.minutes(1))) func eventBetweenTwoRequestsIsAcknowledgedOnceAndTheSecondGetsItsReply() async throws {
    let replies = CallCounter()
    let link = FakeLink { frame in
        frame.type == 0x0200 ? [Frame(group: 0x02, key: 0x00, payload: [UInt8(replies.next())])] : []
    }
    let s = session(link)
    #expect(try await s.request(Command.deviceInfo).payload == [1])
    // Not waited for: the event may be read while idle or while the second request drains the
    // link. Either way it is acknowledged before the second request is written.
    link.deliver([event])
    #expect(try await s.request(Command.deviceInfo).payload == [2])
    #expect(link.sent == [Command.deviceInfo, acknowledgement(of: event), Command.deviceInfo])
}

@Test(.timeLimit(.minutes(1))) func eventDuringAHistoryTransferLeavesTheDataAndChecksumIntact() async throws {
    let link = FakeLink { frame in
        guard frame.type == Command.history(kind).type else { return [] }
        return [header(12), dataFrame([1, 2]), event, dataFrame([3]), endFrame(checksum: CRC16.ccittFalse([1, 2, 3]))]
    }
    let persisted = PersistRecorder()
    let transfer = try await session(link).fetchHistory(kind) { await persisted.add($0) }
    #expect(transfer.data == [1, 2, 3])
    #expect(await persisted.calls.map(\.data) == [[1, 2, 3]])
    // The event was read before the end frame, so it is acknowledged before the transfer is.
    #expect(link.sent == [Command.history(kind), acknowledgement(of: event), Command.historyTransferOK])
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
    let s = session(link)
    await expectTimeout { _ = try await s.request(Command.deviceInfo) }
    link.deliver([late])
    #expect(try await s.request(Command.currentActivity) == activity)
    #expect(try await s.fetchHistory(kind) { _ in }.data == [1])
    // Delivered just before a request of the same kind: the drain drops it first.
    link.deliver([late])
    #expect(try await s.request(Command.deviceInfo) == deviceReply)
}

@Test(.timeLimit(.minutes(1))) func subscribersReceiveEventsAndLiveValuesInOrder() async throws {
    let link = FakeLink()
    let s = session(link)
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
    await link.waitUntilSent(2)
    #expect(link.sent == [acknowledgement(of: event), acknowledgement(of: otherEvent)])
}

@Test(.timeLimit(.minutes(1))) func liveValuesDuringARequestGoToSubscribersNotTheConversation() async throws {
    let live = Frame(group: 0x06, key: 0x01, payload: [60])
    let link = FakeLink { $0.type == 0x0200 ? [live, deviceReply] : [] }
    let s = session(link)
    let stream = await s.uploads()
    #expect(try await s.request(Command.deviceInfo) == deviceReply)
    var iterator = stream.makeAsyncIterator()
    #expect(await iterator.next() == .live(live))
}

@Test(.timeLimit(.minutes(1))) func stopEndsTheStreamAndFailsAWaitingRequest() async throws {
    let link = FakeLink()
    // A reply timeout far beyond the time limit: only `stop` can end this request in time.
    let s = RingSession(link: link, replyTimeout: 600)
    let stream = await s.uploads()
    let pending = Task { try await s.request(Command.deviceInfo) }
    await link.waitUntilSent(1)
    await s.stop()
    do {
        _ = try await pending.value
        Issue.record("expected the request to fail")
    } catch RingError.notConnected {
    }
    var iterator = stream.makeAsyncIterator()
    #expect(await iterator.next() == nil)
    // Stays stopped: later requests fail at once, and new streams are already finished.
    do {
        _ = try await s.request(Command.deviceInfo)
        Issue.record("expected the request to fail")
    } catch RingError.notConnected {
    }
    var late = await s.uploads().makeAsyncIterator()
    #expect(await late.next() == nil)
    #expect(link.sent == [Command.deviceInfo])
}

@Test(.timeLimit(.minutes(1))) func eventsThatArriveWhileStoppedAreAcknowledgedAfterARestart() async throws {
    let link = FakeLink { $0.type == 0x0200 ? [deviceReply] : [] }
    let s = session(link)
    await s.start()
    await s.stop()
    link.deliver([event])
    #expect(link.sent.isEmpty)
    await s.start()
    await link.waitUntilSent(1)
    #expect(try await s.request(Command.deviceInfo) == deviceReply)
    #expect(link.sent == [acknowledgement(of: event), Command.deviceInfo])
}

@Test(.timeLimit(.minutes(1))) func acknowledgementWaitsForTheConversationsWriteInFlight() async throws {
    // The link refuses a second write while one is in flight, as the real connection does.
    let link = FakeLink(guardWrites: true) { $0.type == 0x0200 ? [deviceReply] : [] }
    // Long enough that the backstop cannot cut off the parked write during the test.
    let s = RingSession(link: link, replyTimeout: 30)
    link.parkNextSend()
    let pending = Task { try await s.request(Command.deviceInfo) }
    await waitUntil { link.hasParkedSend }
    // The event arrives while the request's own write is still in progress.
    link.deliver([event])
    // Ends early on a refused write, so a session that does not queue the acknowledgement fails
    // the assertions below instead of spinning here.
    await waitUntil { await s.queuedWrites > 0 || link.events.contains { $0.hasPrefix("busy") } }
    link.releaseParked()
    #expect(try await pending.value == deviceReply)
    #expect(link.sent == [Command.deviceInfo, acknowledgement(of: event)])
    #expect(!link.events.contains { $0.hasPrefix("busy") })
}

@Test(.timeLimit(.minutes(1))) func backstopEndsAStuckAcknowledgementAndTheNextRequestWorks() async throws {
    // The write backstop covers acknowledgements too: a stuck one must not hold up the next request.
    let link = FakeLink { $0.type == 0x0200 ? [deviceReply] : [] }
    let s = session(link)
    await s.start()
    link.parkNextSend()
    link.deliver([event])
    await waitUntil { link.hasParkedSend }
    // Independent watchdog, as in `backstopEndsAWriteOnALinkThatNeverReturns`: without the
    // backstop the request would wait on the parked acknowledgement for ever.
    let watchdogFired = Flag()
    let watchdog = Task {
        try? await Task.sleep(for: .seconds(3))
        guard !Task.isCancelled else { return }
        watchdogFired.set()
        link.releaseParked()
    }
    #expect(try await s.request(Command.deviceInfo) == deviceReply)
    watchdog.cancel()
    #expect(!watchdogFired.isSet, "the session did not give up on the acknowledgement by itself")
    #expect(link.sent == [Command.deviceInfo])
    link.releaseParked()
}

// MARK: withDeadline

@Test(.timeLimit(.minutes(1))) func deadlineHelperReturnsTheOperationsResultWhenItWins() async throws {
    // The long deadline must not keep the call waiting: the timer is cancelled when the operation wins.
    let value = try await withDeadline(3600, "never") { 42 }
    #expect(value == 42)
}

@Test(.timeLimit(.minutes(1))) func deadlineHelperGivesUpOnAnOperationThatIgnoresCancellation() async throws {
    let parked = ParkedOperation()
    await expectTimeout {
        try await withDeadline(0.05, "stuck") { await parked.park() }
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

// @unchecked Sendable: all state is guarded by `lock`.
private final class ParkedOperation: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<Void, Never>?
    func park() async { await withCheckedContinuation { c in lock.withLock { continuation = c } } }
    func release() { lock.withLock { continuation.take() }?.resume() }
}

/// Holds a caller until `release()`; a release that comes first lets the caller straight through.
// @unchecked Sendable: all state is guarded by `lock`.
private final class Gate: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<Void, Never>?
    private var released = false
    var isParked: Bool { lock.withLock { continuation != nil } }
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
