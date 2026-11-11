import Foundation
import Testing
import RingProtocol
@testable import RingTransport

/// A scripted stand-in for the ring. `onSend` returns the frames the "ring" delivers in answer to
/// each frame the session writes; frames are delivered strictly in order and `nextFrame` returns
/// `nil` immediately when none are queued, so a missing reply costs no wall-clock time.
/// `@unchecked Sendable` because all state is guarded by `lock`.
private final class FakeLink: RingLink, @unchecked Sendable {
    private let lock = NSLock()
    private var inbox: [Frame]
    private var log: [String] = []
    private var sentFrames: [Frame] = []
    private let onSend: @Sendable (Frame) -> [Frame]
    // Gate: when armed, the first nextFrame call suspends until `releaseGate()`, so a test can
    // hold one conversation open and check what a second one does meanwhile.
    private var gateArmed = false
    private var gateReached = false
    private var gateContinuation: CheckedContinuation<Void, Never>?

    init(stale: [Frame] = [], onSend: @escaping @Sendable (Frame) -> [Frame] = { _ in [] }) {
        inbox = stale
        self.onSend = onSend
    }

    var sent: [Frame] { lock.withLock { sentFrames } }
    var events: [String] { lock.withLock { log } }

    func record(_ event: String) { lock.withLock { log.append(event) } }

    func send(_ frame: Frame) async throws {
        let replies = onSend(frame)
        lock.withLock {
            sentFrames.append(frame)
            log.append("send \(String(format: "%04X", frame.type))")
            inbox += replies
        }
    }

    func nextFrame(timeout: TimeInterval) async -> Frame? {
        let hold = lock.withLock { () -> Bool in
            defer { gateArmed = false }
            if gateArmed { gateReached = true }
            return gateArmed
        }
        if hold {
            await withCheckedContinuation { continuation in
                // Runs synchronously, before suspension, so releaseGate cannot miss it.
                lock.withLock { gateContinuation = continuation }
            }
        }
        return lock.withLock { inbox.isEmpty ? nil : inbox.removeFirst() }
    }

    func armGate() { lock.withLock { gateArmed = true } }
    var isHeldAtGate: Bool { lock.withLock { gateReached && gateContinuation != nil } }
    func releaseGate() {
        let continuation = lock.withLock { () -> CheckedContinuation<Void, Never>? in
            defer { gateContinuation = nil }
            return gateContinuation
        }
        continuation?.resume()
    }

    func discardPendingFrames() async {
        lock.withLock {
            inbox.removeAll()
            log.append("discard")
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
    let stale = Frame(group: 0x02, key: 0x00, payload: [0xAA])
    let fresh = Frame(group: 0x02, key: 0x00, payload: [0xBB])
    let link = FakeLink(stale: [stale]) { $0.type == 0x0200 ? [fresh] : [] }
    #expect(try await session(link).request(Command.deviceInfo) == fresh)
    #expect(link.events.first == "discard")
}

// The time limit turns a future regression that never reaches the gate into a failure, not a hung CI job.
@Test(.timeLimit(.minutes(1))) func overlappingConversationsRunOneAtATime() async throws {
    let link = FakeLink { frame in [Frame(group: frame.group, key: frame.key, payload: [frame.key])] }
    link.armGate()
    let s = session(link)
    let first = Task { try await s.request(Frame(group: 0x02, key: 0x00)) }
    // Hold the first conversation mid-flight, inside nextFrame.
    while !link.isHeldAtGate { await Task.yield() }
    let second = Task { try await s.request(Frame(group: 0x02, key: 0x0C)) }
    // Give the second request ample time to (wrongly) start. If conversations could interleave
    // it would discard and send now.
    try await Task.sleep(for: .milliseconds(100))
    #expect(link.events == ["discard", "send 0200"])
    link.releaseGate()
    let (ra, rb) = try await (first.value, second.value)
    #expect(ra.payload == [0x00])
    #expect(rb.payload == [0x0C])
    #expect(link.events == ["discard", "send 0200", "discard", "send 020C"])
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
