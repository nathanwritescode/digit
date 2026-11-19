import Foundation
import Testing
import RingProtocol
import RingStore
import RingSync
import RingTransport

// A complete sync through a real `RingSession`, over a minimal fake link that answers like the
// ring. The frames are SYNTHETIC: the layouts follow the protocol notes, the contents are made up.

/// Answers each command the way the ring does: a history request with a header, one data frame and
/// an end frame carrying the checksum (or a short header when `history` has nothing for the kind),
/// and `00` to the clock and interval commands. Frames are handed to the session's reader in order.
/// Every read ends after its timeout, so nothing here can hang a test.
/// `@unchecked Sendable` because all state is guarded by `lock`.
private final class ScriptedLink: RingLink, @unchecked Sendable {
    private let lock = NSLock()
    private let history: [HistoryKind: [UInt8]]
    private var inbox: [Frame] = []
    private var sentFrames: [Frame] = []
    private var reader: (id: Int, continuation: CheckedContinuation<Frame?, Never>)?
    private var lastReaderID = 0
    let writeTimeout: TimeInterval = 0.5

    /// Command types the "ring" takes but never answers.
    private let silent: Set<UInt16>

    init(history: [HistoryKind: [UInt8]], silent: Set<UInt16> = []) {
        self.history = history
        self.silent = silent
    }

    var sent: [Frame] { lock.withLock { sentFrames } }

    /// Whether a frame of `type` was written within `seconds` (polled; ends on cancellation too).
    func waitUntilSent(_ type: UInt16, within seconds: Double = 10) async -> Bool {
        let deadline = ContinuousClock.now + .seconds(seconds)
        while ContinuousClock.now < deadline, !Task.isCancelled {
            if sent.contains(where: { $0.type == type }) { return true }
            try? await Task.sleep(for: .milliseconds(10))
        }
        return sent.contains { $0.type == type }
    }

    private func replies(to frame: Frame) -> [Frame] {
        if silent.contains(frame.type) { return [] }
        if frame.group == Command.healthGroup, let kind = HistoryKind(rawValue: frame.key) {
            let data = history[kind] ?? []
            guard !data.isEmpty else {
                return [Frame(group: Command.healthGroup, key: kind.rawValue, payload: Array(repeating: 2, count: 9))]
            }
            let crc = CRC16.ccittFalse(data)
            return [
                Frame(group: Command.healthGroup, key: kind.rawValue, payload: Array(repeating: 2, count: 12)),
                // Data frames use another key on the ring (0x13 for sleep); any non-end key works here.
                Frame(group: Command.healthGroup, key: 0x13, payload: data),
                Frame(group: Command.healthGroup, key: Command.historyEndKey,
                      payload: [0, 0, 0, 0, UInt8(crc & 0xFF), UInt8(crc >> 8)]),
            ]
        }
        if frame.type == 0x0100 || frame.type == 0x010C {
            return [Frame(group: frame.group, key: frame.key, payload: [0x00])]
        }
        return []
    }

    func send(_ frame: Frame) async throws {
        let waiting = lock.withLock { () -> (CheckedContinuation<Frame?, Never>, Frame)? in
            sentFrames.append(frame)
            inbox += replies(to: frame)
            guard !inbox.isEmpty, let current = reader else { return nil }
            reader = nil
            return (current.continuation, inbox.removeFirst())
        }
        if let (continuation, next) = waiting { continuation.resume(returning: next) }
    }

    func nextFrame(timeout: TimeInterval) async -> Frame? {
        await withCheckedContinuation { (continuation: CheckedContinuation<Frame?, Never>) in
            enum Read { case now(Frame?), wait(Int) }
            let read = lock.withLock { () -> Read in
                if !inbox.isEmpty { return .now(inbox.removeFirst()) }
                if timeout <= 0 { return .now(nil) }
                lastReaderID += 1
                reader = (lastReaderID, continuation)
                return .wait(lastReaderID)
            }
            switch read {
            case .now(let frame):
                // Handed over from another thread, as the real link can.
                DispatchQueue.global().async { continuation.resume(returning: frame) }
            case .wait(let id):
                DispatchQueue.global().asyncAfter(deadline: .now() + timeout) { self.expire(id) }
            }
        }
    }

    private func expire(_ id: Int) {
        let expired = lock.withLock { () -> CheckedContinuation<Frame?, Never>? in
            guard let current = reader, current.id == id else { return nil }
            reader = nil
            return current.continuation
        }
        expired?.resume(returning: nil)
    }

    func discardPartialFrames() async {}
}

@Test(.timeLimit(.minutes(1))) func completeSyncThroughARealSession() async throws {
    let (store, id) = try await makeStore()
    // Combined is empty, so the session's empty-transfer path is covered too.
    var history = syntheticHistory
    history[.combined] = nil
    let link = ScriptedLink(history: history)
    // The session's own clock and zone differ from the sync's, to show the clock-set uses the
    // date and zone the sync passed and not the session's.
    let session = RingSession(
        link: link, replyTimeout: 0.5, conversationTimeout: 10,
        clock: { t0 }, timeZone: { TimeZone(identifier: "UTC")! })
    await session.start()
    let syncDate = Date(timeIntervalSince1970: 1_814_529_600.25)  // 2027-07-01 12:00:00.25 UTC
    let syncZone = try #require(TimeZone(identifier: "America/New_York"))
    let result = await RingSyncer(
        ring: session, store: store, ringID: id, monitoringInterval: .fiveMinutes, budget: .seconds(60),
        clock: { syncDate }, timeZone: { syncZone }
    ).run()
    await session.stop()

    #expect(link.sent == [
        Command.history(.activity), Command.historyTransferOK,
        Command.history(.sleep), Command.historyTransferOK,
        Command.history(.heartRate), Command.historyTransferOK,
        Command.history(.combined),
        Command.setTime(syncDate, timeZone: syncZone),
        Command.setMonitoringInterval(.fiveMinutes),
    ])
    #expect(Command.setTime(syncDate, timeZone: syncZone) != Command.setTime(t0, timeZone: TimeZone(identifier: "UTC")!))
    #expect(result.isComplete)
    guard case .empty = result.fetches[.combined] else {
        Issue.record("combined not empty")
        return
    }
    let expected = ClockSetEvent(
        ringID: id, instant: HistoryTransfer.truncatedToMilliseconds(syncDate),
        timeZoneIdentifier: "America/New_York", utcOffsetSeconds: -14_400)
    #expect(try await store.clockSets(ringID: id).last == expected)
    #expect(try await store.observations(ringID: id).observations.count == 4)
    #expect(try await typedRowCounts(store, id) == [2, 1, 2, 0])
}

/// The realistic stop: the ring takes the clock-set (the frame is written) and then drops out of
/// range before replying, and the owner of the link calls `stop()`. The ring may have applied it,
/// so it must be logged, as unknown, and the interval skipped.
@Test(.timeLimit(.minutes(1))) func stopAfterTheClockSetWasWrittenIsLoggedAsUnknown() async throws {
    let (store, id) = try await makeStore()
    let link = ScriptedLink(history: syntheticHistory, silent: [0x0100])
    // The wait for a reply ends at the reply timeout (one empty read slice) or the conversation
    // timeout, whichever comes first, so both are far longer than the test needs: only the stop
    // can end the wait. (With a short reply timeout, a stop arriving late would end it as a
    // timeout instead, which is also logged as unknown but is not what this test is about.)
    let session = RingSession(link: link, replyTimeout: 20, conversationTimeout: 40)
    await session.start()
    let syncDate = Date(timeIntervalSince1970: 1_814_529_600.25)
    let syncZone = try #require(TimeZone(identifier: "America/New_York"))
    let sync = Task {
        await RingSyncer(
            ring: session, store: store, ringID: id, monitoringInterval: .fiveMinutes, budget: .seconds(60),
            clock: { syncDate }, timeZone: { syncZone }
        ).run()
    }
    let written = await link.waitUntilSent(0x0100)
    await session.stop()
    let result = await sync.value
    #expect(written)

    let expected = ClockSetEvent(
        ringID: id, instant: HistoryTransfer.truncatedToMilliseconds(syncDate),
        timeZoneIdentifier: "America/New_York", utcOffsetSeconds: -14_400)
    #expect(result.clockSet == .unknownAndLogged(expected, error: .ring(.stopped)))
    #expect(try await store.clockSets(ringID: id).last == expected)
    #expect(result.monitoringInterval == .skipped(.fiveMinutes, .clockNotAcknowledged))
    #expect(!link.sent.contains { $0.type == 0x010C })
    #expect(!result.isComplete)
}
