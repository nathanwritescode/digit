import Foundation
import GRDB
import RingProtocol
@testable import RingStore
@testable import RingSync
import RingTransport

// Helpers for the sync tests. Every byte here is SYNTHETIC, built from the documented record
// layouts with made-up values; none comes from a real ring.

// MARK: Time

/// 2027-01-15 08:00:00 UTC. In January London is on UTC+0, so ring-local time equals UTC.
let t0 = Date(timeIntervalSince1970: 1_800_000_000)
let london = TimeZone(identifier: "Europe/London")!

func le32(_ v: UInt32) -> [UInt8] { (0..<4).map { UInt8((v >> (8 * UInt32($0))) & 0xFF) } }
func le16(_ v: Int) -> [UInt8] { [UInt8(v & 0xFF), UInt8((v >> 8) & 0xFF)] }

/// Ring seconds (wall clock since 2000-01-01) for `seconds` after `t0`, in London in January.
func ring(_ seconds: Int) -> UInt32 {
    UInt32(RingTime.ringSeconds(fromWallClock: t0.addingTimeInterval(TimeInterval(seconds))))
}

// MARK: Synthetic records

/// One 14-byte activity bucket: start, end, steps, distance, kcal.
func activityBytes(start: UInt32, end: UInt32, steps: Int) -> [UInt8] {
    le32(start) + le32(end) + le16(steps) + le16(steps / 2) + le16(steps / 25)
}

/// One sleep session: 20-byte header (marker, length, start, end, 8 zero totals bytes) then 8-byte
/// stages (kind, start, 3-byte duration).
func sleepBytes(start: UInt32, end: UInt32, stages: [(kind: UInt8, start: UInt32, seconds: Int)]) -> [UInt8] {
    var out: [UInt8] = [0xAF, 0xFA] + le16(20 + 8 * stages.count) + le32(start) + le32(end) + Array(repeating: 0, count: 8)
    for s in stages {
        out += [s.kind] + le32(s.start) + [UInt8(s.seconds & 0xFF), UInt8((s.seconds >> 8) & 0xFF), UInt8((s.seconds >> 16) & 0xFF)]
    }
    return out
}

/// One 6-byte heart-rate record: time, mode, bpm.
func heartBytes(_ time: UInt32, bpm: UInt8) -> [UInt8] { le32(time) + [0, bpm] }

/// One 20-byte combined record with SpO2, respiration, HRV and CVRR set and everything else zero.
func combinedBytes(_ time: UInt32, spo2: UInt8, resp: UInt8, hrv: UInt8, cvrr: UInt8) -> [UInt8] {
    le32(time) + le16(0) + [0, 0, 0, spo2, resp, hrv, cvrr] + Array(repeating: 0, count: 7)
}

/// A plausible synthetic history for every kind, all before `t0`.
let syntheticHistory: [HistoryKind: [UInt8]] = [
    .activity: activityBytes(start: ring(-7200), end: ring(-5400), steps: 120)
        + activityBytes(start: ring(-5400), end: ring(-3600), steps: 80),
    .sleep: sleepBytes(start: ring(-36_000), end: ring(-32_400), stages: [
        (242, ring(-36_000), 1800), (241, ring(-34_199), 1799),
    ]),
    .heartRate: heartBytes(ring(-3600), bpm: 61) + heartBytes(ring(-3000), bpm: 63),
    .combined: combinedBytes(ring(-3600), spo2: 97, resp: 15, hrv: 40, cvrr: 5),
]

// MARK: Store

/// An in-memory store with one ring, and an earlier clock-set at UTC+0 three days before `t0`, as
/// a ring that has been synced before has.
func makeStore() async throws -> (RingStore, UUID) {
    let store = try RingStore()
    let ring = try await store.registerRing(protocolFamily: "ycbt")
    try await store.recordClockSet(ClockSetEvent(
        ringID: ring.id, instant: t0.addingTimeInterval(-3 * 86_400), timeZoneIdentifier: "Europe/London",
        utcOffsetSeconds: 0))
    return (store, ring.id)
}

/// Makes every insert into `table` fail (optionally only rows matching `condition`), with a
/// trigger, so one kind's store write fails while the others succeed.
func failInserts(into table: String, when condition: String? = nil, _ store: RingStore) throws {
    let when = condition.map { "WHEN \($0)" } ?? ""
    try store.writer.write { db in
        try db.execute(sql: """
            CREATE TRIGGER fail_\(table) BEFORE INSERT ON \(table) \(when)
            BEGIN SELECT RAISE(ABORT, 'synthetic test failure'); END
            """)
    }
}

/// Typed row counts for every kind, read without health values.
func typedRowCounts(_ store: RingStore, _ ringID: UUID) async throws -> [Int] {
    [
        try await store.activityIntervals(ringID: ringID).count,
        try await store.sleepSessions(ringID: ringID).count,
        try await store.heartRateReadings(ringID: ringID).count,
        try await store.vitalsReadings(ringID: ringID).count,
    ]
}

// MARK: Monotonic clock

/// A monotonic clock the fake ring moves forward, so budget tests do not wait.
/// `@unchecked Sendable` because all state is guarded by `lock`.
final class FakeMonotonic: @unchecked Sendable {
    private let lock = NSLock()
    private let base = ContinuousClock.now
    private var elapsed = Duration.zero
    func now() -> ContinuousClock.Instant { lock.withLock { base + elapsed } }
    func advance(by duration: Duration) { lock.withLock { elapsed += duration } }
}

// MARK: Fake ring

struct ScriptedFailure: Error {}

/// A scripted ring for the sync. Every call is logged in order, as one sequence:
/// `fetch <kind>`, then for a transfer with data `persisted <kind>` (or `not acknowledged <kind>`
/// when `persist` threw) and `ack <kind>`; then `setTime` and `interval <minutes>`. Nothing waits,
/// so no test can hang on it.
actor FakeRing: SyncableRing {
    enum Fetch {
        case data([UInt8])
        case empty
        /// Throws before anything is received.
        case fail(any Error)
        /// Persists, then the acknowledgement fails.
        case ackFails(any Error)
        /// Breaks the contract: acknowledges and returns the data without calling `persist`.
        case skipsPersist([UInt8])
    }

    enum Reply {
        case ack
        case fail(any Error)
    }

    private(set) var log: [String] = []
    /// What `setTime` was given, exactly.
    private(set) var clockSent: (date: Date, zone: TimeZone)?
    /// The clock-set frame a session would have written for that date and zone.
    private(set) var clockFrame: Frame?
    /// Kinds whose transfer was in the store (by observation) when the fake acknowledged it.
    private(set) var storedWhenAcknowledged: [HistoryKind] = []
    /// The clock-set log as it was when `setTime` was called.
    private(set) var clockSetsWhenSent: [ClockSetEvent] = []
    /// Typed row counts seen when the interval (or, if none, the clock) was sent.
    private(set) var typedRowsDuringRingSteps: [Int] = []

    private let fetches: [HistoryKind: Fetch]
    private let clock: Reply
    private let interval: Reply
    private let fetchedAt: Date
    private let store: RingStore
    private let ringID: UUID
    /// Called with each log entry as it is written, for moving a fake clock.
    private let onStep: @Sendable (String) -> Void

    init(
        store: RingStore, ringID: UUID,
        fetches: [HistoryKind: Fetch] = syntheticHistory.mapValues { .data($0) },
        clock: Reply = .ack, interval: Reply = .ack,
        fetchedAt: Date = t0,
        onStep: @escaping @Sendable (String) -> Void = { _ in }
    ) {
        self.store = store
        self.ringID = ringID
        self.fetches = fetches
        self.clock = clock
        self.interval = interval
        self.fetchedAt = fetchedAt
        self.onStep = onStep
    }

    private func record(_ entry: String) {
        log.append(entry)
        onStep(entry)
    }

    private func transfer(_ kind: HistoryKind, _ data: [UInt8]) -> HistoryTransfer {
        // Synthetic header: 12 bytes when there is data, 9 (the "empty" length) when there is none.
        HistoryTransfer(kind: kind, header: Array(repeating: 1, count: data.isEmpty ? 9 : 12), data: data,
                        fetchedAt: fetchedAt, timeZoneIdentifier: "Europe/London")
    }

    func fetchHistory(
        _ kind: HistoryKind, persist: @Sendable (HistoryTransfer) async throws -> Void
    ) async throws -> HistoryTransfer {
        record("fetch \(kind.name)")
        let data: [UInt8]
        var ackError: (any Error)?
        switch fetches[kind] ?? .empty {
        case .fail(let error): throw error
        case .empty: return transfer(kind, [])
        case .data(let bytes): data = bytes
        case .skipsPersist(let bytes):
            record("ack \(kind.name)")
            return transfer(kind, bytes)
        case .ackFails(let error):
            data = syntheticHistory[kind] ?? [1]
            ackError = error
        }
        let transfer = transfer(kind, data)
        do {
            try await persist(transfer)
        } catch {
            record("not acknowledged \(kind.name)")
            throw error
        }
        record("persisted \(kind.name)")
        let logged = try await store.observations(ringID: ringID, kind: kind).observations
        if logged.contains(where: { $0.dataLength == data.count && $0.fetchedAt == fetchedAt }) {
            storedWhenAcknowledged.append(kind)
        }
        if let ackError { throw ackError }
        record("ack \(kind.name)")
        return transfer
    }

    func setTime(_ date: Date, timeZone: TimeZone) async throws {
        record("setTime")
        clockSent = (date, timeZone)
        // Built on receipt, as `RingSession.setTime` builds the frame it writes.
        clockFrame = Command.setTime(date, timeZone: timeZone)
        clockSetsWhenSent = try await store.clockSets(ringID: ringID)
        typedRowsDuringRingSteps = try await typedRowCounts(store, ringID)
        if case .fail(let error) = clock { throw error }
    }

    func setMonitoringInterval(_ interval: MonitoringInterval) async throws {
        record("interval \(interval.rawValue)")
        typedRowsDuringRingSteps = try await typedRowCounts(store, ringID)
        if case .fail(let error) = self.interval { throw error }
    }
}

// MARK: Suspending store

/// A store that delegates to a real one but throws `RingStoreError.suspended` where told to,
/// standing in for the app announcing a suspension (which is process-wide, so tests here never call
/// `RingStore.appWillSuspend()`). A call that throws changes nothing, as a refused call does not.
/// `@unchecked Sendable`: the call log is guarded by `lock`.
final class SuspendingStore: SyncStore, @unchecked Sendable {
    let base: RingStore
    private let insertFails: Set<HistoryKind>
    private let clockSetFails: Bool
    private let outcomeFails: (any Error & Sendable)?
    private let ingestFails: Set<HistoryKind>
    private let onClockSet: @Sendable () -> Void
    private let lock = NSLock()
    private var calls: [String] = []

    /// Every call that reached the store, in order (`insert <kind>`, `clockSet`,
    /// `clockSetOutcome <code>`, `ingest <kind>`), including the ones that threw.
    var log: [String] { lock.withLock { calls } }

    init(
        _ base: RingStore, insertFails: Set<HistoryKind> = [], clockSetFails: Bool = false,
        outcomeFails: (any Error & Sendable)? = nil,
        ingestFails: Set<HistoryKind> = [], onClockSet: @escaping @Sendable () -> Void = {}
    ) {
        self.onClockSet = onClockSet
        self.outcomeFails = outcomeFails
        self.base = base
        self.insertFails = insertFails
        self.clockSetFails = clockSetFails
        self.ingestFails = ingestFails
    }

    private func note(_ call: String) { lock.withLock { calls.append(call) } }

    func insert(_ transfer: HistoryTransfer, ringID: UUID) async throws -> InsertOutcome {
        note("insert \(transfer.kind.name)")
        if insertFails.contains(transfer.kind) { throw RingStoreError.suspended }
        return try await base.insert(transfer, ringID: ringID)
    }

    func recordClockSet(_ event: ClockSetEvent) async throws -> Int64 {
        note("clockSet")
        onClockSet()
        if clockSetFails { throw RingStoreError.suspended }
        return try await base.recordClockSet(event)
    }

    /// Throws `outcomeFails` when set (writing nothing), standing in for a suspension or any other
    /// failure of the outcome write.
    func recordClockSetOutcome(id: Int64, outcome: LoggedClockSetOutcome) async throws {
        note("clockSetOutcome \(outcome)")
        if let outcomeFails { throw outcomeFails }
        try await base.recordClockSetOutcome(id: id, outcome: outcome)
    }

    func ingest(ringID: UUID, kind: HistoryKind) async throws -> IngestionResult {
        note("ingest \(kind.name)")
        if ingestFails.contains(kind) { throw RingStoreError.suspended }
        return try await base.ingest(ringID: ringID, kind: kind)
    }
}

/// The log of a complete sync, in order.
let completeLog = RingSyncer.fetchOrder.flatMap { ["fetch \($0.name)", "persisted \($0.name)", "ack \($0.name)"] }
    + ["setTime", "interval 10"]
