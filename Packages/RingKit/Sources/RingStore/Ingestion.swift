import Foundation
import GRDB
import os
import RingProtocol

// Typed ingestion. Raw transfers stay the source of truth; typed rows are always the full result of
// deriving from every surviving transfer, so ingesting twice changes nothing and `rebuild` is just
// "delete, then ingest". The derivation runs outside any transaction, between a short read of its
// inputs and a short write that checks them and applies the result (see `ingest(ringID:kind:)`).

/// Ring-local readings before this mean the ring's clock was never set (it counts from 2000).
private let earliestPlausibleRingTime = Date(timeIntervalSince1970: 1_577_836_800)  // 2020-01-01

private func ms(_ date: Date) -> Int64 { HistoryTransfer.millisecondsSince1970(date) }
private func date(_ ms: Int64) -> Date { HistoryTransfer.date(millisecondsSince1970: ms) }

extension RingStore {
    static let ingestibleKinds: [HistoryKind] = [.activity, .sleep, .heartRate, .combined]

    // MARK: Public API

    /// Blocking form of `ingest(ringID:kind:)` (internal, for tests).
    @discardableResult
    func ingest(ringID: UUID, kind: HistoryKind) throws -> IngestionResult {
        try ingest(ringID: ringID, kind: kind, hooks: IngestionHooks())
    }

    /// Blocking form of `ingest(ringID:kind:hooks:)` (internal, for tests).
    func ingest(ringID: UUID, kind: HistoryKind, hooks: IngestionHooks) throws -> IngestionResult {
        try ingestBlocking(ringID: ringID, kind: kind, hooks: hooks, cancellation: nil)
    }

    private func ingestBlocking(
        ringID: UUID, kind: HistoryKind, hooks: IngestionHooks, cancellation: DerivationCancellation?
    ) throws -> IngestionResult {
        guard Self.ingestibleKinds.contains(kind) else { throw RingStoreError.notIngestible(kind) }
        let results = try deriveAndApply(
            ringID: ringID, kinds: [kind], replacing: false, hooks: hooks, cancellation: cancellation)
        guard let result = results.first(where: { $0.kind == kind })?.result else {
            throw IngestionInvariantError.missingResult
        }
        return result
    }

    /// Makes the typed table for `kind` match what the stored transfers say. Combined records yield
    /// `VitalsReading`s only.
    ///
    /// Time resolution is described on `TimeResolver`. Unusable input never stops the run: bytes
    /// that do not form a whole record are counted in `unusableBytes` (activity and heart-rate
    /// decoders ignore a trailing partial record; the sleep decoder stops at the first thing that
    /// is not a session record, or at a session cut short, so what follows it is counted and not
    /// ingested), and a record the sanity checks refuse is counted and skipped, in
    /// `IngestionResult.rejectedImplausible` or `rejectedFuture`.
    ///
    /// **Derived outside the write.** The inputs (the ring's transfers and fetch log for `kind`,
    /// and its clock-sets) are read in one short read; decoding, hashing and time resolution then
    /// run outside any transaction; one short write transaction checks that the inputs are
    /// unchanged and applies the difference (insert, update, delete, leave unchanged rows alone, so
    /// their ids stay stable). So the write lock is held only for the apply step, and
    /// `appWillSuspend()` does not wait for the derivation. If another write changed the inputs in
    /// between (a new transfer or fetch-log entry, a clock-set), the derivation is stale: it is
    /// done again from a fresh read, at most `maximumDerivationAttempts` times in all, and then
    /// the call throws `RingStoreError.ingestionInputsKeptChanging` with the typed table as it
    /// was. A competing `ingest` of the same kind does not make it stale: the apply step diffs
    /// against the rows as they are inside its own transaction, so the counts describe what this
    /// call changed. How staleness is detected is on `InputFingerprint`.
    ///
    /// Throws `unknownRing(ringID)` if the ring is not stored. A `CancellationError`, or
    /// `RingStoreError.suspended` after `appWillSuspend()`, ends the call and leaves the typed
    /// table as it was: during the derivation it is checked between units of work (each transfer,
    /// and every few hundred records), so a long derivation stops promptly and never reaches the
    /// write; during the apply step it is checked every few hundred rows and the write is rolled
    /// back as a whole. (`notIngestible` is reserved for a future kind and cannot be thrown today.)
    ///
    /// **Executor.** The whole call (read, derivation, write) runs on `ingestionQueue`, a dispatch
    /// queue, at a QoS taken from the calling task's priority, never on a Swift concurrency thread
    /// or the caller's actor: derivation is CPU-bound and would otherwise hold one of the few
    /// cooperative threads for its whole length. The task's cancellation is passed to it through
    /// `DerivationCancellation`.
    @discardableResult
    public func ingest(ringID: UUID, kind: HistoryKind) async throws -> IngestionResult {
        try await ingest(ringID: ringID, kind: kind, hooks: IngestionHooks())
    }

    /// `ingest(ringID:kind:)` with test hooks.
    func ingest(ringID: UUID, kind: HistoryKind, hooks: IngestionHooks) async throws -> IngestionResult {
        try await offCooperativePool { cancellation in
            try self.ingestBlocking(ringID: ringID, kind: kind, hooks: hooks, cancellation: cancellation)
        }
    }

    /// Blocking form of `rebuild(ringID:)` (internal, for tests).
    @discardableResult
    func rebuild(ringID: UUID) throws -> [HistoryKind: IngestionResult] {
        try rebuild(ringID: ringID, hooks: IngestionHooks())
    }

    /// Blocking form of `rebuild(ringID:hooks:)` (internal, for tests).
    func rebuild(ringID: UUID, hooks: IngestionHooks) throws -> [HistoryKind: IngestionResult] {
        try rebuildBlocking(ringID: ringID, hooks: hooks, cancellation: nil)
    }

    private func rebuildBlocking(
        ringID: UUID, hooks: IngestionHooks, cancellation: DerivationCancellation?
    ) throws -> [HistoryKind: IngestionResult] {
        let results = try deriveAndApply(
            ringID: ringID, kinds: Self.ingestibleKinds, replacing: true, hooks: hooks, cancellation: cancellation)
        return Dictionary(results.map { ($0.kind, $0.result) }, uniquingKeysWith: { _, later in later })
    }

    /// Deletes the ring's typed rows for every ingestible kind (activity, sleep, heart rate and
    /// vitals) and derives them again from the stored transfers. The result is what `ingest` would
    /// converge on, keyed by kind. Throws `unknownRing(ringID)` if the ring is not stored.
    ///
    /// Shaped like `ingest`: every kind is derived outside any transaction, then one short write
    /// checks that no kind's inputs changed, and deletes and re-inserts every kind's rows, so the
    /// typed tables are never seen empty or half-built. A stale kind is derived again (the others
    /// keep their derivation and are checked again), with the same bound and error as `ingest`. A
    /// `CancellationError`, `suspended`, or any other failure leaves every typed table as it was.
    @discardableResult
    public func rebuild(ringID: UUID) async throws -> [HistoryKind: IngestionResult] {
        try await rebuild(ringID: ringID, hooks: IngestionHooks())
    }

    /// `rebuild(ringID:)` with test hooks.
    func rebuild(ringID: UUID, hooks: IngestionHooks) async throws -> [HistoryKind: IngestionResult] {
        try await offCooperativePool { cancellation in
            try self.rebuildBlocking(ringID: ringID, hooks: hooks, cancellation: cancellation)
        }
    }

    // MARK: Derive, then apply

    /// How many times `ingest` and `rebuild` derive before giving up on inputs that keep changing.
    /// A sync writes, then ingests, one step at a time, so a stale derivation needs a write from
    /// somewhere else (another task, or another process on the shared file) landing in the gap;
    /// three attempts ride out a burst of those without letting a caller spin.
    static let maximumDerivationAttempts = 3

    /// Points in `ingest` and `rebuild` that tests act at. Production uses the defaults.
    struct IngestionHooks: Sendable {
        /// At every derivation checkpoint, before the cancellation and suspension checks.
        var checkpoint: @Sendable (HistoryKind) throws -> Void = { _ in }
        /// After derivation and before the write, outside any database access, with the attempt
        /// number (from 1).
        var beforeApply: @Sendable (Int) throws -> Void = { _ in }
        /// Inside the write, after the inputs were found unchanged and before anything is applied.
        var duringApply: @Sendable () throws -> Void = {}
    }

    private enum ApplyOutcome: Sendable {
        case applied([(kind: HistoryKind, result: IngestionResult)])
        /// These kinds' inputs changed since they were read.
        case stale([HistoryKind])
    }

    /// A broken internal invariant of ingestion (a kind asked for but not derived). Never expected.
    enum IngestionInvariantError: Error {
        case missingResult
    }

    /// A task's cancellation, for work running on a dispatch queue (where `Task.isCancelled` sees no
    /// task). Set by the async forms' cancellation handler, read at every checkpoint.
    final class DerivationCancellation: Sendable {
        private let cancelled = OSAllocatedUnfairLock(initialState: false)
        var isCancelled: Bool { cancelled.withLock { $0 } }
        func cancel() { cancelled.withLock { $0 = true } }
    }

    /// Where the async forms of `ingest` and `rebuild` run (concurrent; each call is one work item
    /// at its caller's QoS).
    private static let ingestionQueue = DispatchQueue(label: "RingStore.ingestion", attributes: .concurrent)

    /// Runs `work` on `ingestionQueue` at the calling task's priority (as `open(at:)` does) and
    /// awaits it. Cancelling the task sets the flag `work` is given; it does not interrupt it
    /// otherwise.
    private func offCooperativePool<T: Sendable>(
        _ work: @escaping @Sendable (DerivationCancellation) throws -> T
    ) async throws -> T {
        let cancellation = DerivationCancellation()
        let qos = Self.dispatchQoS(for: Task.currentPriority)
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                Self.ingestionQueue.async(qos: DispatchQoS(qosClass: qos, relativePriority: 0), flags: .enforceQoS) {
                    continuation.resume(with: Result { try work(cancellation) })
                }
            }
        } onCancel: {
            cancellation.cancel()
        }
    }

    /// The one implementation of `ingest` and `rebuild`: blocking, so the async forms run it on a
    /// dispatch queue. Results come back in `kinds` order.
    private func deriveAndApply(
        ringID: UUID, kinds: [HistoryKind], replacing: Bool, hooks: IngestionHooks,
        cancellation: DerivationCancellation?
    ) throws -> [(kind: HistoryKind, result: IngestionResult)] {
        let checkCancelled = { () throws -> Void in
            if cancellation?.isCancelled == true || Task.isCancelled { throw CancellationError() }
        }
        // In `kinds` order; a stale kind's plan is replaced in place.
        var plans: [IngestionPlan] = []
        var pending = kinds
        for attempt in 1...Self.maximumDerivationAttempts {
            try checkCancelled()
            let inputs = try readBlocking { db in try Self.loadInputs(db, ringID: ringID, kinds: pending) }
            for input in inputs {
                let plan = try Self.plan(input, ringID: ringID) {
                    try self.derivationCheckpoint(input.kind, hooks, checkCancelled)
                }
                if let index = plans.firstIndex(where: { $0.kind == plan.kind }) {
                    plans[index] = plan
                } else {
                    plans.append(plan)
                }
            }
            try hooks.beforeApply(attempt)
            try checkCancelled()
            let outcome = try writeBlocking { db in
                try Self.applyIfFresh(
                    db, ringID: ringID, plans: plans, replacing: replacing, hooks: hooks, checkCancelled: checkCancelled)
            }
            switch outcome {
            case .applied(let results): return results
            case .stale(let stale): pending = stale
            }
        }
        throw RingStoreError.ingestionInputsKeptChanging
    }

    /// Between units of derivation: no database access is open here, so this is the only place a
    /// cancellation or a suspension can stop it. A suspension announced and lifted between two
    /// checkpoints goes unnoticed, which is harmless: the write that follows runs after the resume.
    private func derivationCheckpoint(
        _ kind: HistoryKind, _ hooks: IngestionHooks, _ checkCancelled: () throws -> Void
    ) throws {
        try hooks.checkpoint(kind)
        try checkCancelled()
        try SuspensionState.checkNotSuspended(observes: observesSuspension)
    }

    /// Identifies what derivation read for one ring and kind: the ids of its transfers, of its
    /// fetch-log entries, and of the ring's clock-sets, each in id order. Read in the same snapshot
    /// as the inputs, and again inside the apply step's write; any difference means the derivation
    /// is stale.
    ///
    /// Ids are enough, with no false negatives through the store's API, because those three tables
    /// are append-only except for pruning, and no statement updates a row of them: a transfer row
    /// is only ever inserted or deleted (pruned), a fetch-log entry or clock-set only inserted.
    /// `history_transfer` and `clock_set` use AUTOINCREMENT, so an id is never reused, and
    /// `transfer_observation` could reuse one only after its highest row was deleted, which nothing
    /// does. Every write that can change a derivation (an `insert` that stores, prunes or only logs
    /// a fetch, a `recordClockSet`) therefore changes the fingerprint; an `insert` of a fetch
    /// already logged at that instant changes nothing and is rightly not stale. Reading ids only
    /// touches the indexes, never a transfer's data, so the check is cheap and the apply step stays
    /// short. A fingerprint of a hash instead of the lists would be smaller but would need hashing
    /// inside the write.
    ///
    /// **Rule:** every table `loadInputs` reads must contribute to this fingerprint, and the id-list
    /// form holds only while those tables stay append-only as described. A change that updates or
    /// deletes their rows (the observation migration in #32, an erase feature, recording clock-set
    /// outcomes in #72) must extend the fingerprint.
    struct InputFingerprint: Equatable, Sendable {
        var transferIDs: [Int64]
        var observationIDs: [Int64]
        var clockSetIDs: [Int64]
    }

    /// Internal only for tests.
    static func fingerprint(_ db: Database, ringID: UUID, kind: HistoryKind) throws -> InputFingerprint {
        let ring = ringID.uuidString
        let code = KindCode.code(for: kind)
        return InputFingerprint(
            transferIDs: try Int64.fetchAll(
                db, sql: "SELECT id FROM history_transfer WHERE ring_id = ? AND kind = ? ORDER BY id",
                arguments: [ring, code]),
            observationIDs: try Int64.fetchAll(
                db, sql: "SELECT id FROM transfer_observation WHERE ring_id = ? AND kind = ? ORDER BY id",
                arguments: [ring, code]),
            clockSetIDs: try Int64.fetchAll(
                db, sql: "SELECT id FROM clock_set WHERE ring_id = ? ORDER BY id", arguments: [ring]))
    }

    /// Everything derivation reads for one kind, from one snapshot, with its fingerprint.
    struct IngestionInputs: Sendable {
        var kind: HistoryKind
        var transfers: [StoredTransfer]
        var observations: [TransferObservation]
        var clockSets: [ClockSetEvent]
        var fingerprint: InputFingerprint
    }

    /// Reads every kind's inputs in the caller's transaction (one snapshot), after checking that
    /// the ring is stored, in `kinds` order. Only reads; no decoding or hashing. Internal only for
    /// the tests' one-transaction reference.
    static func loadInputs(_ db: Database, ringID: UUID, kinds: [HistoryKind]) throws -> [IngestionInputs] {
        try Self.requireRing(db, ringID)
        let clockSets = try Self.clockSets(db, ringID: ringID)
        return try kinds.map { kind in
            IngestionInputs(
                kind: kind,
                transfers: try Self.transfers(db, ringID: ringID, kind: kind, fetchedWithin: nil).transfers,
                observations: try Self.observations(db, ringID: ringID, kind: kind).observations,
                clockSets: clockSets,
                fingerprint: try Self.fingerprint(db, ringID: ringID, kind: kind))
        }
    }

    /// One kind's desired rows, with the counts derivation produced and the fingerprint of the
    /// inputs it came from. The apply step adds `inserted`, `updated`, `deleted` and `unchanged`.
    struct IngestionPlan: Sendable {
        enum Rows: Sendable {
            case heartRate([Int64: HeartRateReading])
            case activity([Int64: ActivityInterval])
            case sleep([Int64: SleepSessionRecord])
            case vitals([Int64: VitalsReading])
        }

        var kind: HistoryKind
        var rows: Rows
        var result: IngestionResult
        var fingerprint: InputFingerprint
    }

    /// Inside the write: if every kind's inputs are as they were read, applies every plan in order
    /// (after deleting each kind's rows first when `replacing`, for `rebuild`); otherwise applies
    /// nothing and names the stale kinds. Runs no decoding, hashing or time resolution.
    private static func applyIfFresh(
        _ db: Database, ringID: UUID, plans: [IngestionPlan], replacing: Bool, hooks: IngestionHooks,
        checkCancelled: () throws -> Void
    ) throws -> ApplyOutcome {
        let stale = try plans.filter { try fingerprint(db, ringID: ringID, kind: $0.kind) != $0.fingerprint }.map(\.kind)
        if !stale.isEmpty { return .stale(stale) }
        try hooks.duringApply()
        try checkCancelled()
        return .applied(try plans.map { plan in
            if replacing { try deleteTypedRows(db, ringID: ringID, kind: plan.kind) }
            return (plan.kind, try apply(db, ringID: ringID, plan, checkpoint: checkCancelled))
        })
    }

    /// Makes one kind's typed table match the plan. Unchanged rows are left alone. `checkpoint`
    /// runs every `checkpointInterval` rows. Internal only for the tests' one-transaction
    /// reference: nothing else may call it without the freshness check in `applyIfFresh`.
    static func apply(
        _ db: Database, ringID: UUID, _ plan: IngestionPlan, checkpoint: () throws -> Void = {}
    ) throws -> IngestionResult {
        var result = plan.result
        switch plan.rows {
        case .heartRate(let rows): try applyHeartRate(db, ringID, rows, &result, checkpoint)
        case .activity(let rows): try applyActivity(db, ringID, rows, &result, checkpoint)
        case .sleep(let rows): try applySleep(db, ringID, rows, &result, checkpoint)
        case .vitals(let rows): try applyVitals(db, ringID, rows, &result, checkpoint)
        }
        return result
    }

    /// Blocking form of `heartRateReadings(ringID:)` (internal, for tests).
    func heartRateReadings(ringID: UUID) throws -> [StoredRow<HeartRateReading>] {
        try readBlocking { db in try Self.heartRateRows(db, ringID: ringID) }
    }

    /// All heart-rate readings for a ring, oldest first; empty if none or the ring is not stored.
    /// Can throw `CancellationError`.
    public func heartRateReadings(ringID: UUID) async throws -> [StoredRow<HeartRateReading>] {
        try await read { db in try Self.heartRateRows(db, ringID: ringID) }
    }

    /// Blocking form of `vitalsReadings(ringID:)` (internal, for tests).
    func vitalsReadings(ringID: UUID) throws -> [StoredRow<VitalsReading>] {
        try readBlocking { db in try Self.vitalsRows(db, ringID: ringID) }
    }

    /// All vitals readings (SpO2, respiration, HRV, CVRR) for a ring, oldest first; empty if none or
    /// the ring is not stored. An absent value is `nil`, never zero. Can throw `CancellationError`.
    public func vitalsReadings(ringID: UUID) async throws -> [StoredRow<VitalsReading>] {
        try await read { db in try Self.vitalsRows(db, ringID: ringID) }
    }

    /// Blocking form of `activityIntervals(ringID:)` (internal, for tests).
    func activityIntervals(ringID: UUID) throws -> [StoredRow<ActivityInterval>] {
        try readBlocking { db in try Self.activityRows(db, ringID: ringID) }
    }

    /// All activity intervals for a ring, oldest first; empty if none or the ring is not stored.
    /// Can throw `CancellationError`.
    public func activityIntervals(ringID: UUID) async throws -> [StoredRow<ActivityInterval>] {
        try await read { db in try Self.activityRows(db, ringID: ringID) }
    }

    /// Blocking form of `sleepSessions(ringID:)` (internal, for tests).
    func sleepSessions(ringID: UUID) throws -> [StoredRow<SleepSessionRecord>] {
        try readBlocking { db in try Self.sleepRows(db, ringID: ringID) }
    }

    /// All sleep sessions for a ring, oldest first, with their stages in order; empty if none or the
    /// ring is not stored. Can throw `CancellationError`.
    public func sleepSessions(ringID: UUID) async throws -> [StoredRow<SleepSessionRecord>] {
        try await read { db in try Self.sleepRows(db, ringID: ringID) }
    }

    // MARK: Derivation

    private struct Derived<Value> {
        var rows: [Int64: Value] = [:]
        var rejected = 0
        var unusableBytes = 0
        var collisions = 0
        var superseded = 0
        var rejectedFuture = 0
        var withoutRow = 0
    }

    /// Derives one kind's desired rows from its inputs: all decoding, hashing and time resolution,
    /// and no database access. `checkpoint` is called between units of work and stops the
    /// derivation by throwing.
    ///
    /// Internal only for the tests' one-transaction reference.
    static func plan(
        _ inputs: IngestionInputs, ringID: UUID, checkpoint: () throws -> Void
    ) throws -> IngestionPlan {
        let kind = inputs.kind
        try checkpoint()
        let transfers = inputs.transfers
        let resolver = TimeResolver(
            ringID: ringID, kind: kind, observations: inputs.observations, clockSets: inputs.clockSets)
        var result = IngestionResult()
        let rows: IngestionPlan.Rows
        switch kind {
        case .heartRate:
            let derived = try derive(
                transfers, resolver, checkpoint, HeartRateRecord.decodeLocated, reference: { $0.time }, times: { [$0.time] },
                // 0 and 255 bpm are not readings (no signal, or a filler byte).
                plausible: { $0.bpm != 0 && $0.bpm != 255 }, startTime: nil, supersedeKey: nil
            ) { r, o in
                let instant = r.time.addingTimeInterval(-TimeInterval(o.seconds))
                return (instant, HeartRateReading(
                    time: instant, bpm: r.bpm, mode: Int(r.mode), utcOffsetSeconds: o.seconds,
                    ringLocalSeconds: RingTime.ringSeconds(fromWallClock: r.time), timeSource: o.source))
            }
            rows = .heartRate(derived.rows)
            record(derived, rows: derived.rows.values.map(\.timeSource), into: &result)
        case .activity:
            let derived = try derive(
                transfers, resolver, checkpoint, ActivityRecord.decodeLocated, reference: { $0.end }, times: { [$0.start, $0.end] },
                // The row's instant and unique key are the start, so it is checked like the end.
                // A zero-length bucket is accepted; one longer than a day is not.
                plausible: {
                    $0.start >= earliestPlausibleRingTime && $0.start <= $0.end && $0.end.timeIntervalSince($0.start) <= 86_400
                },
                startTime: nil, supersedeKey: nil
            ) { r, o in
                let shift = -TimeInterval(o.seconds)
                let start = r.start.addingTimeInterval(shift)
                return (start, ActivityInterval(
                    start: start, end: r.end.addingTimeInterval(shift), steps: r.steps,
                    distanceMeters: r.distanceMeters, kilocalories: r.kilocalories,
                    utcOffsetSeconds: o.seconds,
                    ringLocalSeconds: RingTime.ringSeconds(fromWallClock: r.start), timeSource: o.source))
            }
            rows = .activity(derived.rows)
            record(derived, rows: derived.rows.values.map(\.timeSource), into: &result)
        case .sleep:
            // Keyed exactly as `derive` keys `rows` (the row's instant in ms). Correct only because
            // `build` runs once per kept row, after rejection and supersession, in the order rows are
            // assigned, so on a collision the copy that wins the row also wins the count.
            var skippedByRow: [Int64: Int] = [:]
            let derived = try derive(
                transfers, resolver, checkpoint, SleepSession.decodeLocated, reference: { $0.end },
                times: { r in [r.start, r.end] + r.stages.flatMap { [$0.start, $0.end] } },
                // A session that starts after it ends, is longer than 48 hours, or has a stage that
                // starts outside it or ends more than a minute after it, is garbage even though its end
                // may look plausible (in real nights every last stage ended at the session end or one
                // second before it). A zero-length session is accepted.
                plausible: { r in
                    r.start >= earliestPlausibleRingTime && r.start <= r.end
                        && r.end.timeIntervalSince(r.start) <= 48 * 3600
                        && r.stages.allSatisfy {
                            $0.start >= r.start && $0.start <= r.end
                                && $0.end <= r.end.addingTimeInterval(60)
                        }
                },
                startTime: { $0.start },
                // Sessions with one ring-local start are one session the ring rewrote.
                supersedeKey: { RingTime.ringSeconds(fromWallClock: $0.start) }
            ) { r, o in
                let shift = -TimeInterval(o.seconds)
                let start = r.start.addingTimeInterval(shift)
                skippedByRow[ms(start)] = r.skippedStages
                return (start, SleepSessionRecord(
                    start: start, end: r.end.addingTimeInterval(shift),
                    utcOffsetSeconds: o.seconds,
                    ringLocalSeconds: RingTime.ringSeconds(fromWallClock: r.start), timeSource: o.source,
                    stages: r.stages.map {
                        SleepStageEntry(
                            kind: $0.kind, start: $0.start.addingTimeInterval(shift), durationSeconds: Int($0.duration))
                    }))
            }
            rows = .sleep(derived.rows)
            result.skippedSleepStages = derived.rows.keys.reduce(0) { $0 + (skippedByRow[$1] ?? 0) }
            record(derived, rows: derived.rows.values.map(\.timeSource), into: &result)
        case .combined:
            // Time is resolved like heart rate (the ring stamps it at the reading, not like a sleep
            // session). Records with nothing to store are set aside before collisions are counted.
            let derived = try derive(
                transfers, resolver, checkpoint, CombinedRecord.decodeLocated, reference: { $0.time }, times: { [$0.time] },
                // No per-value plausibility rejection: a record carries four values, so a filler in
                // one is already nil in the decoder and must not discard the others.
                plausible: { _ in true }, startTime: nil, supersedeKey: nil,
                yieldsNoRow: { $0.spo2 == nil && $0.respirationRate == nil && $0.hrv == nil && $0.cvrr == nil }
            ) { r, o in
                let instant = r.time.addingTimeInterval(-TimeInterval(o.seconds))
                return (instant, VitalsReading(
                    time: instant, spo2: r.spo2, respirationRate: r.respirationRate, hrv: r.hrv, cvrr: r.cvrr,
                    utcOffsetSeconds: o.seconds,
                    ringLocalSeconds: RingTime.ringSeconds(fromWallClock: r.time), timeSource: o.source))
            }
            rows = .vitals(derived.rows)
            result.withoutVitals = derived.withoutRow
            record(derived, rows: derived.rows.values.map(\.timeSource), into: &result)
        }
        try checkpoint()
        return IngestionPlan(kind: kind, rows: rows, result: result, fingerprint: inputs.fingerprint)
    }

    private static func record<V>(_ derived: Derived<V>, rows sources: [TimeSource], into result: inout IngestionResult) {
        result.rejectedImplausible = derived.rejected
        result.unusableBytes = derived.unusableBytes
        result.collisions = derived.collisions
        result.superseded = derived.superseded
        result.rejectedFuture = derived.rejectedFuture
        for source in sources { result.rowsByTimeSource[source, default: 0] += 1 }
    }


    /// Records (decoded, resolved or built) between two derivation checkpoints, and rows between
    /// two cancellation checks in the apply step.
    static let checkpointInterval = 256

    /// One place a record's bytes were found.
    private struct Occurrence {
        var transfer: Int
        var offset: Int
        var window: TimeResolver.Window
    }

    /// A record's identity: its bytes and how many identical records came before it in the same
    /// transfer. After the clock goes back the ring repeats wall-clock times, so two different
    /// records can be byte for byte the same; occurrence `n` of some bytes in one transfer and
    /// occurrence `n` in another are taken to be the same record. Known limit (#32): if a rolled-over
    /// buffer dropped an earlier identical record, the numbering shifts and a record can be matched
    /// to the wrong twin.
    private struct IdentityKey: Hashable {
        var bytes: [UInt8]
        var occurrence: Int
    }

    private struct Identity<R> {
        var record: R
        var occurrences: [Occurrence]

        /// Buffer order: later transfer, then later offset.
        var lastPosition: (Int, Int) {
            occurrences.map { ($0.transfer, $0.offset) }.max { $0 < $1 } ?? (0, 0)
        }
    }

    /// Decodes every transfer as wall-clock-as-UTC (what `TimeResolver` expects), then resolves
    /// each distinct record once, from the occurrence with the earliest first sighting.
    ///
    /// That choice needs no tie-break: two transfers can share a first sighting only through an
    /// observation that is a prefix of both, and then the record sits at the same offset in both
    /// with the same earlier fetches, so the evidence is identical (equal first sightings at
    /// different offsets would need two distinct observations at one instant; the first occurrence in
    /// transfer order is then used, which is deterministic).
    ///
    /// Afterwards: sessions the ring rewrote are superseded (see `supersedeKey`), records dated more
    /// than a day after they were first seen are rejected as future, and distinct records that still
    /// share a key are a collision, the later in the buffer winning.
    ///
    /// `checkpoint` runs before each transfer and every `checkpointInterval` records, so a
    /// cancellation or suspension stops a long derivation promptly.
    private static func derive<R: Sendable, V>(
        _ transfers: [StoredTransfer], _ resolver: TimeResolver, _ checkpoint: () throws -> Void,
        _ decode: ([UInt8], TimeZone) -> [Located<R>],
        reference: (R) -> Date,
        times: (R) -> [Date],
        plausible: (R) -> Bool,
        startTime: ((R) -> Date)?,
        supersedeKey: ((R) -> Int)?,
        yieldsNoRow: ((R) -> Bool)? = nil,
        _ build: (R, ResolvedOffset) -> (instant: Date, value: V)
    ) throws -> Derived<V> {
        var derived = Derived<V>()
        var identities: [IdentityKey: Identity<R>] = [:]
        var rejected: Set<IdentityKey> = []
        var work = 0
        func tick() throws {
            work += 1
            if work % checkpointInterval == 0 { try checkpoint() }
        }
        for (index, stored) in transfers.enumerated() {
            try checkpoint()
            let data = stored.transfer.data
            let sightings = resolver.sightings(in: data)
            var seen: [[UInt8]: Int] = [:]
            var consumed = 0
            for located in decode(data, .gmt) where located.offset + located.length <= data.count {
                try tick()
                consumed += located.length
                let bytes = Array(data[located.offset..<located.offset + located.length])
                let key = IdentityKey(bytes: bytes, occurrence: seen[bytes, default: 0])
                seen[bytes, default: 0] += 1
                if reference(located.record) < earliestPlausibleRingTime || !plausible(located.record) {
                    rejected.insert(key)
                    continue
                }
                let window = try resolver.window(sightings: sightings, offset: located.offset, length: located.length)
                identities[key, default: Identity(record: located.record, occurrences: [])]
                    .occurrences.append(Occurrence(transfer: index, offset: located.offset, window: window))
            }
            derived.unusableBytes += data.count - consumed
        }
        derived.rejected = rejected.count

        // Resolve every copy and drop the future ones first, so a garbage rewrite of a sleep
        // session cannot supersede a good copy.
        var survivors: [(identity: Identity<R>, offset: ResolvedOffset)] = []
        for identity in identities.values.sorted(by: { $0.lastPosition < $1.lastPosition }) {
            try tick()
            guard let chosen = identity.occurrences.min(by: { $0.window.firstSeen < $1.window.firstSeen }) else { continue }
            let offset = resolver.resolve(
                window: chosen.window, reference: reference(identity.record), start: startTime?(identity.record))
            // A record cannot have been written after it was fetched (this also catches FF FF FF FF).
            // Every time in it counts, not only the one the offset was resolved from.
            let future = times(identity.record).contains {
                $0.addingTimeInterval(-TimeInterval(offset.seconds)).timeIntervalSince(chosen.window.firstSeen) > 86_400
            }
            if future {
                derived.rejectedFuture += 1
            } else {
                survivors.append((identity, offset))
            }
        }
        if let supersedeKey {
            // Keep the copy latest in the buffer; the others were rewritten by it.
            var latestByKey: [Int: Int] = [:]
            for (i, survivor) in survivors.enumerated() { latestByKey[supersedeKey(survivor.identity.record)] = i }
            let kept = Set(latestByKey.values)
            derived.superseded = survivors.count - kept.count
            survivors = survivors.enumerated().filter { kept.contains($0.offset) }.map(\.element)
        }
        for (identity, offset) in survivors {
            try tick()
            // Counted per distinct record, before `build`, so one with nothing to store can never
            // win a key over a real reading.
            if let yieldsNoRow, yieldsNoRow(identity.record) {
                derived.withoutRow += 1
                continue
            }
            let (instant, value) = build(identity.record, offset)
            let key = ms(instant)
            if derived.rows[key] != nil { derived.collisions += 1 }
            derived.rows[key] = value
        }
        return derived
    }

    // MARK: Applying the desired set

    private static func applyHeartRate(
        _ db: Database, _ ringID: UUID, _ desired: [Int64: HeartRateReading], _ result: inout IngestionResult,
        _ checkpoint: () throws -> Void
    ) throws {
        var existing = Dictionary(
            try heartRateRows(db, ringID: ringID).map { (ms($0.value.time), $0) }, uniquingKeysWith: { a, _ in a })
        for (index, (key, value)) in desired.sorted(by: { $0.key < $1.key }).enumerated() {
            if index % checkpointInterval == checkpointInterval - 1 { try checkpoint() }
            let args: StatementArguments = [
                value.bpm, value.mode, value.utcOffsetSeconds, value.ringLocalSeconds, TimeSourceCode.code(for: value.timeSource),
            ]
            if let row = existing.removeValue(forKey: key) {
                if row.value == value { result.unchanged += 1; continue }
                try db.cachedStatement(
                    sql: """
                        UPDATE heart_rate_reading SET bpm = ?, mode = ?, utc_offset_seconds = ?, ring_local_seconds = ?,
                        time_source = ? WHERE id = ?
                        """
                ).execute(arguments: args + [row.id])
                result.updated += 1
            } else {
                try db.cachedStatement(
                    sql: """
                        INSERT INTO heart_rate_reading (bpm, mode, utc_offset_seconds, ring_local_seconds, time_source,
                        ring_id, time) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """
                ).execute(arguments: args + [ringID.uuidString, key])
                result.inserted += 1
            }
        }
        for row in existing.values {
            try db.cachedStatement(sql: "DELETE FROM heart_rate_reading WHERE id = ?").execute(arguments: [row.id])
            result.deleted += 1
        }
    }

    private static func applyVitals(
        _ db: Database, _ ringID: UUID, _ desired: [Int64: VitalsReading], _ result: inout IngestionResult,
        _ checkpoint: () throws -> Void
    ) throws {
        var existing = Dictionary(
            try vitalsRows(db, ringID: ringID).map { (ms($0.value.time), $0) }, uniquingKeysWith: { a, _ in a })
        for (index, (key, value)) in desired.sorted(by: { $0.key < $1.key }).enumerated() {
            if index % checkpointInterval == checkpointInterval - 1 { try checkpoint() }
            let args: StatementArguments = [
                value.spo2, value.respirationRate, value.hrv, value.cvrr, value.utcOffsetSeconds,
                value.ringLocalSeconds, TimeSourceCode.code(for: value.timeSource),
            ]
            if let row = existing.removeValue(forKey: key) {
                if row.value == value { result.unchanged += 1; continue }
                try db.cachedStatement(
                    sql: """
                        UPDATE vitals_reading SET spo2 = ?, respiration_rate = ?, hrv = ?, cvrr = ?,
                        utc_offset_seconds = ?, ring_local_seconds = ?, time_source = ? WHERE id = ?
                        """
                ).execute(arguments: args + [row.id])
                result.updated += 1
            } else {
                try db.cachedStatement(
                    sql: """
                        INSERT INTO vitals_reading (spo2, respiration_rate, hrv, cvrr, utc_offset_seconds,
                        ring_local_seconds, time_source, ring_id, time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                ).execute(arguments: args + [ringID.uuidString, key])
                result.inserted += 1
            }
        }
        for row in existing.values {
            try db.cachedStatement(sql: "DELETE FROM vitals_reading WHERE id = ?").execute(arguments: [row.id])
            result.deleted += 1
        }
    }

    private static func applyActivity(
        _ db: Database, _ ringID: UUID, _ desired: [Int64: ActivityInterval], _ result: inout IngestionResult,
        _ checkpoint: () throws -> Void
    ) throws {
        var existing = Dictionary(
            try activityRows(db, ringID: ringID).map { (ms($0.value.start), $0) }, uniquingKeysWith: { a, _ in a })
        for (index, (key, value)) in desired.sorted(by: { $0.key < $1.key }).enumerated() {
            if index % checkpointInterval == checkpointInterval - 1 { try checkpoint() }
            let args: StatementArguments = [
                ms(value.end), value.steps, value.distanceMeters, value.kilocalories, value.utcOffsetSeconds,
                value.ringLocalSeconds, TimeSourceCode.code(for: value.timeSource),
            ]
            if let row = existing.removeValue(forKey: key) {
                if row.value == value { result.unchanged += 1; continue }
                try db.cachedStatement(
                    sql: """
                        UPDATE activity_interval SET "end" = ?, steps = ?, distance_meters = ?, kilocalories = ?,
                        utc_offset_seconds = ?, ring_local_seconds = ?, time_source = ? WHERE id = ?
                        """
                ).execute(arguments: args + [row.id])
                result.updated += 1
            } else {
                try db.cachedStatement(
                    sql: """
                        INSERT INTO activity_interval ("end", steps, distance_meters, kilocalories, utc_offset_seconds,
                        ring_local_seconds, time_source, ring_id, start) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                ).execute(arguments: args + [ringID.uuidString, key])
                result.inserted += 1
            }
        }
        for row in existing.values {
            try db.cachedStatement(sql: "DELETE FROM activity_interval WHERE id = ?").execute(arguments: [row.id])
            result.deleted += 1
        }
    }

    private static func applySleep(
        _ db: Database, _ ringID: UUID, _ desired: [Int64: SleepSessionRecord], _ result: inout IngestionResult,
        _ checkpoint: () throws -> Void
    ) throws {
        var existing = Dictionary(
            try sleepRows(db, ringID: ringID).map { (ms($0.value.start), $0) }, uniquingKeysWith: { a, _ in a })
        for (index, (key, value)) in desired.sorted(by: { $0.key < $1.key }).enumerated() {
            if index % checkpointInterval == checkpointInterval - 1 { try checkpoint() }
            let sessionID: Int64
            if let row = existing.removeValue(forKey: key) {
                if row.value == value { result.unchanged += 1; continue }
                try db.cachedStatement(
                    sql: """
                        UPDATE sleep_session SET "end" = ?, utc_offset_seconds = ?, ring_local_seconds = ?,
                        time_source = ? WHERE id = ?
                        """
                ).execute(arguments: [
                    ms(value.end), value.utcOffsetSeconds, value.ringLocalSeconds,
                    TimeSourceCode.code(for: value.timeSource), row.id,
                ])
                try db.cachedStatement(sql: "DELETE FROM sleep_stage WHERE session_id = ?").execute(arguments: [row.id])
                sessionID = row.id
                result.updated += 1
            } else {
                try db.cachedStatement(
                    sql: """
                        INSERT INTO sleep_session (ring_id, start, "end", utc_offset_seconds, ring_local_seconds,
                        time_source) VALUES (?, ?, ?, ?, ?, ?)
                        """
                ).execute(arguments: [
                    ringID.uuidString, key, ms(value.end), value.utcOffsetSeconds, value.ringLocalSeconds,
                    TimeSourceCode.code(for: value.timeSource),
                ])
                sessionID = db.lastInsertedRowID
                result.inserted += 1
            }
            for (ordinal, stage) in value.stages.enumerated() {
                try db.cachedStatement(
                    sql: "INSERT INTO sleep_stage (session_id, ordinal, kind, start, duration_seconds) VALUES (?, ?, ?, ?, ?)"
                ).execute(
                    arguments: [sessionID, ordinal, StageCode.code(for: stage.kind), ms(stage.start), stage.durationSeconds])
            }
        }
        for row in existing.values {
            // The stages go with it (ON DELETE CASCADE).
            try db.cachedStatement(sql: "DELETE FROM sleep_session WHERE id = ?").execute(arguments: [row.id])
            result.deleted += 1
        }
    }

    /// Internal only for the tests' one-transaction reference.
    static func deleteTypedRows(_ db: Database, ringID: UUID, kind: HistoryKind) throws {
        let table: String
        switch kind {
        case .heartRate: table = "heart_rate_reading"
        case .activity: table = "activity_interval"
        case .sleep: table = "sleep_session"  // stages cascade
        case .combined: table = "vitals_reading"
        }
        try db.cachedStatement(sql: "DELETE FROM \(table) WHERE ring_id = ?").execute(arguments: [ringID.uuidString])
    }

    // MARK: Reading rows

    private static func source(_ row: Row) throws -> TimeSource {
        guard let source = TimeSourceCode.source(forCode: try column(row, "time_source")) else {
            throw RingStoreError.corruptRow("time source")
        }
        return source
    }

    static func heartRateRows(_ db: Database, ringID: UUID) throws -> [StoredRow<HeartRateReading>] {
        try Row.fetchAll(
            db, sql: "SELECT * FROM heart_rate_reading WHERE ring_id = ? ORDER BY time", arguments: [ringID.uuidString]
        ).map { row in
            StoredRow(
                id: try column(row, "id"),
                value: HeartRateReading(
                    time: date(try column(row, "time")), bpm: try column(row, "bpm"), mode: try column(row, "mode"),
                    utcOffsetSeconds: try column(row, "utc_offset_seconds"),
                    ringLocalSeconds: try column(row, "ring_local_seconds"), timeSource: try source(row)))
        }
    }

    static func vitalsRows(_ db: Database, ringID: UUID) throws -> [StoredRow<VitalsReading>] {
        try Row.fetchAll(
            db, sql: "SELECT * FROM vitals_reading WHERE ring_id = ? ORDER BY time", arguments: [ringID.uuidString]
        ).map { row in
            StoredRow(
                id: try column(row, "id"),
                value: VitalsReading(
                    time: date(try column(row, "time")), spo2: try column(row, "spo2"),
                    respirationRate: try column(row, "respiration_rate"), hrv: try column(row, "hrv"),
                    cvrr: try column(row, "cvrr"), utcOffsetSeconds: try column(row, "utc_offset_seconds"),
                    ringLocalSeconds: try column(row, "ring_local_seconds"), timeSource: try source(row)))
        }
    }

    static func activityRows(_ db: Database, ringID: UUID) throws -> [StoredRow<ActivityInterval>] {
        try Row.fetchAll(
            db, sql: "SELECT * FROM activity_interval WHERE ring_id = ? ORDER BY start", arguments: [ringID.uuidString]
        ).map { row in
            StoredRow(
                id: try column(row, "id"),
                value: ActivityInterval(
                    start: date(try column(row, "start")), end: date(try column(row, "end")),
                    steps: try column(row, "steps"), distanceMeters: try column(row, "distance_meters"),
                    kilocalories: try column(row, "kilocalories"),
                    utcOffsetSeconds: try column(row, "utc_offset_seconds"),
                    ringLocalSeconds: try column(row, "ring_local_seconds"), timeSource: try source(row)))
        }
    }

    static func sleepRows(_ db: Database, ringID: UUID) throws -> [StoredRow<SleepSessionRecord>] {
        var stages: [Int64: [SleepStageEntry]] = [:]
        let stageRows = try Row.fetchAll(
            db,
            sql: """
                SELECT s.* FROM sleep_stage s JOIN sleep_session p ON p.id = s.session_id
                WHERE p.ring_id = ? ORDER BY s.session_id, s.ordinal
                """,
            arguments: [ringID.uuidString])
        for row in stageRows {
            guard let kind = StageCode.kind(forCode: try column(row, "kind")) else {
                throw RingStoreError.corruptRow("sleep stage kind")
            }
            stages[try column(row, "session_id"), default: []].append(SleepStageEntry(
                kind: kind, start: date(try column(row, "start")), durationSeconds: try column(row, "duration_seconds")))
        }
        return try Row.fetchAll(
            db, sql: "SELECT * FROM sleep_session WHERE ring_id = ? ORDER BY start", arguments: [ringID.uuidString]
        ).map { row in
            let id: Int64 = try column(row, "id")
            return StoredRow(
                id: id,
                value: SleepSessionRecord(
                    start: date(try column(row, "start")), end: date(try column(row, "end")),
                    utcOffsetSeconds: try column(row, "utc_offset_seconds"),
                    ringLocalSeconds: try column(row, "ring_local_seconds"), timeSource: try source(row),
                    stages: stages[id] ?? []))
        }
    }
}
