import Foundation
import GRDB
import RingProtocol

// Typed ingestion. Raw transfers stay the source of truth; typed rows are always the full result of
// deriving from every surviving transfer, so ingesting twice changes nothing and `rebuild` is just
// "delete, then ingest".

/// Ring-local readings before this mean the ring's clock was never set (it counts from 2000).
private let earliestPlausibleRingTime = Date(timeIntervalSince1970: 1_577_836_800)  // 2020-01-01

private func ms(_ date: Date) -> Int64 { HistoryTransfer.millisecondsSince1970(date) }
private func date(_ ms: Int64) -> Date { HistoryTransfer.date(millisecondsSince1970: ms) }

extension RingStore {
    static let ingestibleKinds: [HistoryKind] = [.activity, .sleep, .heartRate]

    // MARK: Public API

    /// Makes the typed table for `kind` match what the stored transfers say, in one write
    /// transaction (a failure leaves the tables as they were). Throws `notIngestible` for the
    /// combined record, which is out of scope here.
    ///
    /// Time resolution is described on `TimeResolver`. Unusable input never stops the run: bytes
    /// that do not form a whole record are counted in `unusableBytes` (activity and heart-rate
    /// decoders ignore a trailing partial record; the sleep decoder stops at the first thing that
    /// is not a session record, or at a session cut short, so what follows it is counted and not
    /// ingested), and a record the sanity checks refuse is counted and skipped, in
    /// `IngestionResult.rejectedImplausible` or `rejectedFuture`.
    @discardableResult
    public func ingest(ringID: UUID, kind: HistoryKind) throws -> IngestionResult {
        guard Self.ingestibleKinds.contains(kind) else { throw RingStoreError.notIngestible(kind) }
        return try writer.write { db in
            try Self.requireRing(db, ringID)
            return try Self.ingest(db, ringID: ringID, kind: kind)
        }
    }

    /// Deletes the ring's typed rows for activity, sleep and heart rate and derives them again from
    /// the stored transfers, in one transaction. The result is what `ingest` would converge on.
    @discardableResult
    public func rebuild(ringID: UUID) throws -> [HistoryKind: IngestionResult] {
        try writer.write { db in
            try Self.requireRing(db, ringID)
            var results: [HistoryKind: IngestionResult] = [:]
            for kind in Self.ingestibleKinds {
                try Self.deleteTypedRows(db, ringID: ringID, kind: kind)
                results[kind] = try Self.ingest(db, ringID: ringID, kind: kind)
            }
            return results
        }
    }

    /// All heart-rate readings for a ring, oldest first.
    public func heartRateReadings(ringID: UUID) throws -> [StoredRow<HeartRateReading>] {
        try writer.read { db in try Self.heartRateRows(db, ringID: ringID) }
    }

    /// All activity intervals for a ring, oldest first.
    public func activityIntervals(ringID: UUID) throws -> [StoredRow<ActivityInterval>] {
        try writer.read { db in try Self.activityRows(db, ringID: ringID) }
    }

    /// All sleep sessions for a ring, oldest first, with their stages in order.
    public func sleepSessions(ringID: UUID) throws -> [StoredRow<SleepSessionRecord>] {
        try writer.read { db in try Self.sleepRows(db, ringID: ringID) }
    }

    // MARK: Derivation

    private struct Derived<Value> {
        var rows: [Int64: Value] = [:]
        var rejected = 0
        var unusableBytes = 0
        var collisions = 0
        var superseded = 0
        var rejectedFuture = 0
    }

    private static func ingest(_ db: Database, ringID: UUID, kind: HistoryKind) throws -> IngestionResult {
        let transfers = try Self.transfers(db, ringID: ringID, kind: kind, fetchedWithin: nil).transfers
        let resolver = TimeResolver(
            ringID: ringID, kind: kind,
            observations: try Self.observations(db, ringID: ringID, kind: kind).observations,
            clockSets: try Self.clockSets(db, ringID: ringID))
        var result = IngestionResult()
        switch kind {
        case .heartRate:
            let derived = try derive(
                transfers, resolver, HeartRateRecord.decodeLocated, reference: { $0.time }, times: { [$0.time] },
                // 0 and 255 bpm are not readings (no signal, or a filler byte).
                plausible: { $0.bpm != 0 && $0.bpm != 255 }, startTime: nil, supersedeKey: nil
            ) { r, o in
                let instant = r.time.addingTimeInterval(-TimeInterval(o.seconds))
                return (instant, HeartRateReading(
                    time: instant, bpm: r.bpm, mode: Int(r.mode), utcOffsetSeconds: o.seconds,
                    ringLocalSeconds: ringSeconds(r.time), timeSource: o.source))
            }
            try applyHeartRate(db, ringID, derived.rows, &result)
            record(derived, rows: derived.rows.values.map(\.timeSource), into: &result)
        case .activity:
            let derived = try derive(
                transfers, resolver, ActivityRecord.decodeLocated, reference: { $0.end }, times: { [$0.start, $0.end] },
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
                    utcOffsetSeconds: o.seconds, ringLocalSeconds: ringSeconds(r.start), timeSource: o.source))
            }
            try applyActivity(db, ringID, derived.rows, &result)
            record(derived, rows: derived.rows.values.map(\.timeSource), into: &result)
        case .sleep:
            let derived = try derive(
                transfers, resolver, SleepSession.decodeLocated, reference: { $0.end },
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
                supersedeKey: { ringSeconds($0.start) }
            ) { r, o in
                let shift = -TimeInterval(o.seconds)
                let start = r.start.addingTimeInterval(shift)
                return (start, SleepSessionRecord(
                    start: start, end: r.end.addingTimeInterval(shift),
                    utcOffsetSeconds: o.seconds, ringLocalSeconds: ringSeconds(r.start), timeSource: o.source,
                    stages: r.stages.map {
                        SleepStageEntry(
                            kind: $0.kind, start: $0.start.addingTimeInterval(shift), durationSeconds: Int($0.duration))
                    }))
            }
            try applySleep(db, ringID, derived.rows, &result)
            record(derived, rows: derived.rows.values.map(\.timeSource), into: &result)
        case .combined:
            throw RingStoreError.notIngestible(kind)
        }
        return result
    }

    private static func record<V>(_ derived: Derived<V>, rows sources: [TimeSource], into result: inout IngestionResult) {
        result.rejectedImplausible = derived.rejected
        result.unusableBytes = derived.unusableBytes
        result.collisions = derived.collisions
        result.superseded = derived.superseded
        result.rejectedFuture = derived.rejectedFuture
        for source in sources { result.rowsByTimeSource[source, default: 0] += 1 }
    }

    /// Seconds since 2000-01-01 of a ring-local reading decoded as if UTC.
    private static func ringSeconds(_ asIfUTC: Date) -> Int { RingTime.ringSeconds(fromWallClock: asIfUTC) }

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
    private static func derive<R: Sendable, V>(
        _ transfers: [StoredTransfer], _ resolver: TimeResolver,
        _ decode: ([UInt8], TimeZone) -> [Located<R>],
        reference: (R) -> Date,
        times: (R) -> [Date],
        plausible: (R) -> Bool,
        startTime: ((R) -> Date)?,
        supersedeKey: ((R) -> Int)?,
        _ build: (R, ResolvedOffset) -> (instant: Date, value: V)
    ) throws -> Derived<V> {
        var derived = Derived<V>()
        var identities: [IdentityKey: Identity<R>] = [:]
        var rejected: Set<IdentityKey> = []
        for (index, stored) in transfers.enumerated() {
            let data = stored.transfer.data
            let sightings = resolver.sightings(in: data)
            var seen: [[UInt8]: Int] = [:]
            var consumed = 0
            for located in decode(data, .gmt) where located.offset + located.length <= data.count {
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
            let (instant, value) = build(identity.record, offset)
            let key = ms(instant)
            if derived.rows[key] != nil { derived.collisions += 1 }
            derived.rows[key] = value
        }
        return derived
    }

    // MARK: Applying the desired set

    private static func applyHeartRate(
        _ db: Database, _ ringID: UUID, _ desired: [Int64: HeartRateReading], _ result: inout IngestionResult
    ) throws {
        var existing = Dictionary(
            try heartRateRows(db, ringID: ringID).map { (ms($0.value.time), $0) }, uniquingKeysWith: { a, _ in a })
        for (key, value) in desired.sorted(by: { $0.key < $1.key }) {
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

    private static func applyActivity(
        _ db: Database, _ ringID: UUID, _ desired: [Int64: ActivityInterval], _ result: inout IngestionResult
    ) throws {
        var existing = Dictionary(
            try activityRows(db, ringID: ringID).map { (ms($0.value.start), $0) }, uniquingKeysWith: { a, _ in a })
        for (key, value) in desired.sorted(by: { $0.key < $1.key }) {
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
        _ db: Database, _ ringID: UUID, _ desired: [Int64: SleepSessionRecord], _ result: inout IngestionResult
    ) throws {
        var existing = Dictionary(
            try sleepRows(db, ringID: ringID).map { (ms($0.value.start), $0) }, uniquingKeysWith: { a, _ in a })
        for (key, value) in desired.sorted(by: { $0.key < $1.key }) {
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

    private static func deleteTypedRows(_ db: Database, ringID: UUID, kind: HistoryKind) throws {
        let table: String
        switch kind {
        case .heartRate: table = "heart_rate_reading"
        case .activity: table = "activity_interval"
        case .sleep: table = "sleep_session"  // stages cascade
        case .combined: throw RingStoreError.notIngestible(kind)
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
