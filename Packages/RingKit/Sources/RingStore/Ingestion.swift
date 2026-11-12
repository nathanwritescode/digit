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
    /// ingested), and a record before 2020 is counted and skipped.
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
            let derived = derive(transfers, resolver, HeartRateRecord.decodeLocated, { $0.time }) { r, o in
                let instant = r.time.addingTimeInterval(-TimeInterval(o.seconds))
                return (instant, HeartRateReading(
                    time: instant, bpm: r.bpm, mode: Int(r.mode), utcOffsetSeconds: o.seconds, timeSource: o.source))
            }
            try applyHeartRate(db, ringID, derived.rows, &result)
            record(derived, rows: derived.rows.values.map(\.timeSource), into: &result)
        case .activity:
            let derived = derive(transfers, resolver, ActivityRecord.decodeLocated, { $0.end }) { r, o in
                let shift = -TimeInterval(o.seconds)
                let start = r.start.addingTimeInterval(shift)
                return (start, ActivityInterval(
                    start: start, end: r.end.addingTimeInterval(shift), steps: r.steps,
                    distanceMeters: r.distanceMeters, kilocalories: r.kilocalories,
                    utcOffsetSeconds: o.seconds, timeSource: o.source))
            }
            try applyActivity(db, ringID, derived.rows, &result)
            record(derived, rows: derived.rows.values.map(\.timeSource), into: &result)
        case .sleep:
            let derived = derive(transfers, resolver, SleepSession.decodeLocated, { $0.end }) { r, o in
                let shift = -TimeInterval(o.seconds)
                let start = r.start.addingTimeInterval(shift)
                return (start, SleepSessionRecord(
                    start: start, end: r.end.addingTimeInterval(shift),
                    utcOffsetSeconds: o.seconds, timeSource: o.source,
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
        result.rejectedBefore2020 = derived.rejected
        result.unusableBytes = derived.unusableBytes
        for source in sources { result.rowsByTimeSource[source, default: 0] += 1 }
    }

    /// Decodes every transfer (as wall-clock-as-UTC, which `TimeResolver` expects), resolves each
    /// whole record's offset and keys the result by its real start instant. A record that appears
    /// in several transfers resolves to one key; the latest transfer's copy wins.
    private static func derive<R: Sendable, V>(
        _ transfers: [StoredTransfer], _ resolver: TimeResolver,
        _ decode: ([UInt8], TimeZone) -> [Located<R>],
        _ reference: (R) -> Date,
        _ build: (R, ResolvedOffset) -> (instant: Date, value: V)
    ) -> Derived<V> {
        var derived = Derived<V>()
        let utc = TimeZone(secondsFromGMT: 0)!
        for stored in transfers {
            let data = stored.transfer.data
            let sightings = resolver.sightings(
                in: data, fetchedAt: stored.transfer.fetchedAt, timeZoneIdentifier: stored.transfer.timeZoneIdentifier)
            var consumed = 0
            for located in decode(data, utc) where located.offset + located.length <= data.count {
                consumed += located.length
                let ref = reference(located.record)
                if ref < earliestPlausibleRingTime {
                    derived.rejected += 1
                    continue
                }
                let offset = resolver.resolve(
                    sightings: sightings, offset: located.offset, length: located.length, reference: ref)
                let (instant, value) = build(located.record, offset)
                let key = ms(instant)
                // Transfers are in fetch order, so the latest copy wins. Copies are normally
                // identical; they differ when a record changed in place (an open sleep session).
                derived.rows[key] = value
            }
            derived.unusableBytes += data.count - consumed
        }
        return derived
    }

    // MARK: Applying the desired set

    private static func applyHeartRate(
        _ db: Database, _ ringID: UUID, _ desired: [Int64: HeartRateReading], _ result: inout IngestionResult
    ) throws {
        var existing = Dictionary(uniqueKeysWithValues: try heartRateRows(db, ringID: ringID).map { (ms($0.value.time), $0) })
        for key in desired.keys.sorted() {
            let value = desired[key]!
            let args: StatementArguments = [value.bpm, value.mode, value.utcOffsetSeconds, TimeSourceCode.code(for: value.timeSource)]
            if let row = existing.removeValue(forKey: key) {
                if row.value == value { result.unchanged += 1; continue }
                try db.execute(
                    sql: "UPDATE heart_rate_reading SET bpm = ?, mode = ?, utc_offset_seconds = ?, time_source = ? WHERE id = ?",
                    arguments: args + [row.id])
                result.updated += 1
            } else {
                try db.execute(
                    sql: "INSERT INTO heart_rate_reading (bpm, mode, utc_offset_seconds, time_source, ring_id, time) VALUES (?, ?, ?, ?, ?, ?)",
                    arguments: args + [ringID.uuidString, key])
                result.inserted += 1
            }
        }
        for row in existing.values {
            try db.execute(sql: "DELETE FROM heart_rate_reading WHERE id = ?", arguments: [row.id])
            result.deleted += 1
        }
    }

    private static func applyActivity(
        _ db: Database, _ ringID: UUID, _ desired: [Int64: ActivityInterval], _ result: inout IngestionResult
    ) throws {
        var existing = Dictionary(uniqueKeysWithValues: try activityRows(db, ringID: ringID).map { (ms($0.value.start), $0) })
        for key in desired.keys.sorted() {
            let value = desired[key]!
            let args: StatementArguments = [
                ms(value.end), value.steps, value.distanceMeters, value.kilocalories, value.utcOffsetSeconds,
                TimeSourceCode.code(for: value.timeSource),
            ]
            if let row = existing.removeValue(forKey: key) {
                if row.value == value { result.unchanged += 1; continue }
                try db.execute(
                    sql: """
                        UPDATE activity_interval SET "end" = ?, steps = ?, distance_meters = ?, kilocalories = ?,
                        utc_offset_seconds = ?, time_source = ? WHERE id = ?
                        """,
                    arguments: args + [row.id])
                result.updated += 1
            } else {
                try db.execute(
                    sql: """
                        INSERT INTO activity_interval ("end", steps, distance_meters, kilocalories, utc_offset_seconds,
                        time_source, ring_id, start) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                    arguments: args + [ringID.uuidString, key])
                result.inserted += 1
            }
        }
        for row in existing.values {
            try db.execute(sql: "DELETE FROM activity_interval WHERE id = ?", arguments: [row.id])
            result.deleted += 1
        }
    }

    private static func applySleep(
        _ db: Database, _ ringID: UUID, _ desired: [Int64: SleepSessionRecord], _ result: inout IngestionResult
    ) throws {
        var existing = Dictionary(uniqueKeysWithValues: try sleepRows(db, ringID: ringID).map { (ms($0.value.start), $0) })
        for key in desired.keys.sorted() {
            let value = desired[key]!
            var sessionID: Int64
            if let row = existing.removeValue(forKey: key) {
                if row.value == value { result.unchanged += 1; continue }
                try db.execute(
                    sql: "UPDATE sleep_session SET \"end\" = ?, utc_offset_seconds = ?, time_source = ? WHERE id = ?",
                    arguments: [ms(value.end), value.utcOffsetSeconds, TimeSourceCode.code(for: value.timeSource), row.id])
                try db.execute(sql: "DELETE FROM sleep_stage WHERE session_id = ?", arguments: [row.id])
                sessionID = row.id
                result.updated += 1
            } else {
                try db.execute(
                    sql: "INSERT INTO sleep_session (ring_id, start, \"end\", utc_offset_seconds, time_source) VALUES (?, ?, ?, ?, ?)",
                    arguments: [
                        ringID.uuidString, key, ms(value.end), value.utcOffsetSeconds,
                        TimeSourceCode.code(for: value.timeSource),
                    ])
                sessionID = db.lastInsertedRowID
                result.inserted += 1
            }
            for (ordinal, stage) in value.stages.enumerated() {
                try db.execute(
                    sql: "INSERT INTO sleep_stage (session_id, ordinal, kind, start, duration_seconds) VALUES (?, ?, ?, ?, ?)",
                    arguments: [sessionID, ordinal, StageCode.code(for: stage.kind), ms(stage.start), stage.durationSeconds])
            }
        }
        for row in existing.values {
            // The stages go with it (ON DELETE CASCADE).
            try db.execute(sql: "DELETE FROM sleep_session WHERE id = ?", arguments: [row.id])
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
        try db.execute(sql: "DELETE FROM \(table) WHERE ring_id = ?", arguments: [ringID.uuidString])
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
                    utcOffsetSeconds: try column(row, "utc_offset_seconds"), timeSource: try source(row)))
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
                    utcOffsetSeconds: try column(row, "utc_offset_seconds"), timeSource: try source(row)))
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
                    utcOffsetSeconds: try column(row, "utc_offset_seconds"), timeSource: try source(row),
                    stages: stages[id] ?? []))
        }
    }
}
