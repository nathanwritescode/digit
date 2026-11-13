import Foundation
import GRDB
import Testing
import RingProtocol
@testable import RingStore

// Vitals (SpO2, respiration, HRV, CVRR) ingested from the combined record. All bytes are SYNTHETIC,
// built from the documented 20-byte layout with made-up values; none come from a real ring.
// Real instants are seconds after t0; the ring's clock is set at t0 with a +1 hour offset.

private func at(_ seconds: Double) -> Date { t0.addingTimeInterval(seconds) }

/// Time u32, steps u16, heart rate, systolic, diastolic, SpO2, respiration, HRV, CVRR, temperature
/// (2 bytes), body fat (2 bytes), glucose, 2 trailing bytes. The estimate and unused fields are
/// parameters so tests can fill them with non-zero values.
private func combinedBytes(
    _ ringSeconds: UInt32, steps: Int = 0, hr: UInt8 = 0, systolic: UInt8 = 0, diastolic: UInt8 = 0,
    spo2: UInt8 = 0, resp: UInt8 = 0, hrv: UInt8 = 0, cvrr: UInt8 = 0,
    temperature: [UInt8] = [0, 0], bodyFat: [UInt8] = [0, 0], glucose: UInt8 = 0, trailing: [UInt8] = [0, 0]
) -> [UInt8] {
    le32(ringSeconds) + le16(steps) + [hr, systolic, diastolic, spo2, resp, hrv, cvrr] + temperature + bodyFat + [glucose] + trailing
}

private func vitals(_ realSeconds: Double, offset: Int = plus1, spo2: UInt8 = 0, resp: UInt8 = 0, hrv: UInt8 = 0, cvrr: UInt8 = 0) -> [UInt8] {
    combinedBytes(wall(at(realSeconds), offset: offset), spo2: spo2, resp: resp, hrv: hrv, cvrr: cvrr)
}

private func clocked(_ store: RingStore) throws -> Fixture { try clockedFixture(store, kinds: [.combined]) }

struct VitalsIngestionTests {
    @Test func aScheduledReadingKeepsAllFourValues() throws {
        try onEachStore { store in
            let f = try clocked(store)
            try f.fetch(.combined, vitals(600, spo2: 97, resp: 16, hrv: 37, cvrr: 30), at: at(3600))
            let r = try store.ingest(ringID: f.ring.id, kind: .combined)
            #expect(r.inserted == 1 && r.withoutVitals == 0 && r.rejectedImplausible == 0)
            #expect(try store.vitalsReadings(ringID: f.ring.id).map(\.value) == [
                VitalsReading(
                    time: at(600), spo2: 97, respirationRate: 16, hrv: 37, cvrr: 30, utcOffsetSeconds: plus1,
                    ringLocalSeconds: Int(wall(at(600), offset: plus1)), timeSource: .resolved)
            ])
        }
    }

    @Test func anOnDemandSpO2RecordGivesARowWithOnlySpO2() throws {
        try onEachStore { store in
            let f = try clocked(store)
            try f.fetch(.combined, vitals(600, spo2: 96), at: at(3600))
            try store.ingest(ringID: f.ring.id, kind: .combined)
            let row = try #require(try store.vitalsReadings(ringID: f.ring.id).first).value
            #expect(row.spo2 == 96 && row.respirationRate == nil && row.hrv == nil && row.cvrr == nil)
            // NULL in the table, not a zero.
            let nulls = try store.writer.read { db in
                try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM vitals_reading WHERE respiration_rate IS NULL AND hrv IS NULL AND cvrr IS NULL")
            }
            #expect(nulls == 1)
        }
    }

    @Test func aRecordWithAllFourAbsentGivesNoRowAndCountsOnce() throws {
        try onEachStore { store in
            let f = try clocked(store)
            // Heart rate and steps alone are not vitals; the same bytes twice are one distinct record.
            let empty = combinedBytes(wall(at(600), offset: plus1), steps: 50, hr: 70)
            try f.fetch(.combined, empty + vitals(1200, spo2: 98), at: at(3600))
            try f.fetch(.combined, empty + vitals(1200, spo2: 98) + empty, at: at(7200))
            let r = try store.ingest(ringID: f.ring.id, kind: .combined)
            // `empty` appears as occurrence 0 in both fetches and as occurrence 1 in the second: two records.
            #expect(r.withoutVitals == 2)
            #expect(r.inserted == 1 && r.rejectedImplausible == 0 && r.rejectedFuture == 0 && r.collisions == 0)
            #expect(try store.vitalsReadings(ringID: f.ring.id).map(\.value.spo2) == [98])
            let again = try store.ingest(ringID: f.ring.id, kind: .combined)
            #expect(again.withoutVitals == 2 && again.inserted == 0 && again.unchanged == 1)
        }
    }

    @Test func anEmptyRecordNeverDisplacesARealOneAtTheSameSecond() throws {
        try onEachStore { store in
            let f = try clocked(store)
            // The empty record is later in the buffer, so it would win a collision if it were a row.
            try f.fetch(.combined, vitals(600, spo2: 97) + combinedBytes(wall(at(600), offset: plus1), hr: 70), at: at(3600))
            let r = try store.ingest(ringID: f.ring.id, kind: .combined)
            #expect(r.collisions == 0 && r.withoutVitals == 1 && r.inserted == 1)
            #expect(try store.vitalsReadings(ringID: f.ring.id).map(\.value.spo2) == [97])
        }
    }

    @Test func aValueOf255IsAbsent() throws {
        try onEachStore { store in
            let f = try clocked(store)
            try f.fetch(
                .combined,
                vitals(600, spo2: 255, resp: 255, hrv: 255, cvrr: 255)  // all absent: no row
                    + vitals(1200, spo2: 95, resp: 255, hrv: 40, cvrr: 255), at: at(3600))
            let r = try store.ingest(ringID: f.ring.id, kind: .combined)
            #expect(r.withoutVitals == 1 && r.inserted == 1)
            let row = try #require(try store.vitalsReadings(ringID: f.ring.id).first).value
            #expect(row.spo2 == 95 && row.respirationRate == nil && row.hrv == 40 && row.cvrr == nil)
        }
    }

    @Test func vendorEstimatesAndOtherFieldsNeverReachTheTable() throws {
        try onEachStore { store in
            let f = try clocked(store)
            try f.fetch(
                .combined,
                combinedBytes(
                    wall(at(600), offset: plus1), steps: 1234, hr: 71, systolic: 118, diastolic: 76, spo2: 97, resp: 16,
                    hrv: 37, cvrr: 30, temperature: [0x2a, 0x05], bodyFat: [0x12, 0x34], glucose: 0x56, trailing: [0xaa, 0xbb]),
                at: at(3600))
            try store.ingest(ringID: f.ring.id, kind: .combined)
            let columns = try store.writer.read { db in
                try Row.fetchAll(db, sql: "SELECT name FROM pragma_table_info('vitals_reading') ORDER BY cid").map { $0["name"] as String }
            }
            #expect(columns == [
                "id", "ring_id", "time", "spo2", "respiration_rate", "hrv", "cvrr", "utc_offset_seconds",
                "ring_local_seconds", "time_source",
            ])
            // None of the values that are not stored can be found in the row either.
            let stored = try store.writer.read { db in
                try Row.fetchOne(db, sql: "SELECT spo2, respiration_rate, hrv, cvrr FROM vitals_reading")
            }
            #expect(try #require(stored).databaseValues.map { Int.fromDatabaseValue($0) } == [97, 16, 37, 30])
            // The heart-rate and activity tables are untouched by a combined ingest.
            #expect(try store.heartRateReadings(ringID: f.ring.id).isEmpty)
            #expect(try store.activityIntervals(ringID: f.ring.id).isEmpty)
        }
    }

    @Test func vitalsTableHasNoReferenceToHistoryTransfer() throws {
        try onEachStore { store in
            let references = try store.writer.read { db in
                try Row.fetchAll(db, sql: "SELECT \"table\" FROM pragma_foreign_key_list('vitals_reading')").map { $0["table"] as String }
            }
            #expect(references == ["ring"])
        }
    }

    @Test func ingestingTwiceChangesNothing() throws {
        try onEachStore { store in
            let f = try clocked(store)
            try f.fetch(.combined, vitals(600, spo2: 97, resp: 16) + vitals(1200, hrv: 40), at: at(3600))
            let first = try store.ingest(ringID: f.ring.id, kind: .combined)
            #expect(first.inserted == 2 && first.unchanged == 0)
            let before = try store.vitalsReadings(ringID: f.ring.id)
            let second = try store.ingest(ringID: f.ring.id, kind: .combined)
            #expect(second.inserted == 0 && second.updated == 0 && second.deleted == 0 && second.unchanged == 2)
            #expect(try store.vitalsReadings(ringID: f.ring.id) == before)
        }
    }

    @Test func rebuildGivesTheSameRowsAndIncludesCombined() throws {
        try onEachStore { store in
            let f = try clocked(store)
            try f.fetch(.combined, vitals(600, spo2: 97, resp: 16, hrv: 37, cvrr: 30), at: at(3600))
            try f.fetch(.combined, vitals(600, spo2: 97, resp: 16, hrv: 37, cvrr: 30) + vitals(1200, spo2: 95), at: at(7200))
            try store.ingest(ringID: f.ring.id, kind: .combined)
            let before = try store.vitalsReadings(ringID: f.ring.id).map(\.value)
            #expect(before.count == 2)
            let results = try store.rebuild(ringID: f.ring.id)
            #expect(results[.combined]?.inserted == 2)
            #expect(try store.vitalsReadings(ringID: f.ring.id).map(\.value) == before)
        }
    }

    @Test func recordsBeforeTheClockWasSetAndFutureRecordsAreRejected() throws {
        try onEachStore { store in
            let f = try clocked(store)
            // Ring seconds 3600 are in the year 2000; the last is a day and more after its fetch.
            try f.fetch(
                .combined,
                combinedBytes(3600, spo2: 97) + vitals(600, spo2: 96) + vitals(3600 + 2 * 86_400, spo2: 95), at: at(3600))
            let r = try store.ingest(ringID: f.ring.id, kind: .combined)
            #expect(r.rejectedImplausible == 1 && r.rejectedFuture == 1 && r.inserted == 1 && r.withoutVitals == 0)
            #expect(try store.vitalsReadings(ringID: f.ring.id).map(\.value.spo2) == [96])
        }
    }

    @Test func distinctRecordsInOneRingSecondAreACollision() throws {
        try onEachStore { store in
            let f = try clocked(store)
            try f.fetch(.combined, vitals(600, spo2: 97) + vitals(600, spo2: 98), at: at(3600))
            let r = try store.ingest(ringID: f.ring.id, kind: .combined)
            #expect(r.collisions == 1 && r.inserted == 1)
            #expect(try store.vitalsReadings(ringID: f.ring.id).map(\.value.spo2) == [98])  // the later one wins
        }
    }

    /// Same shape as `daylightSavingEndingResolvesEachRecordToItsOwnOffset` in TimeResolutionTests.
    @Test func daylightSavingEndingResolvesEachRecordToItsOwnOffset() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            try f.fetch(.combined, [], at: at(-10))
            try f.setEarlierAndFirstClocks(at: at(0), offset: 3600)
            let before = vitals(1800, offset: 3600, spo2: 97)
            try f.fetch(.combined, before, at: at(3600))
            try f.fetch(.combined, before, at: at(7200))
            try f.setClock(at: at(7210), offset: 0)
            let after = vitals(9010, offset: 0, spo2: 96)
            try f.fetch(.combined, before + after, at: at(14400))
            let result = try store.ingest(ringID: f.ring.id, kind: .combined)
            let rows = try store.vitalsReadings(ringID: f.ring.id).map(\.value)
            #expect(rows.map(\.time) == [at(1800), at(9010)])
            #expect(rows.map(\.utcOffsetSeconds) == [3600, 0])
            #expect(rows.map(\.timeSource) == [.resolved, .resolved])
            #expect(result.rowsByTimeSource == [.resolved: 2])
        }
    }

    /// A database at v3 gets the table from `v4-vitals` and ingests combined data afterwards.
    @Test func v3DatabaseMigratesToV4() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let url = dir.appendingPathComponent("ring.sqlite")
        do {
            let pool = try DatabasePool(path: url.path)
            try RingStore.migrator.migrate(pool, upTo: "v3")
            let tables = try pool.read { db in
                try String.fetchAll(db, sql: "SELECT name FROM sqlite_master WHERE name = 'vitals_reading'")
            }
            #expect(tables.isEmpty)
        }
        let store = try RingStore(url: url)
        let f = try clocked(store)
        try f.fetch(.combined, vitals(600, spo2: 97), at: at(3600))
        try store.ingest(ringID: f.ring.id, kind: .combined)
        #expect(try store.vitalsReadings(ringID: f.ring.id).count == 1)
    }
}
