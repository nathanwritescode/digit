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

    @Test func aRecordWithAllFourAbsentGivesNoRowAndCountsOncePerDistinctRecord() throws {
        try onEachStore { store in
            let f = try clocked(store)
            // Heart rate and steps alone are not vitals; the same bytes twice are one distinct record.
            let empty = combinedBytes(wall(at(600), offset: plus1), steps: 50, hr: 70)
            try f.fetch(.combined, empty + vitals(1200, spo2: 98), at: at(3600))
            try f.fetch(.combined, empty + vitals(1200, spo2: 98) + empty, at: at(7200))
            let r = try store.ingest(ringID: f.ring.id, kind: .combined)
            // The first fetch is a strict prefix of the second and is pruned, so one transfer is stored.
            // In it `empty` is occurrence 0 and occurrence 1: two distinct records, each counted once.
            #expect(r.withoutVitals == 2)
            #expect(r.inserted == 1 && r.rejectedImplausible == 0 && r.rejectedFuture == 0 && r.collisions == 0)
            #expect(try store.vitalsReadings(ringID: f.ring.id).map(\.value.spo2) == [98])
            let again = try store.ingest(ringID: f.ring.id, kind: .combined)
            #expect(again.withoutVitals == 2 && again.inserted == 0 && again.unchanged == 1)
        }
    }

    @Test func theSameEmptyRecordInTwoUnrelatedTransfersCountsOnce() throws {
        try onEachStore { store in
            let f = try clocked(store)
            let empty = combinedBytes(wall(at(600), offset: plus1), steps: 50, hr: 70)
            // Neither buffer is a prefix of the other, so both transfers stay stored.
            try f.fetch(.combined, empty + vitals(1200, spo2: 98), at: at(3600))
            try f.fetch(.combined, empty + vitals(1800, spo2: 99), at: at(7200))
            #expect(try store.transfers(ringID: f.ring.id, kind: .combined).transfers.count == 2)
            let first = try store.ingest(ringID: f.ring.id, kind: .combined)
            #expect(first.withoutVitals == 1 && first.inserted == 2)
            let again = try store.ingest(ringID: f.ring.id, kind: .combined)
            #expect(again.withoutVitals == 1 && again.unchanged == 2)
            let rebuilt = try store.rebuild(ringID: f.ring.id)
            #expect(rebuilt[.combined]?.withoutVitals == 1)
        }
    }

    @Test func aRowWhoseValuesWereChangedIsUpdatedBackWithEachColumnInPlace() throws {
        try onEachStore { store in
            let f = try clocked(store)
            try f.fetch(.combined, vitals(600, spo2: 97, resp: 16, hrv: 37, cvrr: 30), at: at(3600))
            try store.ingest(ringID: f.ring.id, kind: .combined)
            let good = try store.vitalsReadings(ringID: f.ring.id)
            // hrv and cvrr are swapped relative to the record, so an UPDATE that crossed them
            // would not restore the original.
            try store.writer.write { db in
                try db.execute(sql: "UPDATE vitals_reading SET spo2 = 1, respiration_rate = 2, hrv = 30, cvrr = 37, time_source = 'assumed'")
            }
            let r = try store.ingest(ringID: f.ring.id, kind: .combined)
            #expect(r.updated == 1 && r.inserted == 0 && r.deleted == 0 && r.unchanged == 0)
            #expect(try store.vitalsReadings(ringID: f.ring.id) == good)
        }
    }

    @Test func aRowWithNoBackingRecordIsDeleted() throws {
        try onEachStore { store in
            let f = try clocked(store)
            try f.fetch(.combined, vitals(600, spo2: 97), at: at(3600))
            try store.ingest(ringID: f.ring.id, kind: .combined)
            try store.writer.write { db in
                try db.execute(
                    sql: """
                        INSERT INTO vitals_reading (ring_id, time, spo2, utc_offset_seconds, ring_local_seconds, time_source)
                        VALUES (?, 1, 90, 0, 0, 'resolved')
                        """, arguments: [f.ring.id.uuidString])
            }
            #expect(try store.vitalsReadings(ringID: f.ring.id).count == 2)
            let r = try store.ingest(ringID: f.ring.id, kind: .combined)
            #expect(r.deleted == 1 && r.unchanged == 1 && r.inserted == 0)
            #expect(try store.vitalsReadings(ringID: f.ring.id).map(\.value.spo2) == [97])
        }
    }

    @Test func aRecordWithOnlyRespirationOrOnlyCVRRGivesARow() throws {
        try onEachStore { store in
            let f = try clocked(store)
            try f.fetch(.combined, vitals(600, resp: 15) + vitals(1200, cvrr: 28), at: at(3600))
            let r = try store.ingest(ringID: f.ring.id, kind: .combined)
            #expect(r.inserted == 2 && r.withoutVitals == 0)
            let rows = try store.vitalsReadings(ringID: f.ring.id).map(\.value)
            #expect(rows.map(\.respirationRate) == [15, nil])
            #expect(rows.map(\.cvrr) == [nil, 28])
            #expect(rows.map(\.spo2) == [nil, nil] && rows.map(\.hrv) == [nil, nil])
        }
    }

    @Test func emptyRecordsThatAreFutureOrBeforeTheClockAreRejectedNotCountedAsWithoutVitals() throws {
        try onEachStore { store in
            let f = try clocked(store)
            // SYNTHETIC. Both have all four absent (heart rate only); the checks run before that test.
            try f.fetch(
                .combined,
                combinedBytes(3600, hr: 70) + combinedBytes(wall(at(3600 + 2 * 86_400), offset: plus1), hr: 70), at: at(3600))
            let r = try store.ingest(ringID: f.ring.id, kind: .combined)
            #expect(r.rejectedImplausible == 1 && r.rejectedFuture == 1 && r.withoutVitals == 0 && r.inserted == 0)
        }
    }

    @Test func theTableRefusesASecondRowForTheSameRingAndTime() throws {
        try onEachStore { store in
            let f = try clocked(store)
            try f.fetch(.combined, vitals(600, spo2: 97), at: at(3600))
            try store.ingest(ringID: f.ring.id, kind: .combined)
            let row = try #require(try store.vitalsReadings(ringID: f.ring.id).first)
            let ms = Int64((row.value.time.timeIntervalSince1970 * 1000).rounded())
            #expect(throws: DatabaseError.self) {
                try store.writer.write { db in
                    try db.execute(
                        sql: "INSERT INTO vitals_reading (ring_id, time, utc_offset_seconds, ring_local_seconds, time_source) VALUES (?, ?, 0, 0, 'resolved')",
                        arguments: [f.ring.id.uuidString, ms])
                }
            }
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
            // The column list above is what proves the unstored values are absent; this only checks
            // that the four stored columns hold the expected values.
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
        let ringID = UUID()
        let data = vitals(600, spo2: 97)
        do {
            let pool = try DatabasePool(path: url.path)
            try RingStore.migrator.migrate(pool, upTo: "v3")
            try pool.write { db in
                try db.execute(
                    sql: "INSERT INTO ring (id, protocol_family, first_seen_at) VALUES (?, 'ycbt', 0)", arguments: [ringID.uuidString])
                let hash = RingStore.contentHash(ringID: ringID, kind: .combined, data: data)
                try db.execute(
                    sql: RingStore.insertTransferSQL,
                    arguments: [ringID.uuidString, "combined", Data([1]), Data(data), 1_800_003_600_000, "Europe/London", Data(hash)])
                try db.execute(
                    sql: RingStore.insertObservationSQL,
                    arguments: [ringID.uuidString, "combined", data.count, Data([1]), 1_800_003_600_000, "Europe/London", Data(hash)])
                try db.execute(
                    sql: """
                        INSERT INTO heart_rate_reading (ring_id, time, bpm, mode, utc_offset_seconds, ring_local_seconds, time_source)
                        VALUES (?, 1800000600000, 61, 0, 3600, 0, 'resolved')
                        """, arguments: [ringID.uuidString])
                let tables = try String.fetchAll(db, sql: "SELECT name FROM sqlite_master WHERE name = 'vitals_reading'")
                #expect(tables.isEmpty)
            }
        }
        let store = try RingStore(url: url)
        // Data written at v3 survives the migration.
        #expect(try store.ring(id: ringID) != nil)
        #expect(try store.transfers(ringID: ringID).transfers.map(\.transfer.data) == [data])
        #expect(try store.heartRateReadings(ringID: ringID).map(\.value.bpm) == [61])
        #expect(try store.vitalsReadings(ringID: ringID).isEmpty)
        try store.recordClockSet(ClockSetEvent(
            ringID: ringID, instant: at(-200_000), timeZoneIdentifier: "Europe/London", utcOffsetSeconds: plus1))
        try store.recordClockSet(ClockSetEvent(
            ringID: ringID, instant: at(0), timeZoneIdentifier: "Europe/London", utcOffsetSeconds: plus1))
        let r = try store.ingest(ringID: ringID, kind: .combined)
        #expect(r.inserted == 1)
        #expect(try store.vitalsReadings(ringID: ringID).count == 1)
    }
}
