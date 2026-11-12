import Foundation
import GRDB
import Testing
import RingProtocol
@testable import RingStore

// Typed ingestion. All bytes are synthetic, built from the documented record layouts.
// Real instants are seconds after t0; the ring's clock is set once to a fixed +1 hour offset
// unless a test says otherwise.

private func at(_ seconds: Double) -> Date { t0.addingTimeInterval(seconds) }
private let plus1 = 3600

/// A fixture whose clock was set at t0 and whose first (empty) fetch preceded it, so records
/// written afterwards resolve with +1 hour.
private func clockedFixture(_ store: RingStore, kinds: [HistoryKind] = [.heartRate]) throws -> Fixture {
    let f = try Fixture(store)
    for kind in kinds { try f.fetch(kind, [], at: at(-10)) }
    try f.setClock(at: at(0), offset: plus1)
    return f
}

private func hr(_ realSeconds: Double, _ bpm: UInt8) -> [UInt8] { hrBytes(wall(at(realSeconds), offset: plus1), bpm: bpm) }

struct IngestionTests {
    @Test func ingestOnceThenTwiceChangesNothing() throws {
        try onEachStore { store in
            let f = try clockedFixture(store, kinds: [.heartRate, .activity, .sleep])
            try f.fetch(.heartRate, hr(600, 60) + hr(1200, 62), at: at(3600))
            try f.fetch(.activity, activityBytes(
                start: wall(at(0), offset: plus1), end: wall(at(1800), offset: plus1), steps: 100, distance: 70, kcal: 4), at: at(3600))
            try f.fetch(.sleep, sleepBytes(
                start: wall(at(100), offset: plus1), end: wall(at(1000), offset: plus1),
                stages: [(242, wall(at(100), offset: plus1), 600), (241, wall(at(701), offset: plus1), 299)]), at: at(3600))

            for kind in [HistoryKind.heartRate, .activity, .sleep] {
                let first = try store.ingest(ringID: f.ring.id, kind: kind)
                #expect(first.inserted > 0 && first.unchanged == 0)
            }
            let hrIDs = try store.heartRateReadings(ringID: f.ring.id).map(\.id)
            let actIDs = try store.activityIntervals(ringID: f.ring.id).map(\.id)
            let sleepIDs = try store.sleepSessions(ringID: f.ring.id).map(\.id)
            let before = try (store.heartRateReadings(ringID: f.ring.id).map(\.value),
                              store.activityIntervals(ringID: f.ring.id).map(\.value),
                              store.sleepSessions(ringID: f.ring.id).map(\.value))
            #expect(hrIDs.count == 2 && actIDs.count == 1 && sleepIDs.count == 1)

            var second = IngestionResult()
            for kind in [HistoryKind.heartRate, .activity, .sleep] {
                let r = try store.ingest(ringID: f.ring.id, kind: kind)
                #expect(r.inserted == 0 && r.updated == 0 && r.deleted == 0)
                second.unchanged += r.unchanged
            }
            #expect(second.unchanged == 4)
            #expect(try store.heartRateReadings(ringID: f.ring.id).map(\.id) == hrIDs)
            #expect(try store.activityIntervals(ringID: f.ring.id).map(\.id) == actIDs)
            #expect(try store.sleepSessions(ringID: f.ring.id).map(\.id) == sleepIDs)

            let session = try #require(try store.sleepSessions(ringID: f.ring.id).first).value
            #expect(session.start == at(100) && session.end == at(1000))
            #expect(session.stages == [
                SleepStageEntry(kind: .light, start: at(100), durationSeconds: 600),
                SleepStageEntry(kind: .deep, start: at(701), durationSeconds: 299),
            ])
            let activity = try #require(try store.activityIntervals(ringID: f.ring.id).first).value
            #expect(activity == ActivityInterval(
                start: at(0), end: at(1800), steps: 100, distanceMeters: 70, kilocalories: 4,
                utcOffsetSeconds: plus1, timeSource: .resolved))
            #expect(try (store.heartRateReadings(ringID: f.ring.id).map(\.value),
                         store.activityIntervals(ringID: f.ring.id).map(\.value),
                         store.sleepSessions(ringID: f.ring.id).map(\.value)) == before)
        }
    }

    @Test func aBufferGrowingAcrossThreeFetchesIsIngestedEachTime() throws {
        try onEachStore { store in
            let f = try clockedFixture(store)
            var data: [UInt8] = []
            var ids: [Int64] = []
            for n in 1...3 {
                data += hr(Double(n - 1) * 3600 + 1800, UInt8(60 + n))
                try f.fetch(.heartRate, data, at: at(Double(n) * 3600))
                let r = try store.ingest(ringID: f.ring.id, kind: .heartRate)
                #expect(r.inserted == 1 && r.unchanged == n - 1 && r.updated == 0 && r.deleted == 0)
                let rows = try store.heartRateReadings(ringID: f.ring.id)
                #expect(rows.count == n)
                // Existing rows keep their ids as the buffer grows.
                #expect(Array(rows.map(\.id).prefix(ids.count)) == ids)
                ids = rows.map(\.id)
                #expect(rows.allSatisfy { $0.value.timeSource == .resolved })
            }
        }
    }

    @Test func twoTransfersSharingRecordsGiveOneRowPerRecord() throws {
        try onEachStore { store in
            let f = try clockedFixture(store)
            let r1 = hr(600, 61), r2 = hr(1200, 62), r3 = hr(5000, 63)
            try f.fetch(.heartRate, r1 + r2, at: at(3600))
            // After a ring reset the buffer starts over, so this is not a prefix of the first.
            try f.fetch(.heartRate, r2 + r3, at: at(7200))
            #expect(try store.transfers(ringID: f.ring.id, kind: .heartRate).transfers.count == 2)
            let r = try store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(r.inserted == 3)
            #expect(try store.heartRateReadings(ringID: f.ring.id).map(\.value.bpm) == [61, 62, 63])
            // Stored transfers are not deleted by ingestion; a record that stops being derived is.
            try store.writer.write { db in try db.execute(sql: "DELETE FROM history_transfer WHERE data = ?", arguments: [Data(r2 + r3)]) }
            let again = try store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(again.deleted == 1 && again.unchanged == 2 && again.inserted == 0)
            #expect(try store.heartRateReadings(ringID: f.ring.id).map(\.value.bpm) == [61, 62])
        }
    }

    @Test func aChangedSleepSessionKeepsItsRowAndReplacesItsStages() throws {
        try onEachStore { store in
            let f = try clockedFixture(store, kinds: [.sleep])
            let s0 = wall(at(100), offset: plus1)
            let open = sleepBytes(start: s0, end: s0 + 600, stages: [(242, s0, 600)])
            try f.fetch(.sleep, open, at: at(3600))
            try store.ingest(ringID: f.ring.id, kind: .sleep)
            let id = try #require(try store.sleepSessions(ringID: f.ring.id).first).id
            // The same session reported again, longer and with different stages; not a prefix.
            let closed = sleepBytes(
                start: s0, end: s0 + 1200, stages: [(242, s0, 500), (243, s0 + 501, 400), (241, s0 + 902, 298)])
            try f.fetch(.sleep, closed, at: at(7200))
            let r = try store.ingest(ringID: f.ring.id, kind: .sleep)
            #expect(r.updated == 1 && r.inserted == 0 && r.deleted == 0)
            let row = try #require(try store.sleepSessions(ringID: f.ring.id).first)
            #expect(row.id == id)
            #expect(row.value.end == at(1300))
            #expect(row.value.stages.map(\.kind) == [.light, .rem, .deep])
            let stageCount = try store.writer.read { try Int.fetchOne($0, sql: "SELECT count(*) FROM sleep_stage") }
            #expect(stageCount == 3)
        }
    }

    @Test func recordsBeforeTheRingClockWasSetAreRejectedAndCounted() throws {
        try onEachStore { store in
            let f = try clockedFixture(store)
            // Raw ring seconds 3600 and 7200 are in the year 2000.
            try f.fetch(.heartRate, hrBytes(3600, bpm: 50) + hr(600, 61) + hrBytes(7200, bpm: 51), at: at(3600))
            let r = try store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(r.rejectedBefore2020 == 2 && r.inserted == 1)
            #expect(try store.heartRateReadings(ringID: f.ring.id).map(\.value.bpm) == [61])
        }
    }

    @Test func trailingBytesAreCountedAndDoNotStopTheRest() throws {
        try onEachStore { store in
            let f = try clockedFixture(store, kinds: [.heartRate, .activity, .sleep])
            try f.fetch(.heartRate, hr(600, 61) + hr(1200, 62) + [1, 2, 3, 4], at: at(3600))
            #expect(try store.ingest(ringID: f.ring.id, kind: .heartRate).unusableBytes == 4)
            #expect(try store.heartRateReadings(ringID: f.ring.id).count == 2)

            let act = activityBytes(start: wall(at(0), offset: plus1), end: wall(at(1800), offset: plus1), steps: 5)
            try f.fetch(.activity, act + [9, 9, 9], at: at(3600))
            #expect(try store.ingest(ringID: f.ring.id, kind: .activity).unusableBytes == 3)
            #expect(try store.activityIntervals(ringID: f.ring.id).count == 1)

            // A good session, a second session cut short, then nothing.
            let s0 = wall(at(100), offset: plus1)
            let good = sleepBytes(start: s0, end: s0 + 600, stages: [(242, s0, 600)])
            let cut = Array(sleepBytes(start: s0 + 5000, end: s0 + 6000, stages: [(242, s0 + 5000, 1000)]).prefix(25))
            try f.fetch(.sleep, good + cut, at: at(3600))
            let r = try store.ingest(ringID: f.ring.id, kind: .sleep)
            #expect(r.unusableBytes == 25 && r.inserted == 1)

            // A second transfer with trailing garbage does not stop other transfers being ingested.
            let r2 = hr(2400, 63)
            try f.fetch(.heartRate, r2 + [7], at: at(7200))
            let again = try store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(again.inserted == 1 && again.unusableBytes == 4 + 1)
        }
    }

    /// A trigger makes the third insert fail after earlier rows of the same run were written.
    @Test func aFailureMidwayLeavesTheTablesAsTheyWere() throws {
        try onEachStore { store in
            let f = try clockedFixture(store)
            try f.fetch(.heartRate, hr(600, 61), at: at(3600))
            try store.ingest(ringID: f.ring.id, kind: .heartRate)
            let before = try store.heartRateReadings(ringID: f.ring.id)

            try store.writer.write { db in
                try db.execute(sql: """
                    CREATE TRIGGER fail_on_99 BEFORE INSERT ON heart_rate_reading WHEN NEW.bpm = 99
                    BEGIN SELECT RAISE(ABORT, 'boom'); END
                    """)
            }
            try f.fetch(.heartRate, hr(600, 61) + hr(1200, 70) + hr(1800, 99), at: at(7200))
            #expect(throws: DatabaseError.self) { try store.ingest(ringID: f.ring.id, kind: .heartRate) }
            #expect(try store.heartRateReadings(ringID: f.ring.id) == before)

            // Same through rebuild: its delete is rolled back too.
            #expect(throws: DatabaseError.self) { try store.rebuild(ringID: f.ring.id) }
            #expect(try store.heartRateReadings(ringID: f.ring.id) == before)
        }
    }

    @Test func rebuildGivesTheSameRowsAsIncrementalIngestion() throws {
        try onEachStore { store in
            let f = try clockedFixture(store, kinds: [.heartRate, .activity, .sleep])
            var hrData: [UInt8] = [], actData: [UInt8] = []
            for n in 1...3 {
                hrData += hr(Double(n - 1) * 3600 + 1800, UInt8(60 + n))
                actData += activityBytes(
                    start: wall(at(Double(n) * 1800), offset: plus1), end: wall(at(Double(n) * 1800 + 1799), offset: plus1), steps: n * 10)
                let s0 = wall(at(Double(n) * 5000), offset: plus1)
                try f.fetch(.heartRate, hrData, at: at(Double(n) * 3600))
                try f.fetch(.activity, actData, at: at(Double(n) * 3600))
                try f.fetch(.sleep, sleepBytes(start: s0, end: s0 + 600, stages: [(242, s0, 600)]), at: at(Double(n) * 3600))
                for kind in [HistoryKind.heartRate, .activity, .sleep] { try store.ingest(ringID: f.ring.id, kind: kind) }
            }
            func values() throws -> ([HeartRateReading], [ActivityInterval], [SleepSessionRecord]) {
                (try store.heartRateReadings(ringID: f.ring.id).map(\.value),
                 try store.activityIntervals(ringID: f.ring.id).map(\.value),
                 try store.sleepSessions(ringID: f.ring.id).map(\.value))
            }
            let incremental = try values()
            #expect(incremental.0.count == 3 && incremental.1.count == 3 && incremental.2.count == 3)
            let results = try store.rebuild(ringID: f.ring.id)
            #expect(results[.heartRate]?.inserted == 3 && results[.sleep]?.inserted == 3)
            #expect(try values() == incremental)
        }
    }

    @Test func combinedIsRejectedExplicitly() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            let missing = UUID()
            #expect(throws: RingStoreError.notIngestible(.combined)) { try store.ingest(ringID: f.ring.id, kind: .combined) }
            #expect(throws: RingStoreError.unknownRing(missing)) {
                try store.ingest(ringID: missing, kind: .heartRate)
            }
        }
    }

    @Test func typedRowsDoNotReferenceHistoryTransfer() throws {
        try onEachStore { store in
            let references = try store.writer.read { db -> [String] in
                var found: [String] = []
                for table in ["heart_rate_reading", "activity_interval", "sleep_session", "sleep_stage"] {
                    for row in try Row.fetchAll(db, sql: "SELECT \"table\" FROM pragma_foreign_key_list('\(table)')") {
                        found.append(row["table"] as String)
                    }
                }
                return found
            }
            #expect(!references.contains("history_transfer"))
            #expect(references.sorted() == ["ring", "ring", "ring", "sleep_session"])
        }
    }

    /// A database at schema v2 with data keeps it when opened (migrating to v3), and then ingests.
    @Test func v2DatabaseMigratesToV3WithItsDataIntact() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let url = dir.appendingPathComponent("ring.sqlite")
        let ringID = UUID()
        let data = hr(600, 61)
        do {
            let pool = try DatabasePool(path: url.path)
            try RingStore.migrator.migrate(pool, upTo: "v2")
            try pool.write { db in
                try db.execute(
                    sql: "INSERT INTO ring (id, protocol_family, first_seen_at) VALUES (?, 'ycbt', 0)", arguments: [ringID.uuidString])
                let hash = RingStore.contentHash(ringID: ringID, kind: .heartRate, data: data)
                try db.execute(
                    sql: RingStore.insertTransferSQL,
                    arguments: [ringID.uuidString, "heartRate", Data([1]), Data(data), 1_800_003_600_000, "Europe/London", Data(hash)])
                try db.execute(
                    sql: RingStore.insertObservationSQL,
                    arguments: [ringID.uuidString, "heartRate", data.count, Data([1]), 1_800_003_600_000, "Europe/London", Data(hash)])
                try db.execute(
                    sql: "INSERT INTO clock_set (ring_id, instant, time_zone_identifier, utc_offset_seconds) VALUES (?, 1800000000000, 'Europe/London', 3600)",
                    arguments: [ringID.uuidString])
                let tables = try String.fetchAll(db, sql: "SELECT name FROM sqlite_master WHERE name = 'heart_rate_reading'")
                #expect(tables.isEmpty)
            }
        }
        let store = try RingStore(url: url)
        #expect(try store.transfers(ringID: ringID).transfers.map(\.transfer.data) == [data])
        #expect(try store.observations(ringID: ringID).observations.count == 1)
        #expect(try store.clockSets(ringID: ringID).count == 1)
        #expect(try store.heartRateReadings(ringID: ringID).isEmpty)
        let r = try store.ingest(ringID: ringID, kind: .heartRate)
        #expect(r.inserted == 1 && r.rowsByTimeSource == [.resolved: 1])
        #expect(try store.heartRateReadings(ringID: ringID).first?.value.time == at(600))
    }
}

struct CarriedFromPruningTests {
    @Test func observationListingCountsRowsWithAnUnknownKind() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            try store.insert(transfer(.heartRate, data: [1, 2]), ringID: ring.id)
            try store.writer.write { db in
                try db.execute(
                    sql: """
                        INSERT INTO transfer_observation
                        (ring_id, kind, data_length, header, fetched_at, time_zone_identifier, content_hash)
                        VALUES (?, 'futureKind', 0, x'', 0, 'UTC', x'00')
                        """,
                    arguments: [ring.id.uuidString])
            }
            let all = try store.observations(ringID: ring.id)
            #expect(all.observations.count == 1 && all.skippedUnknownKind == 1)
            #expect(try store.observations(ringID: ring.id, kind: .heartRate).skippedUnknownKind == 0)
        }
    }

    /// The same ring, kind, data and fetch instant is one observation even when the header or the
    /// zone differ; the first one's header and zone are kept.
    @Test func sameDataAtTheSameInstantIsOneObservationWhateverTheHeaderOrZone() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            try store.insert(transfer(.heartRate, data: [1, 2], header: [1], at: t0, zone: "Europe/London"), ringID: ring.id)
            try store.insert(transfer(.heartRate, data: [1, 2], header: [2, 2], at: t0, zone: "America/New_York"), ringID: ring.id)
            let obs = try store.observations(ringID: ring.id).observations
            #expect(obs.count == 1)
            #expect(obs[0].header == [1] && obs[0].timeZoneIdentifier == "Europe/London")
            // A different instant is a new observation.
            try store.insert(transfer(.heartRate, data: [1, 2], at: t0.addingTimeInterval(1)), ringID: ring.id)
            #expect(try store.observations(ringID: ring.id).observations.count == 2)
        }
    }
}
