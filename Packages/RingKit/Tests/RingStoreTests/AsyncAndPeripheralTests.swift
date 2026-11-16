import Foundation
import GRDB
import Testing
import RingProtocol
@testable import RingStore

// All data here is synthetic.

/// The async twin of `onEachStore`: an in-memory database, then a `DatabasePool` on a temporary file.
func onEachStoreAsync(_ body: (RingStore) async throws -> Void) async throws {
    try await body(RingStore())
    let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: dir) }
    try await body(RingStore(url: dir.appendingPathComponent("ring.sqlite")))
}

/// The internal blocking forms, reachable from an `async` test or closure: inside an async context
/// Swift resolves a plain `store.rings()` to the async overload, so these non-async wrappers pin the
/// blocking one. Calling through `blocking(store)` also means adding an `await` later cannot
/// silently switch a test to the async form.
struct BlockingView {
    let store: RingStore
    func ring(id: UUID) throws -> StoredRing? { try store.ring(id: id) }
    func rings() throws -> [StoredRing] { try store.rings() }
    @discardableResult
    func registerRing(protocolFamily: String, now: Date) throws -> StoredRing {
        try store.registerRing(protocolFamily: protocolFamily, now: now)
    }
    @discardableResult
    func registerRing(protocolFamily: String) throws -> StoredRing { try store.registerRing(protocolFamily: protocolFamily) }
    func updateRing(id: UUID, model: String?, firmware: String?) throws {
        try store.updateRing(id: id, model: model, firmware: firmware)
    }
    func setPeripheralIdentifier(_ identifier: UUID?, forRing id: UUID) throws {
        try store.setPeripheralIdentifier(identifier, forRing: id)
    }
    @discardableResult
    func insert(_ transfer: HistoryTransfer, ringID: UUID) throws -> InsertOutcome {
        try store.insert(transfer, ringID: ringID)
    }
    func transfer(id: Int64) throws -> StoredTransfer? { try store.transfer(id: id) }
    func transfers(ringID: UUID, kind: HistoryKind? = nil, fetchedWithin range: ClosedRange<Date>? = nil) throws -> TransferListing {
        try store.transfers(ringID: ringID, kind: kind, fetchedWithin: range)
    }
    func observations(ringID: UUID) throws -> ObservationListing { try store.observations(ringID: ringID) }
    func recordClockSet(_ event: ClockSetEvent) throws { try store.recordClockSet(event) }
    func clockSets(ringID: UUID) throws -> [ClockSetEvent] { try store.clockSets(ringID: ringID) }
    @discardableResult
    func ingest(ringID: UUID, kind: HistoryKind) throws -> IngestionResult { try store.ingest(ringID: ringID, kind: kind) }
    @discardableResult
    func rebuild(ringID: UUID) throws -> [HistoryKind: IngestionResult] { try store.rebuild(ringID: ringID) }
    func heartRateReadings(ringID: UUID) throws -> [StoredRow<HeartRateReading>] { try store.heartRateReadings(ringID: ringID) }
    func vitalsReadings(ringID: UUID) throws -> [StoredRow<VitalsReading>] { try store.vitalsReadings(ringID: ringID) }
    func activityIntervals(ringID: UUID) throws -> [StoredRow<ActivityInterval>] { try store.activityIntervals(ringID: ringID) }
    func sleepSessions(ringID: UUID) throws -> [StoredRow<SleepSessionRecord>] { try store.sleepSessions(ringID: ringID) }
}

func blocking(_ store: RingStore) -> BlockingView { BlockingView(store: store) }

/// Raw SQL from an async test, where `writer.write` would resolve to its async overload.
private func rawExecute(_ store: RingStore, _ sql: String, _ arguments: StatementArguments) throws {
    try store.writer.write { db in try db.execute(sql: sql, arguments: arguments) }
}

struct AsyncTwinTests {
    @Test func ringFunctionsMatchTheirSynchronousTwins() async throws {
        try await onEachStoreAsync { store in
            let ring = try await store.registerRing(protocolFamily: "ycbt", model: "M", firmware: "1.0", now: t0)
            // The returned value reflects the arguments, not just what the store reads back.
            #expect(ring.protocolFamily == "ycbt" && ring.model == "M" && ring.firmware == "1.0")
            #expect(ring.firstSeenAt == t0 && ring.peripheralIdentifier == nil)
            #expect(try blocking(store).ring(id: ring.id) == ring)
            let syncRing = try blocking(store).registerRing(protocolFamily: "ycbt", now: t0.addingTimeInterval(5))
            #expect(try await store.ring(id: syncRing.id) == syncRing)
            #expect(try await store.ring(id: UUID()) == nil)
            #expect(try await store.rings() == blocking(store).rings())
            #expect(try await store.rings() == [ring, syncRing])
            try await store.updateRing(id: ring.id, model: "N", firmware: "2.0")
            #expect(try blocking(store).ring(id: ring.id)?.firmware == "2.0")
            let missing = UUID()
            await #expect(throws: RingStoreError.unknownRing(missing)) {
                try await store.updateRing(id: missing, model: nil, firmware: nil)
            }
        }
    }

    @Test func transferFunctionsMatchTheirSynchronousTwins() async throws {
        try await onEachStoreAsync { store in
            let ring = try blocking(store).registerRing(protocolFamily: "ycbt")
            let first = transfer(data: [1, 2], at: t0)
            let longer = transfer(data: [1, 2, 3], at: t0.addingTimeInterval(10))
            let outcomeAsync = try await store.insert(first, ringID: ring.id)
            guard case .inserted(let id, 0) = outcomeAsync else { Issue.record("expected insert"); return }
            #expect(try await store.insert(first, ringID: ring.id) == .duplicate(existingID: id))
            // The synchronous twin sees the same state and prunes the first row.
            guard case .inserted(_, let pruned) = try blocking(store).insert(longer, ringID: ring.id) else {
                Issue.record("expected insert"); return
            }
            #expect(pruned == 1)
            #expect(try await store.transfer(id: id) == blocking(store).transfer(id: id))
            #expect(try await store.transfer(id: id) == nil)
            let listing = try await store.transfers(ringID: ring.id)
            #expect(listing == (try blocking(store).transfers(ringID: ring.id)))
            #expect(listing.transfers.count == 1)
            let range = t0...t0.addingTimeInterval(20)
            #expect(try await store.transfers(ringID: ring.id, kind: .heartRate, fetchedWithin: range)
                    == blocking(store).transfers(ringID: ring.id, kind: .heartRate, fetchedWithin: range))
            let log = try await store.observations(ringID: ring.id)
            #expect(log == (try blocking(store).observations(ringID: ring.id)))
            #expect(log.observations.count == 2)
            #expect(try await store.observations(ringID: ring.id, kind: .sleep).observations.isEmpty)
            let missing = UUID()
            await #expect(throws: RingStoreError.unknownRing(missing)) { try await store.insert(first, ringID: missing) }
        }
    }

    /// Synthetic data. Two kinds and four fetch times, so a kind filter and a range each exclude rows.
    @Test func transfersFilterByKindAndFetchedRange() async throws {
        try await onEachStoreAsync { store in
            let ring = try blocking(store).registerRing(protocolFamily: "ycbt")
            func at(_ s: Double) -> Date { t0.addingTimeInterval(s) }
            // Not prefixes of one another, so none is pruned.
            try await store.insert(transfer(.heartRate, data: [1], at: at(0)), ringID: ring.id)
            try await store.insert(transfer(.sleep, data: [2], at: at(10)), ringID: ring.id)
            try await store.insert(transfer(.heartRate, data: [3], at: at(20)), ringID: ring.id)
            try await store.insert(transfer(.sleep, data: [4], at: at(30)), ringID: ring.id)

            func data(_ listing: TransferListing) -> [[UInt8]] { listing.transfers.map(\.transfer.data) }
            #expect(data(try await store.transfers(ringID: ring.id)) == [[1], [2], [3], [4]])
            #expect(data(try await store.transfers(ringID: ring.id, kind: .heartRate)) == [[1], [3]])
            #expect(data(try await store.transfers(ringID: ring.id, kind: .sleep)) == [[2], [4]])
            #expect(data(try await store.transfers(ringID: ring.id, kind: .activity)).isEmpty)
            // Inclusive at both ends, and rows outside it are left out.
            #expect(data(try await store.transfers(ringID: ring.id, fetchedWithin: at(10)...at(20))) == [[2], [3]])
            #expect(data(try await store.transfers(ringID: ring.id, fetchedWithin: at(11)...at(29))) == [[3]])
            #expect(data(try await store.transfers(ringID: ring.id, kind: .sleep, fetchedWithin: at(5)...at(25))) == [[2]])
            #expect(data(try await store.transfers(ringID: ring.id, fetchedWithin: at(40)...at(50))).isEmpty)
            for (kind, range) in [(nil, at(10)...at(20)), (HistoryKind.sleep, at(5)...at(25)), (.heartRate, at(0)...at(100))] {
                #expect(try await store.transfers(ringID: ring.id, kind: kind, fetchedWithin: range)
                        == blocking(store).transfers(ringID: ring.id, kind: kind, fetchedWithin: range))
            }
        }
    }

    @Test func clockSetsMatchTheirSynchronousTwins() async throws {
        try await onEachStoreAsync { store in
            let ring = try blocking(store).registerRing(protocolFamily: "ycbt")
            let a = ClockSetEvent(ringID: ring.id, instant: t0, timeZoneIdentifier: "UTC", utcOffsetSeconds: 0)
            let b = ClockSetEvent(
                ringID: ring.id, instant: t0.addingTimeInterval(60), timeZoneIdentifier: "Europe/London", utcOffsetSeconds: 3600)
            try await store.recordClockSet(a)
            try blocking(store).recordClockSet(b)
            #expect(try await store.clockSets(ringID: ring.id) == [a, b])
            #expect(try await store.clockSets(ringID: ring.id) == blocking(store).clockSets(ringID: ring.id))
            let unknown = ClockSetEvent(ringID: UUID(), instant: t0, timeZoneIdentifier: "UTC", utcOffsetSeconds: 0)
            await #expect(throws: RingStoreError.unknownRing(unknown.ringID)) { try await store.recordClockSet(unknown) }
        }
    }

    @Test func ingestionAndTypedReadsMatchTheirSynchronousTwins() async throws {
        try await onEachStoreAsync { store in
            let f = try Fixture(store)
            let hr0 = wall(t0, offset: 0)
            try f.setEarlierAndFirstClocks(at: t0.addingTimeInterval(-86_400), offset: 0)
            try f.fetch(.heartRate, hrBytes(hr0, bpm: 60) + hrBytes(hr0 + 600, bpm: 61), at: t0.addingTimeInterval(1000))
            try f.fetch(.activity, activityBytes(start: hr0, end: hr0 + 1800, steps: 10), at: t0.addingTimeInterval(1000))
            try f.fetch(
                .sleep, sleepBytes(start: hr0, end: hr0 + 600, stages: [(kind: 2, start: hr0, seconds: 600)]),
                at: t0.addingTimeInterval(1000))

            // A repeat ingest reports no new rows, so compare the rows it leaves rather than results.
            let ingested = try await store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(ingested.inserted == 2)
            #expect(try blocking(store).ingest(ringID: f.ring.id, kind: .heartRate).unchanged == 2)
            try await store.ingest(ringID: f.ring.id, kind: .activity)
            try await store.ingest(ringID: f.ring.id, kind: .sleep)
            // Combined records are ingestible (vitals). `notIngestible` is reserved for a future kind.
            try f.fetch(.combined, combinedBytes(hr0, spo2: 97, resp: 16, hrv: 37, cvrr: 30), at: t0.addingTimeInterval(1000))
            let combined = try await store.ingest(ringID: f.ring.id, kind: .combined)
            #expect(combined.inserted == 1)
            #expect(try blocking(store).ingest(ringID: f.ring.id, kind: .combined).unchanged == 1)
            let missing = UUID()
            await #expect(throws: RingStoreError.unknownRing(missing)) { try await store.ingest(ringID: missing, kind: .heartRate) }

            let hr = try await store.heartRateReadings(ringID: f.ring.id)
            let act = try await store.activityIntervals(ringID: f.ring.id)
            let sleep = try await store.sleepSessions(ringID: f.ring.id)
            #expect(hr.map(\.value) == (try blocking(store).heartRateReadings(ringID: f.ring.id)).map(\.value))
            #expect(act.map(\.value) == (try blocking(store).activityIntervals(ringID: f.ring.id)).map(\.value))
            #expect(sleep.map(\.value) == (try blocking(store).sleepSessions(ringID: f.ring.id)).map(\.value))
            let vitals = try await store.vitalsReadings(ringID: f.ring.id)
            #expect(vitals.map(\.value) == (try blocking(store).vitalsReadings(ringID: f.ring.id)).map(\.value))
            #expect(vitals.map(\.value.spo2) == [97])
            #expect(hr.count == 2 && act.count == 1 && sleep.count == 1)

            let rebuilt = try await store.rebuild(ringID: f.ring.id)
            #expect(rebuilt == (try blocking(store).rebuild(ringID: f.ring.id)))
            #expect(try await store.heartRateReadings(ringID: f.ring.id).map(\.value) == hr.map(\.value))
            #expect(try await store.vitalsReadings(ringID: f.ring.id).map(\.value) == vitals.map(\.value))
            #expect(Set(rebuilt.keys) == [.heartRate, .activity, .sleep, .combined])
            #expect(rebuilt[.combined]?.inserted == 1)
            await #expect(throws: RingStoreError.unknownRing(missing)) { try await store.rebuild(ringID: missing) }
        }
    }
}

struct PeripheralIdentifierTests {
    @Test func setReplaceAndClear() async throws {
        try await onEachStoreAsync { store in
            let ring = try blocking(store).registerRing(protocolFamily: "ycbt")
            let other = try blocking(store).registerRing(protocolFamily: "ycbt")
            #expect(ring.peripheralIdentifier == nil)
            let a = UUID(), b = UUID()
            try await store.setPeripheralIdentifier(a, forRing: ring.id)
            #expect(try blocking(store).ring(id: ring.id)?.peripheralIdentifier == a)
            try blocking(store).setPeripheralIdentifier(b, forRing: ring.id)
            #expect(try await store.ring(id: ring.id)?.peripheralIdentifier == b)
            #expect(try blocking(store).ring(id: other.id)?.peripheralIdentifier == nil)
            try await store.setPeripheralIdentifier(nil, forRing: ring.id)
            #expect(try blocking(store).ring(id: ring.id)?.peripheralIdentifier == nil)
            // Other fields survive.
            try await store.setPeripheralIdentifier(a, forRing: ring.id)
            try blocking(store).updateRing(id: ring.id, model: "M", firmware: "1")
            #expect(try blocking(store).ring(id: ring.id)?.peripheralIdentifier == a)
        }
    }

    /// One identifier, one ring: setting it on a second ring moves it.
    @Test func settingAnIdentifierOnAnotherRingMovesIt() async throws {
        try await onEachStoreAsync { store in
            let a = try blocking(store).registerRing(protocolFamily: "ycbt")
            let b = try blocking(store).registerRing(protocolFamily: "ycbt")
            let third = try blocking(store).registerRing(protocolFamily: "ycbt")
            let x = UUID(), y = UUID()
            try await store.setPeripheralIdentifier(x, forRing: a.id)
            try await store.setPeripheralIdentifier(y, forRing: third.id)
            try await store.setPeripheralIdentifier(x, forRing: b.id)
            #expect(try await store.ring(id: a.id)?.peripheralIdentifier == nil)
            #expect(try await store.ring(id: b.id)?.peripheralIdentifier == x)
            #expect(try await store.ring(id: third.id)?.peripheralIdentifier == y)
            // Setting the value a ring already holds changes nothing.
            try blocking(store).setPeripheralIdentifier(x, forRing: b.id)
            #expect(try await store.ring(id: b.id)?.peripheralIdentifier == x)
            // And back again with the blocking form.
            try blocking(store).setPeripheralIdentifier(x, forRing: a.id)
            #expect(try await store.ring(id: a.id)?.peripheralIdentifier == x)
            #expect(try await store.ring(id: b.id)?.peripheralIdentifier == nil)
        }
    }

    /// The unique index refuses a raw duplicate, so a bug in the move cannot leave two rings with one.
    @Test func theIndexRefusesADuplicateIdentifier() throws {
        try onEachStore { store in
            let a = try store.registerRing(protocolFamily: "ycbt")
            let b = try store.registerRing(protocolFamily: "ycbt")
            let x = UUID().uuidString
            try store.writer.write { db in
                try db.execute(sql: "UPDATE ring SET peripheral_identifier = ? WHERE id = ?", arguments: [x, a.id.uuidString])
            }
            #expect(throws: DatabaseError.self) {
                try store.writer.write { db in
                    try db.execute(sql: "UPDATE ring SET peripheral_identifier = ? WHERE id = ?", arguments: [x, b.id.uuidString])
                }
            }
            // Many rings may have none.
            try store.writer.write { db in
                try db.execute(sql: "UPDATE ring SET peripheral_identifier = NULL")
            }
            #expect(try store.rings().allSatisfy { $0.peripheralIdentifier == nil })
        }
    }

    /// A stored value that is not a UUID reads as nil and does not hide any ring.
    @Test func aCorruptStoredIdentifierReadsAsNil() async throws {
        try await onEachStoreAsync { store in
            let a = try blocking(store).registerRing(protocolFamily: "ycbt")
            let b = try blocking(store).registerRing(protocolFamily: "ycbt")
            let good = UUID()
            try await store.setPeripheralIdentifier(good, forRing: b.id)
            for bad in ["not-a-uuid", ""] {
                try rawExecute(store, "UPDATE ring SET peripheral_identifier = ? WHERE id = ?", [bad, a.id.uuidString])
                #expect(try await store.ring(id: a.id)?.peripheralIdentifier == nil)
                #expect(try await store.ring(id: a.id)?.protocolFamily == "ycbt")
                let rings = try await store.rings()
                #expect(rings.count == 2)
                #expect(rings.first { $0.id == b.id }?.peripheralIdentifier == good)
                #expect(try blocking(store).rings() == rings)
            }
        }
    }

    /// The identifier is a device identifier: it must not reach a string, a debug description or a dump.
    @Test func descriptionsDoNotContainTheIdentifier() {
        let identifier = UUID()
        let ring = StoredRing(
            id: UUID(), protocolFamily: "ycbt", model: "M", firmware: "1", firstSeenAt: t0, peripheralIdentifier: identifier)
        var dumped = ""
        dump(ring, to: &dumped)
        let texts = ["\(ring)", String(describing: ring), String(reflecting: ring), ring.description, ring.debugDescription, dumped]
        for text in texts {
            #expect(!text.contains(identifier.uuidString))
            #expect(!text.lowercased().contains(identifier.uuidString.lowercased()))
            #expect(text.contains("ycbt"))
        }
        #expect(ring.description.contains("peripheralIdentifier: set"))
        var bare = ring
        bare.peripheralIdentifier = nil
        #expect(bare.description.contains("peripheralIdentifier: none"))
        // The mirror keeps every field, with the identifier replaced.
        #expect(Mirror(reflecting: ring).children.count == 6)
    }

    @Test func unknownRingThrows() async throws {
        try await onEachStoreAsync { store in
            let missing = UUID()
            // The unknown-ring error leaves every other row untouched.
            let held = try blocking(store).registerRing(protocolFamily: "ycbt")
            let identifier = UUID()
            try await store.setPeripheralIdentifier(identifier, forRing: held.id)
            await #expect(throws: RingStoreError.unknownRing(missing)) {
                try await store.setPeripheralIdentifier(identifier, forRing: missing)
            }
            #expect(try await store.ring(id: held.id)?.peripheralIdentifier == identifier)
            await #expect(throws: RingStoreError.unknownRing(missing)) {
                try await store.setPeripheralIdentifier(UUID(), forRing: missing)
            }
            #expect(throws: RingStoreError.unknownRing(missing)) {
                try blocking(store).setPeripheralIdentifier(nil, forRing: missing)
            }
        }
    }

    @Test func migratingFromV3KeepsRingsWithNoIdentifier() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let url = dir.appendingPathComponent("ring.sqlite")
        let id = UUID()
        // A database exactly as v3 left it, holding one ring and one clock-set.
        do {
            let queue = try DatabaseQueue(path: url.path)
            try RingStore.migrator.migrate(queue, upTo: "v3")
            try queue.write { db in
                #expect(try !db.columns(in: "ring").contains { $0.name == "peripheral_identifier" })
                try db.execute(
                    sql: "INSERT INTO ring (id, protocol_family, model, firmware, first_seen_at) VALUES (?, 'ycbt', 'M', '1', 1000)",
                    arguments: [id.uuidString])
                try db.execute(
                    sql: "INSERT INTO clock_set (ring_id, instant, time_zone_identifier, utc_offset_seconds) VALUES (?, 5, 'UTC', 0)",
                    arguments: [id.uuidString])
            }
        }
        let store = try RingStore(url: url)
        let ring = try #require(try store.ring(id: id))
        #expect(ring.model == "M" && ring.firmware == "1")
        #expect(ring.peripheralIdentifier == nil)
        #expect(try store.clockSets(ringID: id).count == 1)
        let p = UUID()
        try store.setPeripheralIdentifier(p, forRing: id)
        #expect(try store.ring(id: id)?.peripheralIdentifier == p)
    }

    /// A database exactly as `v4-vitals` left it (rings, a transfer, typed rows of every kind and a
    /// vitals row) gets the column from `v5-ring-peripheral` and keeps everything.
    @Test func migratingFromV4KeepsEverything() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let url = dir.appendingPathComponent("ring.sqlite")
        let id = UUID(), other = UUID()
        let data = hrBytes(1000, bpm: 60)
        try makeV4Database(at: url, id: id, other: other, data: data)
        let store = try RingStore(url: url)
        #expect(try await store.rings().count == 2)
        #expect(try await store.rings().allSatisfy { $0.peripheralIdentifier == nil })
        #expect(try await store.transfers(ringID: id).transfers.map(\.transfer.data) == [data])
        #expect(try await store.heartRateReadings(ringID: id).map(\.value.bpm) == [61])
        #expect(try await store.activityIntervals(ringID: id).map(\.value.steps) == [10])
        #expect(try await store.sleepSessions(ringID: id).count == 1)
        #expect(try await store.vitalsReadings(ringID: id).map(\.value.spo2) == [97])
        let p = UUID()
        try await store.setPeripheralIdentifier(p, forRing: id)
        #expect(try await store.ring(id: id)?.peripheralIdentifier == p)
        #expect(try await store.vitalsReadings(ringID: id).count == 1)
    }
}

/// A database exactly as `v4-vitals` left it. Synchronous on purpose (GRDB's `write` is async-overloaded).
private func makeV4Database(at url: URL, id: UUID, other: UUID, data: [UInt8]) throws {
    let hash = RingStore.contentHash(ringID: id, kind: .heartRate, data: data)
    let queue = try DatabaseQueue(path: url.path)
    try RingStore.migrator.migrate(queue, upTo: "v4-vitals")
    try queue.write { db in
        #expect(try !db.columns(in: "ring").contains { $0.name == "peripheral_identifier" })
        #expect(try db.tableExists("vitals_reading"))
        for ring in [id, other] {
            try db.execute(
                sql: "INSERT INTO ring (id, protocol_family, model, firmware, first_seen_at) VALUES (?, 'ycbt', 'M', '1', 1000)",
                arguments: [ring.uuidString])
        }
        try db.execute(
            sql: RingStore.insertTransferSQL,
            arguments: [id.uuidString, "heartRate", Data([1]), Data(data), 5000, "UTC", Data(hash)])
        try db.execute(
            sql: "INSERT INTO heart_rate_reading (ring_id, time, bpm, mode, utc_offset_seconds, ring_local_seconds, time_source) VALUES (?, 5000, 61, 0, 0, 0, 'resolved')",
            arguments: [id.uuidString])
        try db.execute(
            sql: "INSERT INTO activity_interval (ring_id, start, \"end\", steps, distance_meters, kilocalories, utc_offset_seconds, ring_local_seconds, time_source) VALUES (?, 0, 1800000, 10, 0, 0, 0, 0, 'resolved')",
            arguments: [id.uuidString])
        try db.execute(
            sql: "INSERT INTO sleep_session (ring_id, start, \"end\", utc_offset_seconds, ring_local_seconds, time_source) VALUES (?, 0, 600000, 0, 0, 'resolved')",
            arguments: [id.uuidString])
        try db.execute(
            sql: "INSERT INTO vitals_reading (ring_id, time, spo2, utc_offset_seconds, ring_local_seconds, time_source) VALUES (?, 7000, 97, 0, 0, 'resolved')",
            arguments: [id.uuidString])
    }
}
