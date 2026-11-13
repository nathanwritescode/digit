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

/// The synchronous twins, reachable from an `async` test: inside an async context Swift resolves a
/// plain `store.rings()` to the async overload, so these non-async wrappers pin the sync one.
struct SyncView {
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
    func activityIntervals(ringID: UUID) throws -> [StoredRow<ActivityInterval>] { try store.activityIntervals(ringID: ringID) }
    func sleepSessions(ringID: UUID) throws -> [StoredRow<SleepSessionRecord>] { try store.sleepSessions(ringID: ringID) }
}

func sync(_ store: RingStore) -> SyncView { SyncView(store: store) }

struct AsyncTwinTests {
    @Test func ringFunctionsMatchTheirSynchronousTwins() async throws {
        try await onEachStoreAsync { store in
            let ring = try await store.registerRing(protocolFamily: "ycbt", model: "M", firmware: "1.0", now: t0)
            #expect(try sync(store).ring(id: ring.id) == ring)
            let syncRing = try sync(store).registerRing(protocolFamily: "ycbt", now: t0.addingTimeInterval(5))
            #expect(try await store.ring(id: syncRing.id) == syncRing)
            #expect(try await store.ring(id: UUID()) == nil)
            #expect(try await store.rings() == sync(store).rings())
            #expect(try await store.rings() == [ring, syncRing])
            try await store.updateRing(id: ring.id, model: "N", firmware: "2.0")
            #expect(try sync(store).ring(id: ring.id)?.firmware == "2.0")
            let missing = UUID()
            await #expect(throws: RingStoreError.unknownRing(missing)) {
                try await store.updateRing(id: missing, model: nil, firmware: nil)
            }
        }
    }

    @Test func transferFunctionsMatchTheirSynchronousTwins() async throws {
        try await onEachStoreAsync { store in
            let ring = try sync(store).registerRing(protocolFamily: "ycbt")
            let first = transfer(data: [1, 2], at: t0)
            let longer = transfer(data: [1, 2, 3], at: t0.addingTimeInterval(10))
            let outcomeAsync = try await store.insert(first, ringID: ring.id)
            guard case .inserted(let id, 0) = outcomeAsync else { Issue.record("expected insert"); return }
            #expect(try await store.insert(first, ringID: ring.id) == .duplicate(existingID: id))
            // The synchronous twin sees the same state and prunes the first row.
            guard case .inserted(_, let pruned) = try sync(store).insert(longer, ringID: ring.id) else {
                Issue.record("expected insert"); return
            }
            #expect(pruned == 1)
            #expect(try await store.transfer(id: id) == sync(store).transfer(id: id))
            #expect(try await store.transfer(id: id) == nil)
            let listing = try await store.transfers(ringID: ring.id)
            #expect(listing == (try sync(store).transfers(ringID: ring.id)))
            #expect(listing.transfers.count == 1)
            let range = t0...t0.addingTimeInterval(20)
            #expect(try await store.transfers(ringID: ring.id, kind: .heartRate, fetchedWithin: range)
                    == sync(store).transfers(ringID: ring.id, kind: .heartRate, fetchedWithin: range))
            let log = try await store.observations(ringID: ring.id)
            #expect(log == (try sync(store).observations(ringID: ring.id)))
            #expect(log.observations.count == 2)
            #expect(try await store.observations(ringID: ring.id, kind: .sleep).observations.isEmpty)
            await #expect(throws: RingStoreError.self) { try await store.insert(first, ringID: UUID()) }
        }
    }

    @Test func clockSetsMatchTheirSynchronousTwins() async throws {
        try await onEachStoreAsync { store in
            let ring = try sync(store).registerRing(protocolFamily: "ycbt")
            let a = ClockSetEvent(ringID: ring.id, instant: t0, timeZoneIdentifier: "UTC", utcOffsetSeconds: 0)
            let b = ClockSetEvent(
                ringID: ring.id, instant: t0.addingTimeInterval(60), timeZoneIdentifier: "Europe/London", utcOffsetSeconds: 3600)
            try await store.recordClockSet(a)
            try sync(store).recordClockSet(b)
            #expect(try await store.clockSets(ringID: ring.id) == [a, b])
            #expect(try await store.clockSets(ringID: ring.id) == sync(store).clockSets(ringID: ring.id))
            let unknown = ClockSetEvent(ringID: UUID(), instant: t0, timeZoneIdentifier: "UTC", utcOffsetSeconds: 0)
            await #expect(throws: RingStoreError.self) { try await store.recordClockSet(unknown) }
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
            #expect(try sync(store).ingest(ringID: f.ring.id, kind: .heartRate).unchanged == 2)
            try await store.ingest(ringID: f.ring.id, kind: .activity)
            try await store.ingest(ringID: f.ring.id, kind: .sleep)
            await #expect(throws: RingStoreError.self) { try await store.ingest(ringID: f.ring.id, kind: .combined) }
            await #expect(throws: RingStoreError.self) { try await store.ingest(ringID: UUID(), kind: .heartRate) }

            let hr = try await store.heartRateReadings(ringID: f.ring.id)
            let act = try await store.activityIntervals(ringID: f.ring.id)
            let sleep = try await store.sleepSessions(ringID: f.ring.id)
            #expect(hr.map(\.value) == (try sync(store).heartRateReadings(ringID: f.ring.id)).map(\.value))
            #expect(act.map(\.value) == (try sync(store).activityIntervals(ringID: f.ring.id)).map(\.value))
            #expect(sleep.map(\.value) == (try sync(store).sleepSessions(ringID: f.ring.id)).map(\.value))
            #expect(hr.count == 2 && act.count == 1 && sleep.count == 1)

            let rebuilt = try await store.rebuild(ringID: f.ring.id)
            #expect(rebuilt == (try sync(store).rebuild(ringID: f.ring.id)))
            #expect(try await store.heartRateReadings(ringID: f.ring.id).map(\.value) == hr.map(\.value))
            await #expect(throws: RingStoreError.self) { try await store.rebuild(ringID: UUID()) }
        }
    }
}

struct PeripheralIdentifierTests {
    @Test func setReplaceAndClear() async throws {
        try await onEachStoreAsync { store in
            let ring = try sync(store).registerRing(protocolFamily: "ycbt")
            let other = try sync(store).registerRing(protocolFamily: "ycbt")
            #expect(ring.peripheralIdentifier == nil)
            let a = UUID(), b = UUID()
            try await store.setPeripheralIdentifier(a, forRing: ring.id)
            #expect(try sync(store).ring(id: ring.id)?.peripheralIdentifier == a)
            try sync(store).setPeripheralIdentifier(b, forRing: ring.id)
            #expect(try await store.ring(id: ring.id)?.peripheralIdentifier == b)
            #expect(try sync(store).ring(id: other.id)?.peripheralIdentifier == nil)
            try await store.setPeripheralIdentifier(nil, forRing: ring.id)
            #expect(try sync(store).ring(id: ring.id)?.peripheralIdentifier == nil)
            // Other fields survive.
            try await store.setPeripheralIdentifier(a, forRing: ring.id)
            try sync(store).updateRing(id: ring.id, model: "M", firmware: "1")
            #expect(try sync(store).ring(id: ring.id)?.peripheralIdentifier == a)
        }
    }

    @Test func unknownRingThrows() async throws {
        try await onEachStoreAsync { store in
            let missing = UUID()
            await #expect(throws: RingStoreError.unknownRing(missing)) {
                try await store.setPeripheralIdentifier(UUID(), forRing: missing)
            }
            #expect(throws: RingStoreError.unknownRing(missing)) {
                try sync(store).setPeripheralIdentifier(nil, forRing: missing)
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
}
