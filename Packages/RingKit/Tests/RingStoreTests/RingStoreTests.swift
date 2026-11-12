import Foundation
import GRDB
import Testing
import RingProtocol
@testable import RingStore

// All data here is synthetic.

let t0 = Date(timeIntervalSince1970: 1_800_000_000)

func transfer(
    _ kind: HistoryKind = .heartRate, data: [UInt8] = [1, 2, 3], header: [UInt8] = [9],
    at date: Date? = nil, zone: String = "Europe/London"
) -> HistoryTransfer {
    HistoryTransfer(kind: kind, header: header, data: data, fetchedAt: date ?? t0, timeZoneIdentifier: zone)
}

/// Runs `body` against an in-memory database and against a `DatabasePool` on a temporary file in
/// its own directory (what production uses).
func onEachStore(_ body: (RingStore) throws -> Void) throws {
    try body(RingStore())
    let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: dir) }
    try body(RingStore(url: dir.appendingPathComponent("ring.sqlite")))
}

struct RingStoreTests {
    @Test func migratesFromEmptyAndStartsEmpty() throws {
        try onEachStore { store in
            let rings = try store.rings()
            #expect(rings.isEmpty)
        }
    }

    @Test func registersAndLooksUpRing() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt", model: "M", firmware: "1.0", now: t0)
            #expect(try store.ring(id: ring.id) == ring)
            #expect(try store.rings() == [ring])
            #expect(try store.ring(id: UUID()) == nil)
            try store.updateRing(id: ring.id, model: "M", firmware: "1.1")
            #expect(try store.ring(id: ring.id)?.firmware == "1.1")
            #expect(throws: RingStoreError.self) { try store.updateRing(id: UUID(), model: nil, firmware: nil) }
        }
    }

    @Test func insertsAndReadsBack() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            let t = transfer(data: [])
            guard case .inserted(let id, _) = try store.insert(t, ringID: ring.id) else { Issue.record("expected insert"); return }
            let rows = try store.transfers(ringID: ring.id).transfers
            #expect(rows.count == 1)
            #expect(rows[0].transfer == t)
            #expect(rows[0].ringID == ring.id)
            #expect(try store.transfer(id: id) == rows[0])
            #expect(try store.transfer(id: id + 100) == nil)
        }
    }

    @Test func identicalBytesAreADuplicateEvenWithDifferentTimeHeaderAndZone() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            let first = try store.insert(transfer(), ringID: ring.id)
            let second = try store.insert(
                transfer(header: [7, 7], at: t0.addingTimeInterval(3600), zone: "Asia/Tokyo"), ringID: ring.id)
            guard case .inserted(let id, _) = first else { Issue.record("expected insert"); return }
            #expect(second == .duplicate(existingID: id))
            let rows = try store.transfers(ringID: ring.id).transfers
            #expect(rows.count == 1)
            #expect(rows[0].transfer.timeZoneIdentifier == "Europe/London")  // the first row is kept
        }
    }

    @Test func differentKindOrDataIsNotADuplicate() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            try store.insert(transfer(.heartRate, data: [1]), ringID: ring.id)
            try store.insert(transfer(.sleep, data: [1]), ringID: ring.id)
            try store.insert(transfer(.heartRate, data: [0, 1]), ringID: ring.id)
            #expect(try store.transfers(ringID: ring.id).transfers.count == 3)
        }
    }

    @Test func sameBytesForTwoRingsAreBothKept() throws {
        try onEachStore { store in
            let a = try store.registerRing(protocolFamily: "ycbt")
            let b = try store.registerRing(protocolFamily: "ycbt")
            guard case .inserted = try store.insert(transfer(), ringID: a.id),
                  case .inserted = try store.insert(transfer(), ringID: b.id)
            else { Issue.record("expected two inserts"); return }
            #expect(try store.transfers(ringID: a.id).transfers.count == 1)
            #expect(try store.transfers(ringID: b.id).transfers.count == 1)
        }
    }

    @Test func unknownRingIsRejected() throws {
        try onEachStore { store in
            let id = UUID()
            #expect(throws: RingStoreError.unknownRing(id)) { try store.insert(transfer(), ringID: id) }
            #expect(throws: RingStoreError.unknownRing(id)) {
                try store.recordClockSet(ClockSetEvent(ringID: id, instant: t0, timeZoneIdentifier: "UTC", utcOffsetSeconds: 0))
            }
        }
    }

    @Test func onlyAHashConflictCountsAsADuplicate() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            // A different constraint failing (here the foreign key) must throw, not be ignored.
            #expect(throws: (any Error).self) {
                try store.writer.write { db in
                    try db.execute(
                        sql: """
                            INSERT INTO history_transfer (ring_id, kind, header, data, fetched_at, time_zone_identifier, content_hash)
                            VALUES (?, 'heartRate', x'', x'', 0, 'UTC', x'01')
                            ON CONFLICT(content_hash) DO NOTHING
                            """, arguments: [UUID().uuidString])
                }
            }
            #expect(try store.transfers(ringID: ring.id).transfers.isEmpty)
        }
    }

    @Test func theStoresOwnInsertStatementRejectsOtherConstraintFailures() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            // A NULL header breaks NOT NULL. `INSERT OR IGNORE` would skip it silently; the store's
            // statement must throw. This runs the store's own SQL, so widening its conflict clause fails here.
            #expect(throws: (any Error).self) {
                try store.writer.write { db in
                    try db.execute(
                        sql: RingStore.insertTransferSQL,
                        arguments: [ring.id.uuidString, "heartRate", nil, Data([1]), 0, "UTC", Data([0x01])])
                }
            }
            #expect(try store.transfers(ringID: ring.id).transfers.isEmpty)
        }
    }

    @Test func contentHashIsPinned() throws {
        // Computed independently: SHA-256 of the 16 raw UUID bytes, 0x09, "heartRate", then 01 02 03.
        // If this changes, duplicate detection against existing databases silently breaks.
        let ring = try #require(UUID(uuidString: "00000000-0000-0000-0000-000000000001"))
        let hash = RingStore.contentHash(ringID: ring, kind: .heartRate, data: [1, 2, 3])
        #expect(Hex.encode(hash) == "979e2662281a7573c5b0b1b708f2f3f8aad7fdd4fcdeeaa0710a4dc56cb0941a")
    }

    @Test func listsByKindAndRangeInFetchOrder() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            // Inserted out of order.
            try store.insert(transfer(.heartRate, data: [3], at: t0.addingTimeInterval(200)), ringID: ring.id)
            try store.insert(transfer(.heartRate, data: [1], at: t0), ringID: ring.id)
            try store.insert(transfer(.sleep, data: [2], at: t0.addingTimeInterval(100)), ringID: ring.id)
            func data(_ l: TransferListing) -> [[UInt8]] { l.transfers.map(\.transfer.data) }
            #expect(data(try store.transfers(ringID: ring.id)) == [[1], [2], [3]])
            #expect(data(try store.transfers(ringID: ring.id, kind: .heartRate)) == [[1], [3]])
            let range = t0.addingTimeInterval(50)...t0.addingTimeInterval(200)
            #expect(data(try store.transfers(ringID: ring.id, fetchedWithin: range)) == [[2], [3]])
            #expect(data(try store.transfers(ringID: ring.id, kind: .sleep, fetchedWithin: range)) == [[2]])
        }
    }

    @Test func rangeBoundsAreInclusiveInWholeMilliseconds() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            try store.insert(transfer(data: [1], at: t0), ringID: ring.id)
            try store.insert(transfer(data: [2], at: t0.addingTimeInterval(1)), ringID: ring.id)
            func count(_ r: ClosedRange<Date>) throws -> Int { try store.transfers(ringID: ring.id, fetchedWithin: r).transfers.count }
            #expect(try count(t0...t0.addingTimeInterval(1)) == 2)  // both ends included
            #expect(try count(t0...t0) == 1)
            // Bounds are floored to the millisecond like stored times, so a sub-millisecond offset is the same instant.
            #expect(try count(t0.addingTimeInterval(0.0004)...t0.addingTimeInterval(0.0009)) == 1)
            #expect(try count(t0.addingTimeInterval(0.001)...t0.addingTimeInterval(0.999)) == 0)
        }
    }

    @Test func nonTruncatedTimesReadBackAsTheFloorToTheMillisecond() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            let raw = Date(timeIntervalSince1970: 1_800_000_000.1239)
            try store.insert(transfer(at: raw), ringID: ring.id)
            try store.recordClockSet(ClockSetEvent(ringID: ring.id, instant: raw, timeZoneIdentifier: "UTC", utcOffsetSeconds: 0))
            let floor = HistoryTransfer.date(millisecondsSince1970: 1_800_000_000_123)
            #expect(try store.transfers(ringID: ring.id).transfers[0].transfer.fetchedAt == floor)
            #expect(try store.clockSets(ringID: ring.id)[0].instant == floor)
            // Pre-1970 instants floor toward negative infinity.
            let early = Date(timeIntervalSince1970: -0.0005)
            try store.recordClockSet(ClockSetEvent(ringID: ring.id, instant: early, timeZoneIdentifier: "UTC", utcOffsetSeconds: 0))
            #expect(try store.clockSets(ringID: ring.id)[0].instant == Date(timeIntervalSince1970: -0.001))
        }
    }

    @Test func recordsClockSetsInTimeOrder() throws {
        try onEachStore { store in
            let a = try store.registerRing(protocolFamily: "ycbt")
            let b = try store.registerRing(protocolFamily: "ycbt")
            let late = ClockSetEvent(ringID: a.id, instant: t0.addingTimeInterval(100), timeZoneIdentifier: "Asia/Tokyo", utcOffsetSeconds: 32400)
            let early = ClockSetEvent(ringID: a.id, instant: t0, timeZoneIdentifier: "Europe/London", utcOffsetSeconds: 0)
            try store.recordClockSet(late)
            try store.recordClockSet(early)
            try store.recordClockSet(ClockSetEvent(ringID: b.id, instant: t0, timeZoneIdentifier: "UTC", utcOffsetSeconds: 0))
            #expect(try store.clockSets(ringID: a.id) == [early, late])
        }
    }

    @Test func ringTableHoldsNoDeviceIdentifiers() throws {
        // Only an app-generated id plus family, model, firmware and first-seen time.
        let columns = try RingStore().writer.read { db in try db.columns(in: "ring").map(\.name) }
        #expect(columns == ["id", "protocol_family", "model", "firmware", "first_seen_at"])
    }

    @Test func storageCodesForKindsAreFrozen() {
        // These strings are persisted and hashed. If this test fails, a code changed: do not
        // "fix" the test, restore the code (HistoryKind.name may change freely).
        #expect(HistoryKind.allCases.map(RingStore.KindCode.code(for:)) == ["activity", "sleep", "heartRate", "combined"])
    }

    @Test func unknownKindCodeIsSkippedInListingsAndThrowsByIdAndOthersStillList() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            try store.insert(transfer(.sleep, data: [1]), ringID: ring.id)
            let badID: Int64 = try store.writer.write { db in
                try db.execute(
                    sql: """
                        INSERT INTO history_transfer (ring_id, kind, header, data, fetched_at, time_zone_identifier, content_hash)
                        VALUES (?, 'someFutureKind', x'', x'', 0, 'UTC', x'02')
                        """, arguments: [ring.id.uuidString])
                return db.lastInsertedRowID
            }
            let listing = try store.transfers(ringID: ring.id)
            #expect(listing.transfers.count == 1)
            #expect(listing.skippedUnknownKind == 1)
            #expect(throws: RingStoreError.corruptRow("unknown history kind")) { try store.transfer(id: badID) }
        }
    }

    @Test func corruptValuesThrowInsteadOfTrapping() throws {
        let store = try RingStore()
        let ring = try store.registerRing(protocolFamily: "ycbt")
        // STRICT tables stop most bad types, so corrupt the id text, which the types allow.
        try store.writer.write { db in
            try db.execute(sql: "UPDATE ring SET id = 'not-a-uuid' WHERE id = ?", arguments: [ring.id.uuidString])
        }
        #expect(throws: RingStoreError.corruptRow("ring id")) { try store.rings() }
    }

    @Test func reopensAFileAndReadsItsRows() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let url = dir.appendingPathComponent("ring.sqlite")
        let ringID: UUID
        let firstID: Int64
        do {
            let store = try RingStore(url: url)
            ringID = try store.registerRing(protocolFamily: "ycbt").id
            guard case .inserted(let id, _) = try store.insert(transfer(), ringID: ringID) else {
                Issue.record("expected insert"); return
            }
            firstID = id
            try store.recordClockSet(ClockSetEvent(ringID: ringID, instant: t0, timeZoneIdentifier: "UTC", utcOffsetSeconds: 0))
        }
        let reopened = try RingStore(url: url)
        #expect(try reopened.transfers(ringID: ringID).transfers.count == 1)
        #expect(try reopened.clockSets(ringID: ringID).count == 1)
        #expect(try reopened.insert(transfer(), ringID: ringID) == .duplicate(existingID: firstID))
    }

    @Test func fileIsOwnerOnlyEvenIfItExistedWithLooserMode() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let url = dir.appendingPathComponent("ring.sqlite")
        #expect(FileManager.default.createFile(atPath: url.path, contents: nil, attributes: [.posixPermissions: 0o644]))
        _ = try RingStore(url: url)
        func mode(_ path: String) throws -> Int? { try FileManager.default.attributesOfItem(atPath: path)[.posixPermissions] as? Int }
        #expect(try mode(url.path) == 0o600)
    }

    @Test func failureToOpenReportsPathAndErrno() throws {
        let url = URL(fileURLWithPath: "/nonexistent-directory-for-test/ring.sqlite")
        do {
            _ = try RingStore(url: url)
            Issue.record("expected failure")
        } catch let RingStoreError.fileAccess(path, code) {
            #expect(path == url.path)
            #expect(code == ENOENT)
        }
    }

    @Test func concurrentIdenticalInsertsYieldOneRow() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let store = try RingStore(url: dir.appendingPathComponent("ring.sqlite"))
        let ring = try store.registerRing(protocolFamily: "ycbt")
        let ringID = ring.id
        let outcomes = try await withThrowingTaskGroup(of: InsertOutcome.self) { group in
            for i in 0..<32 {
                group.addTask {
                    try store.insert(transfer(at: t0.addingTimeInterval(Double(i))), ringID: ringID)
                }
            }
            var all: [InsertOutcome] = []
            for try await outcome in group { all.append(outcome) }
            return all
        }
        let inserted = outcomes.filter { if case .inserted = $0 { true } else { false } }
        #expect(inserted.count == 1)
        #expect(outcomes.count == 32)
        #expect(try store.transfers(ringID: ringID).transfers.count == 1)
    }
}
