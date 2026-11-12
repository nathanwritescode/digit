import Foundation
import Testing
import RingProtocol
@testable import RingStore

// All data here is synthetic.
struct RingStoreTests {
    let t0 = Date(timeIntervalSince1970: 1_800_000_000)

    func transfer(
        _ kind: HistoryKind = .heartRate, data: [UInt8] = [1, 2, 3], header: [UInt8] = [9],
        at date: Date? = nil, zone: String = "Europe/London"
    ) -> HistoryTransfer {
        HistoryTransfer(kind: kind, header: header, data: data, fetchedAt: date ?? t0, timeZoneIdentifier: zone)
    }

    @Test func migratesFromEmptyAndStartsEmpty() throws {
        let store = try RingStore()
        #expect(try store.rings().isEmpty)
    }

    @Test func registersAndLooksUpRing() throws {
        let store = try RingStore()
        let ring = try store.registerRing(protocolFamily: "ycbt", model: "M", firmware: "1.0", now: t0)
        #expect(try store.ring(id: ring.id) == ring)
        #expect(try store.rings() == [ring])
        #expect(try store.ring(id: UUID()) == nil)
        try store.updateRing(id: ring.id, model: "M", firmware: "1.1")
        #expect(try store.ring(id: ring.id)?.firmware == "1.1")
        #expect(throws: RingStoreError.self) { try store.updateRing(id: UUID(), model: nil, firmware: nil) }
    }

    @Test func insertsAndReadsBack() throws {
        let store = try RingStore()
        let ring = try store.registerRing(protocolFamily: "ycbt")
        let t = transfer(data: [])
        guard case .inserted = try store.insert(t, ringID: ring.id) else { Issue.record("expected insert"); return }
        let rows = try store.transfers(ringID: ring.id)
        #expect(rows.count == 1)
        #expect(rows[0].transfer == t)
        #expect(rows[0].ringID == ring.id)
    }

    @Test func identicalBytesAreADuplicateEvenWithDifferentTimeHeaderAndZone() throws {
        let store = try RingStore()
        let ring = try store.registerRing(protocolFamily: "ycbt")
        let first = try store.insert(transfer(), ringID: ring.id)
        let second = try store.insert(
            transfer(header: [7, 7], at: t0.addingTimeInterval(3600), zone: "Asia/Tokyo"), ringID: ring.id)
        guard case .inserted(let id) = first else { Issue.record("expected insert"); return }
        #expect(second == .duplicate(existingID: id))
        let rows = try store.transfers(ringID: ring.id)
        #expect(rows.count == 1)
        #expect(rows[0].transfer.timeZoneIdentifier == "Europe/London")  // the first row is kept
    }

    @Test func differentKindOrDataIsNotADuplicate() throws {
        let store = try RingStore()
        let ring = try store.registerRing(protocolFamily: "ycbt")
        try store.insert(transfer(.heartRate, data: [1]), ringID: ring.id)
        try store.insert(transfer(.sleep, data: [1]), ringID: ring.id)
        try store.insert(transfer(.heartRate, data: [1, 0]), ringID: ring.id)
        #expect(try store.transfers(ringID: ring.id).count == 3)
    }

    @Test func sameBytesForTwoRingsAreBothKept() throws {
        let store = try RingStore()
        let a = try store.registerRing(protocolFamily: "ycbt")
        let b = try store.registerRing(protocolFamily: "ycbt")
        guard case .inserted = try store.insert(transfer(), ringID: a.id),
              case .inserted = try store.insert(transfer(), ringID: b.id)
        else { Issue.record("expected two inserts"); return }
        #expect(try store.transfers(ringID: a.id).count == 1)
        #expect(try store.transfers(ringID: b.id).count == 1)
    }

    @Test func unknownRingIsRejected() throws {
        let store = try RingStore()
        let id = UUID()
        #expect(throws: RingStoreError.unknownRing(id)) { try store.insert(transfer(), ringID: id) }
        #expect(throws: RingStoreError.unknownRing(id)) {
            try store.recordClockSet(ClockSetEvent(ringID: id, instant: t0, timeZoneIdentifier: "UTC", utcOffsetSeconds: 0))
        }
    }

    @Test func listsByKindAndRangeInFetchOrder() throws {
        let store = try RingStore()
        let ring = try store.registerRing(protocolFamily: "ycbt")
        // Inserted out of order.
        try store.insert(transfer(.heartRate, data: [3], at: t0.addingTimeInterval(200)), ringID: ring.id)
        try store.insert(transfer(.heartRate, data: [1], at: t0), ringID: ring.id)
        try store.insert(transfer(.sleep, data: [2], at: t0.addingTimeInterval(100)), ringID: ring.id)
        #expect(try store.transfers(ringID: ring.id).map(\.transfer.data) == [[1], [2], [3]])
        #expect(try store.transfers(ringID: ring.id, kind: .heartRate).map(\.transfer.data) == [[1], [3]])
        let range = t0.addingTimeInterval(50)...t0.addingTimeInterval(200)
        #expect(try store.transfers(ringID: ring.id, fetchedWithin: range).map(\.transfer.data) == [[2], [3]])
        #expect(try store.transfers(ringID: ring.id, kind: .sleep, fetchedWithin: range).map(\.transfer.data) == [[2]])
    }

    @Test func fetchedAtKeepsMilliseconds() throws {
        let store = try RingStore()
        let ring = try store.registerRing(protocolFamily: "ycbt")
        let date = HistoryTransfer.truncatedToMilliseconds(Date(timeIntervalSince1970: 1_800_000_000.123))
        try store.insert(transfer(at: date), ringID: ring.id)
        #expect(try store.transfers(ringID: ring.id)[0].transfer.fetchedAt == date)
    }

    @Test func recordsClockSetsInTimeOrder() throws {
        let store = try RingStore()
        let a = try store.registerRing(protocolFamily: "ycbt")
        let b = try store.registerRing(protocolFamily: "ycbt")
        let late = ClockSetEvent(ringID: a.id, instant: t0.addingTimeInterval(100), timeZoneIdentifier: "Asia/Tokyo", utcOffsetSeconds: 32400)
        let early = ClockSetEvent(ringID: a.id, instant: t0, timeZoneIdentifier: "Europe/London", utcOffsetSeconds: 0)
        try store.recordClockSet(late)
        try store.recordClockSet(early)
        try store.recordClockSet(ClockSetEvent(ringID: b.id, instant: t0, timeZoneIdentifier: "UTC", utcOffsetSeconds: 0))
        #expect(try store.clockSets(ringID: a.id) == [early, late])
    }

    @Test func ringTableHoldsNoDeviceIdentifiers() throws {
        // Only an app-generated id plus family, model, firmware and first-seen time.
        let columns = try RingStore().ringColumnNames()
        #expect(columns == ["id", "protocol_family", "model", "firmware", "first_seen_at"])
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
            guard case .inserted(let id) = try store.insert(transfer(), ringID: ringID) else {
                Issue.record("expected insert"); return
            }
            firstID = id
            try store.recordClockSet(ClockSetEvent(ringID: ringID, instant: t0, timeZoneIdentifier: "UTC", utcOffsetSeconds: 0))
        }
        let reopened = try RingStore(url: url)
        #expect(try reopened.transfers(ringID: ringID).count == 1)
        #expect(try reopened.clockSets(ringID: ringID).count == 1)
        #expect(try reopened.insert(transfer(), ringID: ringID) == .duplicate(existingID: firstID))
        #if os(macOS)
        let mode = try FileManager.default.attributesOfItem(atPath: url.path)[.posixPermissions] as? Int
        #expect(mode == 0o600)
        #endif
    }
}
