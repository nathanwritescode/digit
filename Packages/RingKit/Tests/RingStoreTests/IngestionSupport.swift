import Foundation
import RingProtocol
@testable import RingStore

// Helpers for the typed-ingestion tests. All bytes are synthetic, built from the documented
// record layouts; none come from a real ring.

/// Ring seconds (wall clock since 2000-01-01) for a real instant under a UTC offset.
func wall(_ real: Date, offset: Int) -> UInt32 {
    UInt32(real.timeIntervalSince1970 + Double(offset) - 946_684_800)
}

func le32(_ v: UInt32) -> [UInt8] { (0..<4).map { UInt8((v >> (8 * UInt32($0))) & 0xFF) } }
func le16(_ v: Int) -> [UInt8] { [UInt8(v & 0xFF), UInt8((v >> 8) & 0xFF)] }

func hrBytes(_ ringSeconds: UInt32, bpm: UInt8, mode: UInt8 = 0) -> [UInt8] {
    le32(ringSeconds) + [mode, bpm]
}

func activityBytes(start: UInt32, end: UInt32, steps: Int, distance: Int = 0, kcal: Int = 0) -> [UInt8] {
    le32(start) + le32(end) + le16(steps) + le16(distance) + le16(kcal)
}

/// One sleep session: 20-byte header (marker, length, start, end, 8 zero totals bytes) then
/// 8-byte stages (kind, start u32, duration u24).
func sleepBytes(start: UInt32, end: UInt32, stages: [(kind: UInt8, start: UInt32, seconds: Int)]) -> [UInt8] {
    var out: [UInt8] = [0xAF, 0xFA] + le16(20 + 8 * stages.count) + le32(start) + le32(end) + Array(repeating: 0, count: 8)
    for s in stages {
        out += [s.kind] + le32(s.start) + [UInt8(s.seconds & 0xFF), UInt8((s.seconds >> 8) & 0xFF), UInt8((s.seconds >> 16) & 0xFF)]
    }
    return out
}

/// A store with one ring, plus shortcuts for fetches and clock-sets.
struct Fixture {
    let store: RingStore
    let ring: StoredRing

    init(_ store: RingStore) throws {
        self.store = store
        ring = try store.registerRing(protocolFamily: "ycbt")
    }

    @discardableResult
    func fetch(_ kind: HistoryKind, _ data: [UInt8], at date: Date, zone: String = "Europe/London") throws -> InsertOutcome {
        try store.insert(transfer(kind, data: data, header: [UInt8(data.count & 0xFF)], at: date, zone: zone), ringID: ring.id)
    }

    /// The first clock-set the app logs. A record whose window reaches back before the log cannot be
    /// `resolved` within 26 hours of the first clock-set (the ring's earlier clock is unknown), so
    /// tests that want ordinary results put an earlier clock-set with the same offset ahead of it,
    /// as a ring that has been synced for more than a day has.
    func setFirstClock(at instant: Date, offset: Int, zone: String = "Europe/London") throws {
        try setClock(at: t0.addingTimeInterval(-200_000), offset: offset, zone: zone)
        try setClock(at: instant, offset: offset, zone: zone)
    }

    func setClock(at instant: Date, offset: Int, zone: String = "Europe/London") throws {
        try store.recordClockSet(ClockSetEvent(
            ringID: ring.id, instant: instant, timeZoneIdentifier: zone, utcOffsetSeconds: offset))
    }
}

/// An observation for the pure resolver tests.
func observation(
    _ ring: UUID, _ kind: HistoryKind = .heartRate, data: [UInt8], at date: Date, zone: String = "Europe/London", id: Int64
) -> TransferObservation {
    TransferObservation(
        id: id, ringID: ring, kind: kind, dataLength: data.count, header: [],
        contentHash: RingStore.contentHash(ringID: ring, kind: kind, data: data), fetchedAt: date,
        timeZoneIdentifier: zone)
}
