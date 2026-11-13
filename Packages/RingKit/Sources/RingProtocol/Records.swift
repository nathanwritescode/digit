import Foundation

/// The ring stamps records with local wall-clock seconds since 2000-01-01.
public enum RingTime {
    private static let epochOffset: TimeInterval = 946_684_800

    /// Seconds since 2000-01-01 of a wall-clock reading held as a `Date` as if it were UTC (what
    /// the record decoders return before any time zone is applied). The inverse of the first step
    /// of `date(fromRingSeconds:timeZone:)`. Pure arithmetic, no I/O. The result is negative for dates before 2000 and can exceed
    /// `UInt32.max` for dates far past the ring's range, so a caller that narrows it must check.
    public static func ringSeconds(fromWallClock asIfUTC: Date) -> Int {
        Int(asIfUTC.timeIntervalSince1970 - epochOffset)
    }

    public static func date(fromRingSeconds raw: UInt32, timeZone: TimeZone) -> Date {
        let asIfUTC = Date(timeIntervalSince1970: TimeInterval(raw) + epochOffset)
        // Two passes so the offset is taken at the real instant, not the wall-clock one.
        var date = asIfUTC.addingTimeInterval(-TimeInterval(timeZone.secondsFromGMT(for: asIfUTC)))
        date = asIfUTC.addingTimeInterval(-TimeInterval(timeZone.secondsFromGMT(for: date)))
        return date
    }
}

/// Reply to `Command.deviceInfo`. Firmware version verified on an R99 (`… 20 02 …` is 2.32).
/// The battery byte is unverified: it has read 100% at every check so far.
public struct DeviceInfo: Equatable, Sendable {
    public var deviceID: UInt16
    public var firmwareVersion: String
    public var batteryState: UInt8
    public var batteryPercent: Int

    public init?(payload p: [UInt8]) {
        guard p.count >= 6 else { return nil }
        deviceID = p.u16(0)
        firmwareVersion = String(format: "%d.%02d", p[3], p[2])
        batteryState = p[4]
        batteryPercent = min(Int(p[5]), 100)
    }
}

/// Reply to `Command.currentActivity`: today's running totals. Layout verified on an R99.
public struct CurrentActivity: Equatable, Sendable {
    public var steps: Int
    public var kilocalories: Int
    public var distanceMeters: Int

    public init?(payload p: [UInt8]) {
        guard p.count >= 7 else { return nil }
        steps = Int(p.u24(0))
        kilocalories = Int(p.u16(3))
        distanceMeters = Int(p.u16(5))
    }
}

/// A decoded record with the bytes it came from, so ingestion can tell which fetch first
/// contained it. `offset` and `length` are in bytes within the data that was decoded. For a sleep
/// session `length` is the record's declared length, which can run past the end of the data when
/// the data is cut short; callers that need whole records check `offset + length <= data.count`.
public struct Located<Record: Sendable>: Sendable {
    public let record: Record
    public let offset: Int
    public let length: Int

    public init(record: Record, offset: Int, length: Int) {
        self.record = record
        self.offset = offset
        self.length = length
    }
}

extension Located: Equatable where Record: Equatable {}

/// One stored activity interval (history kind `.activity`, 14 bytes each).
/// On an R99 (firmware 2.32) the records were 30-minute buckets, and their step counts agreed with
/// the combined record for the same hour. Verified against one real night; test fixtures are synthetic.
public struct ActivityRecord: Equatable, Sendable {
    public var start: Date
    public var end: Date
    public var steps: Int
    public var distanceMeters: Int
    public var kilocalories: Int

    public static let size = 14

    public static func decode(_ b: [UInt8], timeZone: TimeZone) -> [ActivityRecord] {
        decodeLocated(b, timeZone: timeZone).map(\.record)
    }

    /// Whole records with their byte ranges; a trailing partial record is ignored.
    public static func decodeLocated(_ b: [UInt8], timeZone: TimeZone) -> [Located<ActivityRecord>] {
        stride(from: 0, to: b.count - (size - 1), by: size).map { i in
            Located(
                record: ActivityRecord(
                    start: RingTime.date(fromRingSeconds: b.u32(i), timeZone: timeZone),
                    end: RingTime.date(fromRingSeconds: b.u32(i + 4), timeZone: timeZone),
                    steps: Int(b.u16(i + 8)),
                    distanceMeters: Int(b.u16(i + 10)),
                    kilocalories: Int(b.u16(i + 12))),
                offset: i, length: size)
        }
    }
}

/// One stored heart-rate reading (history kind `.heartRate`, 6 bytes each).
/// On an R99 (firmware 2.32) the readings were one per hour on the hour with mode 0 (documented as automatic),
/// and the values agreed with the combined record. `mode` is kept raw; the meaning of other
/// values is not verified. Verified against one real night; test fixtures are synthetic.
public struct HeartRateRecord: Equatable, Sendable {
    public var time: Date
    public var mode: UInt8
    public var bpm: Int

    public static let size = 6

    public static func decode(_ b: [UInt8], timeZone: TimeZone) -> [HeartRateRecord] {
        decodeLocated(b, timeZone: timeZone).map(\.record)
    }

    /// Whole records with their byte ranges; a trailing partial record is ignored.
    public static func decodeLocated(_ b: [UInt8], timeZone: TimeZone) -> [Located<HeartRateRecord>] {
        stride(from: 0, to: b.count - (size - 1), by: size).map { i in
            Located(
                record: HeartRateRecord(
                    time: RingTime.date(fromRingSeconds: b.u32(i), timeZone: timeZone),
                    mode: b[i + 4],
                    bpm: Int(b[i + 5])),
                offset: i, length: size)
        }
    }
}

public enum SleepStageKind: UInt8, CaseIterable, Sendable {
    case deep = 241
    case light = 242
    case rem = 243
    case awake = 244
    case nap = 245
}

public struct SleepStage: Equatable, Sendable {
    public var kind: SleepStageKind
    public var start: Date
    public var duration: TimeInterval
    public var end: Date { start.addingTimeInterval(duration) }

    /// Whether `next` follows this stage without a real gap. On an R99 (firmware 2.32) each stage
    /// started one second after the previous one ended, so a timeline builder should treat that as
    /// contiguous. The tolerance has no default because it is a per-device observation, not protocol
    /// behaviour; callers must not require exactly one second. A small overlap (`next` starting up to
    /// `tolerance` before this stage ends) also counts as contiguous; `next` earlier than that, or
    /// later than `tolerance` after the end, does not. The decoder never merges or rewrites stages.
    public func isContiguous(with next: SleepStage, tolerance: TimeInterval) -> Bool {
        abs(next.start.timeIntervalSince(end)) <= tolerance
    }
}

/// One night (or nap) of sleep (history kind `.sleep`, variable length).
///
/// Layout: 20-byte header (`AF FA`, record length u16 with header included, start u32,
/// end u32, 8 bytes of totals), then 8-byte stages (kind u8, start u32, duration seconds u24).
/// The header totals are ignored; totals are derived from the stages. On an R99 (firmware 2.32)
/// the header used the newer layout (`FF FF`, then REM, deep and light totals as u16 seconds at
/// offset 12) and the totals equalled the stage sums exactly.
///
/// Quirks seen on that ring, all passed through unchanged: stages start one second after the
/// previous one ends (see `SleepStage.isContiguous`); no awake stages were reported; one night
/// arrived as two sessions a few minutes apart, so a brief wake-up can be a gap between sessions.
/// Verified against one real night; test fixtures are synthetic.
public struct SleepSession: Equatable, Sendable {
    public var start: Date
    public var end: Date
    public var stages: [SleepStage]

    public func total(_ kind: SleepStageKind) -> TimeInterval {
        stages.lazy.filter { $0.kind == kind }.reduce(0) { $0 + $1.duration }
    }

    public static func decode(_ b: [UInt8], timeZone: TimeZone) -> [SleepSession] {
        decodeLocated(b, timeZone: timeZone).map(\.record)
    }

    /// Sessions with their byte ranges. Decoding stops at the first thing that is not a session
    /// record, so bytes after it are not reported. A session cut short by the end of the data is
    /// still returned (as `decode` always did), with its declared `length` running past the data.
    public static func decodeLocated(_ b: [UInt8], timeZone: TimeZone) -> [Located<SleepSession>] {
        let headerSize = 20, stageSize = 8
        var sessions: [Located<SleepSession>] = []
        var i = 0
        while i + headerSize <= b.count {
            let recordLength = Int(b.u16(i + 2))
            // Stop at anything that is not a session record instead of decoding garbage.
            guard b[i] == 0xAF, b[i + 1] == 0xFA, recordLength >= headerSize else { break }
            let recordEnd = min(i + recordLength, b.count)
            var stages: [SleepStage] = []
            var j = i + headerSize
            while j + stageSize <= recordEnd {
                if let kind = SleepStageKind(rawValue: b[j]), b.u24(j + 5) > 0 {
                    stages.append(SleepStage(
                        kind: kind,
                        start: RingTime.date(fromRingSeconds: b.u32(j + 1), timeZone: timeZone),
                        duration: TimeInterval(b.u24(j + 5))))
                }
                j += stageSize
            }
            stages.sort { $0.start < $1.start }
            sessions.append(Located(
                record: SleepSession(
                    start: RingTime.date(fromRingSeconds: b.u32(i + 4), timeZone: timeZone),
                    end: RingTime.date(fromRingSeconds: b.u32(i + 8), timeZone: timeZone),
                    stages: stages),
                offset: i, length: recordLength))
            i += recordLength
        }
        return sessions
    }
}

/// One stored combined record (history kind `.combined`, 20 bytes each).
///
/// Documented layout (little-endian): time u32, steps u16, heart rate, systolic, diastolic, SpO2,
/// respiration, HRV, CVRR (one byte each), temperature integer and fraction, body-fat integer and
/// fraction, glucose, then 2 more bytes. Confirmed on an R99 (firmware 2.32) for time, steps,
/// heart rate, SpO2, respiration, HRV and CVRR (plausible, and consistent with the activity and
/// heart-rate records). The Swift decoder matched an independent decode of a real R99 capture.
///
/// Only values the ring can measure are exposed. A zero in a measured field means "no reading"
/// and becomes nil (an on-demand HRV record is reported to have every other field zero; that comes
/// from another ring of the same family and is unverified on the R99). SpO2 outside 1...100 is
/// not a reading and also becomes nil.
///
/// Deliberately not decoded into properties. They stay only in the module-internal `raw`, so
/// later layers cannot read them; the stored `HistoryTransfer` is the source of truth:
/// - blood pressure (bytes 7-8): filled in by the ring, but a ring cannot measure it; these are
///   vendor estimates and must never be stored or shown as measurements;
/// - temperature (bytes 13-14): identical in every record and not a body temperature on this ring;
/// - body fat and glucose (bytes 15-17): always zero on this ring;
/// - bytes 18-19: documented as reserved, but they varied between records with no visible
///   pattern. They are not understood, so they are not called padding and are left undecoded.
public struct CombinedRecord: Equatable, Sendable {
    public static let size = 20

    public var time: Date
    /// Cross-check only: activity records are the source of steps. A zero here does not mean a
    /// measured zero.
    public var steps: Int
    public var heartRate: Int?
    public var spo2: Int?
    public var respirationRate: Int?
    public var hrv: Int?
    public var cvrr: Int?
    /// The whole 20-byte record exactly as received. Internal on purpose (see above).
    var raw: [UInt8]

    /// Decodes consecutive records; a trailing partial record is ignored.
    public static func decode(_ b: [UInt8], timeZone: TimeZone) -> [CombinedRecord] {
        func reading(_ value: UInt8) -> Int? { value == 0 ? nil : Int(value) }
        return stride(from: 0, to: b.count - (size - 1), by: size).map { i in
            let spo2 = reading(b[i + 9])
            return CombinedRecord(
                time: RingTime.date(fromRingSeconds: b.u32(i), timeZone: timeZone),
                steps: Int(b.u16(i + 4)),
                heartRate: reading(b[i + 6]),
                spo2: spo2.flatMap { (1...100).contains($0) ? $0 : nil },
                respirationRate: reading(b[i + 10]),
                hrv: reading(b[i + 11]),
                cvrr: reading(b[i + 12]),
                raw: Array(b[i..<i + size]))
        }
    }
}

extension Array where Element == UInt8 {
    func u16(_ i: Int) -> UInt16 { UInt16(self[i]) | UInt16(self[i + 1]) << 8 }
    func u24(_ i: Int) -> UInt32 { UInt32(self[i]) | UInt32(self[i + 1]) << 8 | UInt32(self[i + 2]) << 16 }
    func u32(_ i: Int) -> UInt32 { u24(i) | UInt32(self[i + 3]) << 24 }
}
