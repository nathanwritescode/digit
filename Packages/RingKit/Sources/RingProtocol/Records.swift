import Foundation

/// The ring stamps records with local wall-clock seconds since 2000-01-01.
public enum RingTime {
    private static let epochOffset: TimeInterval = 946_684_800

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

/// One stored activity interval (history kind `.activity`, 14 bytes each).
/// Verified against one real night from an R99 (firmware 2.32); test fixtures are synthetic.
public struct ActivityRecord: Equatable, Sendable {
    public var start: Date
    public var end: Date
    public var steps: Int
    public var distanceMeters: Int
    public var kilocalories: Int

    public static func decode(_ b: [UInt8], timeZone: TimeZone) -> [ActivityRecord] {
        stride(from: 0, to: b.count - 13, by: 14).map { i in
            ActivityRecord(
                start: RingTime.date(fromRingSeconds: b.u32(i), timeZone: timeZone),
                end: RingTime.date(fromRingSeconds: b.u32(i + 4), timeZone: timeZone),
                steps: Int(b.u16(i + 8)),
                distanceMeters: Int(b.u16(i + 10)),
                kilocalories: Int(b.u16(i + 12)))
        }
    }
}

/// One stored heart-rate reading (history kind `.heartRate`, 6 bytes each).
/// Verified against one real night from an R99 (firmware 2.32); test fixtures are synthetic.
public struct HeartRateRecord: Equatable, Sendable {
    public var time: Date
    public var mode: UInt8
    public var bpm: Int

    public static func decode(_ b: [UInt8], timeZone: TimeZone) -> [HeartRateRecord] {
        stride(from: 0, to: b.count - 5, by: 6).map { i in
            HeartRateRecord(
                time: RingTime.date(fromRingSeconds: b.u32(i), timeZone: timeZone),
                mode: b[i + 4],
                bpm: Int(b[i + 5]))
        }
    }
}

public enum SleepStageKind: UInt8, Sendable {
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
}

/// One night (or nap) of sleep (history kind `.sleep`, variable length).
///
/// Layout: 20-byte header (`AF FA`, record length u16 with header included, start u32,
/// end u32, 8 bytes of totals), then 8-byte stages (kind u8, start u32, duration seconds u24).
/// The header totals are ignored; totals are derived from the stages.
/// Verified against one real night from an R99 (firmware 2.32); test fixtures are synthetic.
public struct SleepSession: Equatable, Sendable {
    public var start: Date
    public var end: Date
    public var stages: [SleepStage]

    public func total(_ kind: SleepStageKind) -> TimeInterval {
        stages.lazy.filter { $0.kind == kind }.reduce(0) { $0 + $1.duration }
    }

    public static func decode(_ b: [UInt8], timeZone: TimeZone) -> [SleepSession] {
        let headerSize = 20, stageSize = 8
        var sessions: [SleepSession] = []
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
            sessions.append(SleepSession(
                start: RingTime.date(fromRingSeconds: b.u32(i + 4), timeZone: timeZone),
                end: RingTime.date(fromRingSeconds: b.u32(i + 8), timeZone: timeZone),
                stages: stages))
            i += recordLength
        }
        return sessions
    }
}

extension Array where Element == UInt8 {
    func u16(_ i: Int) -> UInt16 { UInt16(self[i]) | UInt16(self[i + 1]) << 8 }
    func u24(_ i: Int) -> UInt32 { UInt32(self[i]) | UInt32(self[i + 1]) << 8 | UInt32(self[i + 2]) << 16 }
    func u32(_ i: Int) -> UInt32 { u24(i) | UInt32(self[i + 3]) << 24 }
}
