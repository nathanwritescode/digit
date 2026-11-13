import Foundation

/// The kinds of stored history the ring can be asked for (group `0x05`).
public enum HistoryKind: UInt8, CaseIterable, Sendable {
    case activity = 0x02
    case sleep = 0x04
    case heartRate = 0x06
    /// Periodic combined record: steps, heart rate, SpO2, HRV, temperature.
    case combined = 0x09

    public var name: String {
        switch self {
        case .activity: "activity"
        case .sleep: "sleep"
        case .heartRate: "heart"
        case .combined: "combined"
        }
    }
}

/// Builders for the frames the app sends.
///
/// The `Health_Delete*` commands (`0x0540`–`0x0552`) are deliberately absent, and the transport
/// refuses them (`Frame.erasesRingData`).
public enum Command {
    public static let healthGroup: UInt8 = 0x05

    /// Frame types known to only read from the ring. Anything else has unknown effects.
    public static let knownReadOnlyTypes: Set<UInt16> = [
        0x0200, 0x020C, 0x0220, 0x0502, 0x0504, 0x0506, 0x0509, 0x051A, 0x051E, 0x0533,
    ]

    /// True only for a known read-only type carrying its documented payload. What these
    /// commands do with any other payload is unknown.
    public static func isKnownReadOnly(_ frame: Frame) -> Bool {
        knownReadOnlyTypes.contains(frame.type) && frame.payload == (frame.type == deviceInfo.type ? deviceInfo.payload : [])
    }
    public static let historyEndKey: UInt8 = 0x80

    /// Sets the ring's clock. The ring keeps local wall-clock time, not UTC, in the Gregorian calendar.
    public static func setTime(_ date: Date, timeZone: TimeZone = .current) -> Frame {
        // Always Gregorian: the user's own calendar (Japanese, Buddhist…) would send the wrong year.
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = timeZone
        let c = calendar.dateComponents([.year, .month, .day, .hour, .minute, .second, .weekday], from: date)
        let year = c.year ?? 2000
        // Calendar weekday is 1 = Sunday; the ring wants 0 = Monday.
        let weekday = ((c.weekday ?? 2) + 5) % 7
        return Frame(group: 0x01, key: 0x00, payload: [
            UInt8(year & 0xFF), UInt8(year >> 8),
            UInt8(c.month ?? 1), UInt8(c.day ?? 1),
            UInt8(c.hour ?? 0), UInt8(c.minute ?? 0), UInt8(c.second ?? 0),
            UInt8(weekday),
        ])
    }

    /// Sets the ring's scheduled-reading interval. This changes a ring setting, so it is not on the
    /// read-only list, and the only way to build it is through the validated `MonitoringInterval`.
    public static func setMonitoringInterval(_ interval: MonitoringInterval) -> Frame {
        Frame(group: 0x01, key: 0x0C, payload: interval.payload)
    }

    public static let deviceInfo = Frame(group: 0x02, key: 0x00, payload: [0x47, 0x43])
    public static let currentActivity = Frame(group: 0x02, key: 0x0C)

    public static func history(_ kind: HistoryKind) -> Frame {
        Frame(group: healthGroup, key: kind.rawValue)
    }

    /// Acknowledges a completed history transfer.
    public static let historyTransferOK = Frame(group: healthGroup, key: historyEndKey, payload: [0x00])
    /// Asks the ring to resend a history transfer whose checksum did not match.
    public static let historyTransferRetry = Frame(group: healthGroup, key: historyEndKey, payload: [0x04])
}
