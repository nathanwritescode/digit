import Foundation

/// How often the ring takes its scheduled readings (`0x010C`). One setting drives heart rate,
/// SpO2, respiration, HRV and CVRR together, because each reading produces a heart-rate record
/// and a combined record.
///
/// The cases are the only values the app may ever send. Another ring of this family is reported
/// to acknowledge any interval byte, so validation on the ring cannot be relied on. Verified on an
/// R99 (firmware 2.32): off, 1, 5 and 10 minutes. 15, 20, 30 and 60 minutes have never been sent.
public enum MonitoringInterval: Int, CaseIterable, Sendable {
    case off = 0
    case oneMinute = 1
    case fiveMinutes = 5
    case tenMinutes = 10
    case fifteenMinutes = 15
    case twentyMinutes = 20
    case thirtyMinutes = 30
    case sixtyMinutes = 60

    /// The minutes between readings, or nil when monitoring is off.
    public var minutes: Int? { self == .off ? nil : rawValue }

    /// An allowed number of minutes (1, 5, 10, 15, 20, 30 or 60); nil for anything else,
    /// including 0, which is `.off` and is not a minute count.
    public init?(minutes: Int) {
        guard minutes > 0, let interval = MonitoringInterval(rawValue: minutes) else { return nil }
        self = interval
    }

    /// The payload bytes: `[enabled, minutes]`. With off, the minutes byte is `0x0A`: the only
    /// value ever sent with off (on the R99, where off worked). Its effect is assumed, not tested.
    var payload: [UInt8] {
        switch self {
        case .off: [0x00, 0x0A]
        default: [0x01, UInt8(rawValue)]
        }
    }
}
