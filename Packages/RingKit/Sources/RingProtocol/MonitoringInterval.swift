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

    /// True for the values confirmed on hardware (R99, firmware 2.32).
    public var isVerifiedOnHardware: Bool {
        switch self {
        case .off, .oneMinute, .fiveMinutes, .tenMinutes: true
        default: false
        }
    }

    /// Parses a command-line value: `off` or one of the allowed minute counts. Anything else,
    /// including `0`, `7`, signs and padding, is nil.
    public init?(argument: String) {
        if argument == "off" { self = .off; return }
        guard !argument.isEmpty, argument.utf8.allSatisfy({ (0x30...0x39).contains($0) }),
              let value = Int(argument), value > 0, String(value) == argument, let interval = MonitoringInterval(rawValue: value) else { return nil }
        self = interval
    }

    /// The payload bytes: `[enabled, minutes]`. When off, the minutes byte is `0x0A`, the value
    /// verified on the R99 (the ring ignores it).
    var payload: [UInt8] {
        switch self {
        case .off: [0x00, 0x0A]
        default: [0x01, UInt8(rawValue)]
        }
    }

    /// True only for an answer of exactly `yes` (any case, surrounding whitespace ignored). End of
    /// input, an empty line or anything else is a refusal.
    public static func isConfirmation(_ answer: String?) -> Bool {
        answer?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() == "yes"
    }

    /// What is shown to the owner before this is sent to a ring.
    public var summary: String {
        switch self {
        case .off: "turn scheduled readings off (heart rate, SpO2, respiration, HRV)"
        default: "take scheduled readings every \(rawValue) minute\(rawValue == 1 ? "" : "s") (heart rate, SpO2, respiration, HRV)"
        }
    }
}
