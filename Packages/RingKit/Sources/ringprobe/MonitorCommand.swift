import Foundation
import RingProtocol

// Helpers for `ringprobe monitor`, kept here (not in RingProtocol) because only the tool needs
// them. Internal, and unit tested through `@testable import ringprobe`.

extension MonitoringInterval {
    /// Parses a command-line value: `off` or one of the allowed minute counts, written canonically
    /// (no sign, padding or leading zero). Anything else, including `0` and `7`, is nil.
    init?(argument: String) {
        if argument == "off" { self = .off; return }
        guard !argument.isEmpty, argument.utf8.allSatisfy({ (0x30...0x39).contains($0) }),
              let value = Int(argument), String(value) == argument,
              let interval = MonitoringInterval(minutes: value) else { return nil }
        self = interval
    }

    /// True only for an answer of exactly `yes` (any case, surrounding whitespace ignored). End of
    /// input, an empty line or anything else is a refusal.
    static func isConfirmation(_ answer: String?) -> Bool {
        answer?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() == "yes"
    }

    /// True for the values confirmed on hardware (R99, firmware 2.32).
    var isVerifiedOnHardware: Bool {
        switch self {
        case .off, .oneMinute, .fiveMinutes, .tenMinutes: true
        default: false
        }
    }

    /// What is shown to the owner before this is sent to a ring.
    var summary: String {
        switch self {
        case .off: "turn scheduled readings off (heart rate, SpO2, respiration, HRV)"
        default: "take scheduled readings every \(rawValue) minute\(rawValue == 1 ? "" : "s") (heart rate, SpO2, respiration, HRV)"
        }
    }
}
