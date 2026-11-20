import Foundation

/// The one stage palette: a colour per timeline lane, for light and dark mode. Plain sRGB values
/// here (DigitCore imports no UI framework); the app turns them into dynamic colours. Chosen by
/// Digit for contrast against the system's card backgrounds in each mode (checked in the tests),
/// and always shown with a text label, so colour is never the only cue.
public enum StagePalette {
    public struct RGB: Sendable, Equatable {
        public let red: Double
        public let green: Double
        public let blue: Double

        public init(hex: UInt32) {
            red = Double((hex >> 16) & 0xFF) / 255
            green = Double((hex >> 8) & 0xFF) / 255
            blue = Double(hex & 0xFF) / 255
        }

        /// WCAG relative luminance.
        public var luminance: Double {
            func linear(_ c: Double) -> Double { c <= 0.03928 ? c / 12.92 : pow((c + 0.055) / 1.055, 2.4) }
            return 0.2126 * linear(red) + 0.7152 * linear(green) + 0.0722 * linear(blue)
        }

        /// WCAG contrast ratio with `other` (1 to 21).
        public func contrast(with other: RGB) -> Double {
            let (a, b) = (luminance + 0.05, other.luminance + 0.05)
            return max(a, b) / min(a, b)
        }
    }

    public struct Pair: Sendable, Equatable {
        public let light: RGB
        public let dark: RGB
    }

    public static func colors(for lane: LaneTimeline.LaneKind) -> Pair {
        switch lane {
        case .awake: Pair(light: RGB(hex: 0xC2410C), dark: RGB(hex: 0xFB923C))
        case .rem: Pair(light: RGB(hex: 0x7C3AED), dark: RGB(hex: 0xC4B5FD))
        case .light: Pair(light: RGB(hex: 0x0E7490), dark: RGB(hex: 0x67E8F9))
        case .deep: Pair(light: RGB(hex: 0x1E3A8A), dark: RGB(hex: 0x93A5FF))
        case .nap: Pair(light: RGB(hex: 0x15803D), dark: RGB(hex: 0x86EFAC))
        }
    }

    /// The card backgrounds the palette is checked against: iOS's grouped secondary background
    /// (white in light mode, #1C1C1E in dark mode).
    public static let lightCard = RGB(hex: 0xFFFFFF)
    public static let darkCard = RGB(hex: 0x1C1C1E)
    /// The least contrast each colour must have with its mode's card (WCAG's level for graphics).
    public static let minimumContrast = 3.0
}
