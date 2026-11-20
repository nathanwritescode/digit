import Foundation

/// The owner's usual range for a metric: the 25th to 75th percentile of its values over the main
/// sleeps of the previous 30 ring days, never including the night being shown
/// (`SleepHistoryWindow`). Digit has no population data, so a night is only ever compared with the
/// owner's own nights.
public struct PersonalRange: Sendable, Equatable {
    public var lower: Double
    public var upper: Double

    /// Nights needed before a range is shown.
    public static let minimumNights = 7
    public static let lowerPercentile = 0.25
    public static let upperPercentile = 0.75

    public init(lower: Double, upper: Double) {
        self.lower = lower
        self.upper = upper
    }

    /// The `p`-th percentile (0 to 1) of `values` by linear interpolation between the closest
    /// ranks (the common "type 7" definition: position `p × (n − 1)` in the sorted values).
    /// `nil` for no values.
    public static func percentile(_ values: [Double], _ p: Double) -> Double? {
        let sorted = values.sorted()
        guard !sorted.isEmpty else { return nil }
        let position = min(max(p, 0), 1) * Double(sorted.count - 1)
        let below = Int(position.rounded(.down))
        let above = min(below + 1, sorted.count - 1)
        let fraction = position - Double(below)
        return sorted[below] + (sorted[above] - sorted[below]) * fraction
    }

    /// The range of `values` (the earlier nights' values only), or how many there are so far.
    public static func of(_ values: [Double]) -> Comparison.Basis {
        guard values.count >= minimumNights,
            let lower = percentile(values, lowerPercentile), let upper = percentile(values, upperPercentile)
        else { return .building(nights: values.count) }
        return .range(PersonalRange(lower: lower, upper: upper))
    }

    /// Where `value` falls: inside the range (edges included) is usual.
    public func tag(_ value: Double) -> Comparison {
        value < lower ? .below : value > upper ? .above : .usual
    }

    /// A value against the usual range.
    public enum Comparison: Sendable, Equatable {
        case usual, above, below
        /// Fewer than `minimumNights` earlier nights have this metric.
        case building(nights: Int)

        public enum Basis: Sendable, Equatable {
            case range(PersonalRange)
            case building(nights: Int)

            public var range: PersonalRange? {
                if case .range(let range) = self { range } else { nil }
            }

            public func compare(_ value: Double) -> Comparison {
                switch self {
                case .range(let range): range.tag(value)
                case .building(let nights): .building(nights: nights)
                }
            }
        }

        /// "Usual", "Above your usual", "Below your usual", or "Building your usual range (3 of 7
        /// nights)". Neutral words: no judgement of better or worse, even for metrics that read
        /// better lower.
        public var text: String {
            switch self {
            case .usual: "Usual"
            case .above: "Above your usual"
            case .below: "Below your usual"
            case .building(let nights):
                "Building your usual range (\(min(nights, PersonalRange.minimumNights)) of \(PersonalRange.minimumNights) nights)"
            }
        }

        /// Outside the range: shown with the one accent colour. Usual and building are neutral.
        public var isOutside: Bool { self == .above || self == .below }
    }

    /// "Shaded: your usual range over the last 30 nights."
    public static let legend = "Shaded: your usual range over the last 30 nights."
}
