import Foundation

/// Digit's own sleep score for a main sleep: 0 to 100, from four parts. An estimate, not a medical
/// or validated measure.
///
/// Every constant is here, so the formula is easy to explain and to change. They are Digit's
/// choices (the planner's, approved by the owner on 2026-11-19), informed by two published sources:
/// Apple's sleep score weights (duration 50, bedtime consistency 30, interruptions 20, stages not
/// counted), and the National Sleep Foundation's consensus on sleep quality (Ohayon et al., *Sleep
/// Health* 2017), which agreed on efficiency, wake after sleep onset and awakenings as quality
/// indicators and not on stage amounts. Hence duration, regularity and continuity carry the score
/// and stages are weighted lightly (they are also the ring's least verified output). No vendor
/// publishes its formula; nothing here copies one.
///
/// Efficiency is shown but not scored: on the R99 a night starts when the ring decided sleep began
/// (there is no separate time in bed and no sleep latency), so it mostly repeats continuity.
public enum SleepScore {
    // MARK: Constants

    /// Duration: full marks at this much time asleep, proportionally fewer below.
    public static let durationPoints = 50.0
    public static let durationTargetSeconds = 7 * 3600

    /// Consistency: the distance between this bedtime and the typical one (the circular median of
    /// earlier main sleeps' bedtimes). Full marks within `consistencyFullWithinSeconds`, falling
    /// linearly to zero at `consistencyZeroAtSeconds`.
    public static let consistencyPoints = 20.0
    public static let consistencyFullWithinSeconds = 30 * 60
    public static let consistencyZeroAtSeconds = 2 * 3600
    /// With fewer earlier main sleeps than this, consistency is awarded in full.
    public static let consistencyMinimumNights = 7
    /// How many ring days before a night its typical bedtime is taken from.
    public static let consistencyLookbackDays = 30

    /// Continuity, minutes part: full at `interruptionFullWithinMinutes` or less, linearly to zero
    /// at `interruptionZeroAtMinutes`.
    public static let interruptionMinutesPoints = 10.0
    public static let interruptionFullWithinMinutes = 20.0
    public static let interruptionZeroAtMinutes = 60.0
    /// Continuity, count part: points by number of interruptions (index), zero beyond the list.
    public static let interruptionCountPoints: [Double] = [10, 10, 5, 2]

    /// Stages: each share earns full marks at its target, proportionally fewer below.
    public static let deepPoints = 5.0
    public static let deepTargetShare = 0.15
    public static let remPoints = 5.0
    public static let remTargetShare = 0.20

    /// Labels: "Good" from `goodFrom`, "Fair" from `fairFrom`, "Poor" below.
    public static let goodFrom = 80
    public static let fairFrom = 60

    public static var maximumContinuityPoints: Double { interruptionMinutesPoints + (interruptionCountPoints.max() ?? 0) }
    public static var maximumStagePoints: Double { deepPoints + remPoints }

    // MARK: Result

    public enum Label: String, Sendable, Equatable, CaseIterable {
        case good = "Good"
        case fair = "Fair"
        case poor = "Poor"

        public init(score: Int) {
            self = score >= SleepScore.goodFrom ? .good : score >= SleepScore.fairFrom ? .fair : .poor
        }
    }

    public struct Result: Sendable, Equatable {
        /// The sum of the parts, rounded to the nearest whole point (halves up).
        public var total: Int
        public var duration: Double
        public var consistency: Double
        public var continuity: Double
        public var stages: Double
        /// False while there are fewer than `consistencyMinimumNights` earlier nights, when
        /// consistency is awarded in full.
        public var consistencyCounted: Bool
        public var label: Label { Label(score: total) }
    }

    // MARK: Parts

    public static func durationPart(secondsAsleep: Int) -> Double {
        min(Double(max(secondsAsleep, 0)) / Double(durationTargetSeconds), 1) * durationPoints
    }

    /// Points for a bedtime `distanceSeconds` from the typical one.
    public static func consistencyPart(distanceSeconds: Int) -> Double {
        let full = Double(consistencyFullWithinSeconds), zero = Double(consistencyZeroAtSeconds)
        let d = Double(max(distanceSeconds, 0))
        if d <= full { return consistencyPoints }
        if d >= zero { return 0 }
        return consistencyPoints * (zero - d) / (zero - full)
    }

    public static func interruptionMinutesPart(seconds: Int) -> Double {
        let minutes = Double(max(seconds, 0)) / 60
        if minutes <= interruptionFullWithinMinutes { return interruptionMinutesPoints }
        if minutes >= interruptionZeroAtMinutes { return 0 }
        return interruptionMinutesPoints * (interruptionZeroAtMinutes - minutes)
            / (interruptionZeroAtMinutes - interruptionFullWithinMinutes)
    }

    public static func interruptionCountPart(count: Int) -> Double {
        interruptionCountPoints.indices.contains(count) ? interruptionCountPoints[count] : 0
    }

    public static func stagePart(deepShare: Double, remShare: Double) -> Double {
        deepPoints * min(max(deepShare, 0) / deepTargetShare, 1) + remPoints * min(max(remShare, 0) / remTargetShare, 1)
    }

    // MARK: Bedtimes

    static let day = 86_400

    /// The shorter way round the clock between two times of day, in seconds (0 to 12 hours).
    public static func circularDistance(_ a: Int, _ b: Int) -> Int {
        let d = abs(a.modulo(day) - b.modulo(day))
        return min(d, day - d)
    }

    /// The typical bedtime of `secondsOfDay` (times of day, seconds after midnight): the circle is
    /// cut at the middle of the largest gap between the times (so a spread of bedtimes around
    /// midnight stays together), the times are read in order from there, and their ordinary median
    /// is taken (the mean of the two middle ones for an even count). `nil` for no times.
    public static func typicalBedtime(_ secondsOfDay: [Int]) -> Int? {
        let times = secondsOfDay.map { $0.modulo(day) }.sorted()
        guard let first = times.first else { return nil }
        // The largest gap between neighbours, the wrap from the last to the first included.
        var cut = first  // unwrap so that the time after the largest gap comes first
        var largest = first + day - times[times.count - 1]
        for (a, b) in zip(times, times.dropFirst()) where b - a > largest {
            largest = b - a
            cut = b
        }
        let unwrapped = times.map { $0 < cut ? $0 + day : $0 }.sorted()
        let n = unwrapped.count
        let median = n % 2 == 1 ? unwrapped[n / 2] : (unwrapped[n / 2 - 1] + unwrapped[n / 2]) / 2
        return median.modulo(day)
    }

    // MARK: Score

    /// The score of a main sleep, given the bedtimes (times of day on each night's own clock) of
    /// the main sleeps of the previous `consistencyLookbackDays` ring days. `nil` for a night with
    /// no time asleep or no length: there is nothing to score.
    public static func score(_ metrics: NightMetrics, earlierBedtimes: [Int]) -> Result? {
        guard metrics.secondsAsleep > 0, metrics.secondsInBed > 0 else { return nil }
        let duration = durationPart(secondsAsleep: metrics.secondsAsleep)
        let counted = earlierBedtimes.count >= consistencyMinimumNights
        let consistency: Double
        if counted, let typical = typicalBedtime(earlierBedtimes) {
            consistency = consistencyPart(distanceSeconds: circularDistance(metrics.bedtimeSecondsOfDay, typical))
        } else {
            consistency = consistencyPoints
        }
        let continuity = interruptionMinutesPart(seconds: metrics.interruptionSeconds)
            + interruptionCountPart(count: metrics.interruptionCount)
        let stages = stagePart(deepShare: metrics.deepShare ?? 0, remShare: metrics.remShare ?? 0)
        let total = Int((duration + consistency + continuity + stages).rounded())
        return Result(
            total: total, duration: duration, consistency: consistency, continuity: continuity, stages: stages,
            consistencyCounted: counted)
    }

    // MARK: Words

    public static let disclaimer = "Digit's sleep score: an estimate, not a medical measure"
    public static let consistencyNotCounted = "Consistency counts once there are 7 nights."

    /// The formula in plain words, one line each, for the screen behind the info button.
    public static let explanation: [String] = [
        "Duration, up to 50 points: full marks for 7 hours asleep or more, proportionally fewer below.",
        "Consistency, up to 20 points: full marks when you went to sleep within 30 minutes of your typical bedtime "
            + "(the middle of your bedtimes over the previous 30 days), falling to none at 2 hours. "
            + "It counts once there are 7 earlier nights; until then it is given in full.",
        "Continuity, up to 20 points: 10 for time interrupted (full at 20 minutes or less, none at 60) and 10 for "
            + "how many interruptions (full for none or one, 5 for two, 2 for three, none for four or more).",
        "Stages, up to 10 points: 5 for deep sleep (full at 15% of time asleep) and 5 for REM (full at 20%).",
        "Labels: 80 and above is Good, 60 to 79 Fair, below 60 Poor.",
        "Digit chose these weights from published guidance on what makes sleep good. They are not a validated "
            + "or medical measure, and the ring's sleep stages are estimates.",
    ]
}

extension Int {
    /// The remainder in `0..<m` for a positive `m`, whatever the sign.
    func modulo(_ m: Int) -> Int {
        let r = self % m
        return r < 0 ? r + m : r
    }
}
