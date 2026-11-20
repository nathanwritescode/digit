import Foundation
import RingProtocol
import RingStore

// The Sleep tab's cards and the night screen's score and quality bars, as values the views only
// lay out. All arithmetic (metrics, score, ranges, chart positions) is here, with tests; all text
// is formatted here, on each night's own clock.

/// A metric a night is compared on.
public enum SleepMetric: String, Sendable, Equatable, CaseIterable, Identifiable {
    case asleep, timeInBed, rem, deep, efficiency, timeToDeepSleep, interruptions

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .asleep: "Time asleep"
        case .timeInBed: "Time in bed"
        case .rem: "REM"
        case .deep: "Deep"
        case .efficiency: "Efficiency"
        case .timeToDeepSleep: "Time to deep sleep"
        case .interruptions: "Interruptions"
        }
    }

    /// The metric's value in its stored unit (seconds, or a fraction for efficiency); `nil` when
    /// the night does not have it (no deep stage for time to deep sleep, no length for efficiency).
    public func value(_ metrics: NightMetrics) -> Double? {
        switch self {
        case .asleep: Double(metrics.secondsAsleep)
        case .timeInBed: Double(metrics.secondsInBed)
        case .rem: Double(metrics.remSeconds)
        case .deep: Double(metrics.deepSeconds)
        case .efficiency: metrics.efficiency
        case .timeToDeepSleep: metrics.secondsToDeepSleep.map(Double.init)
        case .interruptions: Double(metrics.interruptionSeconds)
        }
    }

    /// The value in a chart's unit: hours for durations, percent for efficiency.
    func chartValue(_ value: Double) -> Double {
        self == .efficiency ? value * 100 : value / 3600
    }

    func text(_ value: Double, formatter: HealthFormatter) -> String {
        self == .efficiency ? formatter.percent(value) : formatter.duration(seconds: Int(value.rounded()))
    }

    func spoken(_ value: Double, formatter: HealthFormatter) -> String {
        self == .efficiency ? formatter.percent(value) : formatter.spokenDuration(seconds: Int(value.rounded()))
    }

    /// The usual range of this metric over `earlier` nights (see `SleepHistoryWindow`).
    public func basis(_ earlier: [SleepNightDetail]) -> PersonalRange.Comparison.Basis {
        PersonalRange.of(earlier.compactMap { value(NightMetrics($0)) })
    }
}

/// One day on a 7-night chart. `value` is `nil` on a day without a main sleep (a gap, never zero).
public struct TrendPoint: Sendable, Equatable, Identifiable {
    public let day: RingDay
    /// "F", under the bar.
    public let letter: String
    public let value: Double?
    /// The day the card is about.
    public let isHighlighted: Bool
    /// Points with one segment are joined by a line; a gap starts a new segment.
    public let segment: Int
    public var id: RingDay { day }
}

/// Builds the seven points ending at `last`, one per ring day, with gaps on days without a value.
func trend(
    ending last: RingDay, days: Int = 7, formatter: HealthFormatter, value: (RingDay) -> Double?
) -> [TrendPoint] {
    var segment = 0
    var previousWasGap = false
    return (0..<days).map { index in
        let day = last.adding(days: index - (days - 1))
        let v = value(day)
        if v == nil {
            previousWasGap = true
        } else if previousWasGap {
            segment += 1
            previousWasGap = false
        }
        return TrendPoint(day: day, letter: formatter.weekdayLetter(day), value: v, isHighlighted: day == last, segment: segment)
    }
}

/// A key metric's card: title, the night's value, its tag, and a 7-night chart with the usual
/// range shaded.
public struct MetricCard: Sendable, Equatable, Identifiable {
    public let metric: SleepMetric
    public let title: String
    public let valueText: String
    /// A second line, such as "22% of time asleep"; `nil` when there is none.
    public let detailText: String?
    public let comparison: PersonalRange.Comparison?
    public let points: [TrendPoint]
    /// The usual range in the chart's unit, when there is one.
    public let band: ClosedRange<Double>?
    public let yDomain: ClosedRange<Double>
    public let accessibilityLabel: String
    public var id: SleepMetric { metric }
}

/// The sleep window card: bedtime to wake time for seven nights, on a clock axis.
public struct SleepWindowCard: Sendable, Equatable {
    public struct Bar: Sendable, Equatable, Identifiable {
        public let day: RingDay
        public let letter: String
        /// Hours after noon of the day before, on the night's own clock; `nil` without a main sleep.
        public let from: Double?
        public let to: Double?
        public let isHighlighted: Bool
        public var id: RingDay { day }
    }

    public struct Tick: Sendable, Equatable, Identifiable {
        public let hours: Double
        public let label: String
        public var id: Double { hours }
    }

    public let title = "Sleep window"
    /// "23:10 – 07:05".
    public let valueText: String
    /// The night's time in bed against the usual.
    public let comparison: PersonalRange.Comparison?
    public let bars: [Bar]
    public let yDomain: ClosedRange<Double>
    public let ticks: [Tick]
    public let accessibilityLabel: String
}

/// The score with its parts, for the night screen.
public struct ScoreBreakdown: Sendable, Equatable {
    public struct Chip: Sendable, Equatable, Identifiable {
        public let title: String
        /// "42 of 50".
        public let pointsText: String
        public let accessibilityLabel: String
        public var id: String { title }
    }

    public let result: SleepScore.Result
    public let totalText: String
    public let labelText: String
    public let chips: [Chip]
    /// "Consistency counts once there are 7 nights." while it is awarded in full.
    public let consistencyNote: String?
    public let disclaimer = SleepScore.disclaimer
    public let explanation = SleepScore.explanation
    public let accessibilityLabel: String

    init(_ result: SleepScore.Result, formatter: HealthFormatter) {
        self.result = result
        totalText = formatter.number(result.total)
        labelText = result.label.rawValue
        func chip(_ title: String, _ points: Double, of maximum: Double) -> Chip {
            let got = formatter.number(Int(points.rounded())), max = formatter.number(Int(maximum.rounded()))
            return Chip(title: title, pointsText: "\(got) of \(max)", accessibilityLabel: "\(title), \(got) of \(max) points")
        }
        chips = [
            chip("Duration", result.duration, of: SleepScore.durationPoints),
            chip("Consistency", result.consistency, of: SleepScore.consistencyPoints),
            chip("Continuity", result.continuity, of: SleepScore.maximumContinuityPoints),
            chip("Stages", result.stages, of: SleepScore.maximumStagePoints),
        ]
        consistencyNote = result.consistencyCounted ? nil : SleepScore.consistencyNotCounted
        accessibilityLabel = "Sleep score \(totalText) of 100, \(labelText)"
    }
}

/// A night's value on a track, with the usual range as a band.
public struct RangeBar: Sendable, Equatable, Identifiable {
    public let metric: SleepMetric
    public let title: String
    /// The night's value, or "No deep sleep recorded".
    public let valueText: String
    public let comparison: PersonalRange.Comparison?
    /// Positions on the track, 0 to 1. `marker` is `nil` when the night has no value.
    public let marker: Double?
    public let band: ClosedRange<Double>?
    public let accessibilityLabel: String
    public var id: SleepMetric { metric }

    init(
        metric: SleepMetric, night: NightMetrics, earlier: [SleepNightDetail], formatter: HealthFormatter
    ) {
        self.metric = metric
        title = metric.title
        let value = metric.value(night)
        let basis = metric.basis(earlier)
        let history = earlier.compactMap { metric.value(NightMetrics($0)) }
        let track = Self.track(value: value, range: basis.range, history: history, metric: metric)
        func position(_ x: Double) -> Double { min(max((x - track.lowerBound) / (track.upperBound - track.lowerBound), 0), 1) }
        marker = value.map(position)
        band = basis.range.map { position($0.lower)...position($0.upper) }
        comparison = value.map(basis.compare)
        valueText = value.map { metric.text($0, formatter: formatter) }
            ?? (metric == .timeToDeepSleep ? "No deep sleep recorded" : "None")
        var label = "\(title), \(value.map { metric.spoken($0, formatter: formatter) } ?? valueText)"
        if let range = basis.range {
            label += ", usual \(metric.spoken(range.lower, formatter: formatter)) to \(metric.spoken(range.upper, formatter: formatter))"
        }
        if let comparison { label += ", \(comparison.text)" }
        accessibilityLabel = label
    }

    /// The track's extent in the metric's unit: everything shown on it, with a margin; never below
    /// zero, and at most 100% for efficiency.
    static func track(value: Double?, range: PersonalRange?, history: [Double], metric: SleepMetric) -> ClosedRange<Double> {
        let shown = history + [value, range?.lower, range?.upper].compactMap { $0 }
        var lower = shown.min() ?? 0
        var upper = shown.max() ?? 1
        let minimumSpan = metric == .efficiency ? 0.1 : 30 * 60
        if upper - lower < minimumSpan {
            let middle = (upper + lower) / 2
            lower = middle - minimumSpan / 2
            upper = middle + minimumSpan / 2
        }
        let margin = (upper - lower) * 0.1
        lower = max(lower - margin, 0)
        upper += margin
        if metric == .efficiency { upper = min(upper, 1) }
        if upper <= lower { upper = lower + minimumSpan }
        return lower...upper
    }
}

/// The Sleep tab's cards for the latest main sleep in the window.
public struct SleepOverview: Sendable, Equatable {
    /// The latest main sleep, which the cards are about; tap the last-night card to open it.
    public let night: SleepNightSummary
    /// "Tuesday 17 November", the night's day.
    public let dayTitle: String
    public let scoreValueText: String
    public let scoreLabelText: String
    public let scorePoints: [TrendPoint]
    public let scoreAccessibilityLabel: String
    public let duration: MetricCard
    public let timeline: LaneTimeline
    public let uncertainty: String?
    public let asleep: MetricCard
    public let window: SleepWindowCard
    public let rem: MetricCard
    public let deep: MetricCard
    public let efficiency: MetricCard
    public let legend = PersonalRange.legend
    public let scoreDisclaimer = SleepScore.disclaimer

    /// The days of the 7-night charts.
    public static let trendDays = 7

    /// How far back the Sleep tab reads: the window, plus enough earlier days to give each of the
    /// seven charted nights its own previous 30 days.
    public static func readRange(for window: (first: RingDay, last: RingDay)) -> (first: RingDay, last: RingDay) {
        (window.first.adding(days: -(SleepHistoryWindow.lookbackDays + trendDays)), window.last)
    }

    /// `nil` when no main sleep falls in `days`.
    public init?(_ details: [SleepNightDetail], window days: (first: RingDay, last: RingDay), formatter: HealthFormatter) {
        let mains = details.filter(\.summary.isMainSleep)
        guard let latest = mains.filter({ days.first <= $0.summary.day && $0.summary.day <= days.last })
            .max(by: { $0.summary.day < $1.summary.day })
        else { return nil }
        let summary = latest.summary
        let mainByDay = Dictionary(mains.map { ($0.summary.day, $0) }, uniquingKeysWith: { a, _ in a })
        let metrics = NightMetrics(latest)
        let earlier = SleepHistoryWindow.earlierMainSleeps(before: summary, in: details)
        night = summary
        dayTitle = formatter.day(summary.day)

        func score(_ detail: SleepNightDetail) -> SleepScore.Result? {
            let earlier = SleepHistoryWindow.earlierMainSleeps(before: detail.summary, in: details)
            return SleepScore.score(NightMetrics(detail), earlierBedtimes: earlier.map { NightMetrics($0).bedtimeSecondsOfDay })
        }
        let latestScore = score(latest)
        scoreValueText = latestScore.map { formatter.number($0.total) } ?? "–"
        scoreLabelText = latestScore?.label.rawValue ?? "No score"
        scorePoints = trend(ending: summary.day, formatter: formatter) { day in
            mainByDay[day].flatMap(score).map { Double($0.total) }
        }
        scoreAccessibilityLabel = latestScore.map { "Sleep score \(formatter.number($0.total)) of 100, \($0.label.rawValue)" }
            ?? "No sleep score for this night"

        func card(_ metric: SleepMetric, title: String? = nil, detail: String? = nil) -> MetricCard {
            Self.card(
                metric, title: title, detail: detail, latest: metrics, earlier: earlier, day: summary.day,
                mainByDay: mainByDay, formatter: formatter)
        }
        duration = card(.asleep, title: "Duration")
        asleep = card(.asleep)
        rem = card(.rem, detail: metrics.remShare.map { "\(formatter.percent($0)) of time asleep" })
        deep = card(.deep, detail: metrics.deepShare.map { "\(formatter.percent($0)) of time asleep" })
        efficiency = card(.efficiency)
        window = Self.windowCard(latest: latest, earlier: earlier, mainByDay: mainByDay, formatter: formatter)
        timeline = LaneTimeline(latest, formatter: formatter)
        uncertainty = HealthFormatter.uncertaintyMarker(for: summary.timeSource)
    }

    static func card(
        _ metric: SleepMetric, title: String?, detail: String?, latest: NightMetrics, earlier: [SleepNightDetail],
        day: RingDay, mainByDay: [RingDay: SleepNightDetail], formatter: HealthFormatter
    ) -> MetricCard {
        let value = metric.value(latest)
        let basis = metric.basis(earlier)
        let points = trend(ending: day, formatter: formatter) { day in
            mainByDay[day].flatMap { metric.value(NightMetrics($0)) }.map(metric.chartValue)
        }
        let band = basis.range.map { metric.chartValue($0.lower)...metric.chartValue($0.upper) }
        let shown = points.compactMap(\.value) + [band?.lowerBound, band?.upperBound].compactMap { $0 }
        let top = max(shown.max() ?? 1, metric == .efficiency ? 100 : 1)
        let comparison = value.map(basis.compare)
        let title = title ?? metric.title
        let valueText = value.map { metric.text($0, formatter: formatter) } ?? "None"
        var label = "\(title), \(value.map { metric.spoken($0, formatter: formatter) } ?? valueText)"
        if let detail { label += ", \(detail)" }
        if let comparison { label += ", \(comparison.text)" }
        return MetricCard(
            metric: metric, title: title, valueText: valueText, detailText: detail, comparison: comparison,
            points: points, band: band, yDomain: 0...(metric == .efficiency ? 100 : top * 1.1), accessibilityLabel: label)
    }

    static func windowCard(
        latest: SleepNightDetail, earlier: [SleepNightDetail], mainByDay: [RingDay: SleepNightDetail],
        formatter: HealthFormatter
    ) -> SleepWindowCard {
        let summary = latest.summary
        let noon = 12 * 3600
        let bars = trend(ending: summary.day, formatter: formatter) { _ in nil }.map { point in
            let night = mainByDay[point.day].map(NightMetrics.init)
            let from = night.map { Double(($0.bedtimeSecondsOfDay - noon).modulo(SleepScore.day)) / 3600 }
            return SleepWindowCard.Bar(
                day: point.day, letter: point.letter, from: from,
                to: from.flatMap { from in night.map { from + Double($0.secondsInBed) / 3600 } },
                isHighlighted: point.isHighlighted)
        }
        let low = (bars.compactMap(\.from).min() ?? 10).rounded(.down)
        let high = (bars.compactMap(\.to).max() ?? 20).rounded(.up)
        let domain = low...max(high, low + 1)
        let step = domain.upperBound - domain.lowerBound > 8 ? 4.0 : 2.0
        var ticks: [SleepWindowCard.Tick] = []
        var hours = (domain.lowerBound / step).rounded(.up) * step
        while hours <= domain.upperBound {
            ticks.append(SleepWindowCard.Tick(hours: hours, label: formatter.timeOfDay(secondsAfterMidnight: noon + Int(hours * 3600))))
            hours += step
        }
        let valueText = "\(formatter.time(summary.start, utcOffsetSeconds: summary.utcOffsetSeconds)) – "
            + formatter.time(summary.end, utcOffsetSeconds: summary.utcOffsetSeconds)
        let inBed = SleepMetric.timeInBed
        let comparison = inBed.value(NightMetrics(latest)).map(inBed.basis(earlier).compare)
        var label = "Sleep window, \(valueText.replacingOccurrences(of: " – ", with: " to "))"
        if let comparison { label += ", time in bed \(comparison.text)" }
        return SleepWindowCard(
            valueText: valueText, comparison: comparison, bars: bars, yDomain: domain, ticks: ticks,
            accessibilityLabel: label)
    }
}
