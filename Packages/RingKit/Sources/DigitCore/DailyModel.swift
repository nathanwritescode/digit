import Foundation
import Observation
import RingStore

/// One day as the Days list shows it.
public struct DayRow: Sendable, Equatable, Identifiable {
    public let summary: DailySummary
    public let title: String
    public let stepsText: String
    public let distanceText: String
    /// "52–118 bpm, mean 64"; `nil` on a day without heart-rate readings.
    public let heartRateText: String?
    public let uncertainty: String?
    public let uncertaintyExplanation: String?
    /// The whole row in words, for VoiceOver.
    public let accessibilityLabel: String
    public var id: RingDay { summary.day }

    init(_ summary: DailySummary, formatter: HealthFormatter) {
        self.summary = summary
        title = formatter.day(summary.day)
        stepsText = formatter.steps(summary.steps)
        distanceText = formatter.distance(meters: summary.distanceMeters)
        heartRateText = summary.heartRate.map(formatter.heartRate)
        uncertainty = HealthFormatter.uncertaintyMarker(for: summary.timeSource)
        uncertaintyExplanation = HealthFormatter.uncertaintyExplanation(for: summary.timeSource)
        var label = [title, stepsText, formatter.spokenDistance(meters: summary.distanceMeters)]
        label.append(summary.heartRate.map(formatter.spokenHeartRate) ?? "no heart rate readings")
        if let uncertainty { label.append(uncertainty.lowercased()) }
        accessibilityLabel = label.joined(separator: ", ")
    }
}

/// The Days list: steps and heart rate for the last `RecentDays.count` ring-local days that have
/// any, newest first. Reloaded like `SleepListModel`; it does not observe the store.
@MainActor
@Observable
public final class DailyModel {
    public private(set) var state = DataState<[DayRow]>.loading

    @ObservationIgnored private let pairedRing: PairedRing
    @ObservationIgnored private let history: any RingHistoryReader
    @ObservationIgnored private let now: @Sendable () -> Date
    @ObservationIgnored private let timeZone: @Sendable () -> TimeZone
    @ObservationIgnored private let formatter: HealthFormatter
    @ObservationIgnored private var generation = 0

    public init(
        pairedRing: PairedRing, history: any RingHistoryReader, now: @escaping @Sendable () -> Date,
        timeZone: @escaping @Sendable () -> TimeZone, formatter: HealthFormatter = HealthFormatter()
    ) {
        self.pairedRing = pairedRing
        self.history = history
        self.now = now
        self.timeZone = timeZone
        self.formatter = formatter
    }

    public func reload() async {
        guard pairedRing.isLoaded else { return }
        generation += 1
        let mine = generation
        guard let ringID = pairedRing.ring?.id else {
            state = .notPaired
            return
        }
        let window = RecentDays.window(now: now(), timeZone: timeZone())
        do {
            let days = try await history.dailySummaries(ringID: ringID, from: window.first, through: window.last)
            guard mine == generation else { return }
            state = days.isEmpty
                ? .noData : .loaded(days.sorted { $0.day > $1.day }.map { DayRow($0, formatter: formatter) })
        } catch {
            guard mine == generation else { return }
            state = DataLoadFailure.state(after: error, previous: state)
        }
    }
}
