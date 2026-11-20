import Foundation
import Observation
import RingStore

/// The Sleep tab's cards: the latest main sleep of the last `RecentDays.count` ring-local days,
/// with its score, timeline and key metrics, each against seven nights and the usual range.
///
/// One read per reload (`sleepNightDetails`, over the window and the 37 days before it, so each
/// charted night has its own previous 30 days). Reloaded like `SleepListModel` (by `DigitModel`,
/// and on pull to refresh); it does not observe the store.
@MainActor
@Observable
public final class SleepOverviewModel {
    public private(set) var state = DataState<SleepOverview>.loading

    @ObservationIgnored private let pairedRing: PairedRing
    @ObservationIgnored private let history: any RingHistoryReader
    @ObservationIgnored private let now: @Sendable () -> Date
    @ObservationIgnored private let timeZone: @Sendable () -> TimeZone
    @ObservationIgnored let formatter: HealthFormatter
    /// Only the newest load may change `state`, so an older one finishing late cannot overwrite it.
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
        let range = SleepOverview.readRange(for: window)
        do {
            let details = try await history.sleepNightDetails(ringID: ringID, from: range.first, through: range.last)
            guard mine == generation else { return }
            state = SleepOverview(details, window: window, formatter: formatter).map { .loaded($0) } ?? .noData
        } catch {
            guard mine == generation else { return }
            state = DataLoadFailure.state(after: error, previous: state)
        }
    }
}
