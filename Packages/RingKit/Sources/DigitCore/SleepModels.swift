import Foundation
import Observation
import RingProtocol
import RingStore

/// One night as the sleep list shows it. The text is on the night's own clock
/// (`summary.utcOffsetSeconds`), never the device's zone.
public struct SleepNightRow: Sendable, Equatable, Identifiable {
    public let summary: SleepNightSummary
    /// "Main sleep" or "Nap".
    public let kindText: String
    public let asleepText: String
    public let bedtimeText: String
    public let wakeText: String
    /// "Times uncertain" when the night's times are not `resolved`.
    public let uncertainty: String?
    public let uncertaintyExplanation: String?
    /// The whole row in words, for VoiceOver.
    public let accessibilityLabel: String

    /// A night is found again by its start (see `RingStore.sleepNightDetail`).
    public var id: Date { summary.start }

    init(_ summary: SleepNightSummary, formatter: HealthFormatter) {
        self.summary = summary
        kindText = summary.isMainSleep ? "Main sleep" : "Nap"
        asleepText = formatter.duration(seconds: summary.secondsAsleep)
        bedtimeText = formatter.time(summary.start, utcOffsetSeconds: summary.utcOffsetSeconds)
        wakeText = formatter.time(summary.end, utcOffsetSeconds: summary.utcOffsetSeconds)
        uncertainty = HealthFormatter.uncertaintyMarker(for: summary.timeSource)
        uncertaintyExplanation = HealthFormatter.uncertaintyExplanation(for: summary.timeSource)
        var label = [
            formatter.day(summary.day), kindText.lowercased(),
            "\(formatter.spokenDuration(seconds: summary.secondsAsleep)) asleep",
            "bedtime \(bedtimeText)", "woke \(wakeText)",
        ]
        if let uncertainty { label.append(uncertainty.lowercased()) }
        accessibilityLabel = label.joined(separator: ", ")
    }
}

/// The nights of one ring-local day: main sleep first, then naps, newest first.
public struct SleepDayGroup: Sendable, Equatable, Identifiable {
    public let day: RingDay
    public let title: String
    public let nights: [SleepNightRow]
    public var id: RingDay { day }
}

/// The sleep list: the nights of the last `RecentDays.count` ring-local days, newest day first.
///
/// It reloads when told (`reload()`): `DigitModel` does so when a sync or pairing ends, when the
/// app becomes active, and the screen on pull to refresh. It does not observe the store.
@MainActor
@Observable
public final class SleepListModel {
    public private(set) var state = DataState<[SleepDayGroup]>.loading

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
        do {
            let nights = try await history.sleepNights(ringID: ringID, from: window.first, through: window.last)
            guard mine == generation else { return }
            state = nights.isEmpty ? .noData : .loaded(Self.groups(nights, formatter: formatter))
        } catch {
            guard mine == generation else { return }
            state = DataLoadFailure.state(after: error, previous: state)
        }
    }

    static func groups(_ nights: [SleepNightSummary], formatter: HealthFormatter) -> [SleepDayGroup] {
        Dictionary(grouping: nights, by: \.day)
            .sorted { $0.key > $1.key }
            .map { day, nights in
                let ordered = nights.sorted { a, b in
                    if a.isMainSleep != b.isMainSleep { return a.isMainSleep }
                    return a.start > b.start
                }
                return SleepDayGroup(
                    day: day, title: formatter.day(day), nights: ordered.map { SleepNightRow($0, formatter: formatter) })
            }
    }
}

/// One line of a night's totals.
public struct SleepTotal: Sendable, Equatable, Identifiable {
    public let title: String
    public let value: String
    public let accessibilityLabel: String
    public var id: String { title }
}

/// One stage on the night's timeline.
public struct SleepStageSegment: Sendable, Equatable, Identifiable {
    public let kind: SleepStageKind
    public let name: String
    public let start: Date
    public let end: Date
    /// "Deep, 23:10 to 23:55, 45 minutes", on the night's own clock.
    public let accessibilityLabel: String
    /// Stages are in time order; the position keeps two stages with one start apart.
    public let id: Int
}

/// A night as its screen shows it.
public struct SleepNightPage: Sendable, Equatable {
    public let summary: SleepNightSummary
    public let title: String
    public let kindText: String
    public let bedtimeText: String
    public let wakeText: String
    public let totals: [SleepTotal]
    public let stages: [SleepStageSegment]
    /// Where the timeline's axis marks go: whole hours on the night's own clock (every one, two or
    /// three hours depending on the night's length), never the device zone's hours.
    public let hourTicks: [Date]
    /// The night's clock as a fixed-offset zone, for anything that formats its dates.
    public let timeZone: TimeZone
    public let uncertainty: String?
    public let uncertaintyExplanation: String?

    init(_ detail: SleepNightDetail, formatter: HealthFormatter) {
        let summary = detail.summary
        let offset = summary.utcOffsetSeconds
        self.summary = summary
        title = formatter.day(summary.day)
        kindText = summary.isMainSleep ? "Main sleep" : "Nap"
        bedtimeText = formatter.time(summary.start, utcOffsetSeconds: offset)
        wakeText = formatter.time(summary.end, utcOffsetSeconds: offset)
        func total(_ title: String, _ seconds: Int, spoken: String) -> SleepTotal {
            SleepTotal(
                title: title, value: formatter.duration(seconds: seconds),
                accessibilityLabel: "\(formatter.spokenDuration(seconds: seconds)) \(spoken)")
        }
        let byKind = summary.secondsByKind
        totals = [
            total("Asleep", summary.secondsAsleep, spoken: "asleep"),
            total("Deep", byKind[.deep] ?? 0, spoken: "deep sleep"),
            total("Light", byKind[.light] ?? 0, spoken: "light sleep"),
            total("REM", byKind[.rem] ?? 0, spoken: "REM sleep"),
            total("Nap", byKind[.nap] ?? 0, spoken: "nap"),
            total("Interruptions", summary.interruptionSeconds, spoken: "of interruptions"),
        ]
        stages = detail.stages.enumerated().map { index, stage in
            let end = stage.start.addingTimeInterval(TimeInterval(stage.durationSeconds))
            let name = HealthFormatter.name(of: stage.kind)
            return SleepStageSegment(
                kind: stage.kind, name: name, start: stage.start, end: end,
                accessibilityLabel: "\(name), \(formatter.time(stage.start, utcOffsetSeconds: offset)) to "
                    + "\(formatter.time(end, utcOffsetSeconds: offset)), \(formatter.spokenDuration(seconds: stage.durationSeconds))",
                id: index)
        }
        hourTicks = Self.hourTicks(from: summary.start, to: summary.end, utcOffsetSeconds: offset)
        timeZone = HealthFormatter.zone(utcOffsetSeconds: offset)
        uncertainty = HealthFormatter.uncertaintyMarker(for: summary.timeSource)
        uncertaintyExplanation = HealthFormatter.uncertaintyExplanation(for: summary.timeSource)
    }

    /// Instants in `start...end` that fall on a whole hour of the clock `utcOffsetSeconds` ahead of
    /// UTC, every `step` hours (1 up to 4 hours, 2 up to 10, 3 beyond), aligned to multiples of
    /// the step on that clock (so 00:00, 02:00, 04:00).
    public static func hourTicks(from start: Date, to end: Date, utcOffsetSeconds: Int) -> [Date] {
        let span = end.timeIntervalSince(start)
        guard span >= 0 else { return [] }
        let step = Int64(span <= 4 * 3600 ? 1 : span <= 10 * 3600 ? 2 : 3) * 3600
        let localStart = Int64(start.timeIntervalSince1970.rounded(.up)) + Int64(utcOffsetSeconds)
        let localEnd = Int64(end.timeIntervalSince1970.rounded(.down)) + Int64(utcOffsetSeconds)
        // First multiple of the step at or after the start (floor division, safe for any sign).
        var tick = (localStart + step - 1).floorDivided(by: step) * step
        var ticks: [Date] = []
        while tick <= localEnd {
            ticks.append(Date(timeIntervalSince1970: TimeInterval(tick - Int64(utcOffsetSeconds))))
            tick += step
        }
        return ticks
    }
}

private extension Int64 {
    func floorDivided(by divisor: Int64) -> Int64 {
        let q = self / divisor
        return (self % divisor != 0 && (self < 0) != (divisor < 0)) ? q - 1 : q
    }
}

/// One night, found by its start. When ingestion has moved the night's start since the list was
/// read, the store returns `nil`: the model then says so (`moved`) and asks for the list to be
/// reloaded, and the screen returns to it.
@MainActor
@Observable
public final class NightDetailModel {
    public enum State: Sendable, Equatable {
        case loading
        case loaded(SleepNightPage)
        /// The night is no longer found at this start. Go back to the list, which reloads.
        case moved
        case failed(String)
    }

    public private(set) var state = State.loading
    /// The start the night was opened with.
    public let start: Date

    @ObservationIgnored private let ringID: UUID
    @ObservationIgnored private let history: any RingHistoryReader
    @ObservationIgnored private let formatter: HealthFormatter
    @ObservationIgnored private let onMoved: @MainActor () -> Void
    @ObservationIgnored private var generation = 0

    public init(
        ringID: UUID, start: Date, history: any RingHistoryReader, formatter: HealthFormatter = HealthFormatter(),
        onMoved: @escaping @MainActor () -> Void
    ) {
        self.ringID = ringID
        self.start = start
        self.history = history
        self.formatter = formatter
        self.onMoved = onMoved
    }

    public func reload() async {
        // Once moved, stay moved: the screen is on its way back to the list.
        guard state != .moved else { return }
        generation += 1
        let mine = generation
        do {
            let detail = try await history.sleepNightDetail(ringID: ringID, startingAt: start)
            guard mine == generation, state != .moved else { return }
            if let detail {
                state = .loaded(SleepNightPage(detail, formatter: formatter))
            } else {
                state = .moved
                onMoved()
            }
        } catch {
            guard mine == generation, state != .moved else { return }
            // A refused load keeps the night shown; it is retried when the app becomes active.
            if DataLoadFailure.isSuspension(error), case .loaded = state { return }
            state = .failed(DataLoadFailure.message(for: error))
        }
    }
}
