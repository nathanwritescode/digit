import Foundation
import RingProtocol
import RingStore

/// What Digit derives from one night (`SleepNightDetail`), for the score, the personal ranges and
/// the screens. Pure arithmetic on stored values; no formatting.
///
/// The R99 reports no awake stages and no restlessness: interruptions are the stretches inside the
/// night covered by no session (`SleepNightSummary.interruptions`), and wakes too short for the
/// ring to end a session are invisible. Heart-rate readings are minutes apart, too sparse to find
/// brief wakes, so none are inferred.
public struct NightMetrics: Sendable, Equatable {
    /// Deep, light, REM and nap stages, as stored.
    public var secondsAsleep: Int
    /// Night start to night end ("time in bed": the ring gives no separate in-bed time).
    public var secondsInBed: Int
    public var deepSeconds: Int
    public var lightSeconds: Int
    public var remSeconds: Int
    public var napSeconds: Int
    /// Time asleep over time in bed, at most 1 (overlapping sessions count their stages twice,
    /// which could otherwise push it past 1). `nil` for a night with no length.
    public var efficiency: Double?
    /// Shares of time asleep; `nil` when there is no time asleep.
    public var deepShare: Double?
    public var remShare: Double?
    /// Night start to the start of the first deep stage; `nil` when the night has no deep stage.
    public var secondsToDeepSleep: Int?
    public var interruptionCount: Int
    public var interruptionSeconds: Int
    /// The night's start as a time of day on its own clock (`utcOffsetSeconds`), seconds after
    /// midnight: its bedtime.
    public var bedtimeSecondsOfDay: Int

    public init(_ detail: SleepNightDetail) {
        let summary = detail.summary
        let byKind = summary.secondsByKind
        secondsAsleep = summary.secondsAsleep
        secondsInBed = max(Int(summary.end.timeIntervalSince(summary.start).rounded(.down)), 0)
        deepSeconds = byKind[.deep] ?? 0
        lightSeconds = byKind[.light] ?? 0
        remSeconds = byKind[.rem] ?? 0
        napSeconds = byKind[.nap] ?? 0
        efficiency = secondsInBed > 0 ? min(Double(secondsAsleep) / Double(secondsInBed), 1) : nil
        deepShare = secondsAsleep > 0 ? Double(deepSeconds) / Double(secondsAsleep) : nil
        remShare = secondsAsleep > 0 ? Double(remSeconds) / Double(secondsAsleep) : nil
        secondsToDeepSleep = detail.stages.filter { $0.kind == .deep }.map(\.start).min().map {
            max(Int($0.timeIntervalSince(summary.start).rounded(.down)), 0)
        }
        interruptionCount = summary.interruptions.count
        interruptionSeconds = summary.interruptionSeconds
        bedtimeSecondsOfDay = Self.secondsOfDay(summary.start, utcOffsetSeconds: summary.utcOffsetSeconds)
    }

    /// `date` as seconds after midnight on the clock `utcOffsetSeconds` ahead of UTC.
    public static func secondsOfDay(_ date: Date, utcOffsetSeconds: Int) -> Int {
        (Int(date.timeIntervalSince1970.rounded(.down)) + utcOffsetSeconds).modulo(SleepScore.day)
    }

    /// The stretches the night's sessions cover: start to end without its interruptions. A night's
    /// sleeping heart rate is taken over these.
    public static func sessionCover(_ summary: SleepNightSummary) -> [DateInterval] {
        guard summary.end >= summary.start else { return [] }
        var stretches: [DateInterval] = []
        var from = summary.start
        for gap in summary.interruptions.sorted(by: { $0.start < $1.start }) where gap.start >= from {
            stretches.append(DateInterval(start: from, end: gap.start))
            from = max(from, gap.end)
        }
        if summary.end >= from { stretches.append(DateInterval(start: from, end: summary.end)) }
        return stretches
    }
}

/// The main sleeps a night is compared with: those of the `lookbackDays` ring days before its day,
/// and any other main sleep of its own day (there is none, a day has one), never the night itself.
/// Nights with no time asleep are left out (they have nothing to compare).
public enum SleepHistoryWindow {
    public static let lookbackDays = 30

    public static func earlierMainSleeps(before night: SleepNightSummary, in nights: [SleepNightDetail])
        -> [SleepNightDetail]
    {
        let first = night.day.adding(days: -lookbackDays)
        return nights.filter {
            $0.summary.isMainSleep && $0.summary.start != night.start && $0.summary.secondsAsleep > 0
                && first <= $0.summary.day && $0.summary.day <= night.day
        }
    }
}
