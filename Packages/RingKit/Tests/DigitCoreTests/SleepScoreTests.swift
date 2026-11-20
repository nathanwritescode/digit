import DigitCore
import Foundation
import RingProtocol
import RingStore
import Testing

// Digit's sleep score, the personal ranges and the derived night metrics. Every night and stage
// here is SYNTHETIC, made up in code.

/// 2027-01-15 00:00 UTC.
private let midnight = Date(timeIntervalSince1970: 1_799_971_200)
private func hm(_ h: Int, _ m: Int) -> Int { h * 3600 + m * 60 }

/// A night of `stages` (kind, minutes) laid end to end from `start`, with `gaps` (minutes after
/// the start, minutes long) as interruptions; summary values computed as the store would.
func syntheticNight(
    start: Date, stages: [(SleepStageKind, Int)], gaps: [(after: Int, minutes: Int)] = [], day: RingDay? = nil,
    offset: Int = 0, main: Bool = true
) -> SleepNightDetail {
    var entries: [SleepStageEntry] = []
    var t = start
    for (kind, minutes) in stages {
        if let gap = gaps.first(where: { start.addingTimeInterval(TimeInterval($0.after * 60)) == t }) {
            t = t.addingTimeInterval(TimeInterval(gap.minutes * 60))
        }
        entries.append(SleepStageEntry(kind: kind, start: t, durationSeconds: minutes * 60))
        t = t.addingTimeInterval(TimeInterval(minutes * 60))
    }
    var byKind = Dictionary(uniqueKeysWithValues: SleepStageKind.allCases.map { ($0, 0) })
    for e in entries { byKind[e.kind, default: 0] += e.durationSeconds }
    let interruptions = gaps.map {
        SleepInterruption(
            start: start.addingTimeInterval(TimeInterval($0.after * 60)),
            end: start.addingTimeInterval(TimeInterval(($0.after + $0.minutes) * 60)))
    }
    let end = entries.last.map { $0.start.addingTimeInterval(TimeInterval($0.durationSeconds)) } ?? start
    let summary = SleepNightSummary(
        day: day ?? RingDay(instant: end, utcOffsetSeconds: offset), utcOffsetSeconds: offset, start: start, end: end,
        secondsByKind: byKind, secondsAsleep: [SleepStageKind.deep, .light, .rem, .nap].reduce(0) { $0 + byKind[$1]! },
        interruptions: interruptions, timeSource: .resolved, isMainSleep: main)
    return SleepNightDetail(summary: summary, stages: entries)
}

private func metrics(asleepMinutes: Int, deep: Int = 0, rem: Int = 0, gaps: [(after: Int, minutes: Int)] = []) -> NightMetrics {
    let light = asleepMinutes - deep - rem
    var stages: [(SleepStageKind, Int)] = []
    if light > 0 { stages.append((.light, light)) }
    if deep > 0 { stages.append((.deep, deep)) }
    if rem > 0 { stages.append((.rem, rem)) }
    return NightMetrics(syntheticNight(start: midnight.addingTimeInterval(-3600), stages: stages, gaps: gaps))
}

@Suite(.timeLimit(.minutes(1)))
struct SleepScorePartsTests {
    @Test func durationIsLinearUpToSevenHoursThenFull() {
        #expect(SleepScore.durationPart(secondsAsleep: 0) == 0)
        #expect(SleepScore.durationPart(secondsAsleep: hm(3, 30)) == 25)
        #expect(SleepScore.durationPart(secondsAsleep: hm(7, 0)) == 50)
        #expect(SleepScore.durationPart(secondsAsleep: hm(9, 0)) == 50)
        #expect(SleepScore.durationPart(secondsAsleep: -5) == 0)
    }

    @Test func consistencyIsFullWithinHalfAnHourAndZeroFromTwoHours() {
        #expect(SleepScore.consistencyPart(distanceSeconds: 0) == 20)
        #expect(SleepScore.consistencyPart(distanceSeconds: hm(0, 30)) == 20)
        // Linear between: 75 minutes is halfway from 30 to 120.
        #expect(abs(SleepScore.consistencyPart(distanceSeconds: hm(1, 15)) - 10) < 1e-9)
        #expect(abs(SleepScore.consistencyPart(distanceSeconds: hm(1, 30)) - 20.0 / 3) < 1e-9)
        #expect(SleepScore.consistencyPart(distanceSeconds: hm(2, 0)) == 0)
        #expect(SleepScore.consistencyPart(distanceSeconds: hm(6, 0)) == 0)
    }

    @Test func interruptionMinutesAreFullToTwentyAndZeroFromSixty() {
        #expect(SleepScore.interruptionMinutesPart(seconds: 0) == 10)
        #expect(SleepScore.interruptionMinutesPart(seconds: 20 * 60) == 10)
        #expect(SleepScore.interruptionMinutesPart(seconds: 40 * 60) == 5)
        #expect(SleepScore.interruptionMinutesPart(seconds: 50 * 60) == 2.5)
        #expect(SleepScore.interruptionMinutesPart(seconds: 60 * 60) == 0)
        #expect(SleepScore.interruptionMinutesPart(seconds: 90 * 60) == 0)
    }

    @Test func interruptionCountSteps() {
        #expect([0, 1, 2, 3, 4, 5, 20].map(SleepScore.interruptionCountPart(count:)) == [10, 10, 5, 2, 0, 0, 0])
    }

    @Test func stagesAreProportionalToTheirTargets() {
        #expect(SleepScore.stagePart(deepShare: 0, remShare: 0) == 0)
        #expect(SleepScore.stagePart(deepShare: 0.15, remShare: 0.20) == 10)
        #expect(SleepScore.stagePart(deepShare: 0.30, remShare: 0.40) == 10)
        #expect(abs(SleepScore.stagePart(deepShare: 0.075, remShare: 0.10) - 5) < 1e-9)
        #expect(abs(SleepScore.stagePart(deepShare: 0.15, remShare: 0) - 5) < 1e-9)
    }

    @Test func labelsAtTheirEdges() {
        #expect(SleepScore.Label(score: 59) == .poor)
        #expect(SleepScore.Label(score: 60) == .fair)
        #expect(SleepScore.Label(score: 79) == .fair)
        #expect(SleepScore.Label(score: 80) == .good)
        #expect(SleepScore.Label(score: 100) == .good)
        #expect(SleepScore.Label(score: 0) == .poor)
        #expect(SleepScore.Label.allCases.map(\.rawValue) == ["Good", "Fair", "Poor"])
    }

    @Test func theMaximaAddUpToOneHundred() {
        #expect(SleepScore.durationPoints + SleepScore.consistencyPoints + SleepScore.maximumContinuityPoints
            + SleepScore.maximumStagePoints == 100)
    }
}

@Suite(.timeLimit(.minutes(1)))
struct TypicalBedtimeTests {
    @Test func theCircularMedianKeepsBedtimesAroundMidnightTogether() {
        // 23:00, 23:30, 00:00, 00:30, 01:00: the median is midnight, not noon or 01:00.
        #expect(SleepScore.typicalBedtime([hm(23, 0), hm(23, 30), 0, hm(0, 30), hm(1, 0)]) == 0)
        // An even count takes the middle two's mean, across midnight: 23:40 and 00:20 give 00:00.
        #expect(SleepScore.typicalBedtime([hm(23, 0), hm(23, 40), hm(0, 20), hm(1, 0)]) == 0)
        // Ordinary evenings.
        #expect(SleepScore.typicalBedtime([hm(22, 0), hm(22, 30), hm(23, 0)]) == hm(22, 30))
        #expect(SleepScore.typicalBedtime([hm(23, 50)]) == hm(23, 50))
        #expect(SleepScore.typicalBedtime([]) == nil)
        // Ties: identical bedtimes.
        #expect(SleepScore.typicalBedtime(Array(repeating: hm(23, 15), count: 8)) == hm(23, 15))
    }

    @Test func circularDistanceTakesTheShortWayRound() {
        #expect(SleepScore.circularDistance(hm(23, 50), hm(0, 10)) == hm(0, 20))
        #expect(SleepScore.circularDistance(hm(0, 10), hm(23, 50)) == hm(0, 20))
        #expect(SleepScore.circularDistance(hm(6, 0), hm(18, 0)) == hm(12, 0))
        #expect(SleepScore.circularDistance(hm(22, 0), hm(23, 0)) == hm(1, 0))
    }
}

@Suite(.timeLimit(.minutes(1)))
struct SleepScoreTests {
    private let seven = Array(repeating: hm(23, 0), count: 7)

    @Test func aFullNight() throws {
        // 8 h asleep starting 23:00, deep 20%, REM 25%, no interruptions, bedtime as usual.
        let m = metrics(asleepMinutes: 480, deep: 96, rem: 120)
        let result = try #require(SleepScore.score(m, earlierBedtimes: seven))
        #expect(result.total == 100)
        #expect(result.label == .good)
        #expect(result.consistencyCounted)
    }

    @Test func consistencyAcrossMidnight() throws {
        // A typical bedtime of 23:50 against a night starting at 00:10: 20 minutes, full marks.
        let night = NightMetrics(syntheticNight(start: midnight.addingTimeInterval(600), stages: [(.light, 420)]))
        #expect(night.bedtimeSecondsOfDay == hm(0, 10))
        let result = try #require(SleepScore.score(night, earlierBedtimes: Array(repeating: hm(23, 50), count: 7)))
        #expect(result.consistency == 20)
        // 01:05 is 75 minutes from 23:50 across midnight: halfway down the ramp.
        let late = NightMetrics(syntheticNight(start: midnight.addingTimeInterval(TimeInterval(hm(1, 5))), stages: [(.light, 420)]))
        #expect(abs(try #require(SleepScore.score(late, earlierBedtimes: Array(repeating: hm(23, 50), count: 7))).consistency - 10) < 1e-9)
    }

    @Test func fewerThanSevenEarlierNightsAwardFullConsistency() throws {
        let m = metrics(asleepMinutes: 420)  // starts at 23:00
        let six = Array(repeating: hm(19, 0), count: 6)
        let result = try #require(SleepScore.score(m, earlierBedtimes: six))
        #expect(result.consistency == 20)
        #expect(!result.consistencyCounted)
        // The seventh night counts it: four hours from 19:00 scores nothing.
        let counted = try #require(SleepScore.score(m, earlierBedtimes: six + [hm(19, 0)]))
        #expect(counted.consistency == 0)
        #expect(counted.consistencyCounted)
    }

    @Test func interruptionsCountAndMinutesTogether() throws {
        // Three 15-minute interruptions: 45 minutes (10 × 15/40 = 3.75) and a count of three (2).
        let m = metrics(asleepMinutes: 420, gaps: [(60, 15), (180, 15), (300, 15)])
        #expect(m.interruptionCount == 3)
        #expect(m.interruptionSeconds == 45 * 60)
        let result = try #require(SleepScore.score(m, earlierBedtimes: []))
        #expect(abs(result.continuity - 5.75) < 1e-9)
    }

    @Test func aNightWithoutDeepOrREMScoresNoStagePoints() throws {
        let m = metrics(asleepMinutes: 420)
        #expect(m.deepShare == 0 && m.remShare == 0)
        #expect(m.secondsToDeepSleep == nil)
        let result = try #require(SleepScore.score(m, earlierBedtimes: []))
        #expect(result.stages == 0)
        #expect(result.total == 90)
    }

    @Test func theTotalIsRoundedHalfUp() throws {
        // 7 h asleep (50), consistency in full (no history, 20), one interruption of 42 minutes
        // (count 10, minutes 10 × 18/40 = 4.5), no deep or REM (0): exactly 84.5, which rounds to 85.
        let half = try #require(SleepScore.score(metrics(asleepMinutes: 420, gaps: [(60, 42)]), earlierBedtimes: []))
        #expect(half.duration + half.consistency + half.continuity + half.stages == 84.5)
        #expect(half.total == 85)
        // 400 minutes (47.62) with 7.5% deep (2.5): 90.12 rounds down to 90.
        let down = try #require(SleepScore.score(metrics(asleepMinutes: 400, deep: 30), earlierBedtimes: []))
        #expect(abs(down.duration + down.stages - (400.0 / 420 * 50 + 2.5)) < 1e-9)
        #expect(down.total == 90)
    }

    @Test func aNightWithNoLengthHasNoScore() {
        let empty = NightMetrics(syntheticNight(start: midnight, stages: []))
        #expect(empty.secondsInBed == 0 && empty.secondsAsleep == 0)
        #expect(empty.efficiency == nil && empty.deepShare == nil)
        #expect(SleepScore.score(empty, earlierBedtimes: Array(repeating: 0, count: 10)) == nil)
        // Awake only: a length but no time asleep.
        let awake = NightMetrics(syntheticNight(start: midnight, stages: [(.awake, 30)]))
        #expect(SleepScore.score(awake, earlierBedtimes: []) == nil)
    }
}

@Suite(.timeLimit(.minutes(1)))
struct PersonalRangeTests {
    @Test func percentilesOnAFixedList() {
        let values: [Double] = [7, 1, 3, 5, 9]  // sorted 1 3 5 7 9
        #expect(PersonalRange.percentile(values, 0.25) == 3)
        #expect(PersonalRange.percentile(values, 0.75) == 7)
        #expect(PersonalRange.percentile(values, 0.5) == 5)
        // Between ranks: eight values, position 0.25 × 7 = 1.75.
        let eight: [Double] = [10, 20, 30, 40, 50, 60, 70, 80]
        #expect(PersonalRange.percentile(eight, 0.25) == 27.5)
        #expect(PersonalRange.percentile(eight, 0.75) == 62.5)
        #expect(PersonalRange.percentile([], 0.5) == nil)
        #expect(PersonalRange.percentile([4], 0.75) == 4)
    }

    @Test func aRangeNeedsSevenNights() {
        let six: [Double] = [1, 2, 3, 4, 5, 6]
        #expect(PersonalRange.of(six) == .building(nights: 6))
        #expect(PersonalRange.of([]) == .building(nights: 0))
        #expect(PersonalRange.of(six + [7]) == .range(PersonalRange(lower: 2.5, upper: 5.5)))
        #expect(PersonalRange.Comparison.building(nights: 3).text == "Building your usual range (3 of 7 nights)")
        #expect(PersonalRange.of(six).compare(100) == .building(nights: 6))
    }

    @Test func tiesGiveANarrowOrEmptyBand() {
        let same = Array(repeating: 420.0, count: 9)
        #expect(PersonalRange.of(same) == .range(PersonalRange(lower: 420, upper: 420)))
        #expect(PersonalRange(lower: 420, upper: 420).tag(420) == .usual)
        #expect(PersonalRange(lower: 420, upper: 420).tag(421) == .above)
        let mostly: [Double] = [5, 5, 5, 5, 5, 5, 9, 1]
        #expect(PersonalRange.of(mostly) == .range(PersonalRange(lower: 5, upper: 5)))
    }

    @Test func tagsAtTheBandEdgesAreUsual() {
        let range = PersonalRange(lower: 360, upper: 480)
        #expect(range.tag(360) == .usual)
        #expect(range.tag(480) == .usual)
        #expect(range.tag(359.99) == .below)
        #expect(range.tag(480.01) == .above)
        #expect(PersonalRange.Comparison.usual.text == "Usual")
        #expect(PersonalRange.Comparison.above.text == "Above your usual")
        #expect(PersonalRange.Comparison.below.text == "Below your usual")
        #expect(PersonalRange.Comparison.above.isOutside && !PersonalRange.Comparison.usual.isOutside)
        #expect(!PersonalRange.Comparison.building(nights: 2).isOutside)
    }

    @Test func theNightItselfIsNeverInItsOwnRange() {
        let day = RingDay(year: 2027, month: 1, day: 15)
        func main(_ back: Int, minutes: Int, main: Bool = true) -> SleepNightDetail {
            syntheticNight(
                start: day.adding(days: -back).gmtMidnight.addingTimeInterval(-3600), stages: [(.light, minutes)],
                day: day.adding(days: -back), main: main)
        }
        let tonight = main(0, minutes: 600)
        let earlier = (1...7).map { main($0, minutes: 400) }
        let tooOld = main(31, minutes: 100)
        let nap = main(3, minutes: 30, main: false)
        let compared = SleepHistoryWindow.earlierMainSleeps(before: tonight.summary, in: [tonight, tooOld, nap] + earlier)
        #expect(compared.map(\.summary.start) == earlier.map(\.summary.start))
        // With the night left out, 600 minutes is above a range of 400s.
        #expect(SleepMetric.asleep.basis(compared).compare(600 * 60) == .above)
        // Day 30 back is still in; 31 is not.
        let edge = main(30, minutes: 400)
        #expect(SleepHistoryWindow.earlierMainSleeps(before: tonight.summary, in: [edge, tooOld]).count == 1)
    }
}

@Suite(.timeLimit(.minutes(1)))
struct NightMetricsTests {
    @Test func efficiencyTimeToDeepAndInterruptions() {
        // 23:00: 30 min light, a 30-minute gap, 60 min deep, 30 min REM. In bed 2 h 30 min.
        let night = syntheticNight(
            start: midnight.addingTimeInterval(-3600), stages: [(.light, 30), (.deep, 60), (.rem, 30)], gaps: [(30, 30)])
        let m = NightMetrics(night)
        #expect(m.secondsAsleep == 120 * 60)
        #expect(m.secondsInBed == 150 * 60)
        #expect(m.efficiency == 0.8)
        #expect(m.secondsToDeepSleep == 60 * 60)  // after the light stage and the gap
        #expect(m.interruptionCount == 1 && m.interruptionSeconds == 30 * 60)
        #expect(m.deepShare == 0.5 && m.remShare == 0.25)
        #expect(m.bedtimeSecondsOfDay == hm(23, 0))
        #expect(NightMetrics.sessionCover(night.summary) == [
            DateInterval(start: night.summary.start, duration: 1800),
            DateInterval(start: night.summary.start.addingTimeInterval(3600), end: night.summary.end),
        ])
    }

    @Test func aNightWithoutDeepHasNoTimeToDeepSleep() {
        let m = NightMetrics(syntheticNight(start: midnight, stages: [(.light, 60), (.rem, 30)]))
        #expect(m.secondsToDeepSleep == nil)
        #expect(SleepMetric.timeToDeepSleep.value(m) == nil)
        // Deep first: no time to it.
        #expect(NightMetrics(syntheticNight(start: midnight, stages: [(.deep, 60)])).secondsToDeepSleep == 0)
    }

    @Test func bedtimeIsOnTheNightsOwnClock() {
        // 23:00 UTC is 08:00 at +9 h.
        let m = NightMetrics(syntheticNight(start: midnight.addingTimeInterval(-3600), stages: [(.light, 60)], offset: 9 * 3600))
        #expect(m.bedtimeSecondsOfDay == hm(8, 0))
        let west = NightMetrics(syntheticNight(start: midnight.addingTimeInterval(3600), stages: [(.light, 60)], offset: -5 * 3600))
        #expect(west.bedtimeSecondsOfDay == hm(20, 0))
    }

    @Test func efficiencyNeverPassesOneWhenSessionsOverlap() {
        var night = syntheticNight(start: midnight, stages: [(.light, 60)])
        night.summary.secondsAsleep = 2 * 3600  // two overlapping sessions counted twice
        #expect(NightMetrics(night).efficiency == 1)
    }
}

@Suite(.timeLimit(.minutes(1)))
struct StagePaletteTests {
    @Test func everyLaneHasEnoughContrastInBothModesAndIsDistinct() {
        for lane in LaneTimeline.LaneKind.allCases {
            let pair = StagePalette.colors(for: lane)
            #expect(pair.light.contrast(with: StagePalette.lightCard) >= StagePalette.minimumContrast, "\(lane) light")
            #expect(pair.dark.contrast(with: StagePalette.darkCard) >= StagePalette.minimumContrast, "\(lane) dark")
        }
        let lights = LaneTimeline.LaneKind.allCases.map { StagePalette.colors(for: $0).light }
        #expect(Set(lights.map { "\($0.red)-\($0.green)-\($0.blue)" }).count == lights.count)
    }
}
