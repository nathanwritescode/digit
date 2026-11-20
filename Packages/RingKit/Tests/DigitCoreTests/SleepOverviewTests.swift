import DigitCore
import Foundation
import RingProtocol
import RingStore
import RingSync
import Testing

// The Sleep tab's cards, the night screen's score, quality bars and navigation, and the lane
// timeline, against a fake history reader. Every night, stage and reading here is SYNTHETIC.

private let gb = HealthFormatter(locale: Locale(identifier: "en_GB"))
private func jan(_ n: Int) -> RingDay { RingDay(year: 2027, month: 1, day: n) }

private func clock(_ h: Int, _ m: Int) -> String {
    Date(timeIntervalSince1970: TimeInterval(h * 3600 + m * 60)).formatted(
        Date.FormatStyle(date: .omitted, time: .shortened, locale: Locale(identifier: "en_GB"), timeZone: .gmt))
}

private func compact(_ minutes: Int) -> String {
    Duration.seconds(minutes * 60).formatted(
        Duration.UnitsFormatStyle(allowedUnits: [.hours, .minutes], width: .narrow).locale(Locale(identifier: "en_GB")))
}

/// A main sleep ending on `day`, starting at 23:00 the evening before (UTC), of `light` minutes of
/// light sleep then `deep` of deep and `rem` of REM.
private func mainSleep(_ day: RingDay, light: Int = 300, deep: Int = 60, rem: Int = 60, lateBy: Int = 0) -> SleepNightDetail {
    syntheticNight(
        start: day.gmtMidnight.addingTimeInterval(TimeInterval(-3600 + lateBy * 60)),
        stages: [(.light, light), (.deep, deep), (.rem, rem)], day: day)
}

@MainActor
private func loadedRing(paired: Bool = true) async throws -> PairedRing {
    let store = try RingStore()
    if paired { _ = try await store.registerRing(protocolFamily: "ycbt", model: "R99", firmware: "2.32") }
    let ring = PairedRing()
    try await ring.load(from: store)
    return ring
}

@MainActor
private func overviewModel(_ ring: PairedRing, _ history: FakeHistory) -> SleepOverviewModel {
    SleepOverviewModel(pairedRing: ring, history: history, now: { t0 }, timeZone: { london }, formatter: gb)
}

private func put(_ details: [SleepNightDetail], in history: FakeHistory) {
    for detail in details { history.details[detail.summary.start] = detail }
    history.nights = details.map(\.summary)
}

@MainActor
@Suite(.timeLimit(.minutes(1)))
struct SleepOverviewModelTests {
    @Test func oneReadCoversTheWindowAndEachChartedNightsPrevious30Days() async throws {
        let history = FakeHistory()
        put([mainSleep(jan(15))], in: history)
        let model = overviewModel(try await loadedRing(), history)
        await model.reload()
        // t0 is 2027-01-15 08:00 UTC: the window is 17 Dec to 16 Jan, read from 37 days before it.
        #expect(history.ranges == ["2026-11-10...2027-1-16"])
        #expect(history.calls == ["sleepNightDetails"])
        #expect(model.state.value?.night.day == jan(15))
    }

    @Test func theCardsDescribeTheLatestMainSleepAgainstSevenNightsAndTheUsualRange() async throws {
        let history = FakeHistory()
        // Main sleeps on 1 to 14 January (7 h asleep), none on the 12th, and a long one on the 15th;
        // a nap on the 15th, which is not the main sleep.
        var nights = (1...14).filter { $0 != 12 }.map { mainSleep(jan($0)) }
        let latest = mainSleep(jan(15), light: 420)
        nights.append(latest)
        nights.append(
            syntheticNight(start: jan(15).gmtMidnight.addingTimeInterval(14 * 3600), stages: [(.light, 20)], day: jan(15), main: false))
        put(nights, in: history)
        let model = overviewModel(try await loadedRing(), history)
        await model.reload()
        let overview = try #require(model.state.value)
        #expect(overview.night == latest.summary)
        #expect(overview.dayTitle == "Friday 15 January")

        // Seven days ending on the 15th, the 12th a gap (not zero) that splits the line.
        let points = overview.duration.points
        #expect(points.map(\.day) == (9...15).map(jan))
        #expect(points.map(\.letter) == ["S", "S", "M", "T", "W", "T", "F"])
        #expect(points.map { $0.value == nil } == [false, false, false, true, false, false, false])
        #expect(points.map(\.segment) == [0, 0, 0, 0, 1, 1, 1])
        #expect(points.map(\.isHighlighted) == [false, false, false, false, false, false, true])
        #expect(points.last?.value == 9)  // hours asleep

        // 13 earlier nights all of 7 h: the band is 7 to 7 h and 9 h is above it.
        #expect(overview.duration.band == 7...7)
        #expect(overview.duration.comparison == .above)
        #expect(overview.duration.valueText == gb.duration(seconds: 9 * 3600))
        #expect(overview.asleep.comparison == .above)
        #expect(overview.rem.comparison == .usual)
        #expect(overview.rem.detailText == "\(gb.percent(60.0 / 540)) of time asleep")
        #expect(overview.efficiency.valueText == gb.percent(1))
        #expect(overview.efficiency.yDomain == 0...100)

        // The score: 50 duration, 20 consistency (same bedtime), 20 continuity, stages
        // 5 × (60/540)/0.15 + 5 × (60/540)/0.2.
        let stages = 5 * (60.0 / 540) / 0.15 + 5 * (60.0 / 540) / 0.2
        #expect(overview.scoreValueText == "\(Int((90 + stages).rounded()))")
        #expect(overview.scoreLabelText == "Good")
        #expect(overview.scorePoints.map { $0.value == nil } == [false, false, false, true, false, false, false])

        // The window: 23:00 to 08:00, bars from 11 h after noon.
        #expect(overview.window.valueText == "\(clock(23, 0)) – \(clock(8, 0))")
        #expect(overview.window.bars.last?.from == 11)
        #expect(overview.window.bars.last?.to == 20)
        #expect(overview.window.bars[3].from == nil)
        #expect(overview.window.comparison == .above)
        // 23:00 to 08:00 on a 9-hour axis: ticks every 4 hours, on the night's clock.
        #expect(overview.window.yDomain == 11...20)
        #expect(overview.window.ticks.map(\.label) == [clock(0, 0), clock(4, 0), clock(8, 0)])

        // The timeline of the latest night, on its own clock.
        #expect(overview.timeline.bedtimeText == clock(23, 0))
        #expect(overview.timeline.wakeText == clock(8, 0))
        #expect(overview.uncertainty == nil)
    }

    @Test func withFewerThanSevenEarlierNightsTheRangeIsBuilding() async throws {
        let history = FakeHistory()
        put((10...15).map { mainSleep(jan($0)) }, in: history)
        let model = overviewModel(try await loadedRing(), history)
        await model.reload()
        let overview = try #require(model.state.value)
        #expect(overview.duration.comparison == .building(nights: 5))
        #expect(overview.duration.band == nil)
        #expect(overview.duration.comparison?.text == "Building your usual range (5 of 7 nights)")
    }

    @Test func nightsOnlyBeforeTheWindowAreNoData() async throws {
        let history = FakeHistory()
        put([mainSleep(RingDay(year: 2026, month: 12, day: 1))], in: history)
        let model = overviewModel(try await loadedRing(), history)
        await model.reload()
        #expect(model.state == .noData)

        let unpaired = overviewModel(try await loadedRing(paired: false), FakeHistory())
        await unpaired.reload()
        #expect(unpaired.state == .notPaired)
    }

    @Test func aSuspendedReloadKeepsTheCardsAndFailuresHoldNoDetails() async throws {
        let history = FakeHistory()
        put([mainSleep(jan(15))], in: history)
        let model = overviewModel(try await loadedRing(), history)
        await model.reload()
        let shown = model.state
        history.failure = RingStoreError.suspended
        await model.reload()
        #expect(model.state == shown)
        history.failure = LeakyError()
        await model.reload()
        guard case .failed(let message) = model.state else {
            Issue.record("expected a failure")
            return
        }
        #expect(!message.contains(syntheticName) && !message.contains(syntheticIdentifier.uuidString) && !message.contains("61"))
    }
}

@MainActor
@Suite(.timeLimit(.minutes(1)))
struct NightScreenTests {
    @Test func aMainSleepHasItsScoreQualityBarsAndSleepingHeartRate() async throws {
        let history = FakeHistory()
        let earlier = (1...14).map { mainSleep(jan($0)) }
        // Later to bed by an hour, with a 10-minute interruption after the first hour.
        let tonight = syntheticNight(
            start: jan(15).gmtMidnight, stages: [(.light, 60), (.deep, 60), (.rem, 60), (.light, 240)],
            gaps: [(60, 10)], day: jan(15))
        put(earlier + [tonight], in: history)
        history.heartRate = HeartRateSummary(readingCount: 40, minimumBPM: 48, maximumBPM: 70, meanBPM: 56)
        let model = NightDetailModel(ringID: UUID(), start: tonight.summary.start, history: history, formatter: gb, onMoved: {})
        await model.reload()
        guard case .loaded(let page) = model.state else {
            Issue.record("expected a night, got \(model.state)")
            return
        }
        // The previous 30 days, the night's own day included (it is left out of its own range).
        #expect(history.ranges == ["2026-12-16...2027-1-15"])
        #expect(history.calls == ["sleepNightDetail", "sleepNightDetails", "heartRateSummary"])
        // Heart rate inside the sessions only: the interruption is left out.
        #expect(history.heartRateIntervals == [NightMetrics.sessionCover(tonight.summary)])
        #expect(history.heartRateIntervals.first?.count == 2)
        #expect(page.sleepingHeartRateText == "56 bpm")

        let score = try #require(page.score)
        #expect(score.chips.map(\.title) == ["Duration", "Consistency", "Continuity", "Stages"])
        #expect(score.chips[0].pointsText == "50 of 50")
        // An hour from the typical 23:00: 20 × (120 − 60) / 90.
        #expect(abs(score.result.consistency - 40.0 / 3) < 1e-9)
        #expect(score.chips[1].pointsText == "13 of 20")
        #expect(score.chips[2].pointsText == "20 of 20")
        #expect(score.chips[3].pointsText.hasSuffix(" of 10"))
        #expect(score.consistencyNote == nil)
        #expect(score.disclaimer == "Digit's sleep score: an estimate, not a medical measure")
        #expect(!score.explanation.isEmpty)
        #expect(page.noScoreText == nil)

        #expect(page.quality.map(\.title) == ["Time to deep sleep", "Deep", "REM", "Efficiency", "Interruptions"])
        let toDeep = page.quality[0]
        // Light 60, gap 10: deep starts 70 minutes in; earlier nights took 300 minutes.
        #expect(toDeep.valueText == gb.duration(seconds: 70 * 60))
        #expect(toDeep.comparison == .below)
        let interruptions = page.quality[4]
        #expect(interruptions.comparison == .above)
        for bar in page.quality {
            if let marker = bar.marker { #expect((0...1).contains(marker)) }
            if let band = bar.band { #expect(band.lowerBound >= 0 && band.upperBound <= 1) }
        }
        #expect(page.qualityLegend == "Shaded: your usual range over the last 30 nights.")
    }

    @Test func aNapHasNoScoreOrRangesAndSkipsTheHistoryRead() async throws {
        let history = FakeHistory()
        let nap = syntheticNight(start: jan(15).gmtMidnight.addingTimeInterval(14 * 3600), stages: [(.light, 20)], day: jan(15), main: false)
        put([nap], in: history)
        let model = NightDetailModel(ringID: UUID(), start: nap.summary.start, history: history, formatter: gb, onMoved: {})
        await model.reload()
        guard case .loaded(let page) = model.state else {
            Issue.record("expected a night")
            return
        }
        #expect(page.score == nil && page.quality.isEmpty)
        #expect(page.noScoreText != nil)
        #expect(page.sleepingHeartRateText == "No heart-rate readings")
        #expect(history.count("sleepNightDetails") == 0)
    }

    @Test func aNightWithoutDeepSaysSoAndHasNoMarker() async throws {
        let history = FakeHistory()
        let night = syntheticNight(start: jan(15).gmtMidnight, stages: [(.light, 300)], day: jan(15))
        put([night], in: history)
        let model = NightDetailModel(ringID: UUID(), start: night.summary.start, history: history, formatter: gb, onMoved: {})
        await model.reload()
        guard case .loaded(let page) = model.state else {
            Issue.record("expected a night")
            return
        }
        #expect(page.quality[0].valueText == "No deep sleep recorded")
        #expect(page.quality[0].marker == nil && page.quality[0].comparison == nil)
        #expect(page.score?.consistencyNote == "Consistency counts once there are 7 nights.")
    }

    @Test func previousAndNextMoveBetweenMainSleepsAndStopAtTheEnds() async throws {
        let history = FakeHistory()
        let nights = (13...15).map { mainSleep(jan($0)) }
        put(nights, in: history)
        let starts = nights.map(\.summary.start)
        let model = NightDetailModel(
            ringID: UUID(), start: starts[1], history: history, formatter: gb, mainSleepStarts: starts.reversed(),
            onMoved: {})
        await model.reload()
        #expect(model.previousStart == starts[0] && model.nextStart == starts[2])
        await model.showNext()
        #expect(model.start == starts[2])
        #expect(model.nextStart == nil)
        guard case .loaded(let page) = model.state else {
            Issue.record("expected a night")
            return
        }
        #expect(page.summary.start == starts[2])
        await model.showNext()  // at the end: nothing happens
        #expect(model.start == starts[2])
        await model.showPrevious()
        await model.showPrevious()
        #expect(model.start == starts[0])
        #expect(model.previousStart == nil)
        await model.showPrevious()
        #expect(model.start == starts[0])
    }

    @Test func navigationSkipsNapsAndAMovedNightStillReturnsToTheList() async throws {
        let f = try Fixture()
        try await f.storePairedRing()
        let history = FakeHistory()
        let first = mainSleep(jan(14))
        let nap = syntheticNight(start: jan(14).gmtMidnight.addingTimeInterval(14 * 3600), stages: [(.light, 20)], day: jan(14), main: false)
        let second = mainSleep(jan(15))
        put([first, nap, second], in: history)
        let model = DigitModel(
            services: DigitServices(
                ringAccess: f.access, registry: f.registry, history: history,
                makeSync: { _, _ -> any SyncPhases in fatalError("no sync here") }, now: { t0 }, timeZone: { london }),
            backgroundTasks: FakeBackgroundTasks(), announce: {}, suspend: {}, resume: {})
        await model.load()
        await model.reloadData()
        let detail = try #require(model.nightDetail(startingAt: first.summary.start))
        #expect(detail.nextStart == second.summary.start)
        // Opened on the nap, its neighbours are the main sleeps either side.
        let fromNap = try #require(model.nightDetail(startingAt: nap.summary.start))
        #expect(fromNap.previousStart == first.summary.start && fromNap.nextStart == second.summary.start)

        // The next night has moved by the time it is shown: back to the list, which reloads.
        history.details[second.summary.start] = nil
        let loads = history.count("sleepNights")
        await detail.showNext()
        #expect(detail.state == .moved)
        #expect(await eventually { history.count("sleepNights") > loads })
    }
}

@Suite(.timeLimit(.minutes(1)))
struct LaneTimelineTests {
    @Test func oneLanePerStagePresentWithTheAwakeLaneFromInterruptions() {
        let night = syntheticNight(
            start: jan(15).gmtMidnight.addingTimeInterval(-3600), stages: [(.light, 30), (.deep, 60), (.rem, 70)],
            gaps: [(30, 20)], day: jan(15))
        let timeline = LaneTimeline(night, formatter: gb)
        #expect(timeline.lanes.map(\.kind) == [.awake, .rem, .light, .deep])
        #expect(timeline.lanes.map(\.title) == [
            "Awake · \(compact(20))", "REM · \(compact(70))", "Light · \(compact(30))", "Deep · \(compact(60))",
        ])
        #expect(timeline.blocks.map(\.lane) == [.light, .awake, .deep, .rem])
        #expect(timeline.blocks.map(\.laneIndex) == [2, 0, 3, 1])
        #expect(timeline.blocks[0].startFraction == 0)
        #expect(timeline.blocks[3].endFraction == 1)
        #expect(timeline.blocks[1].headline == "Awake, \(clock(23, 30)) to \(clock(23, 50))")
        #expect(timeline.bedtimeText == clock(23, 0))
        #expect(timeline.wakeText == clock(2, 0))
        #expect(timeline.midpointText == clock(0, 30))
    }

    @Test func theScrubberFindsTheBlockUnderAPointOrTheNearestOne() {
        let night = syntheticNight(start: jan(15).gmtMidnight, stages: [(.light, 60), (.deep, 60)], day: jan(15))
        let timeline = LaneTimeline(night, formatter: gb)
        #expect(timeline.block(atFraction: 0.25)?.lane == .light)
        #expect(timeline.block(atFraction: 0.75)?.lane == .deep)
        #expect(timeline.block(atFraction: -1)?.lane == .light)
        #expect(timeline.block(atFraction: 2)?.lane == .deep)
        let first = timeline.block(after: nil, step: 1)
        #expect(first?.lane == .light)
        #expect(timeline.block(after: first, step: 1)?.lane == .deep)
        #expect(timeline.block(after: first, step: -1)?.lane == .light)
        let empty = LaneTimeline(syntheticNight(start: jan(15).gmtMidnight, stages: [], day: jan(15)), formatter: gb)
        #expect(empty.lanes.isEmpty && empty.block(atFraction: 0.5) == nil)
    }

    @Test func aNapLaneAppearsOnlyWithNapStages() {
        let night = syntheticNight(start: jan(15).gmtMidnight, stages: [(.nap, 30)], day: jan(15))
        #expect(LaneTimeline(night, formatter: gb).lanes.map(\.kind) == [.nap])
    }
}
