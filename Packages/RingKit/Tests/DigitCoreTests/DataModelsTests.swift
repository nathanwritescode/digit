import DigitCore
import Foundation
import RingProtocol
import RingStore
import RingSync
import Testing

// The sleep list, night detail and Days models against a fake history reader, and their reloads
// through `DigitModel`. Every night, stage and day here is SYNTHETIC, made up in code.

private let gb = HealthFormatter(locale: Locale(identifier: "en_GB"))
/// t0 is 2027-01-15 08:00 UTC; London is on UTC+0 in January.
private let day15 = RingDay(year: 2027, month: 1, day: 15)
private func day(_ n: Int) -> RingDay { RingDay(year: 2027, month: 1, day: n) }
private func at(hours: Double) -> Date { t0.addingTimeInterval(hours * 3600) }

/// How en_GB writes the time of day `h:m`, built independently of `HealthFormatter` (a GMT date
/// read in GMT), so the tests do not depend on this ICU version's choice between "8:00" and "08:00".
private func clock(_ h: Int, _ m: Int) -> String {
    Date(timeIntervalSince1970: TimeInterval(h * 3600 + m * 60)).formatted(
        Date.FormatStyle(date: .omitted, time: .shortened, locale: Locale(identifier: "en_GB"), timeZone: .gmt))
}

/// `minutes` as en_GB writes a duration of hours and minutes, built from Foundation's style
/// directly rather than through `HealthFormatter`, so the tests follow this system's wording.
private func dur(_ minutes: Int, wide: Bool = false) -> String {
    Duration.seconds(minutes * 60).formatted(
        Duration.UnitsFormatStyle(allowedUnits: [.hours, .minutes], width: wide ? .wide : .abbreviated)
            .locale(Locale(identifier: "en_GB")))
}

/// `meters` as `locale` writes a road distance, built from Foundation's style directly.
private func road(_ meters: Int, wide: Bool = false, locale: String = "en_GB") -> String {
    Measurement(value: Double(meters), unit: UnitLength.meters).formatted(
        .measurement(width: wide ? .wide : .abbreviated, usage: .road, numberFormatStyle: .number.precision(.fractionLength(0...1)))
            .locale(Locale(identifier: locale)))
}

private func night(
    day: RingDay, start: Date, hours: Double, main: Bool = true, source: TimeSource = .resolved, offset: Int = 0
) -> SleepNightSummary {
    let seconds = Int(hours * 3600)
    var byKind = Dictionary(uniqueKeysWithValues: SleepStageKind.allCases.map { ($0, 0) })
    byKind[.light] = seconds / 2
    byKind[.deep] = seconds / 4
    byKind[.rem] = seconds - seconds / 2 - seconds / 4
    return SleepNightSummary(
        day: day, utcOffsetSeconds: offset, start: start, end: start.addingTimeInterval(TimeInterval(seconds)),
        secondsByKind: byKind, secondsAsleep: seconds, interruptions: [], timeSource: source, isMainSleep: main)
}

private func summary(_ day: RingDay, steps: Int, source: TimeSource = .resolved) -> DailySummary {
    DailySummary(
        day: day, steps: steps, distanceMeters: steps * 3 / 4, kilocalories: steps / 20, activityIntervalCount: 4,
        heartRate: HeartRateDaySummary(readingCount: 10, minimumBPM: 52, maximumBPM: 118, meanBPM: 64),
        timeSource: source)
}

/// An error whose own text carries a made-up name, identifier and value, so tests can check none
/// of it reaches the screen. Not private: a private type's reflected name holds an address, whose
/// digits could match the value by chance.
struct LeakyError: Error, CustomStringConvertible {
    var description: String { "\(syntheticName) \(syntheticIdentifier) 61 bpm" }
}

/// History reads, scripted. `@unchecked Sendable`: every field is guarded by `lock`.
final class FakeHistory: RingHistoryReader, @unchecked Sendable {
    private let lock = NSLock()
    private var _nights: [SleepNightSummary] = []
    private var _details: [Date: SleepNightDetail] = [:]
    private var _days: [DailySummary] = []
    private var _failure: (any Error)?
    private var _calls: [String] = []
    private var _ranges: [String] = []

    var nights: [SleepNightSummary] { get { lock.withLock { _nights } } set { lock.withLock { _nights = newValue } } }
    var details: [Date: SleepNightDetail] { get { lock.withLock { _details } } set { lock.withLock { _details = newValue } } }
    var days: [DailySummary] { get { lock.withLock { _days } } set { lock.withLock { _days = newValue } } }
    /// Thrown by every call while set.
    var failure: (any Error)? { get { lock.withLock { _failure } } set { lock.withLock { _failure = newValue } } }
    var calls: [String] { lock.withLock { _calls } }
    func count(_ call: String) -> Int { calls.filter { $0 == call }.count }
    /// "first...last" for each range asked.
    var ranges: [String] { lock.withLock { _ranges } }

    private func record(_ call: String, _ first: RingDay? = nil, _ last: RingDay? = nil) throws {
        try lock.withLock {
            _calls.append(call)
            if let first, let last { _ranges.append("\(first.year)-\(first.month)-\(first.day)...\(last.year)-\(last.month)-\(last.day)") }
            if let _failure { throw _failure }
        }
    }

    func sleepNights(ringID: UUID, from first: RingDay, through last: RingDay) async throws -> [SleepNightSummary] {
        try record("sleepNights", first, last)
        return nights.filter { first <= $0.day && $0.day <= last }.sorted { $0.start < $1.start }
    }

    func sleepNightDetail(ringID: UUID, startingAt start: Date) async throws -> SleepNightDetail? {
        try record("sleepNightDetail")
        return details[start]
    }

    func dailySummaries(ringID: UUID, from first: RingDay, through last: RingDay) async throws -> [DailySummary] {
        try record("dailySummaries", first, last)
        return days.filter { first <= $0.day && $0.day <= last }.sorted { $0.day < $1.day }
    }
}

/// A paired ring read from an in-memory store (one ring), or from an empty one.
@MainActor
private func loadedRing(paired: Bool = true) async throws -> PairedRing {
    let store = try RingStore()
    if paired { _ = try await store.registerRing(protocolFamily: "ycbt", model: "R99", firmware: "2.32") }
    let ring = PairedRing()
    try await ring.load(from: store)
    return ring
}

@MainActor
private func sleepModel(_ ring: PairedRing, _ history: FakeHistory) -> SleepListModel {
    SleepListModel(pairedRing: ring, history: history, now: { t0 }, timeZone: { london }, formatter: gb)
}

@MainActor
private func dailyModel(_ ring: PairedRing, _ history: FakeHistory) -> DailyModel {
    DailyModel(pairedRing: ring, history: history, now: { t0 }, timeZone: { london }, formatter: gb)
}

@MainActor
@Suite(.timeLimit(.minutes(1)))
struct SleepListModelTests {
    @Test func loadsTheLastThirtyDaysNewestDayFirstWithMainSleepBeforeNaps() async throws {
        let history = FakeHistory()
        let nap = night(day: day15, start: at(hours: 6), hours: 0.5, main: false)
        let lateNap = night(day: day15, start: at(hours: 8), hours: 0.25, main: false)
        let main15 = night(day: day15, start: at(hours: -9), hours: 7.5)
        let main14 = night(day: day(14), start: at(hours: -33), hours: 6)
        let old = night(day: RingDay(year: 2026, month: 12, day: 1), start: at(hours: -1100), hours: 7)
        history.nights = [nap, main14, lateNap, main15, old]
        let model = sleepModel(try await loadedRing(), history)
        #expect(model.state == .loading)

        await model.reload()
        #expect(history.ranges == ["2026-12-17...2027-1-16"])
        let groups = try #require(model.state.value)
        #expect(groups.map(\.day) == [day15, day(14)])
        #expect(groups[0].nights.map(\.summary) == [main15, lateNap, nap])
        #expect(groups[0].title == "Friday 15 January")
        let row = groups[0].nights[0]
        #expect(row.kindText == "Main sleep")
        #expect(row.asleepText == dur(450))
        #expect(row.bedtimeText == clock(23, 0))
        #expect(row.wakeText == clock(6, 30))
        #expect(row.uncertainty == nil)
        #expect(row.accessibilityLabel == "Friday 15 January, main sleep, \(dur(450, wide: true)) asleep, bedtime \(clock(23, 0)), woke \(clock(6, 30))")
        #expect(groups[0].nights[1].kindText == "Nap")
    }

    @Test func anUncertainNightIsMarked() async throws {
        let history = FakeHistory()
        history.nights = [night(day: day15, start: at(hours: -9), hours: 7, source: .ambiguous)]
        let model = sleepModel(try await loadedRing(), history)
        await model.reload()
        let row = try #require(model.state.value?.first?.nights.first)
        #expect(row.uncertainty == "Times uncertain")
        #expect(row.uncertaintyExplanation != nil)
        #expect(row.accessibilityLabel.hasSuffix(", times uncertain"))
    }

    @Test func notPairedAndNoData() async throws {
        let history = FakeHistory()
        let unpaired = sleepModel(try await loadedRing(paired: false), history)
        await unpaired.reload()
        #expect(unpaired.state == .notPaired)
        #expect(history.calls.isEmpty)

        let empty = sleepModel(try await loadedRing(), history)
        await empty.reload()
        #expect(empty.state == .noData)
    }

    @Test func nothingIsReadBeforeThePairedRingIsLoaded() async throws {
        let history = FakeHistory()
        let model = sleepModel(PairedRing(), history)
        await model.reload()
        #expect(model.state == .loading)
        #expect(history.calls.isEmpty)
    }

    @Test func aSuspendedLoadKeepsTheLastDataAndOtherFailuresSayWhatFailedWithoutDetails() async throws {
        let history = FakeHistory()
        history.nights = [night(day: day15, start: at(hours: -9), hours: 7)]
        let model = sleepModel(try await loadedRing(), history)
        await model.reload()
        let shown = model.state
        #expect(shown.value != nil)

        history.failure = RingStoreError.suspended
        await model.reload()
        #expect(model.state == shown)

        history.failure = LeakyError()
        await model.reload()
        guard case .failed(let message) = model.state else {
            Issue.record("expected a failure, got \(model.state)")
            return
        }
        #expect(!message.contains(syntheticName) && !message.contains(syntheticIdentifier.uuidString) && !message.contains("61"))
        #expect(message.hasPrefix("Digit couldn't read your data"))
    }

    @Test func aSuspendedFirstLoadSaysItWasInterrupted() async throws {
        let history = FakeHistory()
        history.failure = RingStoreError.suspended
        let model = sleepModel(try await loadedRing(), history)
        await model.reload()
        #expect(model.state == .failed("Digit was interrupted while reading your data. It tries again when you return to the app."))
    }
}

@MainActor
@Suite(.timeLimit(.minutes(1)))
struct NightDetailModelTests {
    private func detail(_ summary: SleepNightSummary) -> SleepNightDetail {
        SleepNightDetail(summary: summary, stages: [
            SleepStageEntry(kind: .light, start: summary.start, durationSeconds: 1800),
            SleepStageEntry(kind: .deep, start: summary.start.addingTimeInterval(1801), durationSeconds: 2700),
            SleepStageEntry(kind: .rem, start: summary.start.addingTimeInterval(4502), durationSeconds: 600),
        ])
    }

    @Test func loadsTheNightWithItsStagesAndTotalsOnItsOwnClock() async throws {
        let history = FakeHistory()
        // +9 h: 23:00 UTC is 08:00 on the night's clock, whatever the device's zone.
        var summary = night(day: day15, start: at(hours: -9), hours: 1.5, offset: 9 * 3600)
        summary.interruptions = [SleepInterruption(start: at(hours: -8), end: at(hours: -8).addingTimeInterval(300))]
        history.details[summary.start] = detail(summary)
        var moved = 0
        let model = NightDetailModel(
            ringID: UUID(), start: summary.start, history: history, formatter: gb, onMoved: { moved += 1 })
        await model.reload()
        guard case .loaded(let page) = model.state else {
            Issue.record("expected a night, got \(model.state)")
            return
        }
        #expect(page.bedtimeText == clock(8, 0))
        #expect(page.wakeText == clock(9, 30))
        #expect(page.stages.map(\.kind) == [.light, .deep, .rem])
        #expect(page.stages.map(\.name) == ["Light", "Deep", "REM"])
        #expect(page.stages[1].accessibilityLabel == "Deep, \(clock(8, 30)) to \(clock(9, 15)), \(dur(45, wide: true))")
        #expect(page.totals.map(\.title) == ["Asleep", "Deep", "Light", "REM", "Nap", "Interruptions"])
        #expect(page.totals[0].value == dur(90))
        #expect(page.totals[0].accessibilityLabel == "\(dur(90, wide: true)) asleep")
        #expect(page.totals[5].value == dur(5))
        #expect(moved == 0)
    }

    @Test func aNightThatMovedSaysSoAndAsksForTheListToReload() async throws {
        let history = FakeHistory()
        var moved = 0
        let model = NightDetailModel(
            ringID: UUID(), start: at(hours: -9), history: history, formatter: gb, onMoved: { moved += 1 })
        await model.reload()
        #expect(model.state == .moved)
        #expect(moved == 1)
        // Stays moved: no second read, no second request.
        await model.reload()
        #expect(model.state == .moved)
        #expect(moved == 1)
        #expect(history.count("sleepNightDetail") == 1)
    }

    @Test func aSuspendedReloadKeepsTheNightShown() async throws {
        let history = FakeHistory()
        let summary = night(day: day15, start: at(hours: -9), hours: 1.5)
        history.details[summary.start] = detail(summary)
        let model = NightDetailModel(ringID: UUID(), start: summary.start, history: history, formatter: gb, onMoved: {})
        await model.reload()
        let shown = model.state
        history.failure = RingStoreError.suspended
        await model.reload()
        #expect(model.state == shown)
        history.failure = LeakyError()
        await model.reload()
        guard case .failed(let message) = model.state else {
            Issue.record("expected a failure, got \(model.state)")
            return
        }
        #expect(!message.contains(syntheticName) && !message.contains(syntheticIdentifier.uuidString))
    }
}

@MainActor
@Suite(.timeLimit(.minutes(1)))
struct DailyModelTests {
    @Test func loadsDaysNewestFirst() async throws {
        let history = FakeHistory()
        history.days = [summary(day(13), steps: 1000), summary(day15, steps: 8412, source: .assumed), summary(day(14), steps: 0)]
        let model = dailyModel(try await loadedRing(), history)
        await model.reload()
        #expect(history.ranges == ["2026-12-17...2027-1-16"])
        let rows = try #require(model.state.value)
        #expect(rows.map(\.summary.day) == [day15, day(14), day(13)])
        #expect(rows[0].stepsText == "8,412 steps")
        #expect(rows[0].distanceText == road(6309))
        #expect(rows[0].heartRateText == "52–118 bpm, mean 64")
        #expect(rows[0].uncertainty == "Times uncertain")
        #expect(rows[0].accessibilityLabel == "Friday 15 January, 8,412 steps, \(road(6309, wide: true)), heart rate 52 to 118 beats per minute, average 64, times uncertain")
        #expect(rows[1].uncertainty == nil)
    }

    @Test func notPairedNoDataSuspendedAndFailed() async throws {
        let history = FakeHistory()
        let unpaired = dailyModel(try await loadedRing(paired: false), history)
        await unpaired.reload()
        #expect(unpaired.state == .notPaired)

        let model = dailyModel(try await loadedRing(), history)
        await model.reload()
        #expect(model.state == .noData)
        history.failure = RingStoreError.suspended
        await model.reload()
        #expect(model.state == .noData)

        history.failure = nil
        history.days = [summary(day15, steps: 10)]
        await model.reload()
        let shown = model.state
        history.failure = RingStoreError.suspended
        await model.reload()
        #expect(model.state == shown)

        history.failure = LeakyError()
        await model.reload()
        guard case .failed(let message) = model.state else {
            Issue.record("expected a failure, got \(model.state)")
            return
        }
        #expect(!message.contains(syntheticName) && !message.contains(syntheticIdentifier.uuidString) && !message.contains("61"))
    }
}

/// The reloads `DigitModel` drives: after the paired ring is read, after a sync ends (done or
/// failed), and when the scene becomes active.
@MainActor
@Suite(.timeLimit(.minutes(1)))
struct DataReloadTests {
    private func model(_ f: Fixture, _ history: FakeHistory) -> DigitModel {
        let store = f.store
        return DigitModel(
            services: DigitServices(
                ringAccess: f.access, registry: f.registry, history: history,
                makeSync: { ring, ringID in
                    RingSyncer(
                        ring: ring, store: store, ringID: ringID, monitoringInterval: SyncSettings.monitoringInterval,
                        budget: .seconds(60), clock: { t0.addingTimeInterval(60) }, timeZone: { london })
                },
                now: { t0 }, timeZone: { london }),
            backgroundTasks: FakeBackgroundTasks(), announce: {}, suspend: {}, resume: {})
    }

    @Test func loadingThePairedRingLoadsTheDataScreens() async throws {
        let f = try Fixture()
        try await f.storePairedRing()
        let history = FakeHistory()
        history.days = [summary(day15, steps: 10)]
        let model = model(f, history)
        #expect(model.sleep.state == .loading)
        await model.load()
        // Loading reads only the ring; the first activation reads the data, once.
        #expect(history.calls.isEmpty)
        model.sceneDidBecomeActive()
        #expect(await eventually { model.sleep.state == .noData && model.days.state.value?.count == 1 })
        #expect(history.count("sleepNights") == 1 && history.count("dailySummaries") == 1)
    }

    @Test func aLaunchThatStaysInTheBackgroundLoadsTheDataOnTheFirstActivation() async throws {
        let f = try Fixture()
        try await f.storePairedRing()
        let history = FakeHistory()
        history.days = [summary(day15, steps: 10)]
        let model = model(f, history)
        await model.load()
        model.sceneDidEnterBackground()
        try? await Task.sleep(for: .milliseconds(20))
        #expect(history.calls.isEmpty)
        #expect(model.days.state == .loading)
        model.sceneDidBecomeActive()
        #expect(await eventually { model.days.state.value?.count == 1 && model.sleep.state == .noData })
    }

    @Test func tryAgainAfterAFailedLoadAlsoLoadsTheData() async throws {
        let f = try Fixture()
        try await f.storePairedRing()
        f.registry.suspendedCalls = ["rings"]
        let history = FakeHistory()
        let model = model(f, history)
        await model.load()
        #expect(model.loadFailure != nil)
        f.registry.suspendedCalls = []
        await model.retryLoad()
        #expect(model.loadFailure == nil)
        #expect(model.sleep.state == .noData && model.days.state == .noData)
    }

    @Test func notPairedIsShownOnceTheStoreHasBeenRead() async throws {
        let f = try Fixture()
        let model = model(f, FakeHistory())
        await model.load()
        await model.reloadData()
        #expect(model.sleep.state == .notPaired)
        #expect(model.days.state == .notPaired)
    }

    @Test func aSyncThatEndsReloadsTheDataScreensWhetherItCompletedOrFailed() async throws {
        let f = try Fixture()
        try await f.storePairedRing()
        let history = FakeHistory()
        let model = model(f, history)
        await model.load()
        await model.reloadData()  // what the first activation does
        #expect(history.count("sleepNights") == 1 && history.count("dailySummaries") == 1)

        history.nights = [night(day: day15, start: at(hours: -9), hours: 7)]
        await model.sync.sync()
        #expect(await eventually { history.count("sleepNights") == 2 && history.count("dailySummaries") == 2 })
        #expect(await eventually { model.sleep.state.value?.count == 1 })

        f.access.connectProblem = .timedOut
        await model.sync.sync()
        guard case .failed = model.sync.state else {
            Issue.record("expected the sync to fail, got \(model.sync.state)")
            return
        }
        #expect(await eventually { history.count("sleepNights") == 3 && history.count("dailySummaries") == 3 })
    }

    @Test func becomingActiveReloadsAndRetriesASuspendedLoad() async throws {
        let f = try Fixture()
        try await f.storePairedRing()
        let history = FakeHistory()
        history.failure = RingStoreError.suspended
        let model = model(f, history)
        await model.load()
        await model.reloadData()  // what the first activation does
        #expect(model.sleep.state == .failed("Digit was interrupted while reading your data. It tries again when you return to the app."))

        history.failure = nil
        history.nights = [night(day: day15, start: at(hours: -9), hours: 7)]
        model.sceneDidBecomeActive()
        #expect(await eventually { model.sleep.state.value?.count == 1 && model.days.state == .noData })
    }

    @Test func theOpenNightReloadsWithTheListsAndAMovedNightReloadsTheList() async throws {
        let f = try Fixture()
        try await f.storePairedRing()
        let history = FakeHistory()
        let summary = night(day: day15, start: at(hours: -9), hours: 7)
        history.nights = [summary]
        history.details[summary.start] = SleepNightDetail(summary: summary, stages: [])
        let model = model(f, history)
        await model.load()
        let detail = try #require(model.nightDetail(startingAt: summary.start))
        await detail.reload()
        #expect(history.count("sleepNightDetail") == 1)

        // Ingestion moved the night: the open screen learns it on the next reload, and the list reloads.
        history.details = [:]
        let listLoads = history.count("sleepNights")
        model.sceneDidBecomeActive()
        #expect(await eventually { detail.state == .moved })
        #expect(await eventually { history.count("sleepNights") >= listLoads + 2 })
    }
}

@Suite(.timeLimit(.minutes(1)))
struct HealthFormatterTests {
    @Test func durationsInHoursAndMinutesRoundedToTheMinute() {
        #expect(gb.duration(seconds: 7 * 3600 + 12 * 60) == dur(432))
        #expect(gb.duration(seconds: 45 * 60) == dur(45))
        #expect(gb.duration(seconds: 0) == dur(0))
        // Rounded to the nearest minute before formatting, so no seconds are ever shown.
        #expect(gb.duration(seconds: 89) == dur(1))
        #expect(gb.duration(seconds: 90) == dur(2))
        #expect(gb.duration(seconds: 3600 - 20) == dur(60))
        #expect(gb.spokenDuration(seconds: 7 * 3600 + 12 * 60) == dur(432, wide: true))
        #expect(gb.spokenDuration(seconds: 3600 + 60) == dur(61, wide: true))
        // Plurals come from the locale, not from hand-written English.
        #expect(gb.spokenDuration(seconds: 3600).contains("hour") && !gb.spokenDuration(seconds: 3600).contains("hours"))
        #expect(gb.spokenDuration(seconds: 2 * 3600).contains("hours"))
        // Another locale's own words.
        #expect(HealthFormatter(locale: Locale(identifier: "de_DE")).spokenDuration(seconds: 2 * 3600).contains("Stunden"))
    }

    @Test func timesAreOnTheRowsOwnOffsetNotTheDevicesZone() {
        let instant = Date(timeIntervalSince1970: 1_800_000_000)  // 08:00 UTC
        #expect(gb.time(instant, utcOffsetSeconds: 0) == clock(8, 0))
        #expect(gb.time(instant, utcOffsetSeconds: 9 * 3600) == clock(17, 0))
        #expect(gb.time(instant, utcOffsetSeconds: -(4 * 3600 + 30 * 60)) == clock(3, 30))
        // Whatever this machine's zone is, an offset three hours from it gives a different time.
        let device = TimeZone.current.secondsFromGMT(for: instant)
        #expect(gb.time(instant, utcOffsetSeconds: device + 3 * 3600) != gb.time(instant, utcOffsetSeconds: device))
        #expect(gb.time(instant, utcOffsetSeconds: device + 3 * 3600)
            == instant.addingTimeInterval(TimeInterval(device + 3 * 3600)).formatted(
                Date.FormatStyle(date: .omitted, time: .shortened, locale: Locale(identifier: "en_GB"), timeZone: .gmt)))
    }

    /// The night's own whole hours, whatever the device zone: no input here is the device's zone,
    /// so a +05:30 night viewed on a +00:00 device and the reverse are these two cases.
    @Test func hourTicksAreWholeHoursOfTheNightsOwnClock() {
        // 17:00 UTC to 01:00 UTC: 22:30 to 06:30 at +05:30, eight hours, so every two hours.
        let start = Date(timeIntervalSince1970: 1_799_971_200 - 7 * 3600)
        let end = start.addingTimeInterval(8 * 3600)
        let plus530 = 5 * 3600 + 30 * 60
        let ticks = SleepNightPage.hourTicks(from: start, to: end, utcOffsetSeconds: plus530)
        #expect(ticks.map { gb.time($0, utcOffsetSeconds: plus530) } == [clock(0, 0), clock(2, 0), clock(4, 0), clock(6, 0)])
        // On UTC those are half past: never the device zone's hours.
        #expect(ticks.allSatisfy { Int($0.timeIntervalSince1970) % 3600 == 1800 })

        // The reverse: a +00:00 night, ticks on whole UTC hours.
        let utcTicks = SleepNightPage.hourTicks(from: start, to: end, utcOffsetSeconds: 0)
        #expect(utcTicks.map { gb.time($0, utcOffsetSeconds: 0) } == [clock(18, 0), clock(20, 0), clock(22, 0), clock(0, 0)])
        #expect(utcTicks.allSatisfy { Int($0.timeIntervalSince1970) % 3600 == 0 })

        // Short nights tick every hour, long ones every three; a start on the hour is included.
        let short = SleepNightPage.hourTicks(from: start, to: start.addingTimeInterval(2 * 3600), utcOffsetSeconds: 0)
        #expect(short.map { Int($0.timeIntervalSince(start)) } == [0, 3600, 7200])
        let long = SleepNightPage.hourTicks(from: start, to: start.addingTimeInterval(12 * 3600), utcOffsetSeconds: 0)
        #expect(long.count == 4)
        #expect(SleepNightPage.hourTicks(from: end, to: start, utcOffsetSeconds: 0).isEmpty)
    }

    @MainActor @Test func aNightPageCarriesItsOwnZoneAndTicks() async throws {
        let history = FakeHistory()
        let summary = night(day: day15, start: at(hours: -9), hours: 7, offset: 5 * 3600 + 30 * 60)
        history.details[summary.start] = SleepNightDetail(summary: summary, stages: [])
        let model = NightDetailModel(ringID: UUID(), start: summary.start, history: history, formatter: gb, onMoved: {})
        await model.reload()
        guard case .loaded(let page) = model.state else {
            Issue.record("expected a night, got \(model.state)")
            return
        }
        #expect(page.timeZone.secondsFromGMT() == 5 * 3600 + 30 * 60)
        #expect(page.hourTicks == SleepNightPage.hourTicks(from: summary.start, to: summary.end, utcOffsetSeconds: 5 * 3600 + 30 * 60))
        #expect(!page.hourTicks.isEmpty)
    }

    @Test func daysAreReadWithoutAZone() {
        #expect(gb.day(RingDay(year: 2027, month: 1, day: 16)) == "Saturday 16 January")
    }

    @Test func valuesFollowTheLocale() {
        #expect(gb.steps(8412) == "8,412 steps")
        #expect(gb.steps(1) == "1 step")
        // Road units: miles in the UK and the US, kilometres in Germany.
        #expect(gb.distance(meters: 6100) == road(6100))
        #expect(gb.distance(meters: 6100).hasSuffix("mi"))
        #expect(gb.spokenDistance(meters: 6100) == road(6100, wide: true))
        #expect(HealthFormatter(locale: Locale(identifier: "en_US")).distance(meters: 6100) == road(6100, locale: "en_US"))
        let de = HealthFormatter(locale: Locale(identifier: "de_DE"))
        #expect(de.distance(meters: 6100) == road(6100, locale: "de_DE"))
        #expect(de.distance(meters: 6100).hasSuffix("km"))
        // A whole mile is "1 mile", not "1.0 miles".
        #expect(gb.spokenDistance(meters: 1609) == road(1609, wide: true))
        #expect(!gb.spokenDistance(meters: 1609).contains(".0"))
        let hr = HeartRateDaySummary(readingCount: 3, minimumBPM: 52, maximumBPM: 118, meanBPM: 64)
        #expect(gb.heartRate(hr) == "52–118 bpm, mean 64")
        #expect(gb.spokenHeartRate(hr) == "heart rate 52 to 118 beats per minute, average 64")
    }

    @Test func theUncertaintyMarker() {
        #expect(HealthFormatter.uncertaintyMarker(for: .resolved) == nil)
        #expect(HealthFormatter.uncertaintyExplanation(for: .resolved) == nil)
        for source in [TimeSource.ambiguous, .assumed] {
            #expect(HealthFormatter.uncertaintyMarker(for: source) == "Times uncertain")
            #expect(HealthFormatter.uncertaintyExplanation(for: source)?.isEmpty == false)
        }
    }

    @Test func stageNamesAreTextNotOnlyColour() {
        #expect(SleepStageKind.allCases.map(HealthFormatter.name(of:)) == ["Deep", "Light", "REM", "Awake", "Nap"])
    }
}
