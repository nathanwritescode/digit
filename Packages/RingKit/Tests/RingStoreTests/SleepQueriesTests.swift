import Foundation
import GRDB
import RingProtocol
import Testing
@testable import RingStore

// Sleep queries. All rows are synthetic: bytes built from the documented sleep layout and
// ingested through `Fixture` and `ingest` into in-memory databases. Real instants are seconds
// after `base`, 2027-01-15 00:00 UTC; the ring's clock is at UTC (offset 0). The tests that need
// another offset or time source change those columns on the ingested rows, because ingestion
// would need an elaborate clock-set history to produce them and that is not what is under test.

private let base = Date(timeIntervalSince1970: 1_799_971_200)
private func at(_ seconds: Double) -> Date { base.addingTimeInterval(seconds) }
private let hour = 3600.0
private let minute = 60.0
private let day0 = SleepDay(year: 2027, month: 1, day: 15)
private func day(_ n: Int) -> SleepDay { SleepDay(year: 2027, month: 1, day: 15 + n) }

/// A session in seconds after `base`; by default one stage of `kind` covering it.
private struct Session {
    var start: Double
    var end: Double
    var stages: [(kind: UInt8, start: Double, seconds: Int)]

    init(_ start: Double, _ end: Double, kind: UInt8 = 242) {
        self.start = start
        self.end = end
        stages = [(kind, start, Int(end - start))]
    }

    init(_ start: Double, _ end: Double, stages: [(kind: UInt8, start: Double, seconds: Int)]) {
        self.start = start
        self.end = end
        self.stages = stages
    }
}

private func fixture(_ store: RingStore) throws -> Fixture {
    let f = try Fixture(store)
    try f.fetch(.sleep, [], at: at(-900_000))
    try f.setClock(at: at(-1_000_000), offset: 0)
    try f.setClock(at: at(-800_000), offset: 0)
    return f
}

@discardableResult
private func ingest(_ f: Fixture, _ sessions: [Session]) throws -> IngestionResult {
    var bytes: [UInt8] = []
    for s in sessions {
        bytes += sleepBytes(
            start: wall(at(s.start), offset: 0), end: wall(at(s.end), offset: 0),
            stages: s.stages.map { (kind: $0.kind, start: wall(at($0.start), offset: 0), seconds: $0.seconds) })
    }
    // Fetched after every session, so none is dated in the future.
    try f.fetch(.sleep, bytes, at: at(20 * 86400))
    return try f.store.ingest(ringID: f.ring.id, kind: .sleep)
}

private func setColumns(_ f: Fixture, startingAt start: Double, offset: Int? = nil, source: String? = nil) throws {
    try f.store.writer.write { db in
        let ms = Int64(at(start).timeIntervalSince1970 * 1000)
        if let offset {
            try db.execute(
                sql: "UPDATE sleep_session SET utc_offset_seconds = ? WHERE ring_id = ? AND start = ?",
                arguments: [offset, f.ring.id.uuidString, ms])
        }
        if let source {
            try db.execute(
                sql: "UPDATE sleep_session SET time_source = ? WHERE ring_id = ? AND start = ?",
                arguments: [source, f.ring.id.uuidString, ms])
        }
    }
}

private func allNights(_ f: Fixture) async throws -> [SleepNightSummary] {
    try await f.store.sleepNights(ringID: f.ring.id, from: SleepDay(year: 2000, month: 1, day: 1), through: SleepDay(year: 2100, month: 1, day: 1))
}

struct SleepQueriesTests {
    @Test func nightCrossingMidnightBelongsToTheDayItEnds() async throws {
        let f = try fixture(RingStore())
        try ingest(f, [Session(22 * hour, 30 * hour)])  // 22:00 day 0 to 06:00 day 1
        let nights = try await allNights(f)
        #expect(nights.count == 1)
        #expect(nights[0].day == day(1))
        #expect(nights[0].start == at(22 * hour) && nights[0].end == at(30 * hour))
    }

    @Test func gapsAtTheNightBoundary() async throws {
        let f = try fixture(RingStore())
        // 10 minutes apart: one night, one interruption.
        try ingest(f, [Session(0, hour), Session(hour + 10 * minute, 2 * hour)])
        var nights = try await allNights(f)
        #expect(nights.count == 1)
        #expect(nights[0].interruptions == [SleepInterruption(start: at(hour), end: at(hour + 10 * minute))])
        #expect(nights[0].interruptionSeconds == 600)

        // Exactly 60 minutes joins; 60 minutes and one second does not.
        let g = try fixture(RingStore())
        try ingest(g, [Session(0, hour), Session(2 * hour, 3 * hour), Session(3 * hour + 3601, 4 * hour + 3600)])
        nights = try await allNights(g)
        #expect(nights.count == 2)
        #expect(nights[0].end == at(3 * hour) && nights[0].interruptionSeconds == 3600)
        #expect(nights[1].start == at(3 * hour + 3601) && nights[1].interruptions.isEmpty)
    }

    @Test func overlappingAndTouchingSessionsJoinAndAreNotInterruptions() async throws {
        let f = try fixture(RingStore())
        try ingest(f, [Session(0, 2 * hour), Session(hour, 3 * hour), Session(3 * hour, 4 * hour)])
        let nights = try await allNights(f)
        #expect(nights.count == 1)
        #expect(nights[0].interruptions.isEmpty && nights[0].interruptionSeconds == 0)
        // Sums are as stored, not clipped: the overlapping hour counts twice.
        #expect(nights[0].secondsAsleep == 5 * 3600)
    }

    @Test func totalsPerKindAndTimeAsleep() async throws {
        let f = try fixture(RingStore())
        try ingest(f, [
            Session(0, 1000, stages: [(241, 0, 100), (242, 101, 200), (243, 302, 300), (244, 603, 40), (245, 644, 356)]),
            Session(2000, 2500, kind: 241),
        ])
        let night = try #require(try await allNights(f).first)
        #expect(night.secondsByKind == [.deep: 600, .light: 200, .rem: 300, .awake: 40, .nap: 356])
        #expect(night.secondsAsleep == 600 + 200 + 300 + 356)  // not awake
        #expect(night.interruptionSeconds == 1000)
    }

    @Test func mainSleepAndAfternoonNapOnTheSameDay() async throws {
        let f = try fixture(RingStore())
        // Night ending 06:00 on day 1, then a nap 14:00 to 15:00 on day 1; a nap-kind stage in
        // the longer night must not make it a nap.
        try ingest(f, [Session(22 * hour, 30 * hour, kind: 245), Session(38 * hour, 39 * hour)])
        let nights = try await allNights(f)
        #expect(nights.map(\.day) == [day(1), day(1)])
        #expect(nights.map(\.isMainSleep) == [true, false])
        #expect(nights[0].secondsByKind[.nap] == 8 * 3600)
    }

    @Test func tieGoesToTheNightThatEndsLater() async throws {
        let f = try fixture(RingStore())
        try ingest(f, [Session(hour, 3 * hour), Session(14 * hour, 16 * hour)])  // both two hours, day 0
        let nights = try await allNights(f)
        #expect(nights.map(\.isMainSleep) == [false, true])
        #expect(try await f.store.latestSleepNight(ringID: f.ring.id)?.summary == nights[1])
    }

    @Test func eachNightsDayComesFromItsOwnLastSessionsOffset() async throws {
        let f = try fixture(RingStore())
        // Night A ends 23:30 UTC on day 0, night B 23:30 UTC on day 2; a third night C has two
        // sessions whose last decides.
        try ingest(f, [
            Session(21 * hour, 23.5 * hour),
            Session(2 * 86400 + 21 * hour, 2 * 86400 + 23.5 * hour),
            Session(4 * 86400 + 20 * hour, 4 * 86400 + 21 * hour),
            Session(4 * 86400 + 21.5 * hour, 4 * 86400 + 23.5 * hour),
        ])
        try setColumns(f, startingAt: 21 * hour, offset: 3600)  // clock an hour ahead: 00:30 on day 1
        try setColumns(f, startingAt: 4 * 86400 + 20 * hour, offset: 3600)  // first session only
        let nights = try await allNights(f)
        #expect(nights.map(\.day) == [day(1), day(2), day(4)])  // C: its last session's offset 0
        try setColumns(f, startingAt: 4 * 86400 + 21.5 * hour, offset: 3600)
        #expect(try await allNights(f).map(\.day) == [day(1), day(2), day(5)])
    }

    @Test func nightReportsTheLeastCertainSourceAndKeepsEverySession() async throws {
        let f = try fixture(RingStore())
        try ingest(f, [Session(0, hour), Session(hour + 600, 2 * hour), Session(2 * hour + 600, 3 * hour)])
        #expect(try await allNights(f)[0].timeSource == .resolved)
        try setColumns(f, startingAt: hour + 600, source: "ambiguous")
        #expect(try await allNights(f)[0].timeSource == .ambiguous)
        try setColumns(f, startingAt: 2 * hour + 600, source: "assumed")
        try setColumns(f, startingAt: 0, source: "resolved")
        let nights = try await allNights(f)
        #expect(nights.count == 1 && nights[0].timeSource == .assumed)
        #expect(nights[0].secondsAsleep == 3600 + 3000 + 3000)
        #expect(nights[0].interruptions.count == 2)
        #expect(try await f.store.latestSleepNight(ringID: f.ring.id)?.stages.count == 3)
    }

    @Test func sameNightFromNarrowWideAndLatestQueries() async throws {
        let f = try fixture(RingStore())
        // Day 1 ends 06:00 (night of two sessions), day 2 and day 3 have one night each.
        try ingest(f, [
            Session(22 * hour, 26 * hour), Session(26 * hour + 15 * minute, 30 * hour),
            Session(86400 + 22 * hour, 86400 + 30 * hour),
            Session(2 * 86400 + 22 * hour, 2 * 86400 + 30 * hour),
        ])
        let wide = try await allNights(f)
        #expect(wide.map(\.day) == [day(1), day(2), day(3)])

        let exact = try await f.store.sleepNights(ringID: f.ring.id, from: day(1), through: day(1))
        #expect(exact == [wide[0]])
        let around = try await f.store.sleepNights(ringID: f.ring.id, from: day(0), through: day(2))
        #expect(around == [wide[0], wide[1]])
        // The night that began on day 0 is absent from a range starting on day 2 (it ends on day 1)
        // and the range cuts through it without splitting it.
        let late = try await f.store.sleepNights(ringID: f.ring.id, from: day(2), through: day(3))
        #expect(late == [wide[1], wide[2]])
        #expect(try await f.store.sleepNights(ringID: f.ring.id, from: day(4), through: day(9)).isEmpty)
        #expect(try await f.store.sleepNights(ringID: f.ring.id, from: day(0), through: day(0)).isEmpty)

        let latest = try #require(try await f.store.latestSleepNight(ringID: f.ring.id))
        #expect(latest.summary == wide[2])
        let detail = try #require(try await f.store.sleepNightDetail(ringID: f.ring.id, for: wide[0]))
        #expect(detail.summary == wide[0])
        #expect(detail.stages.count == 2)
        #expect(detail.stages.map(\.start) == detail.stages.map(\.start).sorted())
    }

    @Test func detailHasStagesInTimeOrderAndNilWhenTheNightIsGone() async throws {
        let f = try fixture(RingStore())
        try ingest(f, [
            Session(0, 1000, stages: [(241, 0, 400), (243, 401, 599)]),
            Session(1600, 2000, stages: [(242, 1600, 400)]),
        ])
        let summary = try #require(try await allNights(f).first)
        let detail = try #require(try await f.store.sleepNightDetail(ringID: f.ring.id, for: summary))
        #expect(detail.stages.map(\.kind) == [.deep, .rem, .light])
        #expect(detail.summary.interruptions == [SleepInterruption(start: at(1000), end: at(1600))])

        var stale = summary
        stale.start = at(-5)
        #expect(try await f.store.sleepNightDetail(ringID: f.ring.id, for: stale) == nil)
    }

    @Test func emptyRingAndRingsAreIndependent() async throws {
        let store = try RingStore()
        let f = try fixture(store)
        let g = try fixture(store)
        #expect(try await store.latestSleepNight(ringID: f.ring.id) == nil)
        #expect(try await allNights(f).isEmpty)

        try ingest(f, [Session(0, hour)])
        #expect(try await allNights(f).count == 1)
        #expect(try await allNights(g).isEmpty)
        #expect(try await store.latestSleepNight(ringID: g.ring.id) == nil)

        try ingest(g, [Session(5 * hour, 6 * hour), Session(30 * hour, 31 * hour)])
        #expect(try await allNights(f).count == 1)
        #expect(try await allNights(g).count == 2)
        #expect(try await store.latestSleepNight(ringID: f.ring.id)?.summary.start == at(0))
    }

    @Test func observationYieldsNowAndAfterAChangeAndEndsOnCancel() async throws {
        let f = try fixture(RingStore())
        let store = f.store
        let ringID = f.ring.id
        let seen = AsyncStream<SleepNightDetail?>.makeStream()
        let task = Task {
            for try await value in store.observeLatestSleepNight(ringID: ringID) { seen.continuation.yield(value) }
            seen.continuation.finish()
        }
        var values = seen.stream.makeAsyncIterator()

        let initial = await values.next()
        #expect(initial != nil && initial! == nil)  // a first value with no night

        try ingest(f, [Session(0, hour)])
        var latest: SleepNightDetail?
        // Further values may repeat the same state; read until the new night shows.
        while latest == nil {
            guard let next = await values.next() else { break }
            latest = next
        }
        #expect(latest?.summary.start == at(0))

        task.cancel()
        try await task.value  // returns only if the stream ended
    }
}
