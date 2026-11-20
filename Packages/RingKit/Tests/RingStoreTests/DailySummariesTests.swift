import Foundation
import GRDB
import RingProtocol
import Testing
@testable import RingStore

// Daily summaries. All data is SYNTHETIC. The first test ingests bytes built from the documented
// record layouts through `Fixture` (the ring's clock set at t0 with +1 hour, as in the ingestion
// tests). The others write typed rows directly, because the offsets and time sources under test
// would need an elaborate clock-set history to come out of ingestion, and that is not what is
// under test here.

/// 2027-01-15 00:00 UTC.
private let midnight15 = Date(timeIntervalSince1970: 1_799_971_200)
private let hour = 3600.0
/// Hours after 2027-01-15 00:00 UTC.
private func utc(_ hours: Double) -> Date { midnight15.addingTimeInterval(hours * hour) }
private func day(_ n: Int) -> RingDay { RingDay(year: 2027, month: 1, day: n) }

private func insertActivity(
    _ f: Fixture, start: Date, minutes: Double = 30, steps: Int, distance: Int = 0, kcal: Int = 0, offset: Int = 0,
    source: TimeSource = .resolved, ring: UUID? = nil
) throws {
    try f.store.writer.write { db in
        try db.execute(
            sql: """
                INSERT INTO activity_interval (ring_id, start, "end", steps, distance_meters, kilocalories,
                    utc_offset_seconds, ring_local_seconds, time_source) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
                """,
            arguments: [
                (ring ?? f.ring.id).uuidString, HistoryTransfer.millisecondsSince1970(start),
                HistoryTransfer.millisecondsSince1970(start.addingTimeInterval(minutes * 60)), steps, distance, kcal,
                offset, TimeSourceCode.code(for: source),
            ])
    }
}

private func insertHeartRate(
    _ f: Fixture, at time: Date, bpm: Int, offset: Int = 0, source: TimeSource = .resolved, ring: UUID? = nil
) throws {
    try f.store.writer.write { db in
        try db.execute(
            sql: """
                INSERT INTO heart_rate_reading (ring_id, time, bpm, mode, utc_offset_seconds, ring_local_seconds,
                    time_source) VALUES (?, ?, ?, 0, ?, 0, ?)
                """,
            arguments: [
                (ring ?? f.ring.id).uuidString, HistoryTransfer.millisecondsSince1970(time), bpm, offset,
                TimeSourceCode.code(for: source),
            ])
    }
}

private func summaries(_ f: Fixture, _ first: RingDay, _ last: RingDay) async throws -> [DailySummary] {
    try await f.store.dailySummaries(ringID: f.ring.id, from: first, through: last)
}

private func all(_ f: Fixture) async throws -> [DailySummary] {
    try await summaries(f, RingDay(year: 2000, month: 1, day: 1), RingDay(year: 2100, month: 1, day: 1))
}

@Suite(.timeLimit(.minutes(1))) struct DailySummariesTests {
    @Test func ingestedStepsAndHeartRateIgnoreTheCombinedRecord() async throws {
        try await onEachStoreAsync { store in
            let f = try clockedFixture(store, kinds: [.activity, .heartRate, .combined])
            func ring(_ seconds: Double) -> UInt32 { wall(t0.addingTimeInterval(seconds), offset: plus1) }
            // t0 is 2027-01-15 08:00 UTC, 09:00 on the ring's clock.
            try f.fetch(
                .activity,
                activityBytes(start: ring(600), end: ring(2400), steps: 120, distance: 90, kcal: 6)
                    + activityBytes(start: ring(2400), end: ring(4200), steps: 80, distance: 60, kcal: 4),
                at: t0.addingTimeInterval(7200))
            try f.fetch(
                .heartRate, hrBytes(ring(600), bpm: 58) + hrBytes(ring(1200), bpm: 64) + hrBytes(ring(1800), bpm: 71),
                at: t0.addingTimeInterval(7200))
            // A combined record carries its own heart rate and steps; neither may be counted.
            try f.fetch(
                .combined, combinedBytes(ring(900), steps: 999, hr: 150, spo2: 97),
                at: t0.addingTimeInterval(7200))
            for kind in [HistoryKind.activity, .heartRate, .combined] {
                _ = try await store.ingest(ringID: f.ring.id, kind: kind)
            }
            #expect(try await store.vitalsReadings(ringID: f.ring.id).count == 1)

            #expect(try await all(f) == [
                DailySummary(
                    day: day(15), steps: 200, distanceMeters: 150, kilocalories: 10, activityIntervalCount: 2,
                    heartRate: HeartRateDaySummary(readingCount: 3, minimumBPM: 58, maximumBPM: 71, meanBPM: 64),
                    timeSource: .resolved)
            ])
        }
    }

    @Test func aBucketCrossingMidnightCountsOnItsStartDay() async throws {
        let f = try Fixture(RingStore())
        try insertActivity(f, start: utc(23.75), steps: 200)  // 23:45 to 00:15
        try insertActivity(f, start: utc(24.25), steps: 50)  // 00:15 to 00:45
        let days = try await all(f)
        #expect(days.map(\.day) == [day(15), day(16)])
        #expect(days.map(\.steps) == [200, 50])
        #expect(days.map(\.activityIntervalCount) == [1, 1])
    }

    @Test func eachRowIsDatedByItsOwnOffset() async throws {
        let f = try Fixture(RingStore())
        // 03:00 UTC on the 16th at -5 h is 22:00 on the 15th.
        try insertActivity(f, start: utc(27), steps: 10, offset: -5 * 3600)
        try insertHeartRate(f, at: utc(27), bpm: 60, offset: -5 * 3600)
        // 12:00 UTC on the 16th at +0 is the 16th.
        try insertActivity(f, start: utc(36), steps: 20, offset: 0)
        // 22:00 UTC on the 16th at +5 h is 03:00 on the 17th.
        try insertActivity(f, start: utc(46), steps: 30, offset: 5 * 3600)
        try insertHeartRate(f, at: utc(46), bpm: 70, offset: 5 * 3600)
        let days = try await all(f)
        #expect(days.map(\.day) == [day(15), day(16), day(17)])
        #expect(days.map(\.steps) == [10, 20, 30])
        #expect(days.map(\.heartRate?.meanBPM) == [60, nil, 70])
    }

    @Test func rowsJustOutsideTheInstantRangeAreFoundByTheirRingLocalDay() async throws {
        let f = try Fixture(RingStore())
        // 11:00 UTC on the 14th at +14 h is 01:00 on the 15th: inside 15...15.
        try insertHeartRate(f, at: utc(-13), bpm: 55, offset: 14 * 3600)
        // 12:00 UTC on the 15th at -14 h is 22:00 on the 14th: outside.
        try insertHeartRate(f, at: utc(12), bpm: 99, offset: -14 * 3600)
        // 10:00 UTC on the 16th at -12 h is 22:00 on the 15th: inside.
        try insertHeartRate(f, at: utc(34), bpm: 65, offset: -12 * 3600)
        let days = try await summaries(f, day(15), day(15))
        #expect(days.count == 1)
        #expect(days.first?.heartRate == HeartRateDaySummary(readingCount: 2, minimumBPM: 55, maximumBPM: 65, meanBPM: 60))
    }

    @Test func heartRateCountMinimumMaximumAndRoundedMean() async throws {
        let f = try Fixture(RingStore())
        // 15th: 60 and 61, mean 60.5, rounds to 61. 16th: 60, 60, 61, mean 60.33, rounds to 60.
        for (h, bpm) in [(1.0, 60), (2, 61), (25, 60), (26, 60), (27, 61)] {
            try insertHeartRate(f, at: utc(h), bpm: bpm)
        }
        let days = try await all(f)
        #expect(days.map(\.heartRate) == [
            HeartRateDaySummary(readingCount: 2, minimumBPM: 60, maximumBPM: 61, meanBPM: 61),
            HeartRateDaySummary(readingCount: 3, minimumBPM: 60, maximumBPM: 61, meanBPM: 60),
        ])
        // A day with heart rate only has no steps.
        #expect(days.map(\.steps) == [0, 0])
        #expect(days.map(\.activityIntervalCount) == [0, 0])
    }

    @Test func aDayCarriesItsLeastCertainTimeSource() async throws {
        let f = try Fixture(RingStore())
        try insertActivity(f, start: utc(1), steps: 1, source: .resolved)
        try insertHeartRate(f, at: utc(2), bpm: 60, source: .resolved)

        try insertActivity(f, start: utc(25), steps: 1, source: .resolved)
        try insertHeartRate(f, at: utc(26), bpm: 60, source: .ambiguous)

        try insertActivity(f, start: utc(49), steps: 1, source: .assumed)
        try insertHeartRate(f, at: utc(50), bpm: 60, source: .ambiguous)
        try insertHeartRate(f, at: utc(51), bpm: 60, source: .resolved)

        try insertHeartRate(f, at: utc(73), bpm: 60, source: .ambiguous)
        try insertActivity(f, start: utc(74), steps: 1, source: .ambiguous)

        let days = try await all(f)
        #expect(days.map(\.timeSource) == [.resolved, .ambiguous, .assumed, .ambiguous])
        // Rows of every source are counted.
        #expect(days[2].heartRate?.readingCount == 2)
    }

    @Test func theRangeIsInclusiveOldestFirstAndDaysWithoutRowsAreOmitted() async throws {
        let f = try Fixture(RingStore())
        try insertActivity(f, start: utc(10), steps: 1)  // 15th
        try insertHeartRate(f, at: utc(58), bpm: 60)  // 17th
        try insertActivity(f, start: utc(106), steps: 3)  // 19th

        #expect(try await summaries(f, day(15), day(17)).map(\.day) == [day(15), day(17)])
        #expect(try await summaries(f, day(17), day(19)).map(\.day) == [day(17), day(19)])
        #expect(try await summaries(f, day(16), day(16)).isEmpty)
        #expect(try await summaries(f, day(19), day(15)).isEmpty)
        #expect(try await summaries(f, day(14), day(30)).map(\.day) == [day(15), day(17), day(19)])
    }

    @Test func anotherRingsRowsAreLeftOut() async throws {
        let f = try Fixture(RingStore())
        let other = try await f.store.registerRing(protocolFamily: "ycbt").id
        try insertActivity(f, start: utc(10), steps: 500, ring: other)
        try insertHeartRate(f, at: utc(10), bpm: 90, ring: other)
        try insertActivity(f, start: utc(11), steps: 5)
        let days = try await all(f)
        #expect(days.map(\.steps) == [5])
        #expect(days.first?.heartRate == nil)
    }
}
