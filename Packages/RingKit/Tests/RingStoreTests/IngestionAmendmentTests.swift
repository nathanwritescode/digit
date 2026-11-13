import Foundation
import Testing
import RingProtocol
@testable import RingStore

// Resolving each record once, counted collisions, the 2020 cutoff, sleep sanity, ring-local time.
// All bytes are synthetic.

private func at(_ seconds: Double) -> Date { t0.addingTimeInterval(seconds) }

struct IngestionAmendmentTests {
    /// r2 is in two stored transfers that neither extends the other. Resolved separately it would
    /// get two instants (an ambiguous later sighting says 9000, the first says 5400) and two rows.
    /// It must be resolved once, from the earliest sighting.
    @Test func aRecordInTwoNonPrefixTransfersIsResolvedOnceFromTheEarliestSighting() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            try f.fetch(.heartRate, [], at: at(-10))
            try f.setClock(at: at(0), offset: 3600)
            let r1 = hrBytes(wall(at(1800), offset: 3600), bpm: 61)
            let r2 = hrBytes(wall(at(5400), offset: 3600), bpm: 62)
            try f.fetch(.heartRate, r1 + r2, at: at(7200))
            try f.setClock(at: at(7210), offset: 0)
            let r3 = hrBytes(wall(at(10000), offset: 0), bpm: 63)
            try f.fetch(.heartRate, r2 + r3, at: at(14400))
            #expect(try store.transfers(ringID: f.ring.id, kind: .heartRate).transfers.count == 2)

            let result = try store.ingest(ringID: f.ring.id, kind: .heartRate)
            let rows = try store.heartRateReadings(ringID: f.ring.id).map(\.value)
            #expect(rows.count == 3 && result.inserted == 3 && result.collisions == 0)
            #expect(rows.map(\.bpm) == [61, 62, 63])
            #expect(rows.map(\.time) == [at(1800), at(5400), at(10000)])
            #expect(rows[1].utcOffsetSeconds == 3600 && rows[1].timeSource == .resolved)
            // Repeating changes nothing.
            #expect(try store.ingest(ringID: f.ring.id, kind: .heartRate).unchanged == 3)
        }
    }

    @Test func anActivityRecordInTwoNonPrefixTransfersIsResolvedOnceFromTheEarliestSighting() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            try f.fetch(.activity, [], at: at(-10))
            try f.setClock(at: at(0), offset: 3600)
            func bucket(_ start: Double, _ offset: Int, steps: Int) -> [UInt8] {
                activityBytes(start: wall(at(start), offset: offset), end: wall(at(start + 799), offset: offset), steps: steps)
            }
            let a1 = bucket(1000, 3600, steps: 1), a2 = bucket(4000, 3600, steps: 2)
            try f.fetch(.activity, a1 + a2, at: at(7200))
            try f.setClock(at: at(7210), offset: 0)
            let a3 = bucket(9000, 0, steps: 3)
            try f.fetch(.activity, a2 + a3, at: at(14400))

            let result = try store.ingest(ringID: f.ring.id, kind: .activity)
            let rows = try store.activityIntervals(ringID: f.ring.id).map(\.value)
            #expect(rows.count == 3 && result.collisions == 0)
            #expect(rows.map(\.steps) == [1, 2, 3])
            #expect(rows.map(\.start) == [at(1000), at(4000), at(9000)])
            #expect(rows[1].utcOffsetSeconds == 3600 && rows[1].timeSource == .resolved)
        }
    }

    @Test func twoReadingsInTheSameRingSecondAreACountedCollision() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            try f.fetch(.heartRate, [], at: at(-10))
            try f.setClock(at: at(0), offset: 3600)
            let t = wall(at(1800), offset: 3600)
            try f.fetch(.heartRate, hrBytes(t, bpm: 60) + hrBytes(t, bpm: 61), at: at(3600))
            let result = try store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(result.inserted == 1 && result.collisions == 1)
            // The one later in the buffer is kept.
            #expect(try store.heartRateReadings(ringID: f.ring.id).map(\.value.bpm) == [61])
        }
    }

    /// The clock goes back an hour and the sync between failed to fetch, so two buckets with the same
    /// wall-clock start both come out ambiguous at one instant. One row, one counted collision.
    @Test func twoActivityBucketsWithTheSameWallClockStartEitherSideOfAMissedFetchCollide() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            try f.fetch(.activity, [], at: at(-10))
            try f.setClock(at: at(0), offset: 3600)
            try f.setClock(at: at(7200), offset: 0)  // clock back; that sync's fetch was missed
            // Both have the wall-clock start of real instant 9000 under offset 0: the first was
            // written under +3600 (real 5400), the second under 0.
            let base = wall(at(9000), offset: 0)
            let first = activityBytes(start: base, end: base + 600, steps: 10)
            let second = activityBytes(start: base, end: base + 600, steps: 20)
            try f.fetch(.activity, first + second, at: at(14400))
            let result = try store.ingest(ringID: f.ring.id, kind: .activity)
            #expect(result.inserted == 1 && result.collisions == 1)
            #expect(result.rowsByTimeSource == [.ambiguous: 1])
            let row = try #require(try store.activityIntervals(ringID: f.ring.id).first).value
            #expect(row.steps == 20 && row.timeSource == .ambiguous && row.start == at(9000))
        }
    }

    /// 2019-12-31T23:59:59 is rejected and 2020-01-01T00:00:00 is not.
    @Test func theCutoffIsExactlyTheStartOf2020() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            func ring(_ text: String) -> UInt32 {
                UInt32(ISO8601DateFormatter().date(from: text)!.timeIntervalSince1970 - 946_684_800)
            }
            let data = hrBytes(ring("2019-12-31T23:59:59Z"), bpm: 50) + hrBytes(ring("2020-01-01T00:00:00Z"), bpm: 51)
            try f.fetch(.heartRate, data, at: t0, zone: "UTC")
            let result = try store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(result.rejectedBefore2020 == 1 && result.inserted == 1)
            let row = try #require(try store.heartRateReadings(ringID: f.ring.id).first).value
            #expect(row.bpm == 51 && row.time == ISO8601DateFormatter().date(from: "2020-01-01T00:00:00Z"))
        }
    }

    @Test func implausibleSleepSessionsAreRejectedAndCounted() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            try f.fetch(.sleep, [], at: at(-10))
            try f.setClock(at: at(0), offset: 3600)
            let s = wall(at(1000), offset: 3600)
            let good = sleepBytes(start: s, end: s + 600, stages: [(242, s, 600)])
            // Starts in 2000 but ends in 2027.
            let longAgo = sleepBytes(start: 1000, end: s + 600, stages: [(242, s, 600)])
            // Starts after it ends.
            let backwards = sleepBytes(start: s + 5000, end: s + 4000, stages: [(242, s + 5000, 100)])
            // A stage starting in 2000.
            let badStage = sleepBytes(start: s + 9000, end: s + 9600, stages: [(242, s + 9000, 300), (241, 2000, 300)])
            try f.fetch(.sleep, good + longAgo + backwards + badStage, at: at(3600))
            let result = try store.ingest(ringID: f.ring.id, kind: .sleep)
            #expect(result.rejectedBefore2020 == 3 && result.inserted == 1)
            #expect(try store.sleepSessions(ringID: f.ring.id).map(\.value.start) == [at(1000)])
        }
    }

    @Test func theRingLocalTimeIsStoredForEveryTable() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            for kind in [HistoryKind.heartRate, .activity, .sleep] { try f.fetch(kind, [], at: at(-10)) }
            try f.setClock(at: at(0), offset: 3600)
            let w = wall(at(600), offset: 3600)
            try f.fetch(.heartRate, hrBytes(w, bpm: 60), at: at(3600))
            try f.fetch(.activity, activityBytes(start: w, end: w + 1799, steps: 1), at: at(3600))
            try f.fetch(.sleep, sleepBytes(start: w, end: w + 600, stages: [(242, w, 600)]), at: at(3600))
            for kind in [HistoryKind.heartRate, .activity, .sleep] { try store.ingest(ringID: f.ring.id, kind: kind) }
            #expect(try store.heartRateReadings(ringID: f.ring.id).first?.value.ringLocalSeconds == Int(w))
            #expect(try store.activityIntervals(ringID: f.ring.id).first?.value.ringLocalSeconds == Int(w))
            #expect(try store.sleepSessions(ringID: f.ring.id).first?.value.ringLocalSeconds == Int(w))
        }
    }

    @Test func everyStageKindRoundTripsThroughItsCode() throws {
        for kind in SleepStageKind.allCases {
            #expect(StageCode.kind(forCode: StageCode.code(for: kind)) == kind)
        }
        #expect(Set(SleepStageKind.allCases.map(StageCode.code(for:))).count == SleepStageKind.allCases.count)
        try onEachStore { store in
            let f = try Fixture(store)
            try f.fetch(.sleep, [], at: at(-10))
            try f.setClock(at: at(0), offset: 3600)
            let s = wall(at(1000), offset: 3600)
            let stages = SleepStageKind.allCases.enumerated().map { (i, kind) in
                (kind: kind.rawValue, start: s + UInt32(i * 100), seconds: 99)
            }
            try f.fetch(.sleep, sleepBytes(start: s, end: s + 600, stages: stages), at: at(3600))
            try store.ingest(ringID: f.ring.id, kind: .sleep)
            let row = try #require(try store.sleepSessions(ringID: f.ring.id).first).value
            #expect(row.stages.map(\.kind) == SleepStageKind.allCases)
        }
    }
}
