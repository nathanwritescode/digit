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

    // MARK: Identity (bytes plus occurrence number)

    /// After the clock goes back the ring repeats wall-clock times, and a repeated reading with the
    /// same value is byte for byte the same. Two readings with identical bytes in one buffer are two
    /// records, each resolved on its own.
    @Test func twoReadingsWithIdenticalBytesEitherSideOfAClockSetAreTwoRecords() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            try f.fetch(.heartRate, [], at: at(-10))
            try f.setClock(at: at(0), offset: 3600)
            let a = hrBytes(wall(at(5400), offset: 3600), bpm: 58)  // wall clock 9000
            try f.fetch(.heartRate, a, at: at(7200))
            try f.setClock(at: at(7210), offset: 0)
            let b = hrBytes(wall(at(9000), offset: 0), bpm: 58)  // wall clock 9000 again
            #expect(a == b)
            try f.fetch(.heartRate, a + b, at: at(14400))
            let result = try store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(result.inserted == 2 && result.collisions == 0)
            let rows = try store.heartRateReadings(ringID: f.ring.id).map(\.value)
            #expect(rows.map(\.time) == [at(5400), at(9000)])
            #expect(rows.map(\.utcOffsetSeconds) == [3600, 0])
            #expect(rows.map(\.timeSource) == [.resolved, .resolved])
            #expect(try store.ingest(ringID: f.ring.id, kind: .heartRate).unchanged == 2)
        }
    }

    @Test func twoZeroStepActivityBucketsWithIdenticalBytesAreTwoRecords() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            try f.fetch(.activity, [], at: at(-10))
            try f.setClock(at: at(0), offset: 3600)
            // The same wall-clock bucket twice: real 4800-5400 under +3600, then real 8400-9000 under 0.
            let a = activityBytes(start: wall(at(4800), offset: 3600), end: wall(at(5400), offset: 3600), steps: 0)
            try f.fetch(.activity, a, at: at(7200))
            try f.setClock(at: at(7210), offset: 0)
            let b = activityBytes(start: wall(at(8400), offset: 0), end: wall(at(9000), offset: 0), steps: 0)
            #expect(a == b)
            try f.fetch(.activity, a + b, at: at(14400))
            let result = try store.ingest(ringID: f.ring.id, kind: .activity)
            #expect(result.inserted == 2 && result.collisions == 0)
            let rows = try store.activityIntervals(ringID: f.ring.id).map(\.value)
            #expect(rows.map(\.start) == [at(4800), at(8400)])
            #expect(rows.map(\.utcOffsetSeconds) == [3600, 0])
            #expect(rows.map(\.timeSource) == [.resolved, .resolved])
        }
    }

    // MARK: Superseded sleep sessions

    /// The ring rewrote a session after the clock went back: the new copy has the same start bytes,
    /// a later end stamped by the new clock and another stage, and is not a prefix of the old one.
    @Test func aRewrittenSleepSessionKeepsOnlyTheLatestCopyEvenWhenTheCopiesResolveToDifferentInstants() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            try f.fetch(.sleep, [], at: at(-10))
            try f.setClock(at: at(0), offset: 3600)
            let start = wall(at(1000), offset: 3600)
            let v1 = sleepBytes(start: start, end: wall(at(3000), offset: 3600), stages: [(242, start, 1000)])
            try f.fetch(.sleep, v1, at: at(3500))
            try f.setClock(at: at(3510), offset: 0)
            let v2 = sleepBytes(
                start: start, end: wall(at(5000), offset: 0), stages: [(242, start, 1000), (241, start + 301, 500)])
            try f.fetch(.sleep, v2, at: at(7200))
            #expect(try store.transfers(ringID: f.ring.id, kind: .sleep).transfers.count == 2)

            let result = try store.ingest(ringID: f.ring.id, kind: .sleep)
            #expect(result.superseded == 1 && result.collisions == 0 && result.inserted == 1)
            let rows = try store.sleepSessions(ringID: f.ring.id).map(\.value)
            #expect(rows.count == 1)
            #expect(rows[0].stages.count == 2 && rows[0].ringLocalSeconds == Int(start))
            #expect(rows[0].end == at(5000))
        }
    }

    // MARK: Records from the future

    /// A record cannot be written after it was fetched. The fetch is at t0 + 3600, so a record
    /// 24 hours after it is allowed and one a second later is not; FF FF FF FF is year 2136.
    @Test func recordsDatedMoreThanADayAfterTheirFetchAreRejectedAsFuture() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            let fetched = at(3600)
            func ringSeconds(_ d: Date) -> UInt32 { UInt32(d.timeIntervalSince1970 - 946_684_800) }
            let inside = hrBytes(ringSeconds(fetched.addingTimeInterval(86_400)), bpm: 50)
            let outside = hrBytes(ringSeconds(fetched.addingTimeInterval(86_401)), bpm: 51)
            let allFF = [UInt8](repeating: 0xFF, count: 6)
            try f.fetch(.heartRate, inside + outside + allFF, at: fetched, zone: "UTC")
            let result = try store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(result.rejectedFuture == 2 && result.inserted == 1)
            #expect(try store.heartRateReadings(ringID: f.ring.id).map(\.value.bpm) == [50])
        }
    }

    /// The edge is on the resolved instant, so it needs the offset: with no clock-set the zone's offset
    /// (five hours behind in January) is applied before comparing with the fetch.
    @Test func theTwentyFourHourBoundIsMeasuredOnTheResolvedInstantInANonUTCZone() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            let fetched = at(3600)
            func ringSeconds(_ resolved: Date) -> UInt32 { UInt32(resolved.timeIntervalSince1970 - 18000 - 946_684_800) }
            let inside = hrBytes(ringSeconds(fetched.addingTimeInterval(86_400)), bpm: 50)
            let outside = hrBytes(ringSeconds(fetched.addingTimeInterval(86_401)), bpm: 51)
            try f.fetch(.heartRate, inside + outside, at: fetched, zone: "America/New_York")
            let result = try store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(result.rejectedFuture == 1 && result.inserted == 1)
            #expect(try store.heartRateReadings(ringID: f.ring.id).map(\.value.bpm) == [50])
        }
    }

    // MARK: Activity and sleep sanity

    /// A bucket's row instant and key are its start, so the start is checked as well as the end.
    @Test func implausibleActivityBucketStartsAreRejected() throws {
        try onEachStore { store in
            let f = try clockedFixture(store, kinds: [.activity])
            let good = wall(at(1000), offset: 3600)
            let validEnd = good + 1800
            try f.fetch(.activity, [
                activityBytes(start: good, end: validEnd, steps: 1),
                activityBytes(start: 0xFFFF_FFFF, end: validEnd, steps: 2),  // start in 2136
                activityBytes(start: 1000, end: validEnd, steps: 3),  // start in 2000, valid end
                activityBytes(start: validEnd + 10, end: validEnd, steps: 4),  // starts after it ends
                activityBytes(start: good + 5000, end: 0xFFFF_FFFF, steps: 5),  // end in 2136
            ].flatMap { $0 }, at: at(3600))
            let result = try store.ingest(ringID: f.ring.id, kind: .activity)
            #expect(result.inserted == 1 && result.rejectedBefore2020 == 3 && result.rejectedFuture == 1)
            #expect(try store.activityIntervals(ringID: f.ring.id).map(\.value.steps) == [1])
        }
    }

    @Test func sleepStagesOutsideTheSessionAreRejected() throws {
        try onEachStore { store in
            let f = try clockedFixture(store, kinds: [.sleep])
            let s = wall(at(1000), offset: 3600)
            let good = sleepBytes(start: s, end: s + 600, stages: [(242, s, 600)])
            let late = sleepBytes(start: s + 5000, end: s + 5600, stages: [(242, s + 5000, 300), (241, s + 5601, 300)])
            let early = sleepBytes(start: s + 9000, end: s + 9600, stages: [(242, s + 8999, 300)])
            let farFuture = sleepBytes(start: s + 12000, end: s + 12600, stages: [(242, s + 12000, 300), (241, 0xFFFF_FFFF, 300)])
            try f.fetch(.sleep, good + late + early + farFuture, at: at(3600))
            let result = try store.ingest(ringID: f.ring.id, kind: .sleep)
            #expect(result.inserted == 1 && result.rejectedBefore2020 == 3)
        }
    }

    /// A garbage rewrite is judged before it can supersede: the valid copy stays.
    @Test func aGarbageRewriteOfASleepSessionDoesNotSupersedeTheGoodCopy() throws {
        try onEachStore { store in
            let f = try clockedFixture(store, kinds: [.sleep])
            let s = wall(at(1000), offset: 3600)
            try f.fetch(.sleep, sleepBytes(start: s, end: s + 600, stages: [(242, s, 600)]), at: at(3600))
            try f.fetch(.sleep, sleepBytes(start: s, end: 0xFFFF_FFFF, stages: [(242, s, 600)]), at: at(7200))
            #expect(try store.transfers(ringID: f.ring.id, kind: .sleep).transfers.count == 2)
            let result = try store.ingest(ringID: f.ring.id, kind: .sleep)
            #expect(result.inserted == 1 && result.superseded == 0 && result.rejectedFuture == 1)
            let rows = try store.sleepSessions(ringID: f.ring.id).map(\.value)
            #expect(rows.count == 1 && rows[0].end == at(1000 + 600))
        }
    }

    // MARK: Which copy is later in the buffer

    /// Y is in both transfers and Z only in the first, at a later offset than Y there; both land on
    /// one key. Y's later copy is in the later transfer, so Y is later in the buffer and wins.
    @Test func aRecordInALaterTransferIsLaterInTheBufferThanOneAtALaterOffsetOfAnEarlierTransfer() throws {
        try onEachStore { store in
            let f = try clockedFixture(store)
            let t = wall(at(600), offset: plus1)
            let y = hrBytes(t, bpm: 1), z = hrBytes(t, bpm: 2)
            let q = hrBytes(wall(at(1200), offset: plus1), bpm: 3)
            try f.fetch(.heartRate, y + z, at: at(3600))
            try f.fetch(.heartRate, y + q, at: at(7200))
            #expect(try store.transfers(ringID: f.ring.id, kind: .heartRate).transfers.count == 2)
            let result = try store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(result.collisions == 1 && result.inserted == 2)
            #expect(try store.heartRateReadings(ringID: f.ring.id).map(\.value.bpm) == [1, 3])
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
