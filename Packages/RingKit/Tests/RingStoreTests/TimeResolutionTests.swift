import Foundation
import Testing
import RingProtocol
@testable import RingStore

// Time resolution, as pure logic and through ingestion. All bytes are synthetic.
//
// Times are real instants in seconds after t0. The app fetches, then sets the clock, so a clock-set
// made in the same sync as a fetch has an instant a few seconds after it.

private func at(_ seconds: Double) -> Date { t0.addingTimeInterval(seconds) }

/// A record's bytes for the pure tests (the resolver never decodes them, it only hashes prefixes).
private func filler(_ byte: UInt8) -> [UInt8] { [UInt8](repeating: byte, count: 6) }

struct TimeResolutionTests {
    private let ring = UUID()

    private func clock(_ seconds: Double, _ offset: Int) -> ClockSetEvent {
        ClockSetEvent(ringID: ring, instant: at(seconds), timeZoneIdentifier: "Europe/London", utcOffsetSeconds: offset)
    }

    private func obs(_ data: [UInt8], _ seconds: Double, id: Int64, zone: String = "Europe/London") -> TransferObservation {
        observation(ring, data: data, at: at(seconds), zone: zone, id: id)
    }

    /// Resolves the 6-byte record at `offset` of `data`; `reference` is the ring-local reading.
    private func resolve(
        _ observations: [TransferObservation], _ clocks: [ClockSetEvent], data: [UInt8], offset: Int = 0, reference: Date
    ) throws -> ResolvedOffset {
        let r = TimeResolver(ringID: ring, kind: .heartRate, observations: observations, clockSets: clocks)
        return try r.resolve(sightings: r.sightings(in: data), offset: offset, length: 6, reference: reference)
    }

    private func resolved(_ seconds: Int) -> ResolvedOffset { ResolvedOffset(seconds: seconds, source: .resolved) }
    private func ambiguous(_ seconds: Int) -> ResolvedOffset { ResolvedOffset(seconds: seconds, source: .ambiguous) }
    private func assumed(_ seconds: Int) -> ResolvedOffset { ResolvedOffset(seconds: seconds, source: .assumed) }

    // MARK: Pure logic: which clock-set

    @Test func aLoneClockSetMadeInTheSameSyncAsTheLastAbsentFetchIsResolved() throws {
        let rec = filler(1)
        let result = try resolve(
            [obs([], -10, id: 1), obs(rec, 3600, id: 2)], [clock(0, 3600)], data: rec, reference: at(1800 + 3600))
        #expect(result == resolved(3600))
    }

    /// The clock goes back an hour at sync 3, whose fetch repeats sync 2's data. That unchanged
    /// fetch is the last one without the later record, and the clock-set made right after it is
    /// the one that applies. Without that fetch in the log both offsets fit.
    @Test func anUnchangedFetchAtTheClockChangeRulesOutTheOldOffset() throws {
        let r0 = filler(1), r1 = filler(2)
        let both = r0 + r1
        let clocks = [clock(0, 3600), clock(7210, 0)]
        let f1 = obs([], -10, id: 1), f2 = obs(r0, 3600, id: 2), f3 = obs(r0, 7200, id: 3), f4 = obs(both, 14400, id: 4)
        let reference = at(9010)
        #expect(try resolve([f1, f2, f3, f4], clocks, data: both, offset: 6, reference: reference) == resolved(0))
        #expect(try resolve([f1, f2, f4], clocks, data: both, offset: 6, reference: reference) == ambiguous(0))
    }

    @Test func observationsThatAreNotPrefixesAreIgnored() {
        let r = TimeResolver(ringID: ring, kind: .heartRate, observations: [
            obs(filler(9), 100, id: 1), obs(filler(1), 3600, id: 2),
        ], clockSets: [clock(0, 3600)])
        #expect(r.sightings(in: filler(1)).map(\.fetchedAt) == [at(3600)])
    }

    @Test func aWrittenLateRecordIsResolvedWhateverItsOwnTime() throws {
        // A sleep session can be written after its end time: here its end is five minutes before the
        // last fetch that lacked it, yet it was written under the clock set right after that fetch.
        let rec = filler(1)
        let result = try resolve(
            [obs([], -10, id: 1), obs([], 3600, id: 2), obs(rec, 7200, id: 3)],
            [clock(0, 3600), clock(3610, 0)], data: rec, reference: at(3300))
        #expect(result == resolved(0))
    }

    @Test func aLoneClockSetMoreThan600SecondsAfterTheLastAbsentFetchFallsToTheCandidateRules() throws {
        let rec = filler(1)
        let observations = [obs([], -10, id: 1), obs([], 3600, id: 2), obs(rec, 7200, id: 3)]
        // Exactly 600 s after the fetch still counts as the same sync.
        #expect(try resolve(observations, [clock(0, 3600), clock(4200, 0)], data: rec, reference: at(6000)) == resolved(0))
        // 601 s: a fetch was missed, so both clock-sets are candidates and both fit.
        #expect(try resolve(observations, [clock(0, 3600), clock(4201, 0)], data: rec, reference: at(6000)) == ambiguous(0))
    }

    @Test func noLastAbsentFetchMeansEveryEarlierClockSetIsACandidate() throws {
        let rec = filler(1)
        // Only fits the earlier clock-set.
        #expect(try resolve([obs(rec, 3600, id: 1)], [clock(0, 3600), clock(1000, 0)], data: rec, reference: at(4000 + 3600))
            == assumed(0))  // p under +3600 = 4000, far past the period; under 0 = 7600, past the fetch
    }

    @Test func daylightSavingStartingMovesTheClockForward() throws {
        let r0 = filler(1), r1 = filler(2)
        let clocks = [clock(0, 0), clock(3610, 3600)]
        let observations = [obs([], -10, id: 1), obs(r0, 3600, id: 2), obs(r0 + r1, 7200, id: 3)]
        #expect(try resolve(observations, clocks, data: r0 + r1, offset: 6, reference: at(5000 + 3600)) == resolved(3600))
        #expect(try resolve(observations, clocks, data: r0 + r1, offset: 0, reference: at(1800)) == resolved(0))
    }

    @Test func aMissedFetchLeavingTwoClockSetsInTheWindowIsAmbiguous() throws {
        let rec = filler(1)
        let result = try resolve(
            [obs([], -10, id: 1), obs(rec, 14400, id: 2)], [clock(0, 3600), clock(7200, 0)], data: rec, reference: at(9000))
        #expect(result == ambiguous(0))
    }

    @Test func aZoneChangeBetweenSyncsResolvesEachRecordToItsOwnClockSet() throws {
        let r0 = filler(1), r1 = filler(2)
        let clocks = [clock(0, 3600), clock(3610, -18000)]
        let observations = [obs([], -10, id: 1), obs(r0, 3600, id: 2), obs(r0 + r1, 7200, id: 3)]
        #expect(try resolve(observations, clocks, data: r0 + r1, offset: 0, reference: at(1800 + 3600)) == resolved(3600))
        #expect(try resolve(observations, clocks, data: r0 + r1, offset: 6, reference: at(5400 - 18000)) == resolved(-18000))
    }

    @Test func whenNothingFitsTheLatestCandidateIsAssumed() throws {
        let rec = filler(1)
        // Two candidates, neither fits: the latest one is used (not the first).
        let result = try resolve(
            [obs(rec, 3600, id: 1)], [clock(0, 3600), clock(1000, 7200)], data: rec, reference: at(50000))
        #expect(result == assumed(7200))
        // One candidate that does not fit.
        #expect(try resolve([obs(rec, 3600, id: 1)], [clock(0, 3600)], data: rec, reference: at(50000)) == assumed(3600))
    }

    @Test func noClockSetAtAllIsAssumedWithTheZoneOffsetAtTheFirstSightingInstant() throws {
        let rec = filler(1)
        // t0 is in January, so New York is five hours behind.
        #expect(try resolve([obs(rec, 3600, id: 1, zone: "America/New_York")], [], data: rec, reference: at(0))
            == assumed(-18000))
        // A clock-set that is only after the first sighting is not a candidate either.
        #expect(try resolve([obs(rec, 3600, id: 1, zone: "America/New_York")], [clock(3700, 0)], data: rec, reference: at(0))
            == assumed(-18000))
    }

    @Test func aClockSetExactlyAtTheFirstSightingIsNotACandidate() throws {
        let rec = filler(1)
        // The reference fits the earlier clock-set; with the later one counted it would be ambiguous.
        let result = try resolve([obs(rec, 3600, id: 1)], [clock(0, 3600), clock(3600, 0)], data: rec, reference: at(3600))
        #expect(result == resolved(3600))
    }

    // MARK: Pure logic: the 120 second edges

    @Test func theProposedInstantMayBeUpTo120SecondsBeforeTheClockSet() throws {
        let rec = filler(1)
        let observations = [obs(rec, 3600, id: 1)]
        #expect(try resolve(observations, [clock(0, 3600)], data: rec, reference: at(-120 + 3600)) == resolved(3600))
        #expect(try resolve(observations, [clock(0, 3600)], data: rec, reference: at(-121 + 3600)) == assumed(3600))
    }

    @Test func theProposedInstantMayBeUpTo120SecondsAfterTheFirstSighting() throws {
        let rec = filler(1)
        let observations = [obs(rec, 3600, id: 1)]
        // A later clock-set makes the period end far past the fetch, so only the fetch bound applies.
        let clocks = [clock(0, 3600), clock(10000, 0)]
        #expect(try resolve(observations, clocks, data: rec, reference: at(3720 + 3600)) == resolved(3600))
        #expect(try resolve(observations, clocks, data: rec, reference: at(3721 + 3600)) == assumed(3600))
    }

    @Test func theProposedInstantMayBeUpTo120SecondsAfterTheClockSetsPeriodEnds() throws {
        let rec = filler(1)
        let observations = [obs(rec, 3600, id: 1)]
        let clocks = [clock(0, 3600), clock(1000, 0)]
        // The period of the first ends at 1000; the second cannot fit (its proposed instant is past the fetch).
        #expect(try resolve(observations, clocks, data: rec, reference: at(1120 + 3600)) == resolved(3600))
        #expect(try resolve(observations, clocks, data: rec, reference: at(1121 + 3600)) == assumed(0))
    }

    @Test func clockSetsAtTheSameInstantKeepTheOrderTheyWereGiven() throws {
        let rec = filler(1)
        let observations = [obs(rec, 3600, id: 1)]
        // Both fit a reference of 60 s, so the later one in the given order is used.
        let a = clock(0, 60), b = clock(0, 0)
        #expect(try resolve(observations, [a, b], data: rec, reference: at(60)) == ambiguous(0))
        #expect(try resolve(observations, [b, a], data: rec, reference: at(60)) == ambiguous(60))
    }

    /// A fetch after the record's first sighting that again lacked the record (a shorter, later
    /// fetch of the same bytes) is not the last fetch that lacked it before it was seen.
    @Test func aLaterFetchLackingTheRecordDoesNotMoveLastAbsent() throws {
        let both = filler(1) + filler(2)
        let observations = [
            obs(filler(1), 100, id: 1), obs(both, 200, id: 2), obs(filler(1), 300, id: 3),
        ]
        // Correct: last absent 100, window holds two clock-sets, none fits a far reference: assumed.
        // If the later fetch were taken, the latest clock-set would simply be "in force": resolved.
        let result = try resolve(
            observations, [clock(0, 3600), clock(150, 7200), clock(160, 10800)], data: both, offset: 6, reference: at(100_000))
        #expect(result == assumed(10800))
    }

    // MARK: Through ingestion

    @Test func ingestionUsesTheClockSetsOffset() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            try f.fetch(.heartRate, [], at: at(-10))
            try f.setClock(at: at(0), offset: 3600)
            try f.fetch(.heartRate, hrBytes(wall(at(1800), offset: 3600), bpm: 60), at: at(3600))
            let result = try store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(result.inserted == 1 && result.rowsByTimeSource == [.resolved: 1])
            let row = try #require(try store.heartRateReadings(ringID: f.ring.id).first).value
            #expect(row.time == at(1800) && row.utcOffsetSeconds == 3600 && row.timeSource == .resolved)
        }
    }

    @Test func zoneChangeBetweenSyncsUsesEachRecordsOwnOffset() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            try f.fetch(.heartRate, [], at: at(-10))
            try f.setClock(at: at(0), offset: 3600)  // London
            let r0 = hrBytes(wall(at(1800), offset: 3600), bpm: 60)
            try f.fetch(.heartRate, r0, at: at(3600))
            try f.setClock(at: at(3610), offset: -18000, zone: "America/New_York")  // flew west
            let r1 = hrBytes(wall(at(5400), offset: -18000), bpm: 61)
            try f.fetch(.heartRate, r0 + r1, at: at(7200), zone: "America/New_York")
            try store.ingest(ringID: f.ring.id, kind: .heartRate)
            let rows = try store.heartRateReadings(ringID: f.ring.id).map(\.value)
            #expect(rows.map(\.time) == [at(1800), at(5400)])
            #expect(rows.map(\.utcOffsetSeconds) == [3600, -18000])
            #expect(rows.map(\.timeSource) == [.resolved, .resolved])
        }
    }

    /// Daylight saving ends: the clock goes back an hour at sync 3, whose fetch repeats sync 2's
    /// data. The later record's wall clock also fits the old offset; only that duplicate fetch
    /// rules the old offset out. A record from before the change keeps the old offset.
    @Test func daylightSavingEndingResolvesEachRecordToItsOwnOffset() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            try f.fetch(.heartRate, [], at: at(-10))
            try f.setClock(at: at(0), offset: 3600)
            let before = hrBytes(wall(at(1800), offset: 3600), bpm: 60)
            try f.fetch(.heartRate, before, at: at(3600))
            try f.fetch(.heartRate, before, at: at(7200))  // unchanged duplicate at the clock change
            try f.setClock(at: at(7210), offset: 0)
            let after = hrBytes(wall(at(9010), offset: 0), bpm: 61)
            try f.fetch(.heartRate, before + after, at: at(14400))
            let result = try store.ingest(ringID: f.ring.id, kind: .heartRate)
            let rows = try store.heartRateReadings(ringID: f.ring.id).map(\.value)
            #expect(rows.map(\.time) == [at(1800), at(9010)])
            #expect(rows.map(\.utcOffsetSeconds) == [3600, 0])
            #expect(rows.map(\.timeSource) == [.resolved, .resolved])
            #expect(result.rowsByTimeSource == [.resolved: 2])
        }
    }

    @Test func daylightSavingStartingResolvesEachRecordToItsOwnOffset() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            try f.fetch(.heartRate, [], at: at(-10))
            try f.setClock(at: at(0), offset: 0)
            let before = hrBytes(wall(at(1800), offset: 0), bpm: 60)
            try f.fetch(.heartRate, before, at: at(3600))
            try f.setClock(at: at(3610), offset: 3600)  // clock forward an hour
            let after = hrBytes(wall(at(5000), offset: 3600), bpm: 61)
            try f.fetch(.heartRate, before + after, at: at(7200))
            try store.ingest(ringID: f.ring.id, kind: .heartRate)
            let rows = try store.heartRateReadings(ringID: f.ring.id).map(\.value)
            #expect(rows.map(\.time) == [at(1800), at(5000)])
            #expect(rows.map(\.utcOffsetSeconds) == [0, 3600])
            #expect(rows.map(\.timeSource) == [.resolved, .resolved])
        }
    }

    @Test func missedFetchGivesAmbiguousThroughIngestion() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            try f.fetch(.heartRate, [], at: at(-10))
            try f.setClock(at: at(0), offset: 3600)
            try f.setClock(at: at(7200), offset: 0)  // the sync between failed to fetch
            try f.fetch(.heartRate, hrBytes(wall(at(9000), offset: 0), bpm: 60), at: at(14400))
            let result = try store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(result.rowsByTimeSource == [.ambiguous: 1])
            let row = try #require(try store.heartRateReadings(ringID: f.ring.id).first).value
            #expect(row.utcOffsetSeconds == 0 && row.time == at(9000) && row.timeSource == .ambiguous)
        }
    }

    @Test func aRecordNoClockSetFitsIsAssumedThroughIngestion() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            try f.setClock(at: at(0), offset: 3600)
            try f.fetch(.heartRate, hrBytes(wall(at(40000), offset: 3600), bpm: 60), at: at(3600))
            let result = try store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(result.rowsByTimeSource == [.assumed: 1])
            #expect(try store.heartRateReadings(ringID: f.ring.id).first?.value.utcOffsetSeconds == 3600)
        }
    }

    @Test func noClockSetGivesAssumedWithTheFetchZone() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            try f.fetch(
                .heartRate, hrBytes(wall(at(0), offset: -18000), bpm: 60), at: at(3600), zone: "America/New_York")
            let result = try store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(result.rowsByTimeSource == [.assumed: 1])
            let row = try #require(try store.heartRateReadings(ringID: f.ring.id).first).value
            #expect(row.utcOffsetSeconds == -18000 && row.time == at(0))
        }
    }

    /// With no clock-set the offset is the zone's at the fetch instant for the whole sighting, even
    /// for records either side of a daylight-saving change in that zone (the ring does not follow it).
    @Test func noClockSetUsesOneOffsetForRecordsEitherSideOfADaylightSavingChange() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            func instant(_ text: String) -> Date { ISO8601DateFormatter().date(from: text)! }
            func ringSeconds(_ text: String) -> UInt32 { UInt32(instant(text).timeIntervalSince1970 - 946_684_800) }
            // London moved to summer time on 2027-03-28. Both records were stamped by one clock.
            let data = hrBytes(ringSeconds("2027-03-27T12:00:00Z"), bpm: 60) + hrBytes(ringSeconds("2027-03-29T12:00:00Z"), bpm: 61)
            try f.fetch(.heartRate, data, at: instant("2027-03-30T12:00:00Z"), zone: "Europe/London")
            let result = try store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(result.rowsByTimeSource == [.assumed: 2])
            let rows = try store.heartRateReadings(ringID: f.ring.id).map(\.value)
            #expect(rows.map(\.utcOffsetSeconds) == [3600, 3600])
            #expect(rows.map(\.time) == [instant("2027-03-27T11:00:00Z"), instant("2027-03-29T11:00:00Z")])
        }
    }

    /// A sleep session written after its end time (its end is minutes before the last fetch that
    /// lacked it) still takes the clock-set made in that sync.
    @Test func aSleepSessionWrittenLateIsResolvedThroughIngestion() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            try f.fetch(.sleep, [], at: at(-10))
            try f.setClock(at: at(0), offset: 3600)
            try f.fetch(.sleep, [], at: at(3600))
            try f.setClock(at: at(3610), offset: 0)
            let start = wall(at(2400), offset: 0)
            try f.fetch(
                .sleep, sleepBytes(start: start, end: wall(at(3300), offset: 0), stages: [(242, start, 900)]), at: at(7200))
            let result = try store.ingest(ringID: f.ring.id, kind: .sleep)
            #expect(result.rowsByTimeSource == [.resolved: 1])
            let row = try #require(try store.sleepSessions(ringID: f.ring.id).first).value
            #expect(row.utcOffsetSeconds == 0 && row.end == at(3300))
        }
    }
}
