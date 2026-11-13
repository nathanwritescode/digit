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

    /// A fetch of `data`; turned into an observation of the kind under test by `observations`.
    struct Fetch {
        var data: [UInt8]
        var seconds: Double
        var id: Int64
        var zone: String
    }

    private func obs(_ data: [UInt8], _ seconds: Double, id: Int64, zone: String = "Europe/London") -> Fetch {
        Fetch(data: data, seconds: seconds, id: id, zone: zone)
    }

    private func observations(_ fetches: [Fetch], kind: HistoryKind) -> [TransferObservation] {
        fetches.map { observation(ring, kind, data: $0.data, at: at($0.seconds), zone: $0.zone, id: $0.id) }
    }

    /// Resolves the 6-byte record at `offset` of `data`; `reference` is the ring-local reading.
    ///
    /// By default an earlier clock-set with the first one's offset is put 200000 s ahead of the given
    /// ones, as for a ring synced for more than a day: a record whose window reaches back before the
    /// log is never `resolved` within 26 hours of the first clock-set (amendment L), and these tests
    /// are about the other rules. Pass `anchored: false` to test the log exactly as given.
    private func resolve(
        _ fetches: [Fetch], _ clocks: [ClockSetEvent], data: [UInt8], offset: Int = 0,
        reference: Date, kind: HistoryKind = .heartRate, start: Date? = nil, anchored: Bool = true
    ) throws -> ResolvedOffset {
        var log = clocks
        if anchored, let first = clocks.first { log.insert(clock(-200_000, first.utcOffsetSeconds), at: 0) }
        let r = TimeResolver(ringID: ring, kind: kind, observations: observations(fetches, kind: kind), clockSets: log)
        return try r.resolve(
            sightings: r.sightings(in: data), offset: offset, length: 6, reference: reference, start: start)
    }

    private func resolved(_ seconds: Int) -> ResolvedOffset { ResolvedOffset(seconds: seconds, source: .resolved) }
    private func ambiguous(_ seconds: Int) -> ResolvedOffset { ResolvedOffset(seconds: seconds, source: .ambiguous) }
    private func assumed(_ seconds: Int) -> ResolvedOffset { ResolvedOffset(seconds: seconds, source: .assumed) }

    // MARK: Worked cases (the clock goes back one hour at the second sync unless stated)

    // Shared timeline: clock A (+3600) set at 0 after an empty fetch at -10; a fetch at 7200; clock B (0)
    // set at 7210; a fetch at 14400.
    private var clocks: [ClockSetEvent] { [clock(0, 3600), clock(7210, 0)] }

    @Test func aHeartRateReadingWrittenWellAfterTheClockSetResolvesToTheNewOffset() throws {
        let r0 = filler(1), r1 = filler(2)
        let observations = [obs([], -10, id: 1), obs(r0, 7200, id: 2), obs(r0 + r1, 14400, id: 3)]
        // Real 10000, stamped by clock B (offset 0).
        #expect(try resolve(observations, clocks, data: r0 + r1, offset: 6, reference: at(10000)) == resolved(0))
    }

    @Test func aHeartRateReadingFromBeforeThePreviousFetchResolvesToTheOldOffset() throws {
        let r0 = filler(1), r1 = filler(2)
        let observations = [obs([], -10, id: 1), obs(r0, 7200, id: 2), obs(r0 + r1, 14400, id: 3)]
        // Real 5000 under +3600, first seen at 7200: clock B did not exist yet.
        #expect(try resolve(observations, clocks, data: r0 + r1, offset: 0, reference: at(5000 + 3600)) == resolved(3600))
    }

    /// Two readings with identical bytes either side of the clock-set (see identity in the
    /// ingestion tests): the second is at offset 6 and sees the later window.
    @Test func theSecondOfTwoIdenticalReadingsGetsItsOwnOffset() throws {
        let r = filler(7)
        let observations = [obs([], -10, id: 1), obs(r, 7200, id: 2), obs(r + r, 14400, id: 3)]
        // Both have wall clock 9000: real 5400 under +3600, real 9000 under 0.
        #expect(try resolve(observations, clocks, data: r + r, offset: 0, reference: at(9000)) == resolved(3600))
        #expect(try resolve(observations, clocks, data: r + r, offset: 6, reference: at(9000)) == resolved(0))
    }

    @Test func aSleepSessionThatEndedMoreThanAnHourBeforeTheSyncButWasWrittenAfterItKeepsTheOldOffset() throws {
        let rec = filler(1)
        let observations = [obs([], -10, id: 1), obs([], 7200, id: 2), obs(rec, 14400, id: 3)]
        // Ended at real 3000 (more than an hour before the sync at 7200) under +3600; seen first at 14400.
        #expect(try resolve(observations, clocks, data: rec, reference: at(3000 + 3600), kind: .sleep) == resolved(3600))
    }

    @Test func aSleepSessionThatEndedWithinTheHourBeforeTheSyncAndWasWrittenAfterItIsAmbiguous() throws {
        let rec = filler(1)
        let observations = [obs([], -10, id: 1), obs([], 7200, id: 2), obs(rec, 14400, id: 3)]
        // Ended at real 5400, within the hour before the sync, under +3600: wall clock 9000 also fits clock B.
        #expect(try resolve(observations, clocks, data: rec, reference: at(5400 + 3600), kind: .sleep) == ambiguous(0))
    }

    /// Written between the fetch (at 7200) and the clock-set (at 7210), so stamped by the OLD clock.
    /// Clock going back: the old offset's instant is fine, but the new offset also fits when the next
    /// fetch is more than an hour later, so it is ambiguous (and the latest, wrong, offset is used).
    /// Clock going forward: the new offset's instant lies before its own clock-set, so only the old
    /// one fits and the record is resolved correctly.
    @Test func aHeartRateReadingWrittenBetweenTheFetchAndTheClockSetOfOneSync() throws {
        let rec = filler(1)
        let observations = [obs([], 7200, id: 1), obs(rec, 14400, id: 2)]
        // Going back: real 7205 stamped +3600 gives wall 10805.
        #expect(try resolve(observations, clocks, data: rec, reference: at(7205 + 3600)) == ambiguous(0))
        // Going forward (0 then +3600): real 7205 stamped 0 gives wall 7205.
        let forward = [clock(0, 0), clock(7210, 3600)]
        #expect(try resolve(observations, forward, data: rec, reference: at(7205)) == resolved(0))
    }

    @Test func aRetriedClockSetWithTheSameOffsetIsResolvedWhateverTheRecordsTime() throws {
        let rec = filler(1)
        let retried = [clock(0, 3600), clock(5, 3600)]
        #expect(try resolve([obs([], -10, id: 1), obs(rec, 3600, id: 2)], retried, data: rec, reference: at(1800 + 3600)) == resolved(3600))
    }

    @Test func withNoClockSetInTheWindowTheLatestEarlierOneIsUsed() throws {
        let rec = filler(1)
        let several = [clock(0, 3600), clock(100, 7200), clock(200, 0)]
        // Last absent at 300, after all three clock-sets: only the latest (offset 0) is a candidate.
        let observations = [obs([], 300, id: 1), obs(rec, 3600, id: 2)]
        #expect(try resolve(observations, several, data: rec, reference: at(1000)) == resolved(0))
    }

    // MARK: Other rules

    @Test func observationsThatAreNotPrefixesAreIgnored() {
        let r = TimeResolver(ringID: ring, kind: .heartRate, observations: observations([
            obs(filler(9), 100, id: 1), obs(filler(1), 3600, id: 2),
        ], kind: .heartRate), clockSets: [clock(0, 3600)])
        #expect(r.sightings(in: filler(1)).map(\.fetchedAt) == [at(3600)])
    }

    /// The unchanged fetch at the clock change (7200 here, repeating the previous data) is what
    /// rules out the old offset for a reading written after it.
    @Test func anUnchangedFetchAtTheClockChangeRulesOutTheOldOffset() throws {
        let r0 = filler(1), r1 = filler(2)
        let both = r0 + r1
        let f1 = obs([], -10, id: 1), f2 = obs(r0, 3600, id: 2), f3 = obs(r0, 7200, id: 3), f4 = obs(both, 14400, id: 4)
        let reference = at(9010)
        #expect(try resolve([f1, f2, f3, f4], clocks, data: both, offset: 6, reference: reference) == resolved(0))
        #expect(try resolve([f1, f2, f4], clocks, data: both, offset: 6, reference: reference) == ambiguous(0))
    }

    @Test func aMissedFetchLeavingTwoClockSetsInTheWindowIsAmbiguous() throws {
        let rec = filler(1)
        let result = try resolve(
            [obs([], -10, id: 1), obs(rec, 14400, id: 2)], [clock(0, 3600), clock(7200, 0)], data: rec, reference: at(9000))
        #expect(result == ambiguous(0))
    }

    @Test func aZoneChangeBetweenSyncsResolvesEachRecordToItsOwnClockSet() throws {
        let r0 = filler(1), r1 = filler(2)
        let changed = [clock(0, 3600), clock(3610, -18000)]
        let observations = [obs([], -10, id: 1), obs(r0, 3600, id: 2), obs(r0 + r1, 7200, id: 3)]
        #expect(try resolve(observations, changed, data: r0 + r1, offset: 0, reference: at(1800 + 3600)) == resolved(3600))
        #expect(try resolve(observations, changed, data: r0 + r1, offset: 6, reference: at(5400 - 18000)) == resolved(-18000))
    }

    @Test func daylightSavingStartingMovesTheClockForward() throws {
        let r0 = filler(1), r1 = filler(2)
        let forward = [clock(0, 0), clock(3610, 3600)]
        let observations = [obs([], -10, id: 1), obs(r0, 3600, id: 2), obs(r0 + r1, 7200, id: 3)]
        #expect(try resolve(observations, forward, data: r0 + r1, offset: 6, reference: at(5000 + 3600)) == resolved(3600))
        #expect(try resolve(observations, forward, data: r0 + r1, offset: 0, reference: at(1800)) == resolved(0))
    }

    @Test func whenNothingFitsTheLatestCandidateIsAssumed() throws {
        let rec = filler(1)
        // Two candidates with different offsets, neither fits: the latest is used (not the first).
        let result = try resolve(
            [obs(rec, 3600, id: 1)], [clock(0, 3600), clock(1000, 7200)], data: rec, reference: at(50000))
        #expect(result == assumed(7200))
        // A single clock-set is certain when the record postdates it: every candidate shares its offset.
        #expect(try resolve([obs(rec, 3600, id: 1)], [clock(0, 3600)], data: rec, reference: at(1800 + 3600)) == resolved(3600))
    }

    @Test func noClockSetAtAllIsAssumedWithTheZoneOffsetAtTheFirstSightingInstant() throws {
        let rec = filler(1)
        // t0 is in January, so New York is five hours behind.
        #expect(try resolve([obs(rec, 3600, id: 1, zone: "America/New_York")], [], data: rec, reference: at(0))
            == assumed(-18000))
        // A clock-set that is only after the first sighting is not a candidate either.
        #expect(try resolve(
            [obs(rec, 3600, id: 1, zone: "America/New_York")], [clock(3700, 0)], data: rec, reference: at(0), anchored: false)
            == assumed(-18000))
    }

    @Test func aClockSetExactlyAtTheFirstSightingIsNotACandidate() throws {
        let rec = filler(1)
        // Not a candidate, so the lone earlier clock-set is certain. Counted, the two would differ
        // and both fit this reference (ambiguous).
        let result = try resolve([obs(rec, 3600, id: 1)], [clock(0, 3600), clock(3600, 0)], data: rec, reference: at(3600))
        #expect(result == resolved(3600))
    }

    // MARK: The 120 second edges (two candidates, so a fit decides; the other never fits)

    @Test func theProposedInstantMayBeUpTo120SecondsBeforeTheClockSet() throws {
        let rec = filler(1)
        let two = [clock(-50000, 0), clock(0, 3600)]
        let observations = [obs(rec, 3600, id: 1)]
        #expect(try resolve(observations, two, data: rec, reference: at(-120 + 3600)) == resolved(3600))
        #expect(try resolve(observations, two, data: rec, reference: at(-121 + 3600)) == assumed(3600))
    }

    @Test func theProposedInstantMayBeUpTo120SecondsAfterTheFirstSighting() throws {
        let rec = filler(1)
        // The third clock-set is after the first sighting: it only stretches the second's period.
        let three = [clock(-50000, 0), clock(0, 3600), clock(10000, 0)]
        let observations = [obs(rec, 3600, id: 1)]
        #expect(try resolve(observations, three, data: rec, reference: at(3720 + 3600)) == resolved(3600))
        #expect(try resolve(observations, three, data: rec, reference: at(3721 + 3600)) == assumed(3600))
    }

    @Test func theProposedInstantMayBeUpTo120SecondsAfterTheClockSetsPeriodEnds() throws {
        let rec = filler(1)
        let two = [clock(0, 3600), clock(1000, 0)]
        let observations = [obs(rec, 3600, id: 1)]
        // The second cannot fit: its proposed instant is past the fetch.
        #expect(try resolve(observations, two, data: rec, reference: at(1120 + 3600)) == resolved(3600))
        #expect(try resolve(observations, two, data: rec, reference: at(1121 + 3600)) == assumed(0))
    }

    /// Heart rate and activity are written at about their reference time, so a proposed instant
    /// more than 120 s before the last fetch that lacked the record does not fit. A sleep session is
    /// written after it ends, so the bound does not apply to it.
    @Test func theLastAbsentLowerBoundAppliesToHeartRateAndActivityButNotSleep() throws {
        let rec = filler(1)
        let observations = [obs([], 3600, id: 1), obs(rec, 14400, id: 2)]
        // Candidates: +3600 at 0 (at or before the last fetch) and 0 at 7210 (in the window).
        let edge = [clock(0, 3600), clock(7210, 0)]
        // Proposed under +3600 is exactly 120 s before the last absent fetch: fits; the other does not.
        #expect(try resolve(observations, edge, data: rec, reference: at(3480 + 3600)) == resolved(3600))
        for kind in [HistoryKind.heartRate, .activity] {
            #expect(try resolve(observations, edge, data: rec, reference: at(3479 + 3600), kind: kind) == assumed(0))
        }
        #expect(try resolve(observations, edge, data: rec, reference: at(3479 + 3600), kind: .sleep) == resolved(3600))
    }

    @Test func clockSetsAtTheSameInstantKeepTheOrderTheyWereGiven() throws {
        let rec = filler(1)
        let observations = [obs(rec, 3600, id: 1)]
        // Both fit a reference of 60 s, so the later one in the given order is used.
        let a = clock(0, 60), b = clock(0, 0)
        #expect(try resolve(observations, [a, b], data: rec, reference: at(60)) == ambiguous(0))
        #expect(try resolve(observations, [b, a], data: rec, reference: at(60)) == ambiguous(60))
    }

    /// Two fitting clock-sets with the same offset do not make a record ambiguous.
    @Test func fittingCandidatesAreGroupedByOffsetNotByClockSet() throws {
        let rec = filler(1)
        let three = [clock(0, 3600), clock(100, 3600), clock(1000, 0)]
        // Proposed instant 201 under +3600 fits both of the first two; the third does not fit.
        #expect(try resolve([obs(rec, 3600, id: 1)], three, data: rec, reference: at(3801)) == resolved(3600))
    }

    /// A later fetch that again lacked the record (a shorter fetch of the same bytes) is not the
    /// last fetch that lacked it before it was seen.
    @Test func aLaterFetchLackingTheRecordDoesNotMoveLastAbsent() throws {
        let both = filler(1) + filler(2)
        let observations = [
            obs(filler(1), 100, id: 1), obs(both, 200, id: 2), obs(filler(1), 300, id: 3),
        ]
        // Correct: last absent 100, so all three clock-sets are candidates and only the middle
        // one fits this reference (real 100 under offset 7200): resolved. Taking the later fetch
        // would leave only the last clock-set, which the record predates: assumed.
        let result = try resolve(
            observations, [clock(0, 3600), clock(150, 7200), clock(160, 10800)], data: both, offset: 6, reference: at(7300))
        #expect(result == resolved(7200))
    }

    // MARK: Sleep candidates and the same-offset case (J, K)

    /// A record written before the first clock-set the app ever logged (history from before the app,
    /// or after a reinstall) is not confirmed by that clock-set, even though it is the only one.
    @Test func aRecordAWeekOlderThanTheOnlyClockSetIsAssumed() throws {
        let rec = filler(1)
        let week = 7.0 * 86400
        // A single clock-set, exactly as the amendment states it.
        #expect(try resolve(
            [obs(rec, 3600, id: 1)], [clock(0, 3600)], data: rec, reference: at(-week + 3600), anchored: false)
            == assumed(3600))
        // Nor is one dated well after it was first seen.
        #expect(try resolve(
            [obs(rec, 3600, id: 1)], [clock(0, 3600)], data: rec, reference: at(50000), anchored: false) == assumed(3600))
        // The same record shortly after the clock-set is certain.
        #expect(try resolve([obs(rec, 3600, id: 1)], [clock(0, 3600)], data: rec, reference: at(1800 + 3600)) == resolved(3600))
    }

    /// Seconds are relative to 02:00 on one day. c0 at 02:00 (-14400); the session ends 07:30 ring-local
    /// (11:30 UTC = 34200); fetch 11:35 (34500) without it, then c1 at 34505 (-18000); fetch 11:50 (35400)
    /// without it, then c2 at 35405 (-18000); the ring writes it at 12:00; the fetch at 13:00 (39600)
    /// contains it. A clock that went back an hour: the session's wall clock fits both offsets.
    @Test func aSleepSessionWrittenTwoSyncsAfterItEndedAcrossAClockGoingBackIsAmbiguous() throws {
        let rec = filler(1)
        let reference = at(34200 - 14400)  // 07:30 ring-local
        let twoSyncs = [clock(0, -14400), clock(34505, -18000), clock(35405, -18000)]
        let fetches = [obs([], 34500, id: 1), obs([], 35400, id: 2), obs(rec, 39600, id: 3)]
        #expect(try resolve(fetches, twoSyncs, data: rec, reference: reference, kind: .sleep) == ambiguous(-18000))
        // With only the first of the two syncs it is also ambiguous.
        let oneSync = [clock(0, -14400), clock(34505, -18000)]
        let fewer = [obs([], 34500, id: 1), obs(rec, 39600, id: 2)]
        #expect(try resolve(fewer, oneSync, data: rec, reference: reference, kind: .sleep) == ambiguous(-18000))
    }

    @Test func anOrdinarySleepSessionWithNoOffsetChangeIsResolved() throws {
        let rec = filler(1)
        let result = try resolve(
            [obs([], 3600, id: 1), obs(rec, 14400, id: 2)], [clock(0, 3600)], data: rec, reference: at(7000 + 3600), kind: .sleep)
        #expect(result == resolved(3600))
    }

    /// Heart rate and activity keep the narrowing by the last-absent fetch: the older clock-set is
    /// not a candidate, so the later one is certain. (A sleep session would test both.)
    @Test func heartRateCandidatesAreNarrowedByTheLastAbsentFetch() throws {
        let rec = filler(1)
        let observations = [obs([], 3600, id: 1), obs(rec, 14400, id: 2)]
        let two = [clock(0, 3600), clock(100, 0)]
        #expect(try resolve(observations, two, data: rec, reference: at(100)) == resolved(0))
    }

    /// A clock-set made at exactly the last-absent instant counts as at or before it: it is the
    /// clock in force, not one made in the window.
    @Test func aClockSetAtExactlyTheLastAbsentInstantIsAtOrBeforeIt() throws {
        let rec = filler(1)
        let observations = [obs([], 3600, id: 1), obs(rec, 14400, id: 2)]
        let result = try resolve(observations, [clock(0, 7200), clock(3600, 3600)], data: rec, reference: at(3500 + 7200))
        #expect(result == resolved(3600))
    }

    // MARK: Windows that reach before the log (L)

    // Time zero is 12:00:00 UTC. Before the first logged clock-set the ring's offset is unknown: here it
    // was -14400 (set by the vendor's app); the first-ever clock-set sets -18000.

    /// Heart rate at 1-minute monitoring: the reading at 12:00:10 UTC is 08:00:10 on the old clock.
    /// Under the first logged offset (-18000) that looks like 13:00:10 UTC, a valid time just after the
    /// clock-set at 12:00:20, so the logs cannot confirm it.
    @Test func aReadingTakenUnderTheUnknownClockJustBeforeTheFirstClockSetIsAmbiguous() throws {
        let rec = filler(1)
        let result = try resolve(
            [obs([], 0, id: 1), obs(rec, 21600, id: 2)], [clock(20, -18000)], data: rec,
            reference: at(10 - 14400), anchored: false)
        #expect(result == ambiguous(-18000))
    }

    /// A sleep session that ended at 11:40 UTC (07:40 on the old clock), written at 12:10; the first-ever
    /// clock-set is at 12:00:05 and the next fetch at 23:00.
    @Test func aSleepSessionFromBeforeTheFirstClockSetIsAmbiguous() throws {
        let rec = filler(1)
        let result = try resolve(
            [obs([], 0, id: 1), obs(rec, 39600, id: 2)], [clock(5, -18000)], data: rec,
            reference: at(-1200 - 14400), kind: .sleep, anchored: false)
        #expect(result == ambiguous(-18000))
    }

    /// The same ring two days later, nothing else changed: both are resolved.
    @Test func theSameRingTwoDaysLaterIsResolved() throws {
        let rec = filler(1)
        let day2 = 172_800.0
        #expect(try resolve(
            [obs([], day2, id: 1), obs(rec, day2 + 21600, id: 2)], [clock(20, -18000)], data: rec,
            reference: at(day2 + 10 - 18000), anchored: false) == resolved(-18000))
        #expect(try resolve(
            [obs([], day2, id: 1), obs(rec, day2 + 39600, id: 2)], [clock(5, -18000)], data: rec,
            reference: at(day2 - 1200 - 18000), kind: .sleep, anchored: false) == resolved(-18000))
    }

    /// The edge of the 26-hour window, so the bound cannot move unnoticed.
    @Test func theTwentySixHourWindowEdgeIsExactlyTwentySixHoursPlusTheTolerance() throws {
        let rec = filler(1)
        let edge = 26.0 * 3600 + 120
        // Chosen instant exactly at the edge is not "earlier than" it: resolved. One second less: ambiguous.
        func result(_ instant: Double) throws -> ResolvedOffset {
            try resolve(
                [obs(rec, 200_000, id: 1)], [clock(0, 0)], data: rec, reference: at(instant),
                anchored: false)
        }
        #expect(try result(edge) == resolved(0))
        #expect(try result(edge - 1) == ambiguous(0))
    }

    // MARK: A sleep session spanning an offset change (M)

    /// The clock goes from -14400 to -18000 at 07:00 UTC (time zero). The session was stamped 23:00
    /// ring-local at its start (03:00 UTC) and 07:00 ring-local at its end (12:00 UTC).
    @Test func aSleepSessionThatSpansAnOffsetChangeIsAmbiguous() throws {
        let rec = filler(1)
        let log = [clock(-200_000, -14400), clock(0, -18000)]
        let fetches = [obs([], -600, id: 1), obs(rec, 21600, id: 2)]
        let spanning = try resolve(
            fetches, log, data: rec, reference: at(0), kind: .sleep, start: at(-28800), anchored: false)
        #expect(spanning == ambiguous(-18000))
        // A session that started after the clock-set has nothing to span: resolved.
        let after = try resolve(
            fetches, log, data: rec, reference: at(0), kind: .sleep, start: at(-3600), anchored: false)
        #expect(after == resolved(-18000))
    }

    /// Time zero is 07:00 UTC. Offsets -14400 then -18000. The session starts at 06:30 UTC (02:30
    /// ring-local under the old clock); a fetch without it at 07:00, then the clock-set to -18000; it ends
    /// at 12:00 UTC (07:00 ring-local under the new clock) and is fetched at 13:00. The clock change is
    /// early in the session. Decoding the start with the new offset put it after the change and missed it.
    @Test func aClockGoingBackEarlyInASleepSessionIsAmbiguous() throws {
        let rec = filler(1)
        let result = try resolve(
            [obs([], 0, id: 1), obs(rec, 21600, id: 2)], [clock(-200_000, -14400), clock(5, -18000)], data: rec,
            reference: at(0), kind: .sleep, start: at(-1800 - 14400), anchored: false)
        #expect(result == ambiguous(-18000))
    }

    /// A six-hour change (clock forward) three hours after the true start.
    @Test func aSixHourChangeThreeHoursIntoASessionIsAmbiguous() throws {
        let rec = filler(1)
        let result = try resolve(
            [obs([], 10_790, id: 1), obs(rec, 25_200, id: 2)], [clock(-200_000, -21600), clock(10_800, 0)], data: rec,
            reference: at(18_000), kind: .sleep, start: at(-21_600), anchored: false)
        #expect(result == ambiguous(0))
    }

    @Test func clockSetsInsideASessionThatDoNotChangeTheOffsetLeaveItResolved() throws {
        let rec = filler(1)
        let log = [clock(-200_000, 3600), clock(5000, 3600), clock(6000, 3600)]
        let result = try resolve(
            [obs([], 4800, id: 1), obs(rec, 20_000, id: 2)], log, data: rec, reference: at(8000 + 3600), kind: .sleep,
            start: at(4000 + 3600), anchored: false)
        #expect(result == resolved(3600))
    }

    @Test func anOffsetThatChangesAndChangesBackInsideASessionIsAmbiguous() throws {
        let rec = filler(1)
        let log = [clock(-200_000, 0), clock(5000, 3600), clock(6000, 0)]
        let result = try resolve(
            [obs([], 4800, id: 1), obs(rec, 20_000, id: 2)], log, data: rec, reference: at(8000), kind: .sleep,
            start: at(4000), anchored: false)
        #expect(result == ambiguous(0))
    }

    /// Strict bounds. The clock goes forward from offset 0 to 3600 at time 0. The session's end resolves to
    /// `reference - 3600`; it spans the change when the clock-set is after the start as stamped under the old
    /// clock minus 120 s, and before the resolved end. A clock-set exactly at either bound does not count.
    @Test func aClockSetExactlyAtTheBoundsOfASessionIsNotInsideIt() throws {
        let rec = filler(1)
        let log = [clock(-200_000, 0), clock(0, 3600)]
        let fetches = [obs([], -5, id: 1), obs(rec, 10_000, id: 2)]
        func result(end: Double, start: Double) throws -> ResolvedOffset {
            try resolve(fetches, log, data: rec, reference: at(end + 3600), kind: .sleep, start: at(start), anchored: false)
        }
        // Lower bound: the clock-set at 0 is exactly start - 0 - 120 (start 120): not spanning; start 119: spanning.
        #expect(try result(end: 5000, start: 120) == resolved(3600))
        #expect(try result(end: 5000, start: 119) == ambiguous(3600))
        // Upper bound: the resolved end is exactly the clock-set (end 0): not spanning; end 1: spanning.
        #expect(try result(end: 0, start: -1000) == resolved(3600))
        #expect(try result(end: 1, start: -1000) == ambiguous(3600))
    }

    /// A sleep session always reaches back before the log, so even when the last-absent fetch is after
    /// the first clock-set it is `ambiguous` within 26 hours of it. Heart rate in the same log is resolved.
    @Test func aSleepSessionNearTheFirstClockSetIsAmbiguousEvenWithALaterLastAbsentFetch() throws {
        let rec = filler(1)
        let fetches = [obs([], 3600, id: 1), obs(rec, 14_400, id: 2)]
        let log = [clock(0, 3600)]
        #expect(try resolve(fetches, log, data: rec, reference: at(5000 + 3600), kind: .sleep, anchored: false) == ambiguous(3600))
        #expect(try resolve(fetches, log, data: rec, reference: at(5000 + 3600), anchored: false) == resolved(3600))
    }

    // MARK: The same-offset bound (K)

    /// Clock-sets at 0 and 1100 with one offset, last absent at 1000, first seen at 1200: the record
    /// has to postdate the earliest candidate, not the latest, so 500 is resolved.
    @Test func theSameOffsetBoundUsesTheEarliestCandidate() throws {
        let rec = filler(1)
        let result = try resolve(
            [obs([], 1000, id: 1), obs(rec, 1200, id: 2)], [clock(0, 3600), clock(1100, 3600)], data: rec,
            reference: at(500 + 3600))
        #expect(result == resolved(3600))
    }

    /// The bound has the tolerance: 60 s before the clock-set is still confirmed, 121 s is not. (With a
    /// last-absent fetch after the clock-set the fit test alone would refuse both.)
    @Test func theSameOffsetBoundAllowsTheTolerance() throws {
        let rec = filler(1)
        let fetches = [obs([], 1000, id: 1), obs(rec, 1200, id: 2)]
        #expect(try resolve(fetches, [clock(0, 3600)], data: rec, reference: at(-60 + 3600)) == resolved(3600))
        #expect(try resolve(fetches, [clock(0, 3600)], data: rec, reference: at(-121 + 3600)) == assumed(3600))
    }

    // MARK: Through ingestion

    @Test func ingestionUsesTheClockSetsOffset() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            try f.fetch(.heartRate, [], at: at(-10))
            try f.setFirstClock(at: at(0), offset: 3600)
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
            try f.setFirstClock(at: at(0), offset: 3600)  // London
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
            try f.setFirstClock(at: at(0), offset: 3600)
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
            try f.setFirstClock(at: at(0), offset: 0)
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
            try f.setFirstClock(at: at(0), offset: 3600)
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
            try f.setFirstClock(at: at(0), offset: 3600)
            try f.setClock(at: at(1000), offset: 7200)
            try f.fetch(.heartRate, hrBytes(wall(at(40000), offset: 3600), bpm: 60), at: at(3600))
            let result = try store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(result.rowsByTimeSource == [.assumed: 1])
            #expect(try store.heartRateReadings(ringID: f.ring.id).first?.value.utcOffsetSeconds == 7200)
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

    /// Sleep sessions are written some time after they end. Clock A (+3600) is replaced by B (0)
    /// at 7210, and the session first shows up at 14400.
    private func sleepSession(endingAtReal end: Double, _ store: RingStore) throws -> SleepSessionRecord {
        let f = try Fixture(store)
        try f.fetch(.sleep, [], at: at(-10))
        try f.setFirstClock(at: at(0), offset: 3600)
        try f.fetch(.sleep, [], at: at(7200))
        try f.setClock(at: at(7210), offset: 0)
        let start = wall(at(end - 900), offset: 3600)
        try f.fetch(
            .sleep, sleepBytes(start: start, end: wall(at(end), offset: 3600), stages: [(242, start, 900)]), at: at(14400))
        try store.ingest(ringID: f.ring.id, kind: .sleep)
        return try #require(try store.sleepSessions(ringID: f.ring.id).first).value
    }

    @Test func aSleepSessionThatEndedMoreThanAnHourBeforeTheSyncIsResolvedToTheOldOffset() throws {
        try onEachStore { store in
            let row = try sleepSession(endingAtReal: 3000, store)
            #expect(row.timeSource == .resolved && row.utcOffsetSeconds == 3600 && row.end == at(3000))
        }
    }

    @Test func aSleepSessionThatEndedWithinTheHourBeforeTheSyncIsAmbiguous() throws {
        try onEachStore { store in
            let row = try sleepSession(endingAtReal: 5400, store)
            #expect(row.timeSource == .ambiguous && row.utcOffsetSeconds == 0)
        }
    }
}
