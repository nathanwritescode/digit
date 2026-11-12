import Foundation
import Testing
import RingProtocol
@testable import RingStore

// Time resolution, as pure logic and through ingestion. All bytes are synthetic.
//
// Timeline used throughout (real instants, seconds after t0): the app fetches, then sets the clock.

private func at(_ seconds: Double) -> Date { t0.addingTimeInterval(seconds) }

struct TimeResolutionTests {
    private func resolver(
        _ ring: UUID, observations: [TransferObservation], clockSets: [ClockSetEvent]
    ) -> TimeResolver {
        TimeResolver(ringID: ring, kind: .heartRate, observations: observations, clockSets: clockSets)
    }

    private func clock(_ ring: UUID, _ seconds: Double, _ offset: Int) -> ClockSetEvent {
        ClockSetEvent(ringID: ring, instant: at(seconds), timeZoneIdentifier: "Europe/London", utcOffsetSeconds: offset)
    }

    // MARK: Pure logic

    @Test func oneClockSetResolvesWithItsOffset() {
        let ring = UUID()
        let r0 = hrBytes(wall(at(1800), offset: 3600), bpm: 60)
        let obs = [
            observation(ring, data: [], at: at(-10), id: 1),
            observation(ring, data: r0, at: at(3600), id: 2),
        ]
        let r = resolver(ring, observations: obs, clockSets: [clock(ring, 0, 3600)])
        let s = r.sightings(in: r0, fetchedAt: at(3600), timeZoneIdentifier: "Europe/London")
        #expect(r.resolve(sightings: s, offset: 0, length: 6, reference: Date(timeIntervalSince1970: t0.timeIntervalSince1970 + 1800 + 3600))
            == ResolvedOffset(seconds: 3600, source: .resolved))
    }

    /// The clock goes back an hour at the third sync. The third fetch repeats the second's data
    /// unchanged, and it is the only thing that shows the later record did not exist before the
    /// change. With it the old offset is ruled out; without it both offsets fit.
    @Test func anUnchangedFetchAtTheClockChangeRulesOutTheOldOffset() {
        let ring = UUID()
        let r0 = hrBytes(wall(at(1800), offset: 3600), bpm: 60)
        // Written 30 minutes after the clock went back (clock-set B at 7210), so wall clock = real.
        let r1 = hrBytes(wall(at(9010), offset: 0), bpm: 61)
        let both = r0 + r1
        let clocks = [clock(ring, 0, 3600), clock(ring, 7210, 0)]
        let f1 = observation(ring, data: [], at: at(-10), id: 1)
        let f2 = observation(ring, data: r0, at: at(3600), id: 2)
        let f3 = observation(ring, data: r0, at: at(7200), id: 3)  // unchanged duplicate
        let f4 = observation(ring, data: both, at: at(14400), id: 4)
        let reference = Date(timeIntervalSince1970: t0.timeIntervalSince1970 + 9010)

        let full = resolver(ring, observations: [f1, f2, f3, f4], clockSets: clocks)
        let s = full.sightings(in: both, fetchedAt: at(14400), timeZoneIdentifier: "Europe/London")
        #expect(full.resolve(sightings: s, offset: 6, length: 6, reference: reference)
            == ResolvedOffset(seconds: 0, source: .resolved))

        // The same log with the duplicate fetch dropped cannot tell which offset applies.
        let without = resolver(ring, observations: [f1, f2, f4], clockSets: clocks)
        let s2 = without.sightings(in: both, fetchedAt: at(14400), timeZoneIdentifier: "Europe/London")
        #expect(without.resolve(sightings: s2, offset: 6, length: 6, reference: reference)
            == ResolvedOffset(seconds: 0, source: .ambiguous))
    }

    @Test func observationsThatAreNotPrefixesAreIgnored() {
        let ring = UUID()
        let r0 = hrBytes(wall(at(1800), offset: 3600), bpm: 60)
        let other = hrBytes(wall(at(1800), offset: 3600), bpm: 99)  // same length, different bytes
        let r = resolver(ring, observations: [
            observation(ring, data: other, at: at(100), id: 1),
            observation(ring, data: r0, at: at(3600), id: 2),
        ], clockSets: [clock(ring, 0, 3600)])
        let s = r.sightings(in: r0, fetchedAt: at(3600), timeZoneIdentifier: "Europe/London")
        #expect(s.map(\.fetchedAt) == [at(3600)])
    }

    @Test func missedFetchLeavingTwoClockSetsIsAmbiguous() {
        let ring = UUID()
        // Sync 2 failed to fetch but still set the clock, so two clock-sets sit in one window.
        let rec = hrBytes(wall(at(9000), offset: 0), bpm: 60)
        let obs = [
            observation(ring, data: [], at: at(-10), id: 1),
            observation(ring, data: rec, at: at(14400), id: 2),
        ]
        let r = resolver(ring, observations: obs, clockSets: [clock(ring, 0, 3600), clock(ring, 7200, 0)])
        let s = r.sightings(in: rec, fetchedAt: at(14400), timeZoneIdentifier: "Europe/London")
        #expect(r.resolve(sightings: s, offset: 0, length: 6, reference: Date(timeIntervalSince1970: t0.timeIntervalSince1970 + 9000))
            == ResolvedOffset(seconds: 0, source: .ambiguous))
    }

    @Test func noClockSetFallsBackToTheObservationsZone() {
        let ring = UUID()
        let rec = hrBytes(wall(at(0), offset: -18000), bpm: 60)
        let obs = [observation(ring, data: rec, at: at(3600), zone: "America/New_York", id: 1)]
        let r = resolver(ring, observations: obs, clockSets: [])
        let s = r.sightings(in: rec, fetchedAt: at(3600), timeZoneIdentifier: "America/New_York")
        // t0 is in January, so New York is five hours behind.
        #expect(r.resolve(sightings: s, offset: 0, length: 6, reference: Date(timeIntervalSince1970: t0.timeIntervalSince1970 - 18000))
            == ResolvedOffset(seconds: -18000, source: .assumed))
    }

    @Test func aRecordNoClockSetExplainsIsAssumedWithTheLatestCandidate() {
        let ring = UUID()
        // Wall clock far from anything the clock-set at 0 could have produced by the fetch.
        let rec = hrBytes(wall(at(40000), offset: 3600), bpm: 60)
        let obs = [observation(ring, data: rec, at: at(3600), id: 1)]
        let r = resolver(ring, observations: obs, clockSets: [clock(ring, 0, 3600)])
        let s = r.sightings(in: rec, fetchedAt: at(3600), timeZoneIdentifier: "Europe/London")
        #expect(r.resolve(sightings: s, offset: 0, length: 6, reference: Date(timeIntervalSince1970: t0.timeIntervalSince1970 + 43600))
            == ResolvedOffset(seconds: 3600, source: .assumed))
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
}
