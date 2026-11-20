import Foundation
import GRDB
import Testing
import RingProtocol
@testable import RingStore

// Clock-set outcomes (#72): the sync logs a clock-set before sending it, then records what became
// of it, and time resolution ignores entries the ring certainly did not apply. All bytes are
// synthetic.
//
// The shared scenario: the ring's clock was set to +1 h at 0 (after an empty fetch at -10). A
// fetch at 7200 finds nothing; a clock-set to UTC+0 is logged at 7210 but the ring refuses it (a
// phantom), so the ring keeps +1 h. A reading at real 10000 is stamped under +1 h (wall 13600) and
// first seen at 14400. With the phantom treated as real, the old clock's period ends at 7210 and
// the reading fits only the phantom: `resolved` under UTC+0, an hour wrong (13600).

private func at(_ seconds: Double) -> Date { t0.addingTimeInterval(seconds) }

private let reading = hrBytes(wall(at(10_000), offset: plus1), bpm: 70)

/// The scenario in a store, with the phantom's log id.
private func phantomFixture(_ store: RingStore) throws -> (Fixture, phantom: Int64) {
    let f = try clockedFixture(store)
    try f.fetch(.heartRate, [], at: at(7200))
    let phantom = try f.setClock(at: at(7210), offset: 0)
    try f.fetch(.heartRate, reading, at: at(14_400))
    return (f, phantom)
}

@Suite(.timeLimit(.minutes(1))) struct ClockSetOutcomeTests {
    // MARK: Frozen codes

    /// Rows on devices hold these strings. Renaming one would make existing outcomes read as none,
    /// silently turning ignored phantoms back into candidates.
    @Test func outcomeCodesAreFrozen() {
        let expected: [(LoggedClockSetOutcome, String)] = [
            (.acknowledged, "acknowledged"), (.notApplied, "notApplied"), (.notSent, "notSent"), (.unknown, "unknown"),
        ]
        #expect(expected.count == LoggedClockSetOutcome.allCases.count)
        for (outcome, code) in expected {
            #expect(RingStore.ClockSetOutcomeCode.code(for: outcome) == code)
            #expect(RingStore.ClockSetOutcomeCode.outcome(forCode: code) == outcome)
        }
        #expect(RingStore.ClockSetOutcomeCode.outcome(forCode: "applied") == nil)
    }

    @Test func onlyNotAppliedAndNotSentAreLeftOutOfTimeResolution() {
        #expect(LoggedClockSetOutcome.allCases.filter(\.excludesFromTimeResolution) == [.notApplied, .notSent])
    }

    // MARK: Recording

    @Test func recordClockSetReturnsTheEntryIDAndOutcomesAreListedAgainstIt() async throws {
        try await onEachStoreAsync { store in
            let ring = try await store.registerRing(protocolFamily: "ycbt")
            let event = { (seconds: Double) in
                ClockSetEvent(ringID: ring.id, instant: at(seconds), timeZoneIdentifier: "UTC", utcOffsetSeconds: 0)
            }
            let first = try await store.recordClockSet(event(100))
            let second = try blocking(store).recordClockSet(event(50))
            let third = try await store.recordClockSet(event(200))
            #expect(Set([first, second, third]).count == 3)
            try await store.recordClockSetOutcome(id: first, outcome: .notApplied)
            try blocking(store).recordClockSetOutcome(id: third, outcome: .acknowledged)
            let logged = try await store.loggedClockSets(ringID: ring.id)
            // Time order, like `clockSets(ringID:)`, which still lists every entry.
            #expect(logged == [
                LoggedClockSet(id: second, event: event(50), outcome: nil),
                LoggedClockSet(id: first, event: event(100), outcome: .notApplied),
                LoggedClockSet(id: third, event: event(200), outcome: .acknowledged),
            ])
            #expect(try await store.clockSets(ringID: ring.id) == logged.map(\.event))
            #expect(try blocking(store).loggedClockSets(ringID: ring.id) == logged)
            #expect(try await store.loggedClockSets(ringID: UUID()).isEmpty)
        }
    }

    @Test func anOutcomeIsAppendOnly() async throws {
        try await onEachStoreAsync { store in
            let ring = try await store.registerRing(protocolFamily: "ycbt")
            let id = try await store.recordClockSet(
                ClockSetEvent(ringID: ring.id, instant: t0, timeZoneIdentifier: "UTC", utcOffsetSeconds: 0))
            try await store.recordClockSetOutcome(id: id, outcome: .unknown)
            // The same outcome again is a harmless retry.
            try await store.recordClockSetOutcome(id: id, outcome: .unknown)
            await #expect(throws: RingStoreError.clockSetOutcomeConflict(id)) {
                try await store.recordClockSetOutcome(id: id, outcome: .notApplied)
            }
            #expect(try await store.loggedClockSets(ringID: ring.id).map(\.outcome) == [.unknown])
            await #expect(throws: RingStoreError.unknownClockSet(id + 1)) {
                try await store.recordClockSetOutcome(id: id + 1, outcome: .acknowledged)
            }
            let rows = try await store.writer.read { db in try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM clock_set_outcome") }
            #expect(rows == 1)
        }
    }

    // MARK: Time resolution

    /// The acceptance case: the phantom alone would make the reading `resolved` an hour wrong.
    @Test func aNotAppliedPhantomWithAnotherOffsetNoLongerChangesResolution() throws {
        let ring = UUID()
        let observations = [
            observation(ring, data: [], at: at(-10), id: 1), observation(ring, data: [], at: at(7200), id: 2),
            observation(ring, data: reading, at: at(14_400), id: 3),
        ]
        func clock(_ id: Int64, _ seconds: Double, _ offset: Int, _ outcome: LoggedClockSetOutcome?) -> LoggedClockSet {
            LoggedClockSet(
                id: id,
                event: ClockSetEvent(ringID: ring, instant: at(seconds), timeZoneIdentifier: "Europe/London", utcOffsetSeconds: offset),
                outcome: outcome)
        }
        func resolve(phantom: LoggedClockSetOutcome?) throws -> ResolvedOffset {
            let log = [clock(1, -200_000, plus1, .acknowledged), clock(2, 0, plus1, .acknowledged), clock(3, 7210, 0, phantom)]
            let r = TimeResolver(ringID: ring, kind: .heartRate, observations: observations, loggedClockSets: log)
            let w = try r.window(sightings: r.sightings(in: reading), offset: 0, length: 6)
            return r.resolve(window: w, reference: at(13_600))
        }
        // As before #72: no outcome, or `unknown`, keeps the phantom, and it wins.
        #expect(try resolve(phantom: nil) == ResolvedOffset(seconds: 0, source: .resolved))
        #expect(try resolve(phantom: .unknown) == ResolvedOffset(seconds: 0, source: .resolved))
        #expect(try resolve(phantom: .acknowledged) == ResolvedOffset(seconds: 0, source: .resolved))
        // Ignored, the ring's real clock is the only candidate.
        #expect(try resolve(phantom: .notApplied) == ResolvedOffset(seconds: plus1, source: .resolved))
        #expect(try resolve(phantom: .notSent) == ResolvedOffset(seconds: plus1, source: .resolved))
    }

    /// The same phantom against a sleep session and an activity bucket: `phantom` is the phantom's
    /// recorded outcome, the record's bytes are first seen at `firstSeen` after an unchanged fetch at
    /// 7200, and `reference` and `start` are ring-local (stamped under the ring's real +1 h).
    private func resolveWithPhantom(
        _ phantom: LoggedClockSetOutcome?, kind: HistoryKind, reference: Date, start: Date? = nil, firstSeen: Double
    ) throws -> ResolvedOffset {
        let ring = UUID()
        let record = [UInt8](repeating: 1, count: 6)
        let observations = [
            observation(ring, kind, data: [], at: at(-10), id: 1), observation(ring, kind, data: [], at: at(7200), id: 2),
            observation(ring, kind, data: record, at: at(firstSeen), id: 3),
        ]
        let log = [(1, -200_000.0, plus1, LoggedClockSetOutcome?.some(.acknowledged)), (2, 0, plus1, .acknowledged), (3, 7210, 0, phantom)]
            .map { id, seconds, offset, outcome in
                LoggedClockSet(
                    id: Int64(id),
                    event: ClockSetEvent(ringID: ring, instant: at(seconds), timeZoneIdentifier: "Europe/London", utcOffsetSeconds: offset),
                    outcome: outcome)
            }
        let r = TimeResolver(ringID: ring, kind: kind, observations: observations, loggedClockSets: log)
        let w = try r.window(sightings: r.sightings(in: record), offset: 0, length: record.count)
        return r.resolve(window: w, reference: reference, start: start)
    }

    /// A session from real 8000 to 10000 under +1 h, after the phantom: the phantom alone makes it
    /// `resolved` an hour wrong.
    @Test func aNotAppliedPhantomNoLongerChangesASleepSession() throws {
        func resolve(_ phantom: LoggedClockSetOutcome?) throws -> ResolvedOffset {
            try resolveWithPhantom(phantom, kind: .sleep, reference: at(10_000 + 3600), start: at(8000 + 3600), firstSeen: 14_400)
        }
        #expect(try resolve(nil) == ResolvedOffset(seconds: 0, source: .resolved))
        #expect(try resolve(.unknown) == ResolvedOffset(seconds: 0, source: .resolved))
        #expect(try resolve(.notApplied) == ResolvedOffset(seconds: plus1, source: .resolved))
        #expect(try resolve(.notSent) == ResolvedOffset(seconds: plus1, source: .resolved))
    }

    /// A session from real 6000 to 9000 under +1 h spans the phantom's instant, so with the phantom
    /// it is `ambiguous` by the offset-change rule; ignored, there is no offset change and it is
    /// `resolved` to the ring's real clock.
    @Test func aNotAppliedPhantomInsideASleepSessionNoLongerMakesItAmbiguous() throws {
        func resolve(_ phantom: LoggedClockSetOutcome?) throws -> ResolvedOffset {
            try resolveWithPhantom(phantom, kind: .sleep, reference: at(9000 + 3600), start: at(6000 + 3600), firstSeen: 14_400)
        }
        #expect(try resolve(nil) == ResolvedOffset(seconds: 0, source: .ambiguous))
        #expect(try resolve(.unknown) == ResolvedOffset(seconds: 0, source: .ambiguous))
        #expect(try resolve(.notApplied) == ResolvedOffset(seconds: plus1, source: .resolved))
    }

    /// An activity bucket ending at real 10800 under +1 h, first seen at 16000.
    @Test func aNotAppliedPhantomNoLongerChangesAnActivityBucket() throws {
        func resolve(_ phantom: LoggedClockSetOutcome?) throws -> ResolvedOffset {
            try resolveWithPhantom(phantom, kind: .activity, reference: at(10_800 + 3600), firstSeen: 16_000)
        }
        #expect(try resolve(nil) == ResolvedOffset(seconds: 0, source: .resolved))
        #expect(try resolve(.unknown) == ResolvedOffset(seconds: 0, source: .resolved))
        #expect(try resolve(.notApplied) == ResolvedOffset(seconds: plus1, source: .resolved))
        #expect(try resolve(.notSent) == ResolvedOffset(seconds: plus1, source: .resolved))
    }

    /// An entry with no outcome or `unknown` resolves exactly as the plain event list does.
    @Test func entriesWithNoOutcomeOrUnknownResolveAsThePlainLog() throws {
        let ring = UUID()
        let observations = [
            observation(ring, data: [], at: at(-10), id: 1), observation(ring, data: [], at: at(7200), id: 2),
            observation(ring, data: reading, at: at(14_400), id: 3),
        ]
        let events = [(-200_000.0, plus1), (0, plus1), (7210, 0)].map {
            ClockSetEvent(ringID: ring, instant: at($0.0), timeZoneIdentifier: "Europe/London", utcOffsetSeconds: $0.1)
        }
        for outcome in [nil, LoggedClockSetOutcome.unknown, .acknowledged] {
            let logged = events.enumerated().map { LoggedClockSet(id: Int64($0.offset + 1), event: $0.element, outcome: outcome) }
            let plain = TimeResolver(ringID: ring, kind: .heartRate, observations: observations, clockSets: events)
            let withOutcomes = TimeResolver(ringID: ring, kind: .heartRate, observations: observations, loggedClockSets: logged)
            #expect(withOutcomes.clockSets == plain.clockSets)
        }
    }

    // MARK: Through ingestion

    /// Typed rows change only through re-derivation: recording the outcome leaves them alone, and
    /// the next ingest corrects the reading.
    @Test func recordingAnOutcomeChangesRowsOnlyAtTheNextIngest() async throws {
        try await onEachStoreAsync { store in
            let (f, phantom) = try phantomFixture(store)
            try await store.ingest(ringID: f.ring.id, kind: .heartRate)
            let before = try await store.heartRateReadings(ringID: f.ring.id)
            #expect(before.map(\.value.time) == [at(13_600)])
            #expect(before.map(\.value.utcOffsetSeconds) == [0])

            try await store.recordClockSetOutcome(id: phantom, outcome: .notApplied)
            #expect(try await store.heartRateReadings(ringID: f.ring.id) == before)

            let result = try await store.ingest(ringID: f.ring.id, kind: .heartRate)
            // The instant is the row's key, so the corrected reading replaces the wrong one.
            #expect(result.inserted == 1 && result.deleted == 1 && result.updated == 0, "\(result.canonical)")
            let after = try await store.heartRateReadings(ringID: f.ring.id)
            #expect(after.map(\.value.time) == [at(10_000)])
            #expect(after.map(\.value.utcOffsetSeconds) == [plus1])
            #expect(after.map(\.value.timeSource) == [.resolved])
            // The log keeps the phantom.
            #expect(try await store.clockSets(ringID: f.ring.id).count == 3)
        }
    }

    /// An outcome recorded between a derivation's read and its apply step makes it stale, like a
    /// new clock-set: the derivation runs again and the phantom is ignored.
    @Test func anOutcomeRecordedMeanwhileIsUsedByARederivation() async throws {
        try await onEachStoreAsync { store in
            let (f, phantom) = try phantomFixture(store)
            try await store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(try await store.heartRateReadings(ringID: f.ring.id).map(\.value.time) == [at(13_600)])
            let attempts = Locked<[Int]>([])
            let hooks = RingStore.IngestionHooks(beforeApply: { attempt in
                attempts.update { $0.append(attempt) }
                if attempt == 1 { try blocking(store).recordClockSetOutcome(id: phantom, outcome: .notApplied) }
            })
            let raced = try await store.ingest(ringID: f.ring.id, kind: .heartRate, hooks: hooks)
            #expect(attempts.value == [1, 2])
            #expect(raced.inserted == 1 && raced.deleted == 1, "\(raced.canonical)")
            let rows = try await store.heartRateReadings(ringID: f.ring.id)
            #expect(rows.map(\.value.time) == [at(10_000)])
            #expect(rows.map(\.value.timeSource) == [.resolved])
            let again = try await store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(again.inserted == 0 && again.updated == 0 && again.deleted == 0 && again.unchanged == 1)
        }
    }

    /// Recording an outcome the clock-set already has writes nothing, so it is not staleness.
    @Test func theSameOutcomeRecordedAgainMeanwhileIsNotStale() async throws {
        try await onEachStoreAsync { store in
            let (f, phantom) = try phantomFixture(store)
            try await store.recordClockSetOutcome(id: phantom, outcome: .notApplied)
            let attempts = Locked<[Int]>([])
            let hooks = RingStore.IngestionHooks(beforeApply: { attempt in
                attempts.update { $0.append(attempt) }
                try blocking(store).recordClockSetOutcome(id: phantom, outcome: .notApplied)
            })
            _ = try await store.ingest(ringID: f.ring.id, kind: .heartRate, hooks: hooks)
            #expect(attempts.value == [1])
            #expect(try await store.heartRateReadings(ringID: f.ring.id).map(\.value.time) == [at(10_000)])
        }
    }

    // MARK: Migration

    /// A database as `v5-ring-peripheral` left it, with clock-sets and typed rows: the migration
    /// adds the outcome table and keeps everything; old entries read as having no outcome.
    @Test func migratingFromV5KeepsTheLogWithNoOutcomes() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let url = dir.appendingPathComponent("ring.sqlite")
        let id = UUID()
        try makeV5Database(at: url, id: id)
        let store = try RingStore(url: url, observesSuspension: false)
        let logged = try await store.loggedClockSets(ringID: id)
        #expect(logged.map(\.event.utcOffsetSeconds) == [3600, 0])
        #expect(logged.allSatisfy { $0.outcome == nil })
        #expect(try await store.heartRateReadings(ringID: id).map(\.value.bpm) == [61])
        try await store.recordClockSetOutcome(id: logged[1].id, outcome: .notApplied)
        #expect(try await store.loggedClockSets(ringID: id).map(\.outcome) == [nil, .notApplied])
        let next = try await store.recordClockSet(
            ClockSetEvent(ringID: id, instant: at(0), timeZoneIdentifier: "UTC", utcOffsetSeconds: 0))
        #expect(next > (try #require(logged.map(\.id).max())))
    }
}

/// A database exactly as `v5-ring-peripheral` left it, with one ring, two clock-sets and a typed row.
/// Synchronous on purpose (GRDB's `write` is async-overloaded).
private func makeV5Database(at url: URL, id: UUID) throws {
    let queue = try DatabaseQueue(path: url.path)
    try RingStore.migrator.migrate(queue, upTo: "v5-ring-peripheral")
    try queue.write { db in
        #expect(try !db.tableExists("clock_set_outcome"))
        try db.execute(
            sql: "INSERT INTO ring (id, protocol_family, model, firmware, first_seen_at) VALUES (?, 'ycbt', 'M', '1', 1000)",
            arguments: [id.uuidString])
        for (instant, offset) in [(1000, 3600), (2000, 0)] {
            try db.execute(
                sql: "INSERT INTO clock_set (ring_id, instant, time_zone_identifier, utc_offset_seconds) VALUES (?, ?, 'UTC', ?)",
                arguments: [id.uuidString, instant, offset])
        }
        try db.execute(
            sql: "INSERT INTO heart_rate_reading (ring_id, time, bpm, mode, utc_offset_seconds, ring_local_seconds, time_source) VALUES (?, 5000, 61, 0, 0, 0, 'resolved')",
            arguments: [id.uuidString])
    }
}
