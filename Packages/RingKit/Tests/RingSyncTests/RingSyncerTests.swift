import Foundation
import Testing
import RingProtocol
import RingStore
import RingSync
import RingTransport

// The sync against a scripted ring (`FakeRing`) and an in-memory store. All ring data is synthetic.

private func syncer(
    _ ring: FakeRing, _ store: RingStore, _ ringID: UUID,
    interval: MonitoringInterval? = .tenMinutes,
    budget: Duration = .seconds(60),
    clock: Date = t0.addingTimeInterval(60),
    zone: TimeZone = london,
    monotonic: FakeMonotonic = FakeMonotonic()
) -> RingSyncer {
    RingSyncer(
        ring: ring, store: store, ringID: ringID, monitoringInterval: interval, budget: budget,
        clock: { clock }, timeZone: { zone }, monotonicNow: { monotonic.now() })
}

private func isIngested(_ outcome: IngestionOutcome?) -> Bool {
    if case .ingested = outcome { true } else { false }
}

private func isStored(_ outcome: FetchOutcome?) -> Bool {
    if case .stored = outcome { true } else { false }
}

private func allIngested(_ result: SyncResult) -> Bool {
    HistoryKind.allCases.allSatisfy { isIngested(result.ingestions[$0]) }
}

// MARK: Complete sync

@Test(.timeLimit(.minutes(1))) func completeSyncFetchesInOrderThenClockThenIntervalThenIngests() async throws {
    let (store, id) = try await makeStore()
    let ring = FakeRing(store: store, ringID: id)
    let result = await syncer(ring, store, id).run()

    // Everything sent to the ring, as one sequence.
    #expect(await ring.log == completeLog)
    // Each transfer was in the store before the fake acknowledged it.
    #expect(await ring.storedWhenAcknowledged == RingSyncer.fetchOrder)
    // Ingestion had not run while the ring steps did; it ran afterwards.
    #expect(await ring.typedRowsDuringRingSteps == [0, 0, 0, 0])
    #expect(try await typedRowCounts(store, id) == [2, 1, 2, 1])

    for kind in HistoryKind.allCases {
        guard case .stored(.inserted, let bytes) = result.fetches[kind] else {
            Issue.record("\(kind.name) was not stored")
            continue
        }
        #expect(bytes == syntheticHistory[kind]?.count)
        guard case .ingested(let ingested) = result.ingestions[kind] else {
            Issue.record("\(kind.name) was not ingested")
            continue
        }
        #expect(ingested.inserted > 0)
    }
    guard case .acknowledgedAndLogged = result.clockSet else {
        Issue.record("clock-set not acknowledged and logged")
        return
    }
    guard case .accepted(.tenMinutes) = result.monitoringInterval else {
        Issue.record("interval not accepted")
        return
    }
    #expect(try await store.clockSets(ringID: id).count == 2)
    #expect(result.isComplete)
    #expect(!result.clockSetLogIncomplete)
}

@Test(.timeLimit(.minutes(1))) func fetchOrderCoversEveryKindOnce() {
    #expect(Set(RingSyncer.fetchOrder) == Set(HistoryKind.allCases))
    #expect(RingSyncer.fetchOrder.count == HistoryKind.allCases.count)
    #expect(RingSyncer.fetchOrder == [.activity, .sleep, .heartRate, .combined])
}

@Test(.timeLimit(.minutes(1))) func secondSyncOfUnchangedDataChangesNoTypedRows() async throws {
    let (store, id) = try await makeStore()
    _ = await syncer(FakeRing(store: store, ringID: id), store, id).run()
    let before = (
        try await store.activityIntervals(ringID: id), try await store.sleepSessions(ringID: id),
        try await store.heartRateReadings(ringID: id), try await store.vitalsReadings(ringID: id))

    // An hour later the ring returns the same bytes.
    let again = FakeRing(store: store, ringID: id, fetchedAt: t0.addingTimeInterval(3600))
    let result = await syncer(again, store, id, clock: t0.addingTimeInterval(3660)).run()

    #expect(result.isComplete)
    for kind in HistoryKind.allCases {
        guard case .stored(.duplicate, _) = result.fetches[kind] else {
            Issue.record("\(kind.name) was not a duplicate")
            continue
        }
        guard case .ingested(let ingested) = result.ingestions[kind] else {
            Issue.record("\(kind.name) was not ingested")
            continue
        }
        #expect(ingested.inserted == 0 && ingested.updated == 0 && ingested.deleted == 0)
        #expect(ingested.unchanged > 0)
    }
    #expect(try await store.activityIntervals(ringID: id) == before.0)
    #expect(try await store.sleepSessions(ringID: id) == before.1)
    #expect(try await store.heartRateReadings(ringID: id) == before.2)
    #expect(try await store.vitalsReadings(ringID: id) == before.3)
}

// MARK: Fetch failures

@Test(.timeLimit(.minutes(1)), arguments: HistoryKind.allCases)
func failedFetchSkipsClockAndIntervalButNotOtherKindsOrIngestion(_ failing: HistoryKind) async throws {
    let (store, id) = try await makeStore()
    var fetches = syntheticHistory.mapValues { FakeRing.Fetch.data($0) }
    fetches[failing] = .fail(RingError.timeout("synthetic"))
    let ring = FakeRing(store: store, ringID: id, fetches: fetches)
    let result = await syncer(ring, store, id).run()

    let log = await ring.log
    // Every kind was asked for, in order; the clock and interval were never sent.
    #expect(log.filter { $0.hasPrefix("fetch ") } == RingSyncer.fetchOrder.map { "fetch \($0.name)" })
    #expect(!log.contains("setTime"))
    #expect(!log.contains { $0.hasPrefix("interval") })
    for kind in HistoryKind.allCases where kind != failing {
        #expect(isStored(result.fetches[kind]), "\(kind.name)")
    }
    guard case .failed(.transfer, let error) = result.fetches[failing], case RingError.timeout = error else {
        Issue.record("the failing kind is not reported as failed with its error")
        return
    }
    guard case .skipped(.historyIncomplete) = result.clockSet else {
        Issue.record("clock-set not skipped")
        return
    }
    guard case .skipped(.tenMinutes, .clockNotAcknowledged) = result.monitoringInterval else {
        Issue.record("interval not skipped")
        return
    }
    // Only the clock-set seeded before the sync.
    #expect(try await store.clockSets(ringID: id).count == 1)
    #expect(allIngested(result))
    #expect(!result.isComplete)
}

@Test(.timeLimit(.minutes(1))) func storeFailingInsidePersistFailsThatKindAndIsNotAcknowledged() async throws {
    let (store, id) = try await makeStore()
    try failInserts(into: "transfer_observation", when: "NEW.kind = 'sleep'", store)
    let ring = FakeRing(store: store, ringID: id)
    let result = await syncer(ring, store, id).run()

    let log = await ring.log
    #expect(log.contains("not acknowledged sleep"))
    #expect(!log.contains("ack sleep"))
    #expect(!log.contains("setTime"))
    guard case .failed(.store, _) = result.fetches[.sleep] else {
        Issue.record("sleep not reported as a store failure")
        return
    }
    for kind in HistoryKind.allCases where kind != .sleep {
        #expect(isStored(result.fetches[kind]), "\(kind.name)")
    }
    #expect(!result.isComplete)
}

@Test(.timeLimit(.minutes(1))) func failedAcknowledgementAfterStoringIsReportedAsSuch() async throws {
    let (store, id) = try await makeStore()
    var fetches = syntheticHistory.mapValues { FakeRing.Fetch.data($0) }
    fetches[.activity] = .ackFails(RingError.timeout("synthetic"))
    let result = await syncer(FakeRing(store: store, ringID: id, fetches: fetches), store, id).run()
    guard case .failed(.acknowledgement, _) = result.fetches[.activity] else {
        Issue.record("activity not reported as an acknowledgement failure")
        return
    }
    guard case .skipped(.historyIncomplete) = result.clockSet else {
        Issue.record("clock-set not skipped")
        return
    }
}

@Test(.timeLimit(.minutes(1)), arguments: [RingError.stopped, RingError.notConnected])
func sessionEndingDuringAFetchLeavesTheRestNotAttempted(_ error: RingError) async throws {
    let (store, id) = try await makeStore()
    var fetches = syntheticHistory.mapValues { FakeRing.Fetch.data($0) }
    fetches[.sleep] = .fail(error)
    let ring = FakeRing(store: store, ringID: id, fetches: fetches)
    let result = await syncer(ring, store, id).run()

    #expect(await ring.log == ["fetch activity", "persisted activity", "ack activity", "fetch sleep"])
    #expect(isStored(result.fetches[.activity]))
    guard case .failed(.transfer, _) = result.fetches[.sleep] else {
        Issue.record("sleep not failed")
        return
    }
    for kind in [HistoryKind.heartRate, .combined] {
        guard case .notAttempted(.sessionUnavailable) = result.fetches[kind] else {
            Issue.record("\(kind.name) was attempted")
            continue
        }
    }
    guard case .skipped(.historyIncomplete) = result.clockSet else {
        Issue.record("clock-set not skipped")
        return
    }
    #expect(allIngested(result))
}

@Test(.timeLimit(.minutes(1))) func emptyKindIsLoggedAndCountsAsFetched() async throws {
    let (store, id) = try await makeStore()
    var fetches = syntheticHistory.mapValues { FakeRing.Fetch.data($0) }
    fetches[.heartRate] = .empty
    let ring = FakeRing(store: store, ringID: id, fetches: fetches)
    let result = await syncer(ring, store, id).run()

    guard case .empty = result.fetches[.heartRate] else {
        Issue.record("heart rate not reported empty")
        return
    }
    let observations = try await store.observations(ringID: id, kind: .heartRate).observations
    #expect(observations.count == 1)
    #expect(observations.first?.dataLength == 0)
    #expect(observations.first?.fetchedAt == t0)
    // An empty transfer is never acknowledged.
    #expect(!(await ring.log).contains("ack heart"))
    #expect(result.clockSet.wasAcknowledged)
    #expect(result.isComplete)
}

@Test(.timeLimit(.minutes(1))) func emptyKindThatCannotBeLoggedIsAFailure() async throws {
    let (store, id) = try await makeStore()
    try failInserts(into: "transfer_observation", when: "NEW.kind = 'heartRate'", store)
    var fetches = syntheticHistory.mapValues { FakeRing.Fetch.data($0) }
    fetches[.heartRate] = .empty
    let result = await syncer(FakeRing(store: store, ringID: id, fetches: fetches), store, id).run()
    guard case .failed(.store, _) = result.fetches[.heartRate] else {
        Issue.record("heart rate not reported as a store failure")
        return
    }
    guard case .skipped(.historyIncomplete) = result.clockSet else {
        Issue.record("clock-set not skipped")
        return
    }
}

// MARK: Clock

@Test(.timeLimit(.minutes(1))) func clockTimeoutIsLoggedAsUnknownAndSkipsTheInterval() async throws {
    let (store, id) = try await makeStore()
    let ring = FakeRing(store: store, ringID: id, clock: .fail(RingError.timeout("synthetic")))
    let result = await syncer(ring, store, id).run()

    guard case .unknownAndLogged(let event, let error) = result.clockSet, case RingError.timeout = error else {
        Issue.record("clock-set not unknown and logged")
        return
    }
    #expect(try await store.clockSets(ringID: id).last == event)
    #expect(try await store.clockSets(ringID: id).count == 2)
    guard case .skipped(.tenMinutes, .clockNotAcknowledged) = result.monitoringInterval else {
        Issue.record("interval not skipped")
        return
    }
    #expect(!(await ring.log).contains { $0.hasPrefix("interval") })
    #expect(allIngested(result))
    #expect(!result.isComplete)
}

@Test(.timeLimit(.minutes(1)), arguments: [
    RingError.rejected(Frame(group: 0x01, key: 0x00, payload: [0xFC])),
    RingError.stopped,
    RingError.refusedDestructive(Frame(group: 0x05, key: 0x40)),
])
func clockDefinitelyNotAppliedIsNotLoggedAndSkipsTheInterval(_ error: RingError) async throws {
    let (store, id) = try await makeStore()
    let ring = FakeRing(store: store, ringID: id, clock: .fail(error))
    let result = await syncer(ring, store, id).run()

    guard case .notApplied = result.clockSet else {
        Issue.record("clock-set not reported as not applied")
        return
    }
    #expect(try await store.clockSets(ringID: id).count == 1)
    guard case .skipped(.tenMinutes, .clockNotAcknowledged) = result.monitoringInterval else {
        Issue.record("interval not skipped")
        return
    }
    #expect(!(await ring.log).contains { $0.hasPrefix("interval") })
    #expect(!result.isComplete)
}

@Test(.timeLimit(.minutes(1))) func acknowledgedClockThatCannotBeLoggedIsReportedAndNotRetried() async throws {
    let (store, id) = try await makeStore()
    try failInserts(into: "clock_set", store)
    let ring = FakeRing(store: store, ringID: id)
    let result = await syncer(ring, store, id).run()

    guard case .acknowledgedButNotLogged = result.clockSet else {
        Issue.record("clock-set not reported as acknowledged but not logged")
        return
    }
    #expect(result.clockSetLogIncomplete)
    #expect(!result.isComplete)
    // Sent once, and the interval still follows an acknowledged clock-set.
    #expect(await ring.log.filter { $0 == "setTime" }.count == 1)
    guard case .accepted = result.monitoringInterval else {
        Issue.record("interval not sent after an acknowledged clock-set")
        return
    }
}

@Test(.timeLimit(.minutes(1))) func unknownClockThatCannotBeLoggedIsReported() async throws {
    let (store, id) = try await makeStore()
    try failInserts(into: "clock_set", store)
    let ring = FakeRing(store: store, ringID: id, clock: .fail(RingError.timeout("synthetic")))
    let result = await syncer(ring, store, id).run()
    guard case .unknownAndNotLogged = result.clockSet else {
        Issue.record("clock-set not reported as unknown and not logged")
        return
    }
    #expect(result.clockSetLogIncomplete)
}

/// Dates finer than a millisecond, in zones with non-zero offsets, inside and outside daylight saving.
@Test(.timeLimit(.minutes(1)), arguments: [
    // 2027-07-01 12:00:00.123456 UTC: New York on daylight saving time (UTC-4).
    (1_814_529_600.123456, "America/New_York", -14_400),
    // The same instant in Kolkata (UTC+5:30, no daylight saving).
    (1_814_529_600.123456, "Asia/Kolkata", 19_800),
    // 2027-01-15 08:00:00.999 UTC: Auckland on daylight saving time (UTC+13).
    (1_800_000_000.9999, "Pacific/Auckland", 46_800),
])
func loggedClockSetIsExactlyWhatWasSent(_ seconds: Double, _ zoneID: String, _ offset: Int) async throws {
    let (store, id) = try await makeStore()
    let zone = try #require(TimeZone(identifier: zoneID))
    let date = Date(timeIntervalSince1970: seconds)
    let ring = FakeRing(store: store, ringID: id)
    let result = await syncer(ring, store, id, clock: date, zone: zone).run()

    let sent = try #require(await ring.clockSent)
    #expect(sent.date == date)
    #expect(sent.zone.identifier == zoneID)
    guard case .acknowledgedAndLogged(let event) = result.clockSet else {
        Issue.record("clock-set not acknowledged and logged")
        return
    }
    let expected = ClockSetEvent(
        ringID: id, instant: HistoryTransfer.truncatedToMilliseconds(date), timeZoneIdentifier: zoneID,
        utcOffsetSeconds: offset)
    #expect(event == expected)
    #expect(try await store.clockSets(ringID: id).last == expected)
    #expect(HistoryTransfer.millisecondsSince1970(event.instant) == Int64((seconds * 1000).rounded(.down)))
}

// MARK: Interval

@Test(.timeLimit(.minutes(1))) func intervalRefusedByTheRing() async throws {
    let (store, id) = try await makeStore()
    let ring = FakeRing(store: store, ringID: id, interval: .fail(MonitoringSettingError.unsupported))
    let result = await syncer(ring, store, id).run()
    guard case .refused(.tenMinutes, .unsupported) = result.monitoringInterval else {
        Issue.record("interval not refused")
        return
    }
    #expect(!result.isComplete)
    #expect(allIngested(result))
}

@Test(.timeLimit(.minutes(1))) func intervalFailingInTransportIsUnknown() async throws {
    let (store, id) = try await makeStore()
    let ring = FakeRing(store: store, ringID: id, interval: .fail(RingError.timeout("synthetic")))
    let result = await syncer(ring, store, id).run()
    guard case .unknown(.tenMinutes, let error) = result.monitoringInterval, case RingError.timeout = error else {
        Issue.record("interval not unknown")
        return
    }
    #expect(!result.isComplete)
}

@Test(.timeLimit(.minutes(1))) func noIntervalRequestedSendsNone() async throws {
    let (store, id) = try await makeStore()
    let ring = FakeRing(store: store, ringID: id)
    let result = await syncer(ring, store, id, interval: nil).run()
    #expect(await ring.log == Array(completeLog.dropLast()))
    guard case .notRequested = result.monitoringInterval else {
        Issue.record("interval not reported as not requested")
        return
    }
    #expect(result.isComplete)
}

// MARK: Budget

@Test(.timeLimit(.minutes(1))) func budgetRunningOutBeforeAFetch() async throws {
    let (store, id) = try await makeStore()
    let clock = FakeMonotonic()
    // The sleep fetch takes the whole budget.
    let ring = FakeRing(store: store, ringID: id, onStep: { if $0 == "fetch sleep" { clock.advance(by: .seconds(11)) } })
    let result = await syncer(ring, store, id, budget: .seconds(10), monotonic: clock).run()

    #expect(await ring.log == Array(completeLog.prefix(6)))
    #expect(isStored(result.fetches[.sleep]))
    for kind in [HistoryKind.heartRate, .combined] {
        guard case .notAttempted(.budgetExhausted) = result.fetches[kind] else {
            Issue.record("\(kind.name) was attempted")
            continue
        }
    }
    guard case .skipped(.historyIncomplete) = result.clockSet else {
        Issue.record("clock-set not skipped")
        return
    }
    guard case .skipped(.tenMinutes, .clockNotAcknowledged) = result.monitoringInterval else {
        Issue.record("interval not skipped")
        return
    }
    #expect(allIngested(result))
    #expect(try await typedRowCounts(store, id) == [2, 1, 0, 0])
}

@Test(.timeLimit(.minutes(1))) func budgetRunningOutBeforeTheClock() async throws {
    let (store, id) = try await makeStore()
    let clock = FakeMonotonic()
    let ring = FakeRing(store: store, ringID: id, onStep: { if $0 == "fetch combined" { clock.advance(by: .seconds(11)) } })
    let result = await syncer(ring, store, id, budget: .seconds(10), monotonic: clock).run()

    #expect(await ring.log == Array(completeLog.prefix(12)))
    guard case .skipped(.budgetExhausted) = result.clockSet else {
        Issue.record("clock-set not skipped for the budget")
        return
    }
    guard case .skipped(.tenMinutes, .budgetExhausted) = result.monitoringInterval else {
        Issue.record("interval not skipped for the budget")
        return
    }
    #expect(try await store.clockSets(ringID: id).count == 1)
    #expect(allIngested(result))
    #expect(!result.isComplete)
}

@Test(.timeLimit(.minutes(1))) func budgetRunningOutBeforeTheInterval() async throws {
    let (store, id) = try await makeStore()
    let clock = FakeMonotonic()
    let ring = FakeRing(store: store, ringID: id, onStep: { if $0 == "setTime" { clock.advance(by: .seconds(11)) } })
    let result = await syncer(ring, store, id, budget: .seconds(10), monotonic: clock).run()

    #expect(await ring.log == Array(completeLog.dropLast()))
    #expect(result.clockSet.wasAcknowledged)
    guard case .skipped(.tenMinutes, .budgetExhausted) = result.monitoringInterval else {
        Issue.record("interval not skipped for the budget")
        return
    }
    #expect(allIngested(result))
    #expect(!result.isComplete)
}

// MARK: Ingestion

@Test(.timeLimit(.minutes(1))) func ingestionErrorForOneKindDoesNotStopTheOthers() async throws {
    let (store, id) = try await makeStore()
    try failInserts(into: "sleep_session", store)
    let result = await syncer(FakeRing(store: store, ringID: id), store, id).run()

    guard case .failed = result.ingestions[.sleep] else {
        Issue.record("sleep ingestion did not fail")
        return
    }
    for kind in HistoryKind.allCases where kind != .sleep {
        #expect(isIngested(result.ingestions[kind]), "\(kind.name)")
    }
    #expect(try await typedRowCounts(store, id) == [2, 0, 2, 1])
    // The ring steps all completed; only ingestion failed.
    #expect(result.clockSet.wasAcknowledged)
    #expect(!result.isComplete)
}
