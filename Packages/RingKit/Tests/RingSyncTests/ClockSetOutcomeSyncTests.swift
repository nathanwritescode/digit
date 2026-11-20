import Foundation
import Testing
import RingProtocol
@testable import RingStore
@testable import RingSync
import RingTransport

// The sync records each clock-set's outcome against its log entry (#72). All ring data is
// synthetic; suspensions are simulated by `SuspendingStore`, never announced.

private func outcomeSyncer(
    _ ring: FakeRing, _ store: SuspendingStore, _ ringID: UUID,
    monotonic: FakeMonotonic = FakeMonotonic(), maximumLogDelay: Duration = RingSyncer.maximumClockLogDelay
) -> RingSyncer {
    RingSyncer(
        ring: ring, syncStore: store, ringID: ringID, monitoringInterval: .tenMinutes, budget: .seconds(60),
        clock: { t0.addingTimeInterval(60) }, timeZone: { london }, monotonicNow: { monotonic.now() },
        maximumLogDelay: maximumLogDelay)
}

private struct OutcomeWriteFailure: Error {}

/// The outcomes in the store's log, oldest entry first (the first is `makeStore`'s, which has none).
private func storedOutcomes(_ store: RingStore, _ id: UUID) async throws -> [LoggedClockSetOutcome?] {
    try await store.loggedClockSets(ringID: id).map(\.outcome)
}

@Test(.timeLimit(.minutes(1)), arguments: [
    (FakeRing.Reply.ack, LoggedClockSetOutcome.acknowledged),
    (.fail(RingError.rejected(Frame(group: 0x01, key: 0x00, payload: [0xFC]))), .notApplied),
    (.fail(RingError.busy("synthetic")), .notApplied),
    (.fail(RingError.timeout("synthetic")), .unknown),
    (.fail(RingError.notConnected), .unknown),
])
func eachSentClockSetHasItsOutcomeRecordedBeforeIngestion(_ reply: FakeRing.Reply, _ expected: LoggedClockSetOutcome) async throws {
    let (base, id) = try await makeStore()
    let store = SuspendingStore(base)
    let ring = FakeRing(store: base, ringID: id, clock: reply)
    let result = await outcomeSyncer(ring, store, id).run()

    #expect(result.clockSet.loggedOutcome == expected)
    #expect(result.clockSetOutcomeRecord == .recorded(expected))
    #expect(try await storedOutcomes(base, id) == [nil, expected])
    // Recorded in the ring phase, so this sync's own ingestion already sees it.
    let log = store.log
    let recorded = try #require(log.firstIndex(of: "clockSetOutcome \(expected)"))
    let firstIngest = try #require(log.firstIndex { $0.hasPrefix("ingest") })
    #expect(log.firstIndex(of: "clockSet")! < recorded && recorded < firstIngest)
    #expect(result.ingestions.values.allSatisfy { $0.succeeded })
}

@Test(.timeLimit(.minutes(1))) func aClockSetWithheldBecauseLoggingWasSlowIsRecordedAsNotSent() async throws {
    let (base, id) = try await makeStore()
    let monotonic = FakeMonotonic()
    let store = SuspendingStore(base, onClockSet: { monotonic.advance(by: .milliseconds(300)) })
    let ring = FakeRing(store: base, ringID: id)
    let result = await outcomeSyncer(ring, store, id, monotonic: monotonic, maximumLogDelay: .milliseconds(200)).run()

    guard case .notSentLogTooSlow = result.clockSet else {
        Issue.record("clock-set not reported as too slow to send")
        return
    }
    #expect(result.clockSetOutcomeRecord == .recorded(.notSent))
    #expect(try await storedOutcomes(base, id) == [nil, .notSent])
    #expect(!(await ring.log).contains("setTime"))
}

/// No log entry, nothing to record against.
@Test(.timeLimit(.minutes(1))) func aClockSetThatWasNotLoggedOrWasSkippedRecordsNoOutcome() async throws {
    let (base, id) = try await makeStore()
    try failInserts(into: "clock_set", base)
    let notLogged = await outcomeSyncer(FakeRing(store: base, ringID: id), SuspendingStore(base), id).run()
    guard case .notLogged = notLogged.clockSet else {
        Issue.record("clock-set not reported as not logged")
        return
    }
    #expect(notLogged.clockSetOutcomeRecord == .noLogEntry)

    let (other, otherID) = try await makeStore()
    var fetches = syntheticHistory.mapValues { FakeRing.Fetch.data($0) }
    fetches[.sleep] = .fail(RingError.timeout("synthetic"))
    let store = SuspendingStore(other)
    let skipped = await outcomeSyncer(FakeRing(store: other, ringID: otherID, fetches: fetches), store, otherID).run()
    #expect(skipped.clockSet == .skipped(.historyIncomplete))
    #expect(skipped.clockSetOutcomeRecord == .noLogEntry)
    #expect(!store.log.contains { $0.hasPrefix("clockSetOutcome") })
    #expect(try await storedOutcomes(other, otherID) == [nil])
}

/// A failed outcome write is reported and changes nothing else: the interval is still sent and
/// the sync is complete. The entry keeps no outcome, so it resolves as before #72.
@Test(.timeLimit(.minutes(1))) func aFailedOutcomeWriteIsReportedAndOtherwiseChangesNothing() async throws {
    let (base, id) = try await makeStore()
    let store = SuspendingStore(base, outcomeFails: OutcomeWriteFailure())
    let ring = FakeRing(store: base, ringID: id)
    let result = await outcomeSyncer(ring, store, id).run()

    #expect(await ring.log == completeLog)
    #expect(result.clockSet.wasAcknowledged)
    guard case .failed(.acknowledged, .other) = result.clockSetOutcomeRecord else {
        Issue.record("outcome write failure not reported: \(result.clockSetOutcomeRecord)")
        return
    }
    #expect(result.monitoringInterval == .accepted(.tenMinutes))
    #expect(result.ingestions.values.allSatisfy { $0.succeeded })
    #expect(result.isComplete)
    #expect(try await storedOutcomes(base, id) == [nil, nil])
}

/// A failed write after a refused clock-set leaves the phantom without an outcome, as before #72.
@Test(.timeLimit(.minutes(1))) func aFailedOutcomeWriteAfterARefusalLeavesTheEntryWithoutAnOutcome() async throws {
    let (base, id) = try await makeStore()
    let store = SuspendingStore(base, outcomeFails: OutcomeWriteFailure())
    let ring = FakeRing(store: base, ringID: id, clock: .fail(RingError.busy("synthetic")))
    let result = await outcomeSyncer(ring, store, id).run()

    guard case .notAppliedButLogged = result.clockSet else {
        Issue.record("clock-set not reported as not applied")
        return
    }
    guard case .failed(.notApplied, .other) = result.clockSetOutcomeRecord else {
        Issue.record("outcome write failure not reported: \(result.clockSetOutcomeRecord)")
        return
    }
    #expect(result.monitoringInterval == .skipped(.tenMinutes, .clockNotAcknowledged))
    #expect(result.ingestions.values.allSatisfy { $0.succeeded })
    #expect(try await storedOutcomes(base, id) == [nil, nil])
}

/// A suspension while recording the outcome ends the ring phase like any other store step: the
/// clock-set stays as it happened, the interval is skipped and ingestion does not start.
@Test(.timeLimit(.minutes(1))) func aSuspensionWhileRecordingTheOutcomeEndsTheSync() async throws {
    let (base, id) = try await makeStore()
    let store = SuspendingStore(base, outcomeFails: RingStoreError.suspended)
    let ring = FakeRing(store: base, ringID: id)
    let phase = await outcomeSyncer(ring, store, id).syncRing()

    #expect(phase.clockSet.wasAcknowledged)
    #expect(phase.clockSetOutcomeRecord == .failed(.acknowledged, error: .store(.suspended)))
    #expect(phase.monitoringInterval == .skipped(.tenMinutes, .storeSuspended))
    #expect(phase.endedBecauseStoreSuspended)
    #expect(await ring.log == completeLog.filter { $0 != "interval 10" })

    let result = await outcomeSyncer(FakeRing(store: base, ringID: id), SuspendingStore(base, outcomeFails: RingStoreError.suspended), id).run()
    for kind in HistoryKind.allCases { #expect(result.ingestions[kind] == .storeSuspended) }
    #expect(!result.isComplete)
}

@Test func theNewStoreErrorsKeepOnlyTheStoresRowID() {
    #expect(SyncError(RingStoreError.unknownClockSet(7)) == .store(.unknownClockSet(7)))
    #expect(SyncError(RingStoreError.clockSetOutcomeConflict(8)) == .store(.clockSetOutcomeConflict(8)))
    #expect(SyncError(RingStoreError.unknownClockSet(7)).description == "store: the clock-set is not in the store's log")
}
