import Foundation
import RingProtocol
import RingStore
import RingTransport

/// Runs one sync with a ring, in two phases.
///
/// **Ring phase** (`syncRing()`), against a ring that is already connected and whose session is
/// started:
/// 1. Fetch and store each history kind in `fetchOrder` (activity, sleep, heart rate, combined),
///    storing each transfer through `RingStore.insert` as the session's `persist` step, so it is
///    durable before the ring is told it was received. An empty transfer is logged with `insert`
///    afterwards, so the fetch log shows the buffer was empty at this sync. A kind that fails is
///    recorded and the next kind is still fetched, except that `RingError.stopped` or
///    `RingError.notConnected` ends the fetching: the remaining kinds are not attempted. (So the
///    sync relies on a dead link's `send` throwing `notConnected`; a link-level "closed" signal is
///    a follow-up.)
/// 2. Only if every kind was stored or found empty: set the clock, with one reading of the clock
///    and one fixed copy of the zone used for the log entry and the command alike. The entry is
///    logged with `recordClockSet` **before** `0x0100` is sent, and if logging fails the command is
///    not sent (the clock-set is skipped, and so is the interval). A logged clock-set the ring did
///    not apply stays in the log (see "Phantom clock-sets" below). Once the send has finished (or
///    been withheld because logging was too slow), its outcome is recorded against the entry with
///    `recordClockSetOutcome` (best effort, `ClockSetOutcomeRecord`).
/// 3. Only if the ring acknowledged the clock-set, and an interval was supplied: send the
///    monitoring interval. The clock goes first because readings align to the ring's clock.
///
/// **Store suspension.** `RingStoreError.suspended` (the app announced it is about to suspend) in
/// any store step of the ring phase ends it: the failing kind is `failed`, the remaining kinds are
/// `notAttempted(.storeSuspended)`, and the clock-set and the interval are skipped with
/// `.storeSuspended`. A suspension while recording the clock-set's outcome (after the send) leaves
/// the clock-set as it happened, reports the record as failed and skips the interval with
/// `.storeSuspended`. `run()` then does not start the derive phase, and its kinds are
/// `IngestionOutcome.storeSuspended` too. In `ingestAll()` the first suspension makes every remaining
/// kind `IngestionOutcome.storeSuspended`. It is detected on the raw error where it is caught, not on
/// the sanitised `SyncError`. Nothing is retried after the app resumes: that is the caller's
/// scheduling.
///
/// **Derive phase** (`ingestAll()`): ingest every history kind, each on its own. It needs only the
/// store, so the caller can disconnect from the ring first; ingestion's time grows with history
/// and is outside the budget. `run()` runs both phases one after the other, whatever the ring
/// phase did.
///
/// Nothing is retried (a late reply to a timed-out request can be accepted by a retry of the same
/// request), the session is never stopped or restarted (one session per connection is the
/// caller's business), and the sync neither connects nor disconnects.
///
/// **Callers must not run two syncs of one ring at once.** The clock-set relies on every kind
/// having been stored just before it; an overlapping run breaks that reasoning (and the ring
/// carries one conversation at a time anyway).
///
/// **Phantom clock-sets.** The clock-set is logged before it is sent, because a clock-set that was
/// applied but is missing from the log can make time resolution call later records `resolved`
/// under the wrong offset (silent wrong data). A logged clock-set that never happened is the safer
/// error only while its offset equals the ring's real one; a phantom with a different offset
/// misleads the resolver just as a missing entry does. There is no way to delete a log entry (an
/// append-only log the resolver trusts), so an entry for a clock-set the ring did not apply stays:
/// after an error reply or a refusal before writing (`ClockSetOutcome.notAppliedButLogged`:
/// `RingError.rejected`, `refusedDestructive`, `refusedUploadGroup`, `busy`), after any error that
/// leaves the outcome unknown, and when logging was too slow to send safely
/// (`ClockSetOutcome.notSentLogTooSlow`). The sync then records the outcome against the entry
/// (#72), and time resolution ignores entries recorded as `notApplied` or `notSent`. An entry whose
/// outcome is `unknown`, or whose outcome could not be recorded, stays a candidate for time
/// resolution: a phantom there is still the accepted error.
///
/// **Stale time.** The time sent is read before the log write, and the entry must stay the event
/// sent, so it is not re-read. If the log write took longer than `maximumClockLogDelay` (a write
/// queued behind a long ingest, a busy timeout), the time is stale and the command is not sent.
///
/// **Unknown clock-set outcomes.** Every `RingError` except an error reply and the refusals made
/// before writing is taken to mean the command may have reached the ring. Some of those errors
/// are in fact raised before anything is written (the session's deadline check before the write,
/// its wait for event acknowledgements, a `stop()` before the write, and the link's own
/// `notConnected` check before writing, probably the commonest: the ring went out of range after
/// the last acknowledgement), and the session cannot tell the sync which; they are reported as
/// `unknownAndLogged` and leave a phantom entry. Telling them apart needs the session to report
/// whether the command was written (a follow-up).
///
/// **Budget.** There is no task cancellation in the transport yet (#52), so the budget is checked
/// between steps only: before each fetch, before the clock-set and before the interval. Once it
/// has run out the remaining ring steps are skipped (`SyncSkipReason.budgetExhausted`). A step that
/// starts just before the budget runs out can overrun it by one conversation timeout
/// (`RingSession.conversationTimeout`) plus the session's own overrun past a conversation deadline
/// (about three write backstops for a request, and four after the end of `persist` for a history
/// acknowledgement; a write backstop is the link's `writeTimeout` plus `replyTimeout`), plus the
/// time `persist` takes, which is unbounded. The derive phase is not covered by the budget. The
/// budget is measured on the injected monotonic clock (`ContinuousClock` by default, which advances
/// while the device sleeps).
///
/// **Cancellation** of the calling task is not handled (#52): store calls then throw
/// `CancellationError`, which is reported as `SyncError.cancelled` like any other failure.
///
/// Specific to the YCBT family, like `SyncableRing` and `fetchOrder`; it will sit behind the
/// planned ring abstraction when a second family arrives.
///
/// Tested against scripted fakes only (a fake ring, and a real `RingSession` over a fake link);
/// never run against a ring.
public struct RingSyncer: Sendable {
    /// The order the history kinds are fetched in (and results are listed in). YCBT-specific.
    public static let fetchOrder: [HistoryKind] = [.activity, .sleep, .heartRate, .combined]

    private let ring: any SyncableRing
    private let store: any SyncStore
    private let ringID: UUID
    private let monitoringInterval: MonitoringInterval?
    private let budget: Duration
    private let clock: @Sendable () -> Date
    private let timeZone: @Sendable () -> TimeZone
    private let monotonicNow: @Sendable () -> ContinuousClock.Instant
    private let maximumLogDelay: Duration

    /// The longest the clock-set's log write may take before the command is not sent: the time to
    /// send was read before the write, so it must not be old. A second or two of error in a ring
    /// whose records are minutes apart is tolerable; more is not.
    public static let maximumClockLogDelay: Duration = .seconds(2)

    /// - Parameters:
    ///   - ring: the connected ring, normally a started `RingSession`.
    ///   - store: the on-device store.
    ///   - ringID: the ring's id in `store`.
    ///   - monitoringInterval: the interval to send, or `nil` to send none.
    ///   - budget: how long the ring steps may start for (see the type's documentation).
    ///   - clock: read once per clock-set.
    ///   - timeZone: read once per clock-set, together with `clock`. A fixed copy of the zone it
    ///     returns (by identifier) is used for the command, the offset and the logged identifier, so
    ///     an auto-updating zone that changes meanwhile cannot make them disagree.
    ///   - monotonicNow: the clock the budget is measured on.
    public init(
        ring: any SyncableRing,
        store: RingStore,
        ringID: UUID,
        monitoringInterval: MonitoringInterval?,
        budget: Duration,
        clock: @escaping @Sendable () -> Date = { Date() },
        timeZone: @escaping @Sendable () -> TimeZone = { TimeZone.current },
        monotonicNow: @escaping @Sendable () -> ContinuousClock.Instant = { ContinuousClock.now }
    ) {
        self.init(
            ring: ring, syncStore: store, ringID: ringID, monitoringInterval: monitoringInterval, budget: budget,
            clock: clock, timeZone: timeZone, monotonicNow: monotonicNow)
    }

    /// For tests: a store that can be made to throw `RingStoreError.suspended`.
    init(
        ring: any SyncableRing,
        syncStore store: any SyncStore,
        ringID: UUID,
        monitoringInterval: MonitoringInterval?,
        budget: Duration,
        clock: @escaping @Sendable () -> Date,
        timeZone: @escaping @Sendable () -> TimeZone,
        monotonicNow: @escaping @Sendable () -> ContinuousClock.Instant,
        maximumLogDelay: Duration = RingSyncer.maximumClockLogDelay
    ) {
        self.maximumLogDelay = maximumLogDelay
        self.ring = ring
        self.store = store
        self.ringID = ringID
        self.monitoringInterval = monitoringInterval
        self.budget = budget
        self.clock = clock
        self.timeZone = timeZone
        self.monotonicNow = monotonicNow
    }

    /// Runs both phases: `syncRing()`, then `ingestAll()`. Never throws: every failure is in the result.
    /// At most once per session, like `syncRing()`.
    public func run() async -> SyncResult {
        let ringPhase = await syncRing()
        // A suspended store would refuse every ingestion at once; do not start.
        if ringPhase.endedBecauseStoreSuspended {
            return SyncResult(ringPhase: ringPhase, ingestions: await PerKind { _ in .storeSuspended })
        }
        return SyncResult(ringPhase: ringPhase, ingestions: await ingestAll())
    }

    /// The ring phase: fetch and store every kind, set the clock, send the interval. The budget
    /// starts when this is called. Never throws.
    ///
    /// Run it at most once per session (connect, create a session, sync once). A late reply to an
    /// earlier request can be taken as the reply to a later one of the same type, so a second
    /// clock-set on the same session could be answered by a late error reply to the first, be
    /// classed as not applied, and be left out of the log although the ring applied it.
    public func syncRing() async -> RingPhaseResult {
        let deadline = monotonicNow() + budget
        let budgetLeft = { monotonicNow() < deadline }
        let fetched = await fetchAll(budgetLeft: budgetLeft)
        let (clockSet, entryID) = await setClock(
            after: fetched.outcomes, storeSuspended: fetched.storeSuspended, budgetLeft: budgetLeft)
        let record = await recordOutcome(of: clockSet, entryID: entryID)
        let interval = await sendInterval(
            after: clockSet, outcomeRecordSuspended: record.storeSuspended, budgetLeft: budgetLeft)
        // A suspension while logging the clock-set, or while recording its outcome, ends the phase too.
        let suspended = fetched.storeSuspended || clockSet == .skipped(.storeSuspended) || record.storeSuspended
        return RingPhaseResult(
            fetches: fetched.outcomes, clockSet: clockSet, clockSetOutcomeRecord: record.record,
            monitoringInterval: interval, endedBecauseStoreSuspended: suspended)
    }

    /// The derive phase: ingest every kind, each on its own, so one failure does not stop the
    /// others. Needs only the store. Never throws.
    public func ingestAll() async -> PerKind<IngestionOutcome> {
        let store = self.store
        let ringID = self.ringID
        // `PerKind` builds its values one after another, in order, so the flag is set before the
        // next kind runs.
        var suspended = false
        return await PerKind { kind in
            if suspended { return .storeSuspended }
            do {
                return .ingested(try await store.ingest(ringID: ringID, kind: kind))
            } catch {
                if Self.isSuspension(error) { suspended = true }
                return .failed(SyncError(error))
            }
        }
    }

    // MARK: Steps

    /// The outcomes, and whether the store was suspended (which ends the fetching).
    private func fetchAll(budgetLeft: () -> Bool) async -> (outcomes: PerKind<FetchOutcome>, storeSuspended: Bool) {
        var stopReason: SyncSkipReason?
        var outcomes: [HistoryKind: FetchOutcome] = [:]
        for kind in Self.fetchOrder {
            if stopReason == nil, !budgetLeft() { stopReason = .budgetExhausted }
            if let stopReason {
                outcomes[kind] = .notAttempted(stopReason)
                continue
            }
            let step = await fetch(kind)
            outcomes[kind] = step.outcome
            if step.storeSuspended {
                stopReason = .storeSuspended
            } else if let raw = step.raw, Self.endsSession(raw) {
                stopReason = .sessionUnavailable
            }
        }
        let suspended = stopReason == .storeSuspended
        return (outcomes: await PerKind { outcomes[$0]! }, storeSuspended: suspended)
    }

    private struct FetchStep {
        let outcome: FetchOutcome
        /// The raw error, if the fetch itself threw (for deciding whether to go on).
        var raw: (any Error)?
        var storeSuspended = false
    }

    private func fetch(_ kind: HistoryKind) async -> FetchStep {
        let persisted = PersistRecord()
        let store = self.store
        let ringID = self.ringID
        let transfer: HistoryTransfer
        do {
            transfer = try await ring.fetchHistory(kind) { transfer in
                // Touches the store only: `persist` runs while the session's turn is held.
                do {
                    await persisted.succeeded(try await store.insert(transfer, ringID: ringID))
                } catch {
                    await persisted.failed()
                    throw error
                }
            }
        } catch {
            let stage: FetchFailureStage = switch await persisted.state {
            case .notCalled: .transfer
            case .failed: .store
            case .stored: .acknowledgement
            }
            // `SyncableRing` requires `fetchHistory` to rethrow `persist`'s error unchanged.
            return FetchStep(
                outcome: .failed(stage: stage, error: SyncError(error)), raw: error,
                storeSuspended: Self.isSuspension(error))
        }
        if case .stored(let outcome) = await persisted.state {
            return FetchStep(outcome: transfer.data.isEmpty ? .empty : .stored(outcome, byteCount: transfer.data.count))
        }
        // `persist` was not called. For an empty transfer that is the session's normal behaviour;
        // log it here so the fetch log records that the buffer was empty at this sync. For data it
        // is a contract violation: store it anyway, but report the kind as failed, since it was not
        // stored before the ring was told it was received.
        do {
            try await store.insert(transfer, ringID: ringID)
        } catch {
            let stage: FetchFailureStage = transfer.data.isEmpty ? .store : .notPersistedBeforeAcknowledgement
            return FetchStep(outcome: .failed(stage: stage, error: SyncError(error)), storeSuspended: Self.isSuspension(error))
        }
        if transfer.data.isEmpty { return FetchStep(outcome: .empty) }
        return FetchStep(outcome: .failed(stage: .notPersistedBeforeAcknowledgement, error: .persistNotCalled))
    }

    /// The outcome, and the log entry's id when one was written.
    private func setClock(
        after fetches: PerKind<FetchOutcome>, storeSuspended: Bool, budgetLeft: () -> Bool
    ) async -> (ClockSetOutcome, entryID: Int64?) {
        if storeSuspended { return (.skipped(.storeSuspended), nil) }
        guard fetches.values.allSatisfy(\.isFetched) else { return (.skipped(.historyIncomplete), nil) }
        guard budgetLeft() else { return (.skipped(.budgetExhausted), nil) }
        // One reading of each, and a fixed copy of the zone: reading twice, or an auto-updating
        // zone changing in between, could log a different offset from the one sent at a
        // daylight-saving boundary.
        let date = clock()
        let readAt = monotonicNow()
        let given = timeZone()
        let zone = TimeZone(identifier: given.identifier) ?? given
        let event = ClockSetEvent(
            ringID: ringID,
            instant: HistoryTransfer.truncatedToMilliseconds(date),
            timeZoneIdentifier: zone.identifier,
            utcOffsetSeconds: zone.secondsFromGMT(for: date))
        // Log first: an entry for a clock-set that was not applied is the safer error than a
        // clock-set the ring applied that the log does not know about. If logging fails, nothing
        // is sent.
        let entryID: Int64
        do {
            entryID = try await store.recordClockSet(event)
        } catch {
            if Self.isSuspension(error) { return (.skipped(.storeSuspended), nil) }
            return (.notLogged(SyncError(error)), nil)
        }
        // The time was read before the write; the entry must stay the event sent, so it is not
        // re-read. If the write was slow the time is stale: leave the entry as a phantom. This bounds
        // only the log write; the session's write turn and the link's `writeTimeout` can add more
        // before the ring receives the command, bounded by the transport's own deadlines.
        let elapsed = monotonicNow() - readAt
        if elapsed > maximumLogDelay { return (.notSentLogTooSlow(event, elapsed: elapsed), entryID) }
        do {
            try await ring.setTime(date, timeZone: zone)
        } catch {
            if Self.definitelyNotApplied(error) { return (.notAppliedButLogged(event, error: SyncError(error)), entryID) }
            return (.unknownAndLogged(event, error: SyncError(error)), entryID)
        }
        return (.acknowledgedAndLogged(event), entryID)
    }

    /// Records the clock-set's outcome against its log entry (#72). Best effort: a failure leaves
    /// the entry without an outcome, which time resolution treats as before, and changes nothing
    /// else in the sync, except that a suspension ends the ring phase like any other store step.
    /// Runs before the derive phase, so the same sync's ingestion sees the outcome.
    private func recordOutcome(
        of clockSet: ClockSetOutcome, entryID: Int64?
    ) async -> (record: ClockSetOutcomeRecord, storeSuspended: Bool) {
        guard let entryID, let outcome = clockSet.loggedOutcome else { return (.noLogEntry, false) }
        do {
            try await store.recordClockSetOutcome(id: entryID, outcome: outcome)
            return (.recorded(outcome), false)
        } catch {
            return (.failed(outcome, error: SyncError(error)), Self.isSuspension(error))
        }
    }

    private func sendInterval(
        after clockSet: ClockSetOutcome, outcomeRecordSuspended: Bool, budgetLeft: () -> Bool
    ) async -> MonitoringIntervalOutcome {
        guard let interval = monitoringInterval else { return .notRequested }
        if case .skipped(let reason) = clockSet, reason == .budgetExhausted || reason == .storeSuspended {
            return .skipped(interval, reason)
        }
        if outcomeRecordSuspended { return .skipped(interval, .storeSuspended) }
        switch clockSet {
        case .notLogged, .notSentLogTooSlow: return .skipped(interval, .clockNotSent)
        case .acknowledgedAndLogged, .unknownAndLogged, .notAppliedButLogged, .skipped: break
        }
        guard clockSet.wasAcknowledged else { return .skipped(interval, .clockNotAcknowledged) }
        guard budgetLeft() else { return .skipped(interval, .budgetExhausted) }
        do {
            try await ring.setMonitoringInterval(interval)
            return .accepted(interval)
        } catch let refusal as MonitoringSettingError {
            return .refused(interval, refusal)
        } catch {
            return .unknown(interval, error: SyncError(error))
        }
    }

    // MARK: Error classes

    // Both switches name every `RingError` case, so a new case forces a decision; the default
    // covers only errors that are not `RingError`.

    /// The store refused because the app is suspending. Checked on the raw error, never on a `SyncError`.
    static func isSuspension(_ error: any Error) -> Bool {
        (error as? RingStoreError) == .suspended
    }

    /// The session cannot carry another conversation, so later fetches would fail the same way.
    static func endsSession(_ error: any Error) -> Bool {
        guard let error = error as? RingError else { return false }
        switch error {
        case .stopped, .notConnected:
            return true
        case .bluetoothUnavailable, .notFound, .unknownIdentifier, .connectionFailed, .missingService,
             .timeout, .rejected, .checksumMismatch, .refusedDestructive, .busy, .tooManyFrames,
             .refusedUploadGroup:
            return false
        }
    }

    /// The ring certainly did not apply the clock-set: it answered with an error reply, or the
    /// command was refused before anything could be written. Everything else leaves the outcome
    /// unknown, including `stopped` (a `stop()` can come after the write) and errors that are not
    /// `RingError`.
    static func definitelyNotApplied(_ error: any Error) -> Bool {
        guard let error = error as? RingError else { return false }
        switch error {
        case .rejected, .refusedDestructive, .refusedUploadGroup, .busy:
            return true
        case .bluetoothUnavailable, .notFound, .unknownIdentifier, .connectionFailed, .missingService,
             .notConnected, .timeout, .checksumMismatch, .stopped, .tooManyFrames:
            return false
        }
    }
}

/// What the `persist` step did, read once the fetch has returned or thrown.
private actor PersistRecord {
    enum State { case notCalled, stored(InsertOutcome), failed }
    private(set) var state = State.notCalled
    func succeeded(_ outcome: InsertOutcome) { state = .stored(outcome) }
    func failed() { state = .failed }
}
