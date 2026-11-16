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
///    and one fixed copy of the zone used for the command and the log entry alike. A clock-set the
///    ring acknowledged, or whose outcome is unknown, is logged with `recordClockSet`; one the ring
///    certainly did not apply is not (see `definitelyNotApplied`).
/// 3. Only if the ring acknowledged the clock-set, and an interval was supplied: send the
///    monitoring interval. The clock goes first because readings align to the ring's clock.
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
/// **Unknown clock-set outcomes.** Every `RingError` except an error reply and the refusals made
/// before writing (`rejected`, `refusedDestructive`, `refusedUploadGroup`, `busy`) is taken to
/// mean the command may have reached the ring, and the clock-set is logged. Some of those errors
/// are in fact raised before anything is written (the session's deadline check before the write,
/// its wait for event acknowledgements, a `stop()` before the write), and the session cannot tell
/// the sync which; those log a clock-set that never happened. Telling them apart needs the session
/// to report whether the command was written (a follow-up). Logging is the safer error: a
/// `stop()` after the write, the realistic case of a ring that took the clock-set and then went
/// out of range, would otherwise leave a clock-set out of the log.
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
    private let store: RingStore
    private let ringID: UUID
    private let monitoringInterval: MonitoringInterval?
    private let budget: Duration
    private let clock: @Sendable () -> Date
    private let timeZone: @Sendable () -> TimeZone
    private let monotonicNow: @Sendable () -> ContinuousClock.Instant

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
        let fetches = await fetchAll(budgetLeft: budgetLeft)
        let clockSet = await setClock(after: fetches, budgetLeft: budgetLeft)
        let interval = await sendInterval(after: clockSet, budgetLeft: budgetLeft)
        return RingPhaseResult(fetches: fetches, clockSet: clockSet, monitoringInterval: interval)
    }

    /// The derive phase: ingest every kind, each on its own, so one failure does not stop the
    /// others. Needs only the store. Never throws.
    public func ingestAll() async -> PerKind<IngestionOutcome> {
        let store = self.store
        let ringID = self.ringID
        return await PerKind { kind in
            do {
                return .ingested(try await store.ingest(ringID: ringID, kind: kind))
            } catch {
                return .failed(SyncError(error))
            }
        }
    }

    // MARK: Steps

    private func fetchAll(budgetLeft: () -> Bool) async -> PerKind<FetchOutcome> {
        var stopReason: SyncSkipReason?
        var outcomes: [HistoryKind: FetchOutcome] = [:]
        for kind in Self.fetchOrder {
            if stopReason == nil, !budgetLeft() { stopReason = .budgetExhausted }
            if let stopReason {
                outcomes[kind] = .notAttempted(stopReason)
                continue
            }
            let (outcome, raw) = await fetch(kind)
            outcomes[kind] = outcome
            if let raw, Self.endsSession(raw) { stopReason = .sessionUnavailable }
        }
        return await PerKind { outcomes[$0]! }
    }

    /// The outcome, and the raw error if the fetch itself threw (for deciding whether to go on).
    private func fetch(_ kind: HistoryKind) async -> (FetchOutcome, (any Error)?) {
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
            return (.failed(stage: stage, error: SyncError(error)), error)
        }
        if case .stored(let outcome) = await persisted.state {
            return (transfer.data.isEmpty ? .empty : .stored(outcome, byteCount: transfer.data.count), nil)
        }
        // `persist` was not called. For an empty transfer that is the session's normal behaviour;
        // log it here so the fetch log records that the buffer was empty at this sync. For data it
        // is a contract violation: store it anyway, but report the kind as failed, since it was not
        // stored before the ring was told it was received.
        do {
            try await store.insert(transfer, ringID: ringID)
        } catch {
            let stage: FetchFailureStage = transfer.data.isEmpty ? .store : .notPersistedBeforeAcknowledgement
            return (.failed(stage: stage, error: SyncError(error)), nil)
        }
        if transfer.data.isEmpty { return (.empty, nil) }
        return (.failed(stage: .notPersistedBeforeAcknowledgement, error: .persistNotCalled), nil)
    }

    private func setClock(after fetches: PerKind<FetchOutcome>, budgetLeft: () -> Bool) async -> ClockSetOutcome {
        guard fetches.values.allSatisfy(\.isFetched) else { return .skipped(.historyIncomplete) }
        guard budgetLeft() else { return .skipped(.budgetExhausted) }
        // One reading of each, and a fixed copy of the zone: reading twice, or an auto-updating
        // zone changing in between, could log a different offset from the one sent at a
        // daylight-saving boundary.
        let date = clock()
        let given = timeZone()
        let zone = TimeZone(identifier: given.identifier) ?? given
        let event = ClockSetEvent(
            ringID: ringID,
            instant: HistoryTransfer.truncatedToMilliseconds(date),
            timeZoneIdentifier: zone.identifier,
            utcOffsetSeconds: zone.secondsFromGMT(for: date))
        let sendError: SyncError?
        do {
            try await ring.setTime(date, timeZone: zone)
            sendError = nil
        } catch {
            if Self.definitelyNotApplied(error) { return .notApplied(error: SyncError(error)) }
            sendError = SyncError(error)
        }
        do {
            try await store.recordClockSet(event)
        } catch {
            if let sendError { return .unknownAndNotLogged(event, error: sendError, logError: SyncError(error)) }
            return .acknowledgedButNotLogged(event, logError: SyncError(error))
        }
        if let sendError { return .unknownAndLogged(event, error: sendError) }
        return .acknowledgedAndLogged(event)
    }

    private func sendInterval(after clockSet: ClockSetOutcome, budgetLeft: () -> Bool) async -> MonitoringIntervalOutcome {
        guard let interval = monitoringInterval else { return .notRequested }
        if case .skipped(.budgetExhausted) = clockSet { return .skipped(interval, .budgetExhausted) }
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
