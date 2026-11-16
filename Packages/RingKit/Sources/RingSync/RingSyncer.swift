import Foundation
import RingProtocol
import RingStore
import RingTransport

/// Runs one sync against a ring that is already connected and whose session is started:
///
/// 1. Fetch and store each history kind in `fetchOrder` (activity, sleep, heart rate, combined),
///    storing each transfer through `RingStore.insert` as the session's `persist` step, so it is
///    durable before the ring is told it was received. An empty transfer is logged with `insert`
///    afterwards, so the fetch log shows the buffer was empty at this sync. A kind that fails is
///    recorded and the next kind is still fetched, except that `RingError.stopped` or
///    `RingError.notConnected` ends the fetching: the remaining kinds are not attempted.
/// 2. Only if every kind was stored or found empty: set the clock, with one reading of the clock
///    and zone used for both the command and the log entry. A clock-set the ring acknowledged, or
///    whose outcome is unknown (a timeout or other transport error after the command may have been
///    written), is logged with `recordClockSet`; one the ring definitely did not apply (an error
///    reply, `RingError.stopped`, a refusal) is not.
/// 3. Only if the ring acknowledged the clock-set, and an interval was supplied: send the
///    monitoring interval. The clock goes first because readings align to the ring's clock.
/// 4. Always: ingest every history kind, each on its own, whatever happened before.
///
/// Nothing is retried (a late reply to a timed-out request can be accepted by a retry of the same
/// request), the session is never stopped or restarted (one session per connection is the
/// caller's business), and the sync neither connects nor disconnects.
///
/// **Budget.** There is no task cancellation in the transport yet, so the budget is checked
/// between steps only: before each fetch, before the clock-set and before the interval. Once it
/// has run out the remaining ring steps are skipped (`SyncSkipReason.budgetExhausted`); ingestion
/// still runs and is not covered by the budget. A step that starts just before the budget runs
/// out can overrun it by one conversation timeout (`RingSession.conversationTimeout`) plus the
/// session's own overrun past a conversation deadline (about three write backstops for a request,
/// and four after the end of `persist` for a history acknowledgement; a write backstop is the
/// link's `writeTimeout` plus `replyTimeout`), plus the time `persist` takes, which is unbounded.
/// Ingestion's own time comes on top. The budget is measured on the injected monotonic clock
/// (`ContinuousClock` by default, which advances while the device sleeps).
///
/// Tested against scripted fakes only (a fake ring, and a real `RingSession` over a fake link);
/// never run against a ring.
public struct RingSyncer: Sendable {
    /// The order the history kinds are fetched in.
    public static let fetchOrder: [HistoryKind] = [.activity, .sleep, .heartRate, .combined]

    public let ring: any SyncableRing
    public let store: RingStore
    public let ringID: UUID
    /// Sent after an acknowledged clock-set; `nil` means do not send one.
    public let monitoringInterval: MonitoringInterval?
    public let budget: Duration
    private let clock: @Sendable () -> Date
    private let timeZone: @Sendable () -> TimeZone
    private let monotonicNow: @Sendable () -> ContinuousClock.Instant

    /// - Parameters:
    ///   - ring: the connected ring, normally a started `RingSession`.
    ///   - store: the on-device store.
    ///   - ringID: the ring's id in `store`.
    ///   - monitoringInterval: the interval to send, or `nil` to send none.
    ///   - budget: how long the ring steps may start for (see the type's documentation).
    ///   - clock: read once, for the clock-set.
    ///   - timeZone: read once, together with `clock`.
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

    /// Runs the sync once. Never throws: every failure is in the result.
    public func run() async -> SyncResult {
        let deadline = monotonicNow() + budget
        func budgetLeft() -> Bool { monotonicNow() < deadline }

        let fetches = await fetchAll(budgetLeft: budgetLeft)
        let clockSet = await setClock(after: fetches, budgetLeft: budgetLeft)
        let interval = await sendInterval(after: clockSet, budgetLeft: budgetLeft)
        var ingestions: [HistoryKind: IngestionOutcome] = [:]
        for kind in HistoryKind.allCases {
            do {
                ingestions[kind] = .ingested(try await store.ingest(ringID: ringID, kind: kind))
            } catch {
                ingestions[kind] = .failed(error)
            }
        }
        return SyncResult(fetches: fetches, clockSet: clockSet, monitoringInterval: interval, ingestions: ingestions)
    }

    // MARK: Steps

    private func fetchAll(budgetLeft: () -> Bool) async -> [HistoryKind: FetchOutcome] {
        var outcomes: [HistoryKind: FetchOutcome] = [:]
        var stopReason: SyncSkipReason?
        for kind in Self.fetchOrder {
            if stopReason == nil, !budgetLeft() { stopReason = .budgetExhausted }
            if let stopReason {
                outcomes[kind] = .notAttempted(stopReason)
                continue
            }
            let outcome = await fetch(kind)
            outcomes[kind] = outcome
            if case .failed(_, let error) = outcome, Self.endsSession(error) {
                stopReason = .sessionUnavailable
            }
        }
        return outcomes
    }

    private func fetch(_ kind: HistoryKind) async -> FetchOutcome {
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
            return .failed(stage: stage, error: error)
        }
        if case .stored(let outcome) = await persisted.state {
            return transfer.data.isEmpty ? .empty : .stored(outcome, byteCount: transfer.data.count)
        }
        // The session does not call `persist` for an empty transfer; log it here so the fetch log
        // records that the buffer was empty at this sync. (A conformer that skipped `persist` for
        // data would have it stored here too, though only after acknowledging it.)
        let outcome: InsertOutcome
        do {
            outcome = try await store.insert(transfer, ringID: ringID)
        } catch {
            return .failed(stage: .store, error: error)
        }
        return transfer.data.isEmpty ? .empty : .stored(outcome, byteCount: transfer.data.count)
    }

    private func setClock(after fetches: [HistoryKind: FetchOutcome], budgetLeft: () -> Bool) async -> ClockSetOutcome {
        guard Self.fetchOrder.allSatisfy({ fetches[$0]?.isFetched == true }) else { return .skipped(.historyIncomplete) }
        guard budgetLeft() else { return .skipped(.budgetExhausted) }
        // One reading for both the command and the log entry: reading twice could log a different
        // offset from the one sent at a daylight-saving boundary.
        let date = clock()
        let zone = timeZone()
        let event = ClockSetEvent(
            ringID: ringID,
            instant: HistoryTransfer.truncatedToMilliseconds(date),
            timeZoneIdentifier: zone.identifier,
            utcOffsetSeconds: zone.secondsFromGMT(for: date))
        let sendError: (any Error)?
        do {
            try await ring.setTime(date, timeZone: zone)
            sendError = nil
        } catch {
            if Self.definitelyNotApplied(error) { return .notApplied(error: error) }
            sendError = error
        }
        do {
            try await store.recordClockSet(event)
        } catch {
            if let sendError { return .unknownAndNotLogged(event, error: sendError, logError: error) }
            return .acknowledgedButNotLogged(event, logError: error)
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
            return .unknown(interval, error: error)
        }
    }

    // MARK: Error classes

    /// The session cannot carry another conversation, so later fetches would fail the same way.
    static func endsSession(_ error: any Error) -> Bool {
        switch error as? RingError {
        case .stopped, .notConnected: true
        default: false
        }
    }

    /// The ring definitely did not apply the clock-set: it answered with an error, or the session
    /// refused before writing. Every other error leaves the outcome unknown.
    ///
    /// `RingError.stopped` is counted here, as #51 specifies, but the session also throws it when
    /// `stop()` comes after the command was written (while the reply is awaited). The ring may then
    /// have applied a clock-set that is not logged. Unresolved: the session does not say which.
    static func definitelyNotApplied(_ error: any Error) -> Bool {
        switch error as? RingError {
        case .rejected, .stopped, .refusedDestructive, .refusedUploadGroup: true
        default: false
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
