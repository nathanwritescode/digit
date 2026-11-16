import Foundation
import RingProtocol
import RingStore
import RingTransport

// What one sync did. Counts, outcomes and errors only: no history bytes, no decoded reading and no
// device identifier is ever put in here, so the whole value can be shown or logged by the app.
// Errors are carried as values; this target prints and logs nothing.

/// Why a step of the sync was not run.
public enum SyncSkipReason: Sendable, Equatable {
    /// The sync's time budget had run out when the step was due.
    case budgetExhausted
    /// An earlier fetch failed with `RingError.stopped` or `RingError.notConnected`, so the session
    /// cannot carry another conversation.
    case sessionUnavailable
    /// Clock only: not every history kind was fetched and stored (or found empty and logged).
    /// Decided with the owner: the clock is set only after every kind has been stored, so the fetch
    /// log can show which records predate the clock-set.
    case historyIncomplete
    /// Monitoring interval only: the clock-set was not acknowledged by the ring.
    case clockNotAcknowledged
}

/// Where a history fetch failed.
public enum FetchFailureStage: Sendable, Equatable {
    /// Requesting or receiving the transfer; nothing was stored.
    case transfer
    /// The store threw inside `persist` (or, for an empty transfer, while logging it). The ring was
    /// not told the transfer was received.
    case store
    /// The transfer was stored, then acknowledging it to the ring failed. The data is in the store,
    /// but the kind still counts as failed: the ring was not confirmed to have been told.
    case acknowledgement
}

/// What happened to one history kind's fetch.
public enum FetchOutcome: Sendable {
    /// Fetched, stored before it was acknowledged, and acknowledged. `byteCount` is the length of
    /// the transfer's data.
    case stored(InsertOutcome, byteCount: Int)
    /// The ring had nothing of this kind. The empty transfer was logged as an observation, so the
    /// fetch log shows the buffer was empty at this sync. Counts as fetched.
    case empty
    case failed(stage: FetchFailureStage, error: any Error)
    case notAttempted(SyncSkipReason)

    /// Whether this kind counts as fetched for the clock-set rule: stored, or empty and logged.
    public var isFetched: Bool {
        switch self {
        case .stored, .empty: true
        case .failed, .notAttempted: false
        }
    }
}

/// What happened to the clock-set.
///
/// Time resolution treats every logged clock-set as one that happened, so a clock-set the ring
/// may have applied is logged and one it definitely did not apply is not.
public enum ClockSetOutcome: Sendable {
    /// The ring acknowledged it and it was logged.
    case acknowledgedAndLogged(ClockSetEvent)
    /// No acknowledgement was seen, but the command may have reached the ring (a timeout or another
    /// transport error), so it was logged as if it happened.
    case unknownAndLogged(ClockSetEvent, error: any Error)
    /// The ring definitely did not apply it: it answered with an error, or nothing was written
    /// (`RingError.stopped`, a refusal). Not logged.
    case notApplied(error: any Error)
    case skipped(SyncSkipReason)
    /// The ring acknowledged it but logging it failed. **The clock-set log is now incomplete**,
    /// which breaks the `resolved` guarantee of time resolution. Not retried.
    case acknowledgedButNotLogged(ClockSetEvent, logError: any Error)
    /// The outcome was unknown and logging it failed too. The log may be incomplete, as above.
    case unknownAndNotLogged(ClockSetEvent, error: any Error, logError: any Error)

    /// Whether the ring acknowledged the clock-set, logged or not.
    public var wasAcknowledged: Bool {
        switch self {
        case .acknowledgedAndLogged, .acknowledgedButNotLogged: true
        default: false
        }
    }

    /// Whether the clock-set log may now miss a clock-set the ring applied.
    public var leftLogIncomplete: Bool {
        switch self {
        case .acknowledgedButNotLogged, .unknownAndNotLogged: true
        default: false
        }
    }
}

/// What happened to the monitoring interval.
public enum MonitoringIntervalOutcome: Sendable {
    case accepted(MonitoringInterval)
    /// The ring answered with a refusal.
    case refused(MonitoringInterval, MonitoringSettingError)
    /// A transport failure: the ring may or may not have applied it. It is re-sent on every sync.
    case unknown(MonitoringInterval, error: any Error)
    case skipped(MonitoringInterval, SyncSkipReason)
    /// The caller supplied no interval, so none was sent.
    case notRequested
}

/// What happened to one kind's ingestion.
public enum IngestionOutcome: Sendable {
    case ingested(IngestionResult)
    case failed(any Error)
}

/// The result of one `RingSyncer.run()`.
public struct SyncResult: Sendable {
    /// Every history kind, in the order fetched.
    public var fetches: [HistoryKind: FetchOutcome]
    public var clockSet: ClockSetOutcome
    public var monitoringInterval: MonitoringIntervalOutcome
    /// Every history kind's ingestion; ingestion runs whatever happened before it.
    public var ingestions: [HistoryKind: IngestionOutcome]

    /// The single verdict for the app: every kind stored or empty, the clock acknowledged and
    /// logged, the interval accepted or not requested, and every ingestion succeeded.
    public var isComplete: Bool {
        let fetched = HistoryKind.allCases.allSatisfy { fetches[$0]?.isFetched == true }
        let clocked = if case .acknowledgedAndLogged = clockSet { true } else { false }
        let interval = switch monitoringInterval {
        case .accepted, .notRequested: true
        default: false
        }
        let ingested = HistoryKind.allCases.allSatisfy {
            if case .ingested = ingestions[$0] { true } else { false }
        }
        return fetched && clocked && interval && ingested
    }

    /// Whether the clock-set log may now miss a clock-set the ring applied. The app should report
    /// this prominently: time resolution can no longer be trusted to call a record `resolved`.
    public var clockSetLogIncomplete: Bool { clockSet.leftLogIncomplete }
}
