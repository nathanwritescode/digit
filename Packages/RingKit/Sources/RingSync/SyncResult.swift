import Foundation
import RingProtocol
import RingStore
import RingTransport

// What one sync did. A result holds exactly:
// - history kinds, outcome and skip-reason cases, and failure stages;
// - byte counts per transfer, and the store's `InsertOutcome` (its own row ids and a prune count);
// - `IngestionResult` counts only (rows inserted, updated, deleted and unchanged; records rejected,
//   superseded, colliding or without vitals; unusable bytes; skipped sleep stages; rows per time
//   source);
// - in a clock-set entry, the store's own ring id, the instant sent (whole milliseconds), the zone
//   identifier and the UTC offset;
// - the monitoring interval, and `MonitoringSettingError` codes;
// - sanitised errors (`SyncError`, never a raw error): ring error case names, the session's
//   timeout text, command types and reply codes; store failures with the store's own ring ids, an
//   errno, or a constant naming what could not be read; SQLite result codes; an error's type name.
// Row ids and the store's ring ids are the app's own, not device identifiers. It holds no history
// bytes, no decoded reading, no `Frame`, no file path and no device identifier. This target prints
// and logs nothing.

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
    /// but the kind still counts as failed: a write that fails points to a sick link, and the ring
    /// may still be waiting for the acknowledgement, so the clock is not set on top of it.
    case acknowledgement
    /// The ring side returned data without calling `persist` (a contract violation; `RingSession`
    /// never does this). The data was stored afterwards if it could be, but not before the ring was
    /// told it was received, so the kind counts as failed. The error is `.persistNotCalled`, or the
    /// store's error if storing it afterwards failed.
    case notPersistedBeforeAcknowledgement
}

/// What happened to one history kind's fetch.
public enum FetchOutcome: Sendable, Equatable {
    /// Fetched, stored before it was acknowledged, and acknowledged. `byteCount` is the length of
    /// the transfer's data.
    case stored(InsertOutcome, byteCount: Int)
    /// The ring had nothing of this kind. The empty transfer was logged as an observation, so the
    /// fetch log shows the buffer was empty at this sync. Counts as fetched.
    case empty
    case failed(stage: FetchFailureStage, error: SyncError)
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
/// may have applied is logged and one it certainly did not apply is not.
public enum ClockSetOutcome: Sendable, Equatable {
    /// The ring acknowledged it and it was logged.
    case acknowledgedAndLogged(ClockSetEvent)
    /// No acknowledgement was seen and the command may have reached the ring (a timeout, a stop,
    /// a lost connection or another transport error), so it was logged as if it happened. Some of
    /// these errors are in fact raised before anything is written (see `RingSyncer`); those log a
    /// clock-set that never happened.
    case unknownAndLogged(ClockSetEvent, error: SyncError)
    /// The ring certainly did not apply it: it answered with an error reply, or the session or the
    /// link refused the command before writing it (`RingError.refusedDestructive`,
    /// `refusedUploadGroup`, `busy`). Not logged.
    case notApplied(error: SyncError)
    case skipped(SyncSkipReason)
    /// The ring acknowledged it but logging it failed. **The clock-set log is now incomplete**,
    /// which breaks the `resolved` guarantee of time resolution. Reported here only; nothing
    /// persists it yet. Not retried.
    case acknowledgedButNotLogged(ClockSetEvent, logError: SyncError)
    /// The outcome was unknown and logging it failed too. The log may be incomplete, as above.
    case unknownAndNotLogged(ClockSetEvent, error: SyncError, logError: SyncError)

    /// Whether the ring acknowledged the clock-set, logged or not.
    public var wasAcknowledged: Bool {
        switch self {
        case .acknowledgedAndLogged, .acknowledgedButNotLogged: true
        case .unknownAndLogged, .notApplied, .skipped, .unknownAndNotLogged: false
        }
    }

    /// Whether the clock-set log may now miss a clock-set the ring applied.
    public var leftLogIncomplete: Bool {
        switch self {
        case .acknowledgedButNotLogged, .unknownAndNotLogged: true
        case .acknowledgedAndLogged, .unknownAndLogged, .notApplied, .skipped: false
        }
    }
}

/// What happened to the monitoring interval.
public enum MonitoringIntervalOutcome: Sendable, Equatable {
    case accepted(MonitoringInterval)
    /// The ring answered with a refusal.
    case refused(MonitoringInterval, MonitoringSettingError)
    /// A transport failure: the ring may or may not have applied it. It is re-sent on every sync.
    case unknown(MonitoringInterval, error: SyncError)
    case skipped(MonitoringInterval, SyncSkipReason)
    /// The caller supplied no interval, so none was sent.
    case notRequested
}

/// What happened to one kind's ingestion.
public enum IngestionOutcome: Sendable, Equatable {
    case ingested(IngestionResult)
    case failed(SyncError)

    public var succeeded: Bool {
        if case .ingested = self { true } else { false }
    }
}

/// One value per history kind, in `RingSyncer.fetchOrder`. Always holds every kind, so looking a
/// kind up never fails.
public struct PerKind<Value: Sendable & Equatable>: Sendable, Equatable {
    public struct Entry: Sendable, Equatable {
        public let kind: HistoryKind
        public let value: Value
    }

    /// Every kind, in `RingSyncer.fetchOrder`.
    public let entries: [Entry]

    /// Builds one value for every kind, in order.
    init(_ value: (HistoryKind) async -> Value) async {
        var entries: [Entry] = []
        for kind in RingSyncer.fetchOrder { entries.append(Entry(kind: kind, value: await value(kind))) }
        self.entries = entries
    }

    public subscript(kind: HistoryKind) -> Value {
        // `fetchOrder` holds every `HistoryKind` (tested), so this always finds one.
        entries.first { $0.kind == kind }!.value
    }

    public var values: [Value] { entries.map(\.value) }
}

/// The ring phase of a sync (`RingSyncer.syncRing()`): fetching, the clock-set and the interval.
public struct RingPhaseResult: Sendable, Equatable {
    public let fetches: PerKind<FetchOutcome>
    public let clockSet: ClockSetOutcome
    public let monitoringInterval: MonitoringIntervalOutcome
}

/// The result of one sync: the ring phase and the ingestion phase together.
public struct SyncResult: Sendable, Equatable {
    /// Every history kind, in fetch order.
    public let fetches: PerKind<FetchOutcome>
    public let clockSet: ClockSetOutcome
    public let monitoringInterval: MonitoringIntervalOutcome
    /// Every history kind's ingestion, in the same order; ingestion runs whatever happened before it.
    public let ingestions: PerKind<IngestionOutcome>

    /// Composes the two phases, for a caller that runs them separately (disconnecting between).
    public init(ringPhase: RingPhaseResult, ingestions: PerKind<IngestionOutcome>) {
        fetches = ringPhase.fetches
        clockSet = ringPhase.clockSet
        monitoringInterval = ringPhase.monitoringInterval
        self.ingestions = ingestions
    }

    /// The single verdict for the app: every kind stored or empty, the clock acknowledged and
    /// logged, the interval accepted or not requested, and every ingestion succeeded.
    public var isComplete: Bool {
        let fetched = fetches.values.allSatisfy(\.isFetched)
        let clocked = if case .acknowledgedAndLogged = clockSet { true } else { false }
        let interval = switch monitoringInterval {
        case .accepted, .notRequested: true
        case .refused, .unknown, .skipped: false
        }
        return fetched && clocked && interval && ingestions.values.allSatisfy(\.succeeded)
    }

    /// Whether the clock-set log may now miss a clock-set the ring applied. The app should report
    /// this prominently: time resolution can no longer be trusted to call a record `resolved`.
    public var clockSetLogIncomplete: Bool { clockSet.leftLogIncomplete }
}
