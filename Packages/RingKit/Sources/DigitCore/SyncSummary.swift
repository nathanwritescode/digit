import Foundation
import RingProtocol
import RingStore
import RingSync

/// What the sync screen shows about one sync: counts and outcomes only, never a health value,
/// a ring name or an identifier. Built from the sync's own result.
public struct SyncSummary: Sendable, Equatable {
    public struct KindLine: Sendable, Equatable, Identifiable {
        public let kind: HistoryKind
        public let fetch: FetchStatus
        public let ingestion: IngestionStatus
        public var id: UInt8 { kind.rawValue }

        public var title: String { SyncSummary.title(of: kind) }
    }

    public enum FetchStatus: Sendable, Equatable {
        /// Stored before the ring was told it was received.
        case stored
        /// The ring had nothing of this kind.
        case empty
        /// The text of the sanitised error (`SyncError.description`).
        case failed(String)
        case notAttempted

        public var text: String {
            switch self {
            case .stored: "copied from the ring"
            case .empty: "nothing on the ring"
            case .failed(let why): "failed: \(why)"
            case .notAttempted: "not attempted"
            }
        }
    }

    public enum IngestionStatus: Sendable, Equatable {
        /// Rows inserted and updated; records rejected (implausible or dated in the future).
        case counts(inserted: Int, updated: Int, rejected: Int)
        case failed(String)
        /// Not run: the sync was interrupted before it.
        case notRun

        public var text: String {
            switch self {
            case .counts(let inserted, let updated, let rejected):
                "\(inserted) new, \(updated) updated, \(rejected) rejected"
            case .failed(let why): "not processed: \(why)"
            case .notRun: "not processed"
            }
        }
    }

    /// Every history kind, in the sync's fetch order.
    public let kinds: [KindLine]
    public let clockSet: ClockSetOutcome
    public let interval: MonitoringIntervalOutcome
    /// When the sync finished.
    public let finishedAt: Date
    /// The ring reported a new firmware version that could not be saved (the sync went on).
    public let firmwareNotSaved: Bool

    /// Plain words for the clock-set.
    public var clockText: String { Self.text(for: clockSet) }

    /// Whether the monitoring interval reached the ring and was accepted.
    public var intervalSent: Bool {
        if case .accepted = interval { true } else { false }
    }

    public var intervalText: String { Self.text(for: interval) }

    init(result: SyncResult, finishedAt: Date, firmwareNotSaved: Bool) {
        kinds = result.fetches.entries.map { entry in
            KindLine(kind: entry.kind, fetch: Self.status(entry.value), ingestion: Self.status(result.ingestions[entry.kind]))
        }
        clockSet = result.clockSet
        interval = result.monitoringInterval
        self.finishedAt = finishedAt
        self.firmwareNotSaved = firmwareNotSaved
    }

    /// For a sync whose derive phase did not run.
    init(ringPhase: RingPhaseResult, finishedAt: Date, firmwareNotSaved: Bool) {
        kinds = ringPhase.fetches.entries.map { entry in
            KindLine(kind: entry.kind, fetch: Self.status(entry.value), ingestion: .notRun)
        }
        clockSet = ringPhase.clockSet
        interval = ringPhase.monitoringInterval
        self.finishedAt = finishedAt
        self.firmwareNotSaved = firmwareNotSaved
    }

    // MARK: Texts

    public static func title(of kind: HistoryKind) -> String {
        switch kind {
        case .activity: "Activity"
        case .sleep: "Sleep"
        case .heartRate: "Heart rate"
        case .combined: "Other readings"
        }
    }

    static func status(_ outcome: FetchOutcome) -> FetchStatus {
        switch outcome {
        case .stored: .stored
        case .empty: .empty
        case .failed(_, let error): .failed(error.description)
        case .notAttempted: .notAttempted
        }
    }

    static func status(_ outcome: IngestionOutcome) -> IngestionStatus {
        switch outcome {
        case .ingested(let result):
            .counts(inserted: result.inserted, updated: result.updated, rejected: result.rejectedImplausible + result.rejectedFuture)
        case .failed(let error): .failed(error.description)
        case .storeSuspended: .notRun
        }
    }

    /// Plain words for every monitoring-interval outcome, naming the interval that outcome carries.
    public static func text(for outcome: MonitoringIntervalOutcome) -> String {
        func describe(_ interval: MonitoringInterval) -> String {
            interval.minutes.map { "every \($0) minutes" } ?? "off"
        }
        switch outcome {
        case .accepted(let interval):
            return interval == .off
                ? "The ring's scheduled readings are off."
                : "The ring will take readings \(describe(interval))."
        case .refused(let interval, _):
            return "The ring rejected the reading interval (\(describe(interval))). The next sync sends it again."
        case .unknown(let interval, _):
            return "Digit couldn't confirm the reading interval (\(describe(interval))); the ring may have applied it. The next sync sends it again."
        case .skipped(let interval, _):
            return "The reading interval (\(describe(interval))) wasn't sent. The next sync sends it."
        case .notRequested:
            return "No reading interval was sent."
        }
    }

    /// Plain words for every clock-set outcome. A clock-set that was logged but not applied, or
    /// whose outcome is unknown, is not something the owner can fix: the next sync sets it again.
    public static func text(for outcome: ClockSetOutcome) -> String {
        switch outcome {
        case .acknowledgedAndLogged:
            "The ring's clock was set."
        case .unknownAndLogged:
            "Digit couldn't confirm that the ring's clock was set. Nothing to do: the next sync sets it again."
        case .notAppliedButLogged:
            "The ring didn't take the new time. Nothing to do: the next sync sets it again."
        case .notLogged:
            "The ring's clock wasn't set, because Digit couldn't record it. The next sync sets it again."
        case .notSentLogTooSlow:
            "The ring's clock wasn't set, because recording it took too long. The next sync sets it again."
        case .skipped(let reason):
            switch reason {
            case .budgetExhausted: "The ring's clock wasn't set: the sync ran out of time. The next sync sets it."
            case .sessionUnavailable: "The ring's clock wasn't set: the connection was lost. The next sync sets it."
            case .historyIncomplete:
                "The ring's clock wasn't set, because not all of its history was copied. The next sync sets it."
            case .storeSuspended: "The ring's clock wasn't set: the sync was interrupted. The next sync sets it."
            case .clockNotAcknowledged, .clockNotSent: "The ring's clock wasn't set. The next sync sets it."
            }
        }
    }
}
