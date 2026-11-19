import Foundation
import Observation
import RingStore
import RingSync

/// "Sync now": connect to the paired ring, run the ring phase of a sync, disconnect, then run the
/// derive phase, and show what happened in counts.
///
/// The ring's only connection is not held through ingestion: the session is stopped and the link
/// disconnected before `ingestAll()` starts. The derive phase is skipped when the ring phase ended
/// because the store was suspended.
///
/// Results never carry a raw error, only `SyncError` (from `RingSync`) and `RingAccessProblem`;
/// nothing here logs or prints.
@MainActor
@Observable
public final class SyncModel {
    public enum State: Sendable, Equatable {
        case idle
        case connecting
        case syncing
        case done(SyncSummary)
        case failed(SyncFailure)
    }

    public private(set) var state = State.idle

    @ObservationIgnored private let pairedRing: PairedRing
    @ObservationIgnored private let services: DigitServices
    /// Told when a sync starts and ends.
    @ObservationIgnored private let lifecycle: (any RingWorkLifecycle)?

    public init(pairedRing: PairedRing, services: DigitServices, lifecycle: (any RingWorkLifecycle)? = nil) {
        self.pairedRing = pairedRing
        self.services = services
        self.lifecycle = lifecycle
    }

    /// Whether a sync is in progress.
    public var isRunning: Bool {
        switch state {
        case .connecting, .syncing: true
        case .idle, .done, .failed: false
        }
    }

    /// Whether "Sync now" can start: a ring with a saved identifier is paired and nothing else is
    /// using the ring.
    public var canSync: Bool { pairedRing.canSync && pairedRing.activity == nil }

    /// Plain words for the current state. Holds no name, identifier or health value.
    public var statusText: String {
        switch state {
        case .idle: "Not synced yet."
        case .connecting: "Connecting to the ring…"
        case .syncing: "Syncing…"
        case .done: "Sync complete."
        case .failed(let failure): failure.message
        }
    }

    /// Runs one sync. A tap while a sync (or pairing) runs does nothing.
    public func sync() async {
        guard let ring = pairedRing.ring, let identifier = ring.peripheralIdentifier else { return }
        await pairedRing.using(.syncing, lifecycle: lifecycle) {
            state = .connecting
            state = await run(ring: ring, identifier: identifier)
        }
    }

    private func run(ring: StoredRing, identifier: UUID) async -> State {
        let connected: any ConnectedRing
        do {
            connected = try await services.ringAccess.connect(identifier: identifier)
        } catch {
            return .failed(SyncFailure(reason: error == .unknownIdentifier ? .needsPairing : .ring(error)))
        }
        state = .syncing
        let details: RingDeviceDetails
        do {
            details = try await connected.deviceDetails()
        } catch {
            await connected.disconnect()
            return .failed(SyncFailure(reason: .ring(error)))
        }

        // Ring phase, then let go of the ring before anything else.
        let phases = services.makeSync(connected.syncableRing, ring.id)
        let ringPhase = await phases.syncRing()
        await connected.disconnect()

        var firmwareNotSaved = false
        if !ringPhase.endedBecauseStoreSuspended, details.firmware != ring.firmware {
            do {
                try await services.registry.updateRing(id: ring.id, model: ring.model, firmware: details.firmware)
                pairedRing.ring?.firmware = details.firmware
            } catch let error as RingStoreError where error == .suspended {
                return .failed(SyncFailure(
                    reason: .interrupted,
                    summary: SyncSummary(ringPhase: ringPhase, finishedAt: services.now(), firmwareNotSaved: true)))
            } catch {
                // Not worth failing the sync over: the next sync tries again.
                firmwareNotSaved = true
            }
        }

        if ringPhase.endedBecauseStoreSuspended {
            return .failed(SyncFailure(
                reason: .interrupted,
                summary: SyncSummary(ringPhase: ringPhase, finishedAt: services.now(), firmwareNotSaved: firmwareNotSaved)))
        }

        // Derive phase: the store only.
        let result = SyncResult(ringPhase: ringPhase, ingestions: await phases.ingestAll())
        let summary = SyncSummary(result: result, finishedAt: services.now(), firmwareNotSaved: firmwareNotSaved)
        if Self.hitSuspension(result) {
            return .failed(SyncFailure(reason: .interrupted, summary: summary))
        }
        return result.isComplete ? .done(summary) : .failed(SyncFailure(reason: .incomplete, summary: summary))
    }

    /// Whether any step was refused or skipped because the store was suspended.
    static func hitSuspension(_ result: SyncResult) -> Bool {
        let suspended = SyncError.store(.suspended)
        for fetch in result.fetches.values {
            switch fetch {
            case .failed(_, let error) where error == suspended: return true
            case .notAttempted(.storeSuspended): return true
            default: break
            }
        }
        switch result.clockSet {
        case .skipped(.storeSuspended): return true
        case .notLogged(let error) where error == suspended: return true
        default: break
        }
        if case .skipped(_, .storeSuspended) = result.monitoringInterval { return true }
        for ingestion in result.ingestions.values {
            switch ingestion {
            case .storeSuspended: return true
            case .failed(let error) where error == suspended: return true
            default: break
            }
        }
        return false
    }
}

/// Why a sync did not complete, with the counts of what it did, if it got that far.
public struct SyncFailure: Sendable, Equatable {
    public enum Reason: Sendable, Equatable {
        /// The ring could not be reached or read.
        case ring(RingAccessProblem)
        /// The saved identifier is no longer known: the owner should pair again.
        case needsPairing
        /// The store was suspended with the app during the sync. Try again.
        case interrupted
        /// The sync ran, but some step did not succeed; `summary` says which.
        case incomplete
    }

    public let reason: Reason
    public let summary: SyncSummary?

    public init(reason: Reason, summary: SyncSummary? = nil) {
        self.reason = reason
        self.summary = summary
    }

    /// Whether the way forward is pairing again.
    public var needsPairing: Bool { reason == .needsPairing }

    public var message: String {
        switch reason {
        case .ring(let problem): problem.message
        case .needsPairing: RingAccessProblem.unknownIdentifier.message
        case .interrupted: "The sync was interrupted when Digit went to the background. Try again."
        case .incomplete: "The sync didn't finish everything. Details below; syncing again usually completes it."
        }
    }
}
