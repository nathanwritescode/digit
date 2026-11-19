import Foundation
import Observation
import RingStore
import RingSync

/// Pairing: find rings nearby, connect to the one the owner picks, read its firmware, and save it
/// (with this phone's identifier for it) in the store. Also "Forget this ring".
///
/// One ring in v1: when the store already holds a ring, pairing attaches the new identifier to
/// that ring instead of registering a second one, so history is not split when the identifier
/// changes or the app is paired again. `confirmationMessage` says so before the owner confirms.
///
/// Nothing here logs or prints, and no text it produces holds a ring name, an identifier or a
/// health value.
@MainActor
@Observable
public final class PairingModel {
    public enum State: Sendable, Equatable {
        case idle
        case scanning
        /// Candidates, strongest first.
        case found([RingCandidate])
        case nothingFound
        case connecting
        case paired
        case failed(PairingFailure)
    }

    public private(set) var state = State.idle
    /// Why the last "Forget this ring" failed, if it did; cleared by the next attempt.
    public private(set) var forgetFailure: PairingFailure?

    private let pairedRing: PairedRing
    private let services: DigitServices

    public init(pairedRing: PairedRing, services: DigitServices) {
        self.pairedRing = pairedRing
        self.services = services
    }

    /// Whether a scan or a pick can start now (no sync or other flow is using the ring).
    public var canStart: Bool { pairedRing.isLoaded && pairedRing.activity == nil }

    /// Whether pairing will attach to a ring already in the store rather than register one.
    public var replacesStoredRing: Bool { pairedRing.ring != nil }

    /// What the owner confirms before a pick is paired.
    public var confirmationMessage: String {
        replacesStoredRing
            ? "This replaces the ring Digit is paired with; its history is kept."
            : "Digit will connect to this ring and remember it on this iPhone."
    }

    /// Plain words for the current state. Holds no name, identifier or health value.
    public var statusText: String {
        switch state {
        case .idle: "Put your ring on or next to this iPhone, then look for it."
        case .scanning: "Looking for rings nearby…"
        case .found(let candidates): candidates.count == 1 ? "Found one ring. Choose it to pair." : "Found \(candidates.count) rings. Choose yours."
        case .nothingFound:
            "No ring found. Make sure it's charged, close to this iPhone and not connected to another phone or app, then try again."
        case .connecting: "Connecting to the ring…"
        case .paired: "Ring paired."
        case .failed(let failure): failure.message
        }
    }

    /// Scans for rings. Does nothing while another flow uses the ring.
    public func findRings() async {
        guard pairedRing.claim(.pairing) else { return }
        defer { pairedRing.release() }
        state = .scanning
        do {
            let heard = try await services.ringAccess.discoverRings(for: SyncSettings.discoveryDuration)
            // Strongest first. The transport already orders by RSSI; this stable sort by the coarse
            // indication keeps its order within a level and guarantees the order shown.
            let candidates = heard.enumerated()
                .sorted { $0.element.signal != $1.element.signal ? $0.element.signal > $1.element.signal : $0.offset < $1.offset }
                .map(\.element)
            state = candidates.isEmpty ? .nothingFound : .found(candidates)
        } catch {
            state = .failed(.ring(error))
        }
    }

    /// Connects to `candidate`, reads its firmware, saves it, then disconnects. Does nothing while
    /// another flow uses the ring. Nothing is saved unless the device information was read.
    public func pair(_ candidate: RingCandidate) async {
        guard pairedRing.claim(.pairing) else { return }
        defer { pairedRing.release() }
        state = .connecting
        let connected: any ConnectedRing
        do {
            connected = try await services.ringAccess.connect(identifier: candidate.identifier)
        } catch {
            state = .failed(.ring(error))
            return
        }
        let details: RingDeviceDetails
        do {
            details = try await connected.deviceDetails()
        } catch {
            await connected.disconnect()
            state = .failed(.ring(error))
            return
        }
        let outcome = await save(identifier: candidate.identifier, details: details)
        await connected.disconnect()
        state = outcome
    }

    /// Back to the start, for a new scan.
    public func reset() {
        guard pairedRing.activity != .pairing else { return }
        state = .idle
    }

    /// Clears the saved identifier, so Digit stops connecting to the ring. Deletes nothing from
    /// the store and sends nothing to the ring. Does nothing while another flow uses the ring.
    public func forget() async {
        guard let ring = pairedRing.ring, pairedRing.claim(.forgetting) else { return }
        defer { pairedRing.release() }
        forgetFailure = nil
        do {
            try await services.registry.setPeripheralIdentifier(nil, forRing: ring.id)
            pairedRing.ring = try await services.registry.ring(id: ring.id)
            state = .idle
        } catch {
            forgetFailure = PairingFailure(storeError: error)
        }
    }

    private func save(identifier: UUID, details: RingDeviceDetails) async -> State {
        let registry = services.registry
        do {
            // Read again rather than trust what was loaded: the store is the source of truth.
            let ringID: UUID
            if let existing = try await registry.rings().first {
                ringID = existing.id
                if existing.firmware != details.firmware {
                    try await registry.updateRing(id: existing.id, model: existing.model, firmware: details.firmware)
                }
            } else {
                ringID = try await registry.registerRing(
                    protocolFamily: SyncSettings.protocolFamily, model: SyncSettings.model,
                    firmware: details.firmware, now: services.now()
                ).id
            }
            try await registry.setPeripheralIdentifier(identifier, forRing: ringID)
            pairedRing.ring = try await registry.ring(id: ringID)
            return .paired
        } catch {
            // A ring may have been registered before the failure; show what the store now holds.
            if let rings = try? await registry.rings() { pairedRing.ring = rings.first }
            return .failed(PairingFailure(storeError: error))
        }
    }
}

/// Why pairing (or forgetting) failed.
public enum PairingFailure: Sendable, Equatable {
    case ring(RingAccessProblem)
    /// The store refused because the app was being suspended. Try again.
    case interrupted
    /// The identifier is held by another stored ring. Cannot happen with one ring in v1 (pairing
    /// attaches to the stored ring); reported rather than trapped.
    case identifierHeldByAnotherRing
    /// Any other store failure, reduced to what is safe to show.
    case store(SyncError)

    init(storeError error: any Error) {
        switch error as? RingStoreError {
        case .suspended: self = .interrupted
        case .peripheralIdentifierInUse: self = .identifierHeldByAnotherRing
        default: self = .store(SyncError(error))
        }
    }

    public var message: String {
        switch self {
        case .ring(let problem): problem.message
        case .interrupted: "Pairing was interrupted when Digit went to the background. Try again."
        case .identifierHeldByAnotherRing:
            "Digit couldn't save the ring: another stored ring already uses it. This is a bug in Digit."
        case .store(let error): "Digit couldn't save the ring (\(error.description)). Try again."
        }
    }
}
