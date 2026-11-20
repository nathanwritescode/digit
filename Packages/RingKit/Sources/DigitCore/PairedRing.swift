import Foundation
import Observation
import RingStore

/// What ring work tells the app's lifecycle, so it runs inside a background task and the store's
/// suspension is announced only once it has ended (`SuspensionLifecycle`).
@MainActor
public protocol RingWorkLifecycle: AnyObject {
    func ringWorkWillStart()
    func ringWorkDidEnd()
}

/// What both models share: the ring Digit is paired with, and which flow is using the ring.
///
/// **One ring in v1.** The store's oldest ring is the paired ring. Pairing again attaches the new
/// identifier to it rather than registering a second ring, so its history is never split.
///
/// **One flow at a time.** Pairing, syncing and forgetting each claim the ring for their whole
/// run; a second claim is refused, so the two models never use the ring (or its one connection)
/// at the same time.
@MainActor
@Observable
public final class PairedRing {
    public enum Activity: Sendable, Equatable {
        case pairing, syncing, forgetting
    }

    /// The paired ring as last read from the store, or `nil` when the store holds none.
    public internal(set) var ring: StoredRing?
    /// Whether the store has been read. Until then nothing can start.
    public internal(set) var isLoaded = false
    /// The flow using the ring, if any.
    public private(set) var activity: Activity?
    /// Called when a flow that held the ring ends (after the lifecycle hears of it), so the data
    /// screens can reload: a sync, even a failed one, can have added rows, and pairing can have
    /// stored the first ring.
    @ObservationIgnored var workDidEnd: (@MainActor (Activity) -> Void)?

    public init(ring: StoredRing? = nil, isLoaded: Bool = false) {
        self.ring = ring
        self.isLoaded = isLoaded
    }

    /// Whether a ring with a saved identifier is paired, so it can be synced.
    public var canSync: Bool { ring?.peripheralIdentifier != nil }

    /// Claims the ring for `activity`. False, and nothing changes, if another flow holds it or the
    /// store has not been read.
    func claim(_ activity: Activity) -> Bool {
        guard isLoaded, self.activity == nil else { return false }
        self.activity = activity
        return true
    }

    func release() {
        activity = nil
    }

    /// Runs `body` with the ring claimed for `activity`, inside the lifecycle's background task.
    /// Does nothing if the claim is refused. The claim is released before the lifecycle hears the
    /// work ended, so the lifecycle sees no ring work running when it decides to announce.
    func using(_ activity: Activity, lifecycle: any RingWorkLifecycle, _ body: () async -> Void) async {
        guard claim(activity) else { return }
        lifecycle.ringWorkWillStart()
        await body()
        release()
        lifecycle.ringWorkDidEnd()
        workDidEnd?(activity)
    }

    /// Reads the paired ring from the store: the oldest ring, since v1 has one.
    public func load(from registry: any RingRegistry) async throws {
        ring = try await registry.rings().first
        isLoaded = true
    }
}
