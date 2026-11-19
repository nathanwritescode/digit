import Foundation
import Observation
import RingStore
import RingSync

/// The app's root model: the paired ring, the pairing and sync models, and the store's
/// suspension around background tasks, wired together.
///
/// The app holds one for the life of the process, together with the store it was built from (see
/// `DigitServices.init(ringAccess:store:)`): releasing the last reference to a `RingStore` closes
/// its pool, and closing the last WAL connection checkpoints under an exclusive lock.
@MainActor
@Observable
public final class DigitModel {
    public let pairedRing: PairedRing
    public let pairing: PairingModel
    public let sync: SyncModel
    public let lifecycle: SuspensionLifecycle
    /// Why the paired ring could not be read from the store, if it could not.
    public private(set) var loadFailure: String?

    private let services: DigitServices

    public init(
        services: DigitServices,
        backgroundTasks: any BackgroundTaskRunner,
        announce: @escaping @MainActor () -> Void,
        suspend: @escaping @Sendable () async -> Void,
        resume: @escaping @MainActor () -> Void
    ) {
        self.services = services
        let pairedRing = PairedRing()
        let lifecycle = SuspensionLifecycle(
            isRingInUse: { [weak pairedRing] in pairedRing?.activity != nil },
            backgroundTasks: backgroundTasks, announce: announce, suspend: suspend, resume: resume)
        self.pairedRing = pairedRing
        self.lifecycle = lifecycle
        self.pairing = PairingModel(pairedRing: pairedRing, services: services, lifecycle: lifecycle)
        self.sync = SyncModel(pairedRing: pairedRing, services: services, lifecycle: lifecycle)
    }

    /// Reads the paired ring from the store. Call once the store is open, before telling the
    /// lifecycle the scene is in the background (an announcement would refuse it); a failed load
    /// is retried when the scene becomes active.
    public func load() async {
        do {
            try await pairedRing.load(from: services.registry)
            loadFailure = nil
        } catch {
            loadFailure = Self.loadFailureMessage(error)
        }
    }

    /// The scene became active: resume the store at once, then retry a load that failed (in a task
    /// of its own, so the resume is never delayed behind it).
    public func sceneDidBecomeActive() {
        lifecycle.sceneDidBecomeActive()
        if loadFailure != nil { Task { await load() } }
    }

    public func sceneDidEnterBackground() {
        lifecycle.sceneDidEnterBackground()
    }

    static func loadFailureMessage(_ error: any Error) -> String {
        if (error as? RingStoreError) == .suspended {
            return "Digit was interrupted while reading its data. It tries again when you return to the app."
        }
        return "Digit couldn't read its data on this iPhone (\(SyncError(error).description)). It tries again when you return to the app."
    }
}
