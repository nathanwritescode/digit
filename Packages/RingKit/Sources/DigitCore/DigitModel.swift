import Foundation
import Observation
import RingStore

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
        suspend: @escaping @Sendable () async -> Void,
        resume: @escaping @MainActor () -> Void
    ) {
        self.services = services
        let pairedRing = PairedRing()
        let sync = SyncModel(pairedRing: pairedRing, services: services)
        self.pairedRing = pairedRing
        self.pairing = PairingModel(pairedRing: pairedRing, services: services)
        self.sync = sync
        self.lifecycle = SuspensionLifecycle(
            isSyncRunning: { [weak sync] in sync?.isRunning ?? false },
            backgroundTasks: backgroundTasks, suspend: suspend, resume: resume)
        sync.lifecycle = lifecycle
    }

    /// Reads the paired ring from the store. Call once the store is open, and again to retry.
    public func load() async {
        do {
            try await pairedRing.load(from: services.registry)
            loadFailure = nil
        } catch {
            loadFailure = PairingFailure(storeError: error).message
        }
    }
}
