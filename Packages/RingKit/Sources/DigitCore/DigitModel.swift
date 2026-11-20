import Foundation
import Observation
import RingStore
import RingSync

/// The app's root model: the paired ring, the pairing and sync models, the data screens' models,
/// and the store's suspension around background tasks, wired together.
///
/// The data models reload when the paired ring has been read, when a sync or pairing ends (done or
/// failed: a partial sync can still add rows), when the scene becomes active, and on pull to
/// refresh. Nothing observes the store yet (no `ValueObservation`), so a store suspension cannot
/// end an observation that would then need re-subscribing.
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
    /// The Sleep tab's cards.
    public let overview: SleepOverviewModel
    /// Every night of the window, behind "See all nights".
    public let sleep: SleepListModel
    public let days: DailyModel
    /// Why the paired ring could not be read from the store, if it could not.
    public private(set) var loadFailure: String?

    @ObservationIgnored private let services: DigitServices
    /// The night screen on display, if any, so it reloads with the lists.
    @ObservationIgnored private weak var nightDetail: NightDetailModel?

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
        self.overview = SleepOverviewModel(
            pairedRing: pairedRing, history: services.history, now: services.now, timeZone: services.timeZone)
        self.sleep = SleepListModel(
            pairedRing: pairedRing, history: services.history, now: services.now, timeZone: services.timeZone)
        self.days = DailyModel(
            pairedRing: pairedRing, history: services.history, now: services.now, timeZone: services.timeZone)
        pairedRing.workDidEnd = { [weak self] _ in
            guard let self else { return }
            Task { await self.reloadData() }
        }
    }

    /// The model for one night's screen, opened from the Sleep tab or the list of nights. Previous
    /// and next move between the main sleeps the list holds. When the night has moved, the Sleep
    /// tab and the list reload.
    public func nightDetail(startingAt start: Date) -> NightDetailModel? {
        guard let ringID = pairedRing.ring?.id else { return nil }
        let mains = (sleep.state.value ?? []).flatMap(\.nights).map(\.summary).filter(\.isMainSleep).map(\.start)
        let model = NightDetailModel(
            ringID: ringID, start: start, history: services.history, mainSleepStarts: mains
        ) { [weak self] in
            guard let self else { return }
            Task {
                await self.overview.reload()
                await self.sleep.reload()
            }
        }
        nightDetail = model
        return model
    }

    /// Reloads every data model. Reads are short and are refused cleanly by a suspension, so they
    /// are not ring work and take no background task.
    public func reloadData() async {
        await overview.reload()
        await sleep.reload()
        await days.reload()
        await nightDetail?.reload()
    }

    /// Reads the paired ring from the store. Call once the store is open, before telling the
    /// lifecycle the scene is in the background (an announcement would refuse it); a failed load
    /// is retried when the scene becomes active.
    ///
    /// It does not read the data screens' data: the app applies the scene phase straight after,
    /// and the first activation (at once, or later for a launch that stays in the background)
    /// reloads them, so a cold launch reads them once.
    public func load() async {
        do {
            try await pairedRing.load(from: services.registry)
            loadFailure = nil
        } catch {
            loadFailure = Self.loadFailureMessage(error)
        }
    }

    /// "Try Again" after a failed load: loads, then reloads the data screens, since no activation
    /// follows a tap.
    public func retryLoad() async {
        await load()
        if loadFailure == nil { await reloadData() }
    }

    /// The scene became active: resume the store at once, then retry a load that failed, or reload
    /// the data screens (in a task of its own, so the resume is never delayed behind it). This is
    /// also how a data load refused by a suspension is retried.
    public func sceneDidBecomeActive() {
        lifecycle.sceneDidBecomeActive()
        if loadFailure != nil {
            Task { await retryLoad() }
        } else if pairedRing.isLoaded {
            Task { await reloadData() }
        }
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
