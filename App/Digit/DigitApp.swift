import DigitCore
import RingStore
import SwiftUI

@main
struct DigitApp: App {
    @State private var root = AppRoot()
    @Environment(\.scenePhase) private var scenePhase

    var body: some Scene {
        WindowGroup {
            ContentView(root: root)
                .task { await root.start() }
        }
        .onChange(of: scenePhase, initial: true) { _, phase in
            root.scenePhaseChanged(phase)
        }
    }
}

/// Opens the store at launch and holds it, with the app's model, for the life of the process:
/// releasing the last reference to the store would close its pool, and closing the last WAL
/// connection checkpoints under an exclusive lock.
@Observable
final class AppRoot {
    enum State {
        case opening
        case ready(DigitModel)
        case failed(String)
    }

    private(set) var state = State.opening
    /// Never released while the app runs (see the type's comment).
    private var store: RingStore?
    private var scenePhase: ScenePhase?
    private var started = false
    private let backgroundTasks = UIKitBackgroundTasks()

    func start() async {
        guard !started else { return }
        started = true
        guard let container = FileManager.default.containerURL(
            forSecurityApplicationGroupIdentifier: DatabaseLocation.appGroupIdentifier)
        else {
            state = .failed("Digit's storage on this iPhone isn't available. Try reinstalling the app.")
            return
        }
        let store: RingStore
        do {
            // Off the main actor: the directory and the store are opened on background threads.
            store = try await DatabaseLocation.openStore(inAppGroupContainer: container)
        } catch {
            // Only the sanitised text: a raw error can hold the container's path.
            state = .failed(DatabaseLocation.openFailureMessage(error))
            return
        }
        self.store = store
        let model = DigitModel(
            services: DigitServices(ringAccess: BluetoothRingAccess(), store: store),
            backgroundTasks: backgroundTasks,
            announce: { RingStore.announceSuspension() },
            // Drain only: the lifecycle has already announced, and a drain that never announces
            // cannot suspend the store again after a resume. It throws only if its task is
            // cancelled, and this one never is.
            suspend: { try? await RingStore.drainSuspension() },
            resume: { RingStore.appDidResume() })
        state = .ready(model)
        // Scene changes that came while the store was opening are applied after the load: had the
        // scene gone to the background meanwhile, applying that first would announce the
        // suspension and refuse the load. A load refused by a later suspension is retried when the
        // scene becomes active (`DigitModel.sceneDidBecomeActive()`).
        await model.load()
        if let scenePhase { apply(scenePhase, to: model) }
    }

    func scenePhaseChanged(_ phase: ScenePhase) {
        scenePhase = phase
        if case .ready(let model) = state { apply(phase, to: model) }
    }

    private func apply(_ phase: ScenePhase, to model: DigitModel) {
        switch phase {
        // Resumes the store at once, then retries a failed load.
        case .active: model.sceneDidBecomeActive()
        case .background: model.sceneDidEnterBackground()
        case .inactive: break
        @unknown default: break
        }
    }
}

/// `UIApplication`'s background tasks for `SuspensionLifecycle`.
final class UIKitBackgroundTasks: BackgroundTaskRunner {
    func begin(expirationHandler: @escaping @MainActor () -> Void) -> BackgroundTaskToken? {
        let identifier = UIApplication.shared.beginBackgroundTask(withName: "Ring sync") {
            expirationHandler()
        }
        return identifier == .invalid ? nil : BackgroundTaskToken(rawValue: identifier.rawValue)
    }

    func end(_ token: BackgroundTaskToken) {
        UIApplication.shared.endBackgroundTask(UIBackgroundTaskIdentifier(rawValue: token.rawValue))
    }
}
