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
            // `appWillSuspend()` throws only if its task is cancelled, and this one never is.
            suspend: { try? await RingStore.appWillSuspend() },
            resume: { RingStore.appDidResume() })
        state = .ready(model)
        // Catch up with the scene, which may have changed while the store was opening.
        if let scenePhase { apply(scenePhase, to: model) }
        await model.load()
    }

    func scenePhaseChanged(_ phase: ScenePhase) {
        scenePhase = phase
        if case .ready(let model) = state { apply(phase, to: model) }
    }

    private func apply(_ phase: ScenePhase, to model: DigitModel) {
        switch phase {
        case .active: model.lifecycle.sceneDidBecomeActive()
        case .background: model.lifecycle.sceneDidEnterBackground()
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
