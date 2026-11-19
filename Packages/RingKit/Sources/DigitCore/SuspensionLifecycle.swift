import Foundation

/// A background task, as the app's platform provides it (`UIApplication.beginBackgroundTask` in
/// the app). Injected so tests can fake it.
@MainActor
public protocol BackgroundTaskRunner: AnyObject {
    /// Begins a task. `expirationHandler` runs on the main actor shortly before the system's time
    /// runs out. Returns `nil` if the system refused one.
    func begin(expirationHandler: @escaping @MainActor () -> Void) -> BackgroundTaskToken?
    func end(_ token: BackgroundTaskToken)
}

/// Names one background task.
public struct BackgroundTaskToken: Hashable, Sendable {
    public let rawValue: Int
    public init(rawValue: Int) { self.rawValue = rawValue }
}

/// Decides when to announce the store's suspension and when to end background tasks.
///
/// `RingStore.appWillSuspend()` is process-wide: from the announcement every store call is
/// refused, and it returns only when the calls in progress have finished. Announcing while a sync
/// runs aborts the sync (possibly between logging a clock-set and sending it), so:
/// - every sync runs inside a background task, begun when it starts (`syncWillStart`);
/// - the suspension is announced only while the app is in the background, and only when no sync
///   is running (on entering the background, or when a running sync ends) or a background task is
///   about to expire (its expiration handler);
/// - a background task is ended only after the announcement has returned (or straight away when
///   the app is in the foreground and nothing needs announcing);
/// - becoming active resumes the store, before any store call.
///
/// The announcement is never cancelled (`appWillSuspend()` throws only then). A sync still running
/// at expiration is aborted by the announcement: its store calls are refused, and it ends in the
/// "interrupted" state when its ring calls return (no cancellation yet, #52).
///
/// The process-wide functions are injected (`suspend`, `resume`), so tests never call them.
/// Unverified on a device: whether iOS spares the app (no `0xdead10cc`), and how long an
/// expiration handler may keep its task while the announcement drains (the hardware check on #53).
@MainActor
public final class SuspensionLifecycle: SyncLifecycle {
    private let isSyncRunning: @MainActor () -> Bool
    private let tasks: any BackgroundTaskRunner
    private let suspend: @Sendable () async -> Void
    private let resume: @MainActor () -> Void

    /// Whether the scene is in the background.
    public private(set) var isInBackground = false
    /// The task the running sync holds, until it is ended or handed to an announcement.
    private var syncTask: BackgroundTaskToken?
    /// Whether the suspension was announced since the scene last became active. A second
    /// announcement would be harmless, but there is nothing for it to do.
    private var hasAnnounced = false
    /// Announcements started and not yet returned (for tests and the app's diagnostics).
    public private(set) var announcementsInFlight = 0

    /// - Parameters:
    ///   - isSyncRunning: whether a sync is in progress now.
    ///   - suspend: announces the suspension and returns when the store is quiet
    ///     (`RingStore.appWillSuspend()` in the app; its `CancellationError` cannot happen, since
    ///     the announcement is never cancelled).
    ///   - resume: `RingStore.appDidResume()` in the app.
    public init(
        isSyncRunning: @escaping @MainActor () -> Bool,
        backgroundTasks: any BackgroundTaskRunner,
        suspend: @escaping @Sendable () async -> Void,
        resume: @escaping @MainActor () -> Void
    ) {
        self.isSyncRunning = isSyncRunning
        self.tasks = backgroundTasks
        self.suspend = suspend
        self.resume = resume
    }

    // MARK: Scene

    /// The scene became active: resume the store before anything uses it.
    public func sceneDidBecomeActive() {
        isInBackground = false
        hasAnnounced = false
        resume()
    }

    /// The scene moved to the background. With no sync running, announce now, under a short
    /// background task of its own so the app is not suspended before the announcement returns.
    /// With a sync running, wait for it to end or for its task to expire.
    public func sceneDidEnterBackground() {
        guard !isInBackground else { return }
        isInBackground = true
        guard !isSyncRunning() else { return }
        // Nothing more to do at expiration: the announcement is already under way, and the task
        // ends when it returns.
        announce(thenEnd: tasks.begin(expirationHandler: {}))
    }

    // MARK: Sync

    public func syncWillStart() {
        if let syncTask { tasks.end(syncTask) }
        syncTask = tasks.begin(expirationHandler: { [weak self] in self?.syncTaskWillExpire() })
    }

    public func syncDidEnd() {
        let token = syncTask
        syncTask = nil
        if isInBackground, !hasAnnounced {
            announce(thenEnd: token)
        } else if let token {
            tasks.end(token)
        }
    }

    private func syncTaskWillExpire() {
        guard let token = syncTask else { return }
        syncTask = nil
        // Aborts the sync if it is still running; it then ends as "interrupted".
        announce(thenEnd: token)
    }

    // MARK: Announcing

    /// Announces, and ends `token` once the announcement has returned. A second announcement while
    /// one waits is harmless (both return when the calls drain), and a resume ends every wait.
    private func announce(thenEnd token: BackgroundTaskToken?) {
        hasAnnounced = true
        announcementsInFlight += 1
        let suspend = self.suspend
        let tasks = self.tasks
        Task { [weak self] in
            await suspend()
            self?.announcementsInFlight -= 1
            if let token { tasks.end(token) }
        }
    }
}
