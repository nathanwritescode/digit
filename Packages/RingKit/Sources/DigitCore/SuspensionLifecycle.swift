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
/// refused, and it returns only when the calls in progress have finished. Announcing while the
/// ring is in use aborts that work (a sync possibly between logging a clock-set and sending it),
/// so:
/// - ring work (a sync, pairing or forgetting) runs inside a background task, begun when it
///   starts, and begun again if it is still running when the scene becomes active or enters the
///   background without one (a task that expired earlier is not replaced by iOS);
/// - the suspension is announced only while the app is in the background, and only when no ring
///   work is running (on entering the background, or when the work ends) or a background task is
///   about to expire (its expiration handler);
/// - on the normal path a task is ended only after the announcement has returned (or straight
///   away when the app is in the foreground and nothing needs announcing);
/// - every announcement first tells the store synchronously (`announce`,
///   `RingStore.announceSuspension()`: new calls are refused and GRDB interrupts its writers), then
///   starts the drain (`suspend`, `RingStore.appWillSuspend()`, which announces again idempotently
///   and waits for calls in progress);
/// - an expiration handler announces synchronously, starts the drain and then ends its own task at
///   once, because iOS kills an app that has not ended a task by the time it expires. The store
///   has been told before the task ends, but a write between two statements may still hold its
///   lock until its next statement, so ending the task before the drain finishes is best effort:
///   since #69 ingestion's write is a short apply step, so the drain is normally brief;
/// - becoming active resumes the store, before any store call. The drain's own announcement runs
///   after a hop off the main actor, so it can land after a later activation; it then refuses
///   foreground calls briefly until its continuation resumes the store again (unless a later
///   background period has announced). Rare, and it recovers by itself.
///
/// Every token is ended exactly once, whichever path gets there first. The announcement is never
/// cancelled (`appWillSuspend()` throws only then). A sync still running at expiration is aborted
/// by the announcement: its store calls are refused, and it ends in the "interrupted" state when
/// its ring calls return (no cancellation yet, #52).
///
/// The process-wide functions are injected (`suspend`, `resume`), so tests never call them.
/// Unverified on a device: whether iOS spares the app (no `0xdead10cc`), including at an expiry
/// during the ring phase and during ingestion (the hardware check on #53).
@MainActor
public final class SuspensionLifecycle: RingWorkLifecycle {
    private let isRingInUse: @MainActor () -> Bool
    private let tasks: any BackgroundTaskRunner
    private let announceNow: @MainActor () -> Void
    private let suspend: @Sendable () async -> Void
    private let resume: @MainActor () -> Void

    /// Whether the scene is in the background.
    public private(set) var isInBackground = false
    /// Background tasks begun and not yet ended, by an id of this type's own (the handler of a task
    /// is created before its token exists).
    private var liveTasks: [Int: BackgroundTaskToken] = [:]
    private var lastTaskID = 0
    /// The task the running ring work holds, until it is ended or handed to an announcement.
    private var workTask: Int?
    /// Whether the suspension was announced since the scene last became active. A second
    /// announcement would be harmless, but there is nothing for it to do.
    private var hasAnnounced = false
    /// Bumped on every activation, so an announcement that returns afterwards knows to resume.
    private var activation = 0

    /// - Parameters:
    ///   - isRingInUse: whether a sync, pairing or forgetting is in progress now.
    ///   - announce: tells the store synchronously (`RingStore.announceSuspension()` in the app).
    ///   - suspend: the drain: announces (idempotently) and returns when the store is quiet
    ///     (`RingStore.appWillSuspend()` in the app; its `CancellationError` cannot happen, since
    ///     it is never cancelled).
    ///   - resume: `RingStore.appDidResume()` in the app. Safe to call more than once.
    public init(
        isRingInUse: @escaping @MainActor () -> Bool,
        backgroundTasks: any BackgroundTaskRunner,
        announce: @escaping @MainActor () -> Void,
        suspend: @escaping @Sendable () async -> Void,
        resume: @escaping @MainActor () -> Void
    ) {
        self.isRingInUse = isRingInUse
        self.tasks = backgroundTasks
        self.announceNow = announce
        self.suspend = suspend
        self.resume = resume
    }

    // MARK: Scene

    /// The scene became active: resume the store before anything uses it, and make sure running
    /// ring work holds a background task again.
    public func sceneDidBecomeActive() {
        isInBackground = false
        hasAnnounced = false
        activation += 1
        resume()
        if isRingInUse() { holdWorkTask() }
    }

    /// The scene moved to the background. With no ring work running, announce now, under a short
    /// background task of its own so the app is not suspended before the announcement returns.
    /// With work running, make sure it holds a task and wait for it to end or for the task to
    /// expire.
    public func sceneDidEnterBackground() {
        guard !isInBackground else { return }
        isInBackground = true
        if isRingInUse() {
            holdWorkTask()
            return
        }
        announce(thenEnd: beginTask())
    }

    // MARK: Ring work

    public func ringWorkWillStart() {
        if let workTask { end(workTask) }
        workTask = nil
        holdWorkTask()
    }

    public func ringWorkDidEnd() {
        let id = workTask
        workTask = nil
        if isInBackground, !hasAnnounced {
            announce(thenEnd: id)
        } else if let id {
            end(id)
        }
    }

    private func holdWorkTask() {
        guard workTask == nil else { return }
        workTask = beginTask()
    }

    // MARK: Tasks

    /// Begins a task whose expiration announces synchronously, starts the drain and ends the task
    /// at once.
    private func beginTask() -> Int? {
        lastTaskID += 1
        let id = lastTaskID
        guard let token = tasks.begin(expirationHandler: { [weak self] in self?.taskWillExpire(id) }) else {
            return nil
        }
        liveTasks[id] = token
        return id
    }

    private func taskWillExpire(_ id: Int) {
        guard liveTasks[id] != nil else { return }
        if workTask == id { workTask = nil }
        // Aborts ring work still running; a sync then ends as "interrupted". The store is told
        // synchronously inside `announce` before this returns.
        if isInBackground, !hasAnnounced { announce(thenEnd: nil) }
        // Synchronously, as iOS requires; the drain may still be running (best effort).
        end(id)
    }

    /// Ends a task once; later calls for the same id do nothing.
    private func end(_ id: Int) {
        if let token = liveTasks.removeValue(forKey: id) { tasks.end(token) }
    }

    // MARK: Announcing

    /// Tells the store now, then drains, and ends task `id` once the drain has returned (unless its
    /// expiration ended it first). A second drain while one waits is harmless (both return when the
    /// calls drain), and a resume ends every wait.
    private func announce(thenEnd id: Int?) {
        hasAnnounced = true
        announceNow()
        let suspend = self.suspend
        let activationAtStart = activation
        Task { [weak self] in
            await suspend()
            guard let self else { return }
            // The drain's own announcement may have reached the store after a resume the app made
            // in the meantime (it hops to another thread first). Resume again unless a later
            // background period has announced on its own: the store must not stay suspended while
            // the app is active, or while ring work it never meant to abort runs in the background.
            let overtaken = self.activation != activationAtStart
            if !self.isInBackground || (overtaken && !self.hasAnnounced) { self.resume() }
            if let id { self.end(id) }
        }
    }
}
