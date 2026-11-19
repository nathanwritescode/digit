import DigitCore
import Foundation
import Testing

// When the store's suspension is announced and background tasks end. The process-wide
// `RingStore.appWillSuspend()` and `appDidResume()` are never called here: fakes stand in for them.

/// Background tasks, recorded. `begin` hands out increasing tokens.
@MainActor
final class FakeBackgroundTasks: BackgroundTaskRunner {
    private(set) var events: [String] = []
    private(set) var handlers: [BackgroundTaskToken: @MainActor () -> Void] = [:]
    private var next = 0

    func begin(expirationHandler: @escaping @MainActor () -> Void) -> BackgroundTaskToken? {
        next += 1
        let token = BackgroundTaskToken(rawValue: next)
        handlers[token] = expirationHandler
        events.append("begin \(next)")
        return token
    }

    func end(_ token: BackgroundTaskToken) {
        events.append("end \(token.rawValue)")
    }

    func expire(_ rawValue: Int) {
        handlers[BackgroundTaskToken(rawValue: rawValue)]?()
    }
}

/// The announcement and the resume, recorded; the announcement returns only once released, as
/// `appWillSuspend()` returns only when the store's calls have drained.
/// `@unchecked Sendable`: guarded by `lock`.
final class FakeSuspension: @unchecked Sendable {
    private let lock = NSLock()
    private var log: [String] = []
    let gate = Gate()

    var events: [String] { lock.withLock { log } }
    func record(_ event: String) { lock.withLock { log.append(event) } }

    func suspend() async {
        record("announce")
        await gate.wait()
        record("announced")
    }
}

@MainActor
private final class Harness {
    let tasks = FakeBackgroundTasks()
    let suspension = FakeSuspension()
    var syncRunning = false
    var resumes = 0
    lazy var lifecycle = SuspensionLifecycle(
        isSyncRunning: { [unowned self] in self.syncRunning },
        backgroundTasks: tasks,
        suspend: { [suspension] in await suspension.suspend() },
        resume: { [unowned self] in self.resumes += 1 })

    func startSync() {
        syncRunning = true
        lifecycle.syncWillStart()
    }

    func endSync() {
        syncRunning = false
        lifecycle.syncDidEnd()
    }
}

@MainActor
@Suite(.timeLimit(.minutes(1)))
struct SuspensionLifecycleTests {
    @Test func backgroundWithNoSyncAnnouncesAtOnceAndEndsTheTaskAfterwards() async {
        let h = Harness()
        h.lifecycle.sceneDidEnterBackground()
        #expect(await eventually { h.suspension.events == ["announce"] })
        // Not ended while the announcement waits.
        #expect(h.tasks.events == ["begin 1"])
        h.suspension.gate.open()
        #expect(await eventually { h.tasks.events == ["begin 1", "end 1"] })
        #expect(h.suspension.events == ["announce", "announced"])
    }

    @Test func backgroundDuringASyncWaitsForTheSyncToEnd() async {
        let h = Harness()
        h.startSync()
        h.lifecycle.sceneDidEnterBackground()
        try? await Task.sleep(for: .milliseconds(20))
        #expect(h.suspension.events.isEmpty)
        #expect(h.tasks.events == ["begin 1"])

        h.endSync()
        #expect(await eventually { h.suspension.events == ["announce"] })
        #expect(h.tasks.events == ["begin 1"])
        h.suspension.gate.open()
        #expect(await eventually { h.tasks.events == ["begin 1", "end 1"] })
    }

    @Test func expirationDuringASyncAnnouncesFirst() async {
        let h = Harness()
        h.startSync()
        h.lifecycle.sceneDidEnterBackground()
        h.tasks.expire(1)
        #expect(await eventually { h.suspension.events == ["announce"] })
        #expect(h.tasks.events == ["begin 1"])
        h.suspension.gate.open()
        #expect(await eventually { h.tasks.events == ["begin 1", "end 1"] })

        // The aborted sync ends later: no second announcement, and the task is not ended again.
        h.endSync()
        try? await Task.sleep(for: .milliseconds(20))
        #expect(h.suspension.events == ["announce", "announced"])
        #expect(h.tasks.events == ["begin 1", "end 1"])
    }

    @Test func aSyncEndingInTheForegroundEndsItsTaskWithoutAnnouncing() async {
        let h = Harness()
        h.lifecycle.sceneDidBecomeActive()
        h.startSync()
        h.endSync()
        #expect(h.tasks.events == ["begin 1", "end 1"])
        try? await Task.sleep(for: .milliseconds(20))
        #expect(h.suspension.events.isEmpty)
    }

    @Test func becomingActiveResumes() {
        let h = Harness()
        h.lifecycle.sceneDidBecomeActive()
        #expect(h.resumes == 1)
        h.lifecycle.sceneDidEnterBackground()
        h.suspension.gate.open()
        h.lifecycle.sceneDidBecomeActive()
        #expect(h.resumes == 2)
        #expect(!h.lifecycle.isInBackground)
    }

    @Test func returningBeforeTheSyncEndsDoesNotAnnounce() async {
        let h = Harness()
        h.startSync()
        h.lifecycle.sceneDidEnterBackground()
        h.lifecycle.sceneDidBecomeActive()
        h.endSync()
        #expect(h.tasks.events == ["begin 1", "end 1"])
        try? await Task.sleep(for: .milliseconds(20))
        #expect(h.suspension.events.isEmpty)
    }
}

// MARK: Through the models

@MainActor
@Suite(.timeLimit(.minutes(1)))
struct SyncInsideABackgroundTaskTests {
    @Test func aSyncRunsInsideABackgroundTaskAndAnnouncesWhenItEndsInTheBackground() async throws {
        let f = try Fixture()
        try await f.storePairedRing()
        let tasks = FakeBackgroundTasks()
        let suspension = FakeSuspension()
        suspension.gate.open()
        let sync = f.sync
        let lifecycle = SuspensionLifecycle(
            isSyncRunning: { sync.isRunning }, backgroundTasks: tasks,
            suspend: { await suspension.suspend() }, resume: {})
        f.sync.lifecycle = lifecycle
        let gate = Gate()
        f.access.connectGate = gate

        let done = Flag()
        Task { await sync.sync(); done.value = true }
        #expect(await eventually { f.log.count("connect") == 1 })
        #expect(tasks.events == ["begin 1"])
        lifecycle.sceneDidEnterBackground()
        #expect(suspension.events.isEmpty)

        gate.open()
        #expect(await eventually { done.value })
        #expect(await eventually { tasks.events == ["begin 1", "end 1"] })
        #expect(suspension.events == ["announce", "announced"])
    }
}

/// A flag a test's task sets on the main actor.
@MainActor
final class Flag {
    var value = false
}
