import DigitCore
import Foundation
import Testing

// When the store's suspension is announced and background tasks end. The process-wide
// `RingStore.appWillSuspend()` and `appDidResume()` are never called here: fakes stand in for them.

/// Background tasks, recorded. `begin` hands out increasing tokens.
@MainActor
final class FakeBackgroundTasks: BackgroundTaskRunner {
    private(set) var events: [String] = []
    private var handlers: [Int: @MainActor () -> Void] = [:]
    private var next = 0

    func begin(expirationHandler: @escaping @MainActor () -> Void) -> BackgroundTaskToken? {
        next += 1
        handlers[next] = expirationHandler
        events.append("begin \(next)")
        return BackgroundTaskToken(rawValue: next)
    }

    func end(_ token: BackgroundTaskToken) {
        events.append("end \(token.rawValue)")
    }

    /// Runs task `rawValue`'s expiration handler, as iOS does shortly before its time runs out.
    func expire(_ rawValue: Int) {
        handlers[rawValue]?()
    }
}

/// The announcement and the resume, recorded in one sequence. The announcement first gives up its
/// thread (as the real one hops off the main actor before announcing), then records `announce`,
/// and returns (`announced`) only once released, as `appWillSuspend()` returns only when the
/// store's calls have drained. `@unchecked Sendable`: guarded by `lock`.
final class FakeSuspension: @unchecked Sendable {
    private let lock = NSLock()
    private var log: [String] = []
    let gate = Gate()

    var events: [String] { lock.withLock { log } }
    func record(_ event: String) { lock.withLock { log.append(event) } }
    func count(_ event: String) -> Int { events.filter { $0 == event }.count }

    func suspend() async {
        await Task.yield()
        record("announce")
        await gate.wait()
        record("announced")
    }
}

@MainActor
private final class Harness {
    let tasks = FakeBackgroundTasks()
    let suspension = FakeSuspension()
    var ringInUse = false
    lazy var lifecycle = SuspensionLifecycle(
        isRingInUse: { [unowned self] in self.ringInUse },
        backgroundTasks: tasks,
        suspend: { [suspension] in await suspension.suspend() },
        resume: { [suspension] in suspension.record("resume") })

    func startWork() {
        ringInUse = true
        lifecycle.ringWorkWillStart()
    }

    func endWork() {
        ringInUse = false
        lifecycle.ringWorkDidEnd()
    }

    /// Gives queued work a chance to run, for checks that something did not happen.
    func settle() async {
        try? await Task.sleep(for: .milliseconds(20))
    }
}

@MainActor
@Suite(.timeLimit(.minutes(1)))
struct SuspensionLifecycleTests {
    @Test func backgroundWithNoWorkAnnouncesAtOnceAndEndsTheTaskAfterwards() async {
        let h = Harness()
        h.lifecycle.sceneDidEnterBackground()
        #expect(await eventually { h.suspension.events == ["announce"] })
        // Not ended while the announcement waits.
        #expect(h.tasks.events == ["begin 1"])
        h.suspension.gate.open()
        #expect(await eventually { h.tasks.events == ["begin 1", "end 1"] })
        #expect(h.suspension.events == ["announce", "announced"])
    }

    @Test func backgroundDuringWorkWaitsForTheWorkToEnd() async {
        let h = Harness()
        h.startWork()
        h.lifecycle.sceneDidEnterBackground()
        await h.settle()
        #expect(h.suspension.events.isEmpty)
        #expect(h.tasks.events == ["begin 1"])

        h.endWork()
        #expect(await eventually { h.suspension.events == ["announce"] })
        #expect(h.tasks.events == ["begin 1"])
        h.suspension.gate.open()
        #expect(await eventually { h.tasks.events == ["begin 1", "end 1"] })
    }

    @Test func expiryStartsTheAnnouncementAndEndsItsTaskAtOnce() async {
        let h = Harness()
        h.startWork()
        h.lifecycle.sceneDidEnterBackground()
        h.tasks.expire(1)
        // Ended synchronously in the handler, before the announcement has even started.
        #expect(h.tasks.events == ["begin 1", "end 1"])
        #expect(await eventually { h.suspension.events == ["announce"] })
        h.suspension.gate.open()
        #expect(await eventually { h.suspension.events == ["announce", "announced"] })
        await h.settle()

        // The aborted work ends later: no second announcement, and the task is not ended again.
        h.endWork()
        await h.settle()
        #expect(h.suspension.count("announce") == 1)
        #expect(h.tasks.events == ["begin 1", "end 1"])
    }

    @Test func expiryOfTheAnnouncementsOwnTaskEndsItOnce() async {
        let h = Harness()
        h.lifecycle.sceneDidEnterBackground()
        #expect(await eventually { h.suspension.events == ["announce"] })
        h.tasks.expire(1)
        #expect(h.tasks.events == ["begin 1", "end 1"])
        h.suspension.gate.open()
        #expect(await eventually { h.suspension.events == ["announce", "announced"] })
        await h.settle()
        #expect(h.tasks.events == ["begin 1", "end 1"])
        #expect(h.suspension.count("announce") == 1)
    }

    /// The sequence from review: the first task expires mid-sync, the owner returns while the sync
    /// carries on, then leaves again. The sync must hold a new task, and its end must announce
    /// under it.
    @Test func workStillRunningAfterAnExpiryGetsANewTask() async {
        let h = Harness()
        h.suspension.gate.open()
        h.startWork()
        h.lifecycle.sceneDidEnterBackground()
        h.tasks.expire(1)
        #expect(await eventually { h.suspension.events == ["announce", "announced"] })

        h.lifecycle.sceneDidBecomeActive()
        #expect(h.tasks.events == ["begin 1", "end 1", "begin 2"])
        h.lifecycle.sceneDidEnterBackground()
        await h.settle()
        // Still running: no announcement, and task 2 is held.
        #expect(h.suspension.count("announce") == 1)
        #expect(h.tasks.events == ["begin 1", "end 1", "begin 2"])

        h.endWork()
        #expect(await eventually { h.tasks.events == ["begin 1", "end 1", "begin 2", "end 2"] })
        #expect(h.suspension.events.suffix(2) == ["announce", "announced"])
        #expect(h.suspension.count("announce") == 2)
    }

    @Test func workEndingInTheForegroundEndsItsTaskWithoutAnnouncing() async {
        let h = Harness()
        h.lifecycle.sceneDidBecomeActive()
        h.startWork()
        h.endWork()
        #expect(h.tasks.events == ["begin 1", "end 1"])
        await h.settle()
        #expect(h.suspension.count("announce") == 0)
    }

    @Test func becomingActiveResumes() {
        let h = Harness()
        h.lifecycle.sceneDidBecomeActive()
        #expect(h.suspension.events == ["resume"])
        #expect(!h.lifecycle.isInBackground)
    }

    @Test func returningBeforeTheWorkEndsDoesNotAnnounce() async {
        let h = Harness()
        h.startWork()
        h.lifecycle.sceneDidEnterBackground()
        h.lifecycle.sceneDidBecomeActive()
        h.endWork()
        #expect(h.tasks.events == ["begin 1", "end 1"])
        await h.settle()
        #expect(h.suspension.count("announce") == 0)
    }

    @Test func anAnnouncementOvertakenByAResumeResumesAgain() async {
        let h = Harness()
        h.suspension.gate.open()
        h.lifecycle.sceneDidEnterBackground()
        // Back before the announcement has reached the store.
        h.lifecycle.sceneDidBecomeActive()
        #expect(await eventually { h.suspension.events == ["resume", "announce", "announced", "resume"] })
        #expect(await eventually { h.tasks.events == ["begin 1", "end 1"] })
    }

    @Test func aLateAnnouncementDoesNotResumeOverALaterOne() async {
        let h = Harness()
        h.lifecycle.sceneDidEnterBackground()
        h.lifecycle.sceneDidBecomeActive()
        h.lifecycle.sceneDidEnterBackground()
        #expect(await eventually { h.suspension.count("announce") == 2 })
        h.suspension.gate.open()
        #expect(await eventually { h.suspension.count("announced") == 2 })
        await h.settle()
        // The only resume is the activation's: the store stays suspended in the background.
        #expect(h.suspension.count("resume") == 1)
        #expect(h.suspension.events.last == "announced")
    }
}

// MARK: Through the models

@MainActor
@Suite(.timeLimit(.minutes(1)))
struct RingWorkInsideABackgroundTaskTests {
    private func fixture(_ tasks: FakeBackgroundTasks, _ suspension: FakeSuspension) throws -> Fixture {
        try Fixture { pairedRing in
            SuspensionLifecycle(
                isRingInUse: { pairedRing.activity != nil }, backgroundTasks: tasks,
                suspend: { await suspension.suspend() }, resume: {})
        }
    }

    @Test func aSyncRunsInsideABackgroundTaskAndAnnouncesWhenItEndsInTheBackground() async throws {
        let tasks = FakeBackgroundTasks()
        let suspension = FakeSuspension()
        suspension.gate.open()
        let f = try fixture(tasks, suspension)
        try await f.storePairedRing()
        let gate = Gate()
        f.access.connectGate = gate

        let done = Flag()
        let sync = f.sync
        Task { await sync.sync(); done.value = true }
        #expect(await eventually { f.log.count("connect") == 1 })
        #expect(tasks.events == ["begin 1"])
        f.lifecycle?.sceneDidEnterBackground()
        #expect(suspension.events.isEmpty)

        gate.open()
        #expect(await eventually { done.value })
        #expect(await eventually { tasks.events == ["begin 1", "end 1"] })
        #expect(suspension.events == ["announce", "announced"])
    }

    @Test func pairingAndForgettingRunInsideABackgroundTask() async throws {
        let tasks = FakeBackgroundTasks()
        let suspension = FakeSuspension()
        let f = try fixture(tasks, suspension)
        try await f.load()
        f.lifecycle?.sceneDidBecomeActive()
        await f.pairing.findRings()
        #expect(tasks.events == ["begin 1", "end 1"])
        await f.pairing.pair(RingCandidate(identifier: syntheticIdentifier, name: syntheticName, signal: .strong))
        #expect(tasks.events == ["begin 1", "end 1", "begin 2", "end 2"])
        await f.pairing.forget()
        #expect(tasks.events == ["begin 1", "end 1", "begin 2", "end 2", "begin 3", "end 3"])
        #expect(suspension.events.isEmpty)
    }

    @Test func pairingInTheBackgroundDefersTheAnnouncement() async throws {
        let tasks = FakeBackgroundTasks()
        let suspension = FakeSuspension()
        suspension.gate.open()
        let f = try fixture(tasks, suspension)
        try await f.load()
        let gate = Gate()
        f.access.connectGate = gate
        let done = Flag()
        let pairing = f.pairing
        let pick = RingCandidate(identifier: syntheticIdentifier, name: syntheticName, signal: .strong)
        Task { await pairing.pair(pick); done.value = true }
        #expect(await eventually { f.log.count("connect") == 1 })
        f.lifecycle?.sceneDidEnterBackground()
        try? await Task.sleep(for: .milliseconds(20))
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
