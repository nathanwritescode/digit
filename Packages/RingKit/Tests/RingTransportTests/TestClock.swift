import Foundation
@testable import RingTransport

/// A monotonic clock that moves only when the test moves it, shared by a session's deadlines and
/// timers and by the fake link's own write deadline (#75).
///
/// Because no timer can fire until the test moves the clock, a test that expects a reply cannot
/// time out however long the reply takes to be handed over, and a test that expects a timeout
/// says exactly when it happens. A sleeper whose instant has already been reached when it starts
/// returns at once, so moving the clock before a timer task has started still ends that timer.
///
/// `stepPerRead` makes every read of `now` move the clock first, for the tests that cross a
/// deadline after a fixed number of reads. `advance(by:afterReads:)` jumps once at a chosen read.
/// `@unchecked Sendable` because all state is guarded by `lock`.
final class TestClock: @unchecked Sendable {
    private let lock = NSLock()
    let start = ContinuousClock.now
    private var offset: Duration = .zero
    private let stepPerRead: Duration?
    private var pendingJump: (reads: Int, by: Duration)?
    private var sleepers: [Int: (instant: ContinuousClock.Instant, continuation: CheckedContinuation<Void, Error>)] = [:]
    private var lastSleeperID = 0

    init(stepPerRead: Duration? = nil) { self.stepPerRead = stepPerRead }

    /// What a session is given.
    var session: SessionClock {
        SessionClock(now: { self.now() }, sleep: { try await self.sleep(until: $0) })
    }

    /// How far the clock has moved since it was made.
    var elapsed: Duration { lock.withLock { offset } }

    /// Sleepers not yet woken or cancelled.
    var pendingSleepers: Int { lock.withLock { sleepers.count } }

    func now() -> ContinuousClock.Instant {
        let (instant, due) = lock.withLock { () -> (ContinuousClock.Instant, [CheckedContinuation<Void, Error>]) in
            if let step = stepPerRead { offset += step }
            if let jump = pendingJump {
                if jump.reads == 0 {
                    offset += jump.by
                    pendingJump = nil
                } else {
                    pendingJump = (jump.reads - 1, jump.by)
                }
            }
            return (start + offset, takeDue())
        }
        for continuation in due { continuation.resume() }
        return instant
    }

    func advance(by seconds: Double) {
        let due = lock.withLock { () -> [CheckedContinuation<Void, Error>] in
            offset += .seconds(seconds)
            return takeDue()
        }
        for continuation in due { continuation.resume() }
    }

    /// Waits until at least `count` timers are armed within a millisecond of `seconds` after
    /// `start`, then moves the clock to the latest of them (never back). A timer counts once its
    /// sleeper has started, which is after its instant was fixed, so the timers due then are
    /// exactly the ones the test means. Returns false, without moving the clock, if that does not
    /// happen within `limit` seconds of real time or the task is cancelled.
    func advanceToTimers(near seconds: Double, count: Int = 1, within limit: Double = 10) async -> Bool {
        let target = start + .seconds(seconds)
        let end = ContinuousClock.now + .seconds(limit)
        while !Task.isCancelled, ContinuousClock.now < end {
            let found = lock.withLock {
                sleepers.values.map(\.instant).filter { abs(($0 - target) / .seconds(1)) < 0.001 }
            }
            if found.count >= count, let latest = found.max() {
                let due = lock.withLock { () -> [CheckedContinuation<Void, Error>] in
                    offset = max(offset, latest - start)
                    return takeDue()
                }
                for continuation in due { continuation.resume() }
                return true
            }
            try? await Task.sleep(for: .milliseconds(1))
        }
        return false
    }

    /// Moves the clock on by `seconds` just before the read that follows the next `reads` reads.
    func advance(by seconds: Double, afterReads reads: Int) {
        lock.withLock { pendingJump = (reads, .seconds(seconds)) }
    }

    /// Returns once the clock reaches `instant`; throws `CancellationError` if the task is cancelled first.
    func sleep(until instant: ContinuousClock.Instant) async throws {
        let id = lock.withLock { () -> Int in
            lastSleeperID += 1
            return lastSleeperID
        }
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                let immediate = lock.withLock { () -> Result<Void, Error>? in
                    if Task.isCancelled { return .failure(CancellationError()) }
                    if instant <= start + offset { return .success(()) }
                    sleepers[id] = (instant, continuation)
                    return nil
                }
                if let immediate { continuation.resume(with: immediate) }
            }
        } onCancel: {
            lock.withLock { sleepers.removeValue(forKey: id) }?.continuation.resume(throwing: CancellationError())
        }
    }

    /// Like `sleep(until:)`, but ignores cancellation, as a link's own write deadline does.
    func sleepIgnoringCancellation(until instant: ContinuousClock.Instant) async {
        // An unstructured task is not cancelled when the task awaiting it is.
        await Task { try? await self.sleep(until: instant) }.value
    }

    /// Removes and returns the sleepers that are due, earliest first. Call with the lock held.
    private func takeDue() -> [CheckedContinuation<Void, Error>] {
        let now = start + offset
        let due = sleepers.filter { $0.value.instant <= now }
            .sorted { ($0.value.instant, $0.key) < ($1.value.instant, $1.key) }
        for (id, _) in due { sleepers[id] = nil }
        return due.map(\.value.continuation)
    }
}

/// Holds threads of the cooperative pool in short blocking stretches, as a blocking helper or a
/// CPU-heavy test in the same process would. This is what made the transport tests time out with
/// their replies already queued (#68): each scheduling hop could be delayed by 80 to 180 ms.
///
/// `RINGKIT_THREAD_LOAD=1` starts it for the whole run of the transport tests (the first
/// `FakeLink` starts it); `threadHoldingLoadDoesNotChangeOutcomes` runs it in every run.
enum ThreadLoad {
    static let enabledForWholeRun = ProcessInfo.processInfo.environment["RINGKIT_THREAD_LOAD"] == "1"

    /// Started at most once per process, and runs until the process ends.
    static let wholeRun: Void = {
        Task { _ = await start(tasks: defaultTasks, blockMilliseconds: 150) }
    }()

    /// One per thread of the cooperative pool (one per processor, or one with
    /// LIBDISPATCH_COOPERATIVE_POOL_STRICT=1), so every thread is held three quarters of the time.
    static let defaultTasks = ProcessInfo.processInfo.environment["LIBDISPATCH_COOPERATIVE_POOL_STRICT"] == "1"
        ? 1 : ProcessInfo.processInfo.activeProcessorCount

    /// Starts `tasks` tasks that each block their thread for `blockMilliseconds` at a time, with
    /// 50 ms between stretches, until the returned task is cancelled. The gaps keep other work
    /// moving, as in #68, where hops were delayed rather than stopped. Returns once every one of them
    /// has started blocking.
    static func start(tasks: Int, blockMilliseconds: UInt32) async -> Task<Void, Never> {
        let started = Counter()
        let load = Task.detached {
            await withTaskGroup(of: Void.self) { group in
                for _ in 0..<tasks {
                    group.addTask {
                        started.increment()
                        while !Task.isCancelled {
                            block(milliseconds: blockMilliseconds)
                            try? await Task.sleep(for: .milliseconds(50))
                        }
                    }
                }
            }
        }
        while started.value < tasks { try? await Task.sleep(for: .milliseconds(1)) }
        return load
    }

    /// `@unchecked Sendable` because all state is guarded by `lock`.
    private final class Counter: @unchecked Sendable {
        private let lock = NSLock()
        private var count = 0
        var value: Int { lock.withLock { count } }
        func increment() { lock.withLock { count += 1 } }
    }

    /// Synchronous on purpose: it holds the calling thread.
    private static func block(milliseconds: UInt32) { usleep(milliseconds * 1000) }
}
