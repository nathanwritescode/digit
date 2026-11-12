import Foundation

/// Runs `operation` but gives up after `seconds` with `RingError.timeout(what)`.
///
/// Unlike a task group, this does not wait for `operation` to finish after the deadline: a
/// `CheckedContinuation` ignores cancellation, so a group would hang on exactly the stuck call it
/// is meant to cut off. The abandoned operation is cancelled and left to finish (or not) on its own.
/// The caller's continuation is resumed exactly once, by whichever of the two finishes first.
func withDeadline<T: Sendable>(
    _ seconds: TimeInterval,
    _ what: String,
    _ operation: @escaping @Sendable () async throws -> T
) async throws -> T {
    let once = Once<T>()
    return try await withCheckedThrowingContinuation { continuation in
        once.arm(continuation)
        let sleeper = SleeperBox()
        let work = Task {
            do { once.finish(.success(try await operation())) } catch { once.finish(.failure(error)) }
            sleeper.cancel()
        }
        sleeper.set(Task {
            try? await Task.sleep(for: .seconds(seconds))
            if once.finish(.failure(RingError.timeout(what))) { work.cancel() }
        })
    }
}

/// Holds a continuation and resumes it at most once.
/// `@unchecked Sendable` because all state is guarded by `lock`.
private final class Once<T: Sendable>: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<T, Error>?

    func arm(_ continuation: CheckedContinuation<T, Error>) {
        lock.withLock { self.continuation = continuation }
    }

    /// Returns whether this call was the one that resumed the continuation.
    @discardableResult
    func finish(_ result: Result<T, Error>) -> Bool {
        let taken = lock.withLock { () -> CheckedContinuation<T, Error>? in
            defer { continuation = nil }
            return continuation
        }
        guard let taken else { return false }
        taken.resume(with: result)
        return true
    }
}

/// Lets the operation's task cancel the timer task, which is created after it.
/// `@unchecked Sendable` because all state is guarded by `lock`.
private final class SleeperBox: @unchecked Sendable {
    private let lock = NSLock()
    private var task: Task<Void, Never>?
    private var cancelled = false

    func set(_ task: Task<Void, Never>) {
        let cancelNow = lock.withLock { () -> Bool in
            self.task = task
            return cancelled
        }
        if cancelNow { task.cancel() }
    }

    func cancel() {
        let task = lock.withLock { () -> Task<Void, Never>? in
            cancelled = true
            return self.task
        }
        task?.cancel()
    }
}
