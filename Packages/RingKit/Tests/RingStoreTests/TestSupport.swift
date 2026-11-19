import Foundation
import os
import Testing

// Waiting helpers shared by the store tests. `.timeLimit` does not stop an `await` on an
// unstructured task, so every such wait goes through `finished(_:)`, and waits on a semaphore go
// through `wait(for:)`, both bounded at ten seconds.

/// A thread-safe value for hooks to write and tests to read.
final class Locked<Value: Sendable>: Sendable {
    private let stored: OSAllocatedUnfairLock<Value>
    init(_ value: Value) { stored = OSAllocatedUnfairLock(initialState: value) }
    var value: Value { stored.withLock { $0 } }
    func update(_ change: @Sendable (inout Value) -> Void) { stored.withLock { change(&$0) } }
}

/// Waits for `semaphore` on a dispatch thread, so a Swift concurrency thread is never blocked.
/// Fails the test instead of hanging if it is not signalled within ten seconds.
func wait(for semaphore: DispatchSemaphore) async {
    let signalled = await withCheckedContinuation { continuation in
        DispatchQueue.global().async {
            continuation.resume(returning: semaphore.wait(timeout: .now() + 10) == .success)
        }
    }
    #expect(signalled, "timed out waiting for the other side")
}

/// Awaits `task` for at most ten seconds. Records an issue and returns nil on timeout, so a hung
/// task fails its test instead of hanging the run (the task itself is left behind).
func finished<T: Sendable, F: Error>(_ task: Task<T, F>) async -> Result<T, F>? {
    let box = OSAllocatedUnfairLock<Result<T, F>?>(initialState: nil)
    Task { let result = await task.result; box.withLock { $0 = result } }
    let ok = await eventually { box.withLock { $0 != nil } }
    #expect(ok, "timed out waiting for a task to finish")
    return box.withLock { $0 }
}

/// Polls `condition` until it holds or `seconds` pass; returns whether it held.
func eventually(within seconds: Double = 10, _ condition: () -> Bool) async -> Bool {
    let deadline = Date().addingTimeInterval(seconds)
    while !condition() {
        if Date() > deadline { return false }
        try? await Task.sleep(for: .milliseconds(2))
    }
    return true
}
