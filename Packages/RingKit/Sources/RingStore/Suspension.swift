import Foundation
import GRDB
import os

// Suspension handling for a database in an App Group container.
//
// iOS kills an app that holds a lock on an SQLite file in a shared container while it is suspended
// (termination code 0xdead10cc). GRDB's answer, followed here, is in its "Sharing a Database"
// guide, section "How to limit the 0xDEAD10CC exception" (GRDB 7.12,
// `GRDB/Documentation.docc/DatabaseSharing.md`): open the database with
// `Configuration.observesSuspensionNotifications`, post `Database.suspendNotification` when the app
// is about to be suspended and `Database.resumeNotification` when it resumes. GRDB marks the
// feature experimental.
//
// What GRDB does then (from `Database.suspend()` and `checkForSuspensionViolation(from:)`): it
// calls `sqlite3_interrupt` on the pool's writer connection, and from then on any statement on that
// connection that could take a lock rolls the open transaction back and throws `SQLITE_ABORT`
// ("Database is suspended"). ROLLBACK, COMMIT and RELEASE are let through because they release
// locks. GRDB does not interrupt Swift code between two statements, so a write keeps its lock until
// it reaches its next statement; and it does not wake a write waiting in the busy timeout (measured
// in review of #64: the write took the lock two seconds after the announcement). Reads in WAL mode
// are deliberately not suspended: GRDB treats them as safe and never interrupts a pool's readers.
//
// The store adds, on top:
// - A contract: after the announcement nothing touches the store, and every call fails the same
//   way. So every call, reads included, is refused before touching SQLite while suspended. This
//   is a choice of interface, stricter than GRDB (which lets WAL reads through), not a claim that
//   WAL reads are a 0xdead10cc hazard.
// - A drain: `appWillSuspend()` returns normally only once every call that was already in
//   progress has finished (so the store holds no lock), or once `appDidResume()` ends the wait; it
//   throws `CancellationError` if its task is cancelled first, when a lock may still be held. Each guarded call is counted while it
//   runs, and checks again for a suspension as the first thing inside its transaction, so one that
//   only got the lock after the announcement (the busy-wait case) does no work.
// - Error mapping, so the app never has to import GRDB to recognise a suspension: GRDB's
//   interruption errors (`SQLITE_ABORT`, `SQLITE_INTERRUPT`) become `RingStoreError.suspended` when
//   a suspension was announced since the call started; `SQLITE_BUSY` (any, not only a write whose
//   busy timeout ran out) becomes it only while the store is suspended at the moment the error is
//   mapped, so contention that outlasts a suspension and a resume stays a plain busy error.

/// Process-wide suspension state. GRDB's notifications are process-wide (posted on
/// `NotificationCenter.default` and observed with no sender filter), so suspension is too: it
/// applies to every store in the process that observes it.
enum SuspensionState {
    private struct State: Sendable {
        var suspended = false
        /// Incremented by every announcement, so a call can tell that one happened while it ran
        /// even if the app has resumed since.
        var epoch: UInt64 = 0
        /// Guarded calls in progress (opening and active sleep observations included).
        var inFlight = 0
        /// Announcements waiting for `inFlight` to reach zero, by waiter id (so a cancelled wait can
        /// take itself out).
        var drainWaiters: [UInt64: CheckedContinuation<Bool, Never>] = [:]
        var nextWaiterID: UInt64 = 0
        /// How to end each active sleep observation, by registration id.
        var observationEnders: [UInt64: @Sendable () -> Void] = [:]
        var nextObservationID: UInt64 = 0
    }

    private static let state = OSAllocatedUnfairLock(initialState: State())
    /// Serialises the flag change with GRDB's notification in `announce` and `resume`, so a
    /// suspend and a resume racing each other cannot leave the store's flag and GRDB's
    /// disagreeing. A separate `NSLock`, not `state`'s lock, so `state` is never held while
    /// observers run. It IS held while `NotificationCenter.post` runs every observer of GRDB's
    /// two notifications, and it is not reentrant: **an observer of those notifications must never
    /// call `appWillSuspend()` or `appDidResume()`** (it would deadlock). Restructuring to post
    /// outside the lock would need a hand-over protocol between racing announcements and resumes,
    /// which is not worth it for a rule that is easy to follow.
    private static let announcementLock = NSLock()

    /// The synchronous half of `appWillSuspend()` (`RingStore.announceSuspension()`): refuse new
    /// calls, end every sleep observation and have GRDB interrupt and suspend its writers.
    /// Idempotent: while already suspended it does nothing (the flag, GRDB's state and the ended
    /// observations are already as an announcement leaves them).
    static func announce() {
        let enders: [@Sendable () -> Void]? = announcementLock.withLock {
            // Flag first, so new calls are refused before GRDB interrupts the running one.
            let enders: [@Sendable () -> Void]? = state.withLock { state in
                if state.suspended { return nil }
                state.suspended = true
                state.epoch &+= 1
                let enders = Array(state.observationEnders.values)
                state.observationEnders = [:]
                return enders
            }
            guard let enders else { return nil }
            NotificationCenter.default.post(name: Database.suspendNotification, object: nil)
            return enders
        }
        for end in enders ?? [] { end() }
    }

    /// Waits until no guarded call is in progress, or until the app resumes. Throws
    /// `CancellationError` if the calling task is cancelled first, leaving the suspension in force
    /// (a lock may still be held).
    static func drain() async throws {
        let id = state.withLock { state in
            state.nextWaiterID &+= 1
            return state.nextWaiterID
        }
        // The continuation's value is whether the wait was cancelled.
        let cancelled = await withTaskCancellationHandler {
            await withCheckedContinuation { (continuation: CheckedContinuation<Bool, Never>) in
                let now: Bool? = state.withLock { state in
                    // Checked under the lock: a cancellation that arrives later finds the waiter
                    // registered, and one that arrived earlier is seen here, so neither is lost.
                    // Already drained (or resumed) wins over a cancellation.
                    if state.inFlight == 0 || !state.suspended { return false }
                    if Task.isCancelled { return true }
                    state.drainWaiters[id] = continuation
                    return nil
                }
                if let now { continuation.resume(returning: now) }
            }
        } onCancel: {
            let waiter = state.withLock { state in state.drainWaiters.removeValue(forKey: id) }
            waiter?.resume(returning: true)
        }
        if cancelled { throw CancellationError() }
    }

    static func resume() {
        let waiters = announcementLock.withLock {
            // GRDB first, so nothing passes the store's check while GRDB still refuses it.
            NotificationCenter.default.post(name: Database.resumeNotification, object: nil)
            return state.withLock { state in
                state.suspended = false
                defer { state.drainWaiters = [:] }
                return Array(state.drainWaiters.values)
            }
        }
        for waiter in waiters { waiter.resume(returning: false) }
    }

    /// Starts a guarded call: `RingStoreError.suspended` if suspended now, otherwise counts the
    /// call as in progress and returns the current epoch. Every successful `enter` must be paired
    /// with one `leave`.
    static func enter() throws -> UInt64 {
        try state.withLock { state in
            if state.suspended { throw RingStoreError.suspended }
            state.inFlight += 1
            return state.epoch
        }
    }

    static func leave() {
        let waiters = state.withLock { state in
            state.inFlight -= 1
            guard state.inFlight == 0 else { return [CheckedContinuation<Bool, Never>]() }
            defer { state.drainWaiters = [:] }
            return Array(state.drainWaiters.values)
        }
        for waiter in waiters { waiter.resume(returning: false) }
    }

    /// `enter` for a sleep observation, registering how to end it in the same critical section,
    /// so an announcement either refuses it or ends it, never neither.
    static func enterObservation(end: @escaping @Sendable () -> Void) throws -> (epoch: UInt64, id: UInt64) {
        try state.withLock { state in
            if state.suspended { throw RingStoreError.suspended }
            state.inFlight += 1
            state.nextObservationID &+= 1
            state.observationEnders[state.nextObservationID] = end
            return (state.epoch, state.nextObservationID)
        }
    }

    static func leaveObservation(id: UInt64) {
        state.withLock { state in _ = state.observationEnders.removeValue(forKey: id) }
        leave()
    }

    /// Whether a suspension can explain an interruption (`SQLITE_ABORT`, `SQLITE_INTERRUPT`) seen by
    /// a call that entered at `epoch`: the store is suspended now, or was suspended at some point
    /// since.
    static func hasSuspended(since epoch: UInt64) -> Bool {
        state.withLock { state in state.suspended || state.epoch != epoch }
    }

    /// Throws `suspended` if the store is suspended now. Used inside transactions and during
    /// opening: a call that queued across a suspension and a resume must not be refused, because
    /// nothing is suspended any more. (Interruptions that happened during a call still map to
    /// `suspended` through `hasSuspended(since:)`.) A store that does not observe suspension
    /// (`observes: false`) never throws.
    static func checkNotSuspended(observes: Bool) throws {
        if observes, isSuspended { throw RingStoreError.suspended }
    }

    /// Throws `suspended` if a suspension was announced since `epoch`, even if the app has resumed
    /// since. For a sleep observation, which an announcement ends for good. `nil` never throws.
    static func check(since epoch: UInt64?) throws {
        if let epoch, hasSuspended(since: epoch) { throw RingStoreError.suspended }
    }

    // For tests.
    static var inFlightCount: Int { state.withLock { $0.inFlight } }
    static var isSuspended: Bool { state.withLock { $0.suspended } }
}

extension RingStore {
    // MARK: App lifecycle

    /// The synchronous half of `appWillSuspend()`, for a background task's expiration handler.
    ///
    /// iOS requires an expiration handler to end its task before it returns, so it cannot await
    /// the drain. Calling this first means the store has been told before the task ends: from its
    /// return every call (reads included) and every open throws `RingStoreError.suspended`, every
    /// sleep observation ends with `suspended`, and GRDB has interrupted its writers, so a write in
    /// progress is rolled back at its next statement. It does not wait for that: a write between
    /// two statements can still hold its lock when this returns, so the app should still start
    /// `appWillSuspend()` to drain, and ending the task straight after is best effort.
    ///
    /// Synchronous, nonisolated and safe from any thread. Idempotent: while already suspended it
    /// does nothing. Process-wide, like `appWillSuspend()`; undone by `appDidResume()`. Never call it
    /// from an observer of GRDB's suspend and resume notifications. Added for #53 (planner decision).
    public static func announceSuspension() {
        SuspensionState.announce()
    }

    /// The waiting half of `appWillSuspend()`, without announcing: returns once no guarded call is
    /// in progress, or at once when the store is not suspended (nothing announced, or
    /// `appDidResume()` already called), or when `appDidResume()` ends the wait. Throws
    /// `CancellationError` if its task is cancelled first, with the suspension still in force.
    ///
    /// For an app that announces with `announceSuspension()` and drains separately: because this
    /// never announces, a drain that runs late (after the app has resumed) cannot suspend the store
    /// again. Process-wide. Added for #53 (planner decision).
    public static func drainSuspension() async throws {
        try await SuspensionState.drain()
    }

    /// Call when the app is about to be suspended, and **await it before ending the background
    /// task**: it returns only when no store in this process holds a database lock. It announces
    /// first, as `announceSuspension()` does (nothing more if that already ran), then drains.
    ///
    /// Suspension is process-wide, so announce at the end of the background task that was keeping
    /// the app alive, or in its expiration handler, and not merely when the scene moves to the
    /// background: a sync running under a background task would be aborted by an announcement made
    /// then.
    ///
    /// **Keep the store alive across a suspension**, or release it before announcing: closing the
    /// last connection of a WAL database checkpoints under an exclusive lock, which is exactly what
    /// `0xdead10cc` punishes. Never call this or `appDidResume()` from an observer of GRDB's
    /// suspend and resume notifications (see `SuspensionState.announcementLock`).
    ///
    /// From the moment it is called, every call on a file-backed store (reads included) and every
    /// attempt to open one throws `RingStoreError.suspended`, and every sleep observation ends with
    /// `suspended`. Calls already in progress are waited for:
    /// - a write is rolled back at its next statement and throws `suspended`, keeping its lock until
    ///   it gets there (Swift work between two statements is not interrupted); one with only its
    ///   commit left completes instead. Either way it is never half-applied;
    /// - a write still waiting for the lock (another connection holds it) is not woken: when it gets
    ///   the lock it fails with `suspended` before doing any work, and releases it; if its busy
    ///   timeout runs out first, the busy error becomes `suspended` only if the store is still
    ///   suspended at that moment (after a resume it is plain contention and passes through);
    /// - a read in progress completes normally.
    ///
    /// So the wait lasts as long as the slowest call in progress takes to reach its next statement
    /// or finish: the apply step of a long `ingest` delays it (its derivation does not: it runs
    /// outside any database access, and stops at its next checkpoint once the announcement is
    /// made). **There is no timeout here**; the caller decides how long it can wait, by
    /// cancelling the task that awaits it. **Cancelling it throws
    /// `CancellationError`**, with the suspension still in force and a lock possibly still held; a
    /// normal return means the calls drained (no lock is held), or `appDidResume()` ended the wait
    /// (the store is no longer suspended). Calling it again while one is waiting is harmless (both
    /// return when the calls drain), and `appDidResume()` ends every wait at once.
    ///
    /// Posts GRDB's `Database.suspendNotification`, which is process-wide, so the app does not need
    /// to import GRDB. In-memory stores are not affected. Unverified on a device: tests cannot
    /// suspend a process.
    public static func appWillSuspend() async throws {
        SuspensionState.announce()
        try await SuspensionState.drain()
    }

    /// Call when the app has resumed: when it returns to the foreground, and at the start of every
    /// background task or background-mode callback that may use the store (GRDB's guide asks for
    /// both). Undoes `appWillSuspend()` for every store in this process and ends any wait in it.
    /// Safe to call from any thread and when not suspended.
    public static func appDidResume() {
        SuspensionState.resume()
    }

    // MARK: Guarded database access

    // Every read and write on an open store goes through these four, so the suspension check, the
    // in-progress count and the error mapping cannot be skipped by one call. The opener goes
    // through the same blocking guard (`guardingSuspensionBlocking(observes:_:)`); only the sleep
    // observation (`observeLatestSleepNight`) uses the pieces (`SuspensionState.enterObservation`
    // and `leaveObservation`, `check(since:)`, `mapSuspension`) directly. The blocking pair serves the internal blocking forms used by
    // tests; distinct names keep a synchronous overload from being picked silently in an async
    // context.

    func read<T: Sendable>(_ value: @Sendable (Database) throws -> T) async throws -> T {
        try await guardingSuspension { epoch in
            try await writer.read { db in
                try SuspensionState.checkNotSuspended(observes: epoch != nil)
                return try value(db)
            }
        }
    }

    func write<T: Sendable>(_ updates: @Sendable (Database) throws -> T) async throws -> T {
        try await guardingSuspension { epoch in
            try await writer.write { db in
                // First thing inside the transaction: a write that only got the lock after the
                // announcement (it was waiting in the busy timeout) does no work and rolls back.
                try SuspensionState.checkNotSuspended(observes: epoch != nil)
                return try updates(db)
            }
        }
    }

    func readBlocking<T>(_ value: (Database) throws -> T) throws -> T {
        try guardingSuspensionBlocking { epoch in
            try writer.read { db in
                try SuspensionState.checkNotSuspended(observes: epoch != nil)
                return try value(db)
            }
        }
    }

    func writeBlocking<T>(_ updates: (Database) throws -> T) throws -> T {
        try guardingSuspensionBlocking { epoch in
            try writer.write { db in
                try SuspensionState.checkNotSuspended(observes: epoch != nil)
                return try updates(db)
            }
        }
    }

    /// `body` gets the epoch the call entered at, or `nil` for a store that ignores suspension.
    private func guardingSuspension<T>(_ body: (UInt64?) async throws -> T) async throws -> T {
        guard observesSuspension else { return try await body(nil) }
        let epoch = try SuspensionState.enter()
        defer { SuspensionState.leave() }
        do {
            return try await body(epoch)
        } catch {
            throw Self.mapSuspension(error, since: epoch)
        }
    }

    private func guardingSuspensionBlocking<T>(_ body: (UInt64?) throws -> T) throws -> T {
        try Self.guardingSuspensionBlocking(observes: observesSuspension, body)
    }

    /// Static so the opener, which has no store yet, uses the same enter, leave and mapping.
    static func guardingSuspensionBlocking<T>(observes: Bool, _ body: (UInt64?) throws -> T) throws -> T {
        guard observes else { return try body(nil) }
        let epoch = try SuspensionState.enter()
        defer { SuspensionState.leave() }
        do {
            return try body(epoch)
        } catch {
            throw mapSuspension(error, since: epoch)
        }
    }

    /// `SQLITE_ABORT` and `SQLITE_INTERRUPT` are what GRDB throws for a suspended database (its
    /// guide, "How to limit the 0xDEAD10CC exception"); they become `suspended` if a suspension was
    /// announced since the call started, even if the app has resumed since, because the interrupt
    /// happened during it. `SQLITE_BUSY` (any, whatever its cause) becomes `suspended` only if the
    /// store is suspended at the moment the error is mapped: GRDB's interrupt does not wake a write
    /// waiting in the busy timeout, so a busy error that arrives during a suspension is most
    /// plausibly the suspension's doing, but one that arrives after the app resumed is contention
    /// and is passed on unchanged. Everything else, and a task's `CancellationError` (which GRDB
    /// checks before suspension), is never mapped.
    static func mapSuspension(_ error: any Error, since epoch: UInt64) -> any Error {
        guard let error = error as? DatabaseError else { return error }
        if error.isInterruptionError, SuspensionState.hasSuspended(since: epoch) {
            return RingStoreError.suspended
        }
        if error.resultCode == .SQLITE_BUSY, SuspensionState.isSuspended {
            return RingStoreError.suspended
        }
        return error
    }
}

/// The task behind a sleep observation, cancellable from an announcement that may arrive before
/// the task handle exists.
final class ObservationTaskHandle: Sendable {
    private let state = OSAllocatedUnfairLock<(task: Task<Void, Never>?, cancelled: Bool)>(initialState: (nil, false))

    func set(_ task: Task<Void, Never>) {
        let cancelled = state.withLock { state in
            state.task = task
            return state.cancelled
        }
        if cancelled { task.cancel() }
    }

    func cancel() {
        let task = state.withLock { state in
            state.cancelled = true
            return state.task
        }
        task?.cancel()
    }
}
