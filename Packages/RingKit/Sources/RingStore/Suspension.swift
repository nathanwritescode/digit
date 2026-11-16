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
// - A drain: `appWillSuspend()` returns only once every call that was already in progress has
//   finished, so when it returns the store holds no lock. Each guarded call is counted while it
//   runs, and checks again for a suspension as the first thing inside its transaction, so one that
//   only got the lock after the announcement (the busy-wait case) does no work.
// - Error mapping: GRDB's interruption errors become `RingStoreError.suspended` only when a
//   suspension can explain them, so the app never has to import GRDB to recognise them.

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
        /// Announcements waiting for `inFlight` to reach zero.
        var drainWaiters: [CheckedContinuation<Void, Never>] = []
        /// How to end each active sleep observation, by registration id.
        var observationEnders: [UInt64: @Sendable () -> Void] = [:]
        var nextObservationID: UInt64 = 0
    }

    private static let state = OSAllocatedUnfairLock(initialState: State())
    /// Serialises the flag change with GRDB's notification in `announce` and `resume`, so a
    /// suspend and a resume racing each other cannot leave the store's flag and GRDB's
    /// disagreeing. A separate `NSLock`, not `state`'s unfair lock, because posting runs
    /// observers' code and an unfair lock must not be held across arbitrary code.
    private static let announcementLock = NSLock()

    /// The synchronous half of `appWillSuspend()`: refuse new calls, end every sleep observation
    /// and have GRDB interrupt and suspend its writers.
    static func announce() {
        let enders = announcementLock.withLock {
            // Flag first, so new calls are refused before GRDB interrupts the running one.
            let enders = state.withLock { state in
                state.suspended = true
                state.epoch &+= 1
                let enders = Array(state.observationEnders.values)
                state.observationEnders = [:]
                return enders
            }
            NotificationCenter.default.post(name: Database.suspendNotification, object: nil)
            return enders
        }
        for end in enders { end() }
    }

    /// Waits until no guarded call is in progress, or until the app resumes.
    static func drain() async {
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            let now = state.withLock { state in
                if state.inFlight == 0 || !state.suspended { return true }
                state.drainWaiters.append(continuation)
                return false
            }
            if now { continuation.resume() }
        }
    }

    static func resume() {
        let waiters = announcementLock.withLock {
            // GRDB first, so nothing passes the store's check while GRDB still refuses it.
            NotificationCenter.default.post(name: Database.resumeNotification, object: nil)
            return state.withLock { state in
                state.suspended = false
                defer { state.drainWaiters = [] }
                return state.drainWaiters
            }
        }
        for waiter in waiters { waiter.resume() }
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
            guard state.inFlight == 0 else { return [CheckedContinuation<Void, Never>]() }
            defer { state.drainWaiters = [] }
            return state.drainWaiters
        }
        for waiter in waiters { waiter.resume() }
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

    /// Whether a suspension can explain an interruption seen by a call that entered at `epoch`:
    /// the store is suspended now, or was suspended at some point since.
    static func hasSuspended(since epoch: UInt64) -> Bool {
        state.withLock { state in state.suspended || state.epoch != epoch }
    }

    /// Throws `suspended` if a suspension was announced since `epoch`. `nil` (a store that does
    /// not observe suspension) never throws.
    static func check(since epoch: UInt64?) throws {
        if let epoch, hasSuspended(since: epoch) { throw RingStoreError.suspended }
    }

    // For tests.
    static var inFlightCount: Int { state.withLock { $0.inFlight } }
    static var isSuspended: Bool { state.withLock { $0.suspended } }
}

extension RingStore {
    // MARK: App lifecycle

    /// Call when the app is about to be suspended (for example when the scene moves to the
    /// background, and at the end of every background task that used the store), and **await it
    /// before ending the background task**: it returns only when no store in this process holds a
    /// database lock.
    ///
    /// From the moment it is called, every call on a file-backed store (reads included) and every
    /// attempt to open one throws `RingStoreError.suspended`, and every sleep observation ends with
    /// `suspended`. Calls already in progress are waited for:
    /// - a write is rolled back at its next statement and throws `suspended`, keeping its lock until
    ///   it gets there (Swift work between two statements is not interrupted); one with only its
    ///   commit left completes instead. Either way it is never half-applied;
    /// - a write still waiting for the lock (another connection holds it) is not woken: when it gets
    ///   the lock it fails with `suspended` before doing any work, and releases it;
    /// - a read in progress completes normally.
    ///
    /// So the wait lasts as long as the slowest call in progress takes to reach its next statement
    /// or finish: a long `ingest` delays it. **There is no timeout here**; the caller decides how
    /// long it can wait. Calling it again while one is waiting is harmless (both return when the
    /// calls drain), and `appDidResume()` ends every wait at once.
    ///
    /// Posts GRDB's `Database.suspendNotification`, which is process-wide, so the app does not need
    /// to import GRDB. In-memory stores are not affected. Unverified on a device: tests cannot
    /// suspend a process.
    public static func appWillSuspend() async {
        SuspensionState.announce()
        await SuspensionState.drain()
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
    // in-progress count and the error mapping cannot be skipped by one call. Two paths use the same
    // pieces (`SuspensionState.enter` and `leave`, `check(since:)`, `mapSuspension`) directly
    // instead: opening (`init(url:observesSuspension:hooks:)`) and the sleep observation
    // (`observeLatestSleepNight`). The blocking pair serves the internal blocking forms used by
    // tests; distinct names keep a synchronous overload from being picked silently in an async
    // context.

    func read<T: Sendable>(_ value: @Sendable (Database) throws -> T) async throws -> T {
        try await guardingSuspension { epoch in
            try await writer.read { db in
                try SuspensionState.check(since: epoch)
                return try value(db)
            }
        }
    }

    func write<T: Sendable>(_ updates: @Sendable (Database) throws -> T) async throws -> T {
        try await guardingSuspension { epoch in
            try await writer.write { db in
                // First thing inside the transaction: a write that only got the lock after the
                // announcement (it was waiting in the busy timeout) does no work and rolls back.
                try SuspensionState.check(since: epoch)
                return try updates(db)
            }
        }
    }

    func readBlocking<T>(_ value: (Database) throws -> T) throws -> T {
        try guardingSuspensionBlocking { epoch in
            try writer.read { db in
                try SuspensionState.check(since: epoch)
                return try value(db)
            }
        }
    }

    func writeBlocking<T>(_ updates: (Database) throws -> T) throws -> T {
        try guardingSuspensionBlocking { epoch in
            try writer.write { db in
                try SuspensionState.check(since: epoch)
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
        guard observesSuspension else { return try body(nil) }
        let epoch = try SuspensionState.enter()
        defer { SuspensionState.leave() }
        do {
            return try body(epoch)
        } catch {
            throw Self.mapSuspension(error, since: epoch)
        }
    }

    /// `SQLITE_ABORT` and `SQLITE_INTERRUPT` are what GRDB throws for a suspended database (its
    /// guide, "How to limit the 0xDEAD10CC exception"). They become `suspended` only if a
    /// suspension happened since the call started; otherwise (for example an interrupt from
    /// elsewhere) GRDB's error is passed on unchanged. Task cancellation reaches here as
    /// `CancellationError`, which GRDB checks before suspension, so it is never mapped.
    static func mapSuspension(_ error: any Error, since epoch: UInt64) -> any Error {
        if let error = error as? DatabaseError, error.isInterruptionError, SuspensionState.hasSuspended(since: epoch) {
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
