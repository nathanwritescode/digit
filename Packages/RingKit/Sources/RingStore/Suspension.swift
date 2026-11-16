import Foundation
import GRDB

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
// interrupts the statement the pool's writer connection is running, and from then on any
// statement on that connection that could take a lock rolls the open transaction back and throws
// `SQLITE_ABORT` ("Database is suspended"). ROLLBACK, COMMIT and RELEASE are let through because
// they release locks. Reads in WAL mode are deliberately not suspended: GRDB never interrupts a
// pool's reader connections.
//
// The store adds two things on top:
// - It refuses every operation, reads included, while suspended, before touching SQLite, so no
//   new lock of any kind (a WAL read lock is a lock on the `-shm` file) is taken after the
//   announcement. That is stricter than GRDB, which lets WAL reads through. A read that had
//   already started is not interrupted and completes.
// - It maps GRDB's interruption errors to `RingStoreError.suspended` only when a suspension can
//   explain them (see `SuspensionState`), so the app never has to import GRDB to recognise them.

/// Process-wide suspension state. GRDB's notifications are process-wide (posted on
/// `NotificationCenter.default` and observed with no sender filter), so suspension is too: it
/// applies to every store in the process that observes it.
enum SuspensionState {
    private struct State {
        var suspended = false
        /// Incremented by every suspension, so an operation can tell that one happened while it ran
        /// even if the app has resumed since.
        var epoch: UInt64 = 0
    }

    private static let lock = NSLock()
    // Guarded by `lock`.
    nonisolated(unsafe) private static var state = State()
    /// Serialises the two announcements with their notifications, so a suspend and a resume
    /// racing each other cannot leave the store's flag and GRDB's disagreeing.
    private static let announcementLock = NSLock()

    static func suspend() {
        announcementLock.withLock {
            // Flag first, so new operations are refused before GRDB interrupts the running one.
            lock.withLock {
                state.suspended = true
                state.epoch &+= 1
            }
            NotificationCenter.default.post(name: Database.suspendNotification, object: nil)
        }
    }

    static func resume() {
        announcementLock.withLock {
            // GRDB first, so nothing passes the store's check while GRDB still refuses it.
            NotificationCenter.default.post(name: Database.resumeNotification, object: nil)
            lock.withLock { state.suspended = false }
        }
    }

    /// The current epoch, or `RingStoreError.suspended` if the store is suspended now.
    static func enter() throws -> UInt64 {
        try lock.withLock {
            if state.suspended { throw RingStoreError.suspended }
            return state.epoch
        }
    }

    /// Whether a suspension can explain an interruption seen by an operation that entered at
    /// `epoch`: the store is suspended now, or was suspended at some point since.
    static func hasSuspended(since epoch: UInt64) -> Bool {
        lock.withLock { state.suspended || state.epoch != epoch }
    }
}

extension RingStore {
    // MARK: App lifecycle

    /// Call when the app is about to be suspended (for example from the scene moving to the
    /// background, and at the end of every background task that used the store). Suspends every
    /// file-backed store in this process: from the moment this returns, no store takes a new
    /// database lock, and every call (reads included) throws `RingStoreError.suspended`. A write
    /// in progress is interrupted, rolled back as a whole and throws `suspended`, unless only its
    /// commit was left to run: GRDB lets a commit through because it releases the lock, so that
    /// write completes and returns normally. Either way it is never half-applied. A read already in
    /// progress is not interrupted and completes. Opening a store while suspended also throws
    /// `suspended`.
    ///
    /// Posts GRDB's `Database.suspendNotification`, which is process-wide, so the app does not need
    /// to import GRDB. Safe to call from any thread and more than once. In-memory stores are not
    /// affected. Unverified on a device: tests cannot suspend a process.
    public static func appWillSuspend() {
        SuspensionState.suspend()
    }

    /// Call when the app has resumed: when it returns to the foreground, and at the start of every
    /// background task or background-mode callback that may use the store (GRDB's guide asks for
    /// both). Undoes `appWillSuspend()` for every store in this process. Safe to call from any
    /// thread and when not suspended.
    public static func appDidResume() {
        SuspensionState.resume()
    }

    // MARK: Guarded database access

    // Every database access in the store goes through these four, so the suspension check and the
    // error mapping cannot be skipped by one call. The blocking pair serves the internal blocking
    // forms used by tests; distinct names keep a synchronous overload from being picked silently
    // in an async context.

    func read<T: Sendable>(_ value: @Sendable (Database) throws -> T) async throws -> T {
        try await guardingSuspension { try await writer.read(value) }
    }

    func write<T: Sendable>(_ updates: @Sendable (Database) throws -> T) async throws -> T {
        try await guardingSuspension { try await writer.write(updates) }
    }

    func readBlocking<T>(_ value: (Database) throws -> T) throws -> T {
        try guardingSuspensionBlocking { try writer.read(value) }
    }

    func writeBlocking<T>(_ updates: (Database) throws -> T) throws -> T {
        try guardingSuspensionBlocking { try writer.write(updates) }
    }

    func guardingSuspension<T>(_ body: () async throws -> T) async throws -> T {
        guard observesSuspension else { return try await body() }
        let epoch = try SuspensionState.enter()
        do {
            return try await body()
        } catch {
            throw Self.mapSuspension(error, since: epoch)
        }
    }

    func guardingSuspensionBlocking<T>(_ body: () throws -> T) throws -> T {
        guard observesSuspension else { return try body() }
        let epoch = try SuspensionState.enter()
        do {
            return try body()
        } catch {
            throw Self.mapSuspension(error, since: epoch)
        }
    }

    /// `SQLITE_ABORT` and `SQLITE_INTERRUPT` are what GRDB throws for a suspended database (its
    /// guide, "How to limit the 0xDEAD10CC exception"). They become `suspended` only if a
    /// suspension happened since the operation started; otherwise (for example an interrupt from
    /// elsewhere) GRDB's error is passed on unchanged. Task cancellation reaches here as
    /// `CancellationError`, which GRDB checks before suspension, so it is never mapped.
    static func mapSuspension(_ error: any Error, since epoch: UInt64) -> any Error {
        if let error = error as? DatabaseError, error.isInterruptionError, SuspensionState.hasSuspended(since: epoch) {
            return RingStoreError.suspended
        }
        return error
    }
}
