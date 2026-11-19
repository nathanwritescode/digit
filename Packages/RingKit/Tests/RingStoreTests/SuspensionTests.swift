import Foundation
import GRDB
import os
import SQLite3
import Testing
import RingProtocol
@testable import RingStore

// All data here is synthetic.
//
// Suspension is process-wide (GRDB's notifications are, and `swift test` runs every target in one
// process), so every test that suspends, or opens a store that observes suspension, lives in this
// one serialised suite, and no test anywhere else may call `appWillSuspend()` or `appDidResume()`.
// Every other file-backed test opens its store with `observesSuspension: false`. Each test that
// suspends resumes in a `defer`, so a failure cannot leave the process suspended.
//
// None of this suspends a real process: it checks the store's side of the contract (what happens
// after the announcement), not that iOS spares the app. Waits use signals; the few fixed delays are
// marked and only bound how long a test looks for something that must not happen.

/// A fresh directory dedicated to one database, removed afterwards.
private func withDatabaseURL(_ body: (URL) async throws -> Void) async throws {
    let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: dir) }
    try await body(dir.appendingPathComponent("ring.sqlite"))
}

/// Waits for `semaphore` on a dispatch thread, so a Swift concurrency thread is never blocked.
/// Fails the test instead of hanging if it is not signalled within ten seconds.
private func wait(for semaphore: DispatchSemaphore) async {
    let signalled = await withCheckedContinuation { continuation in
        DispatchQueue.global().async {
            continuation.resume(returning: semaphore.wait(timeout: .now() + 10) == .success)
        }
    }
    #expect(signalled, "timed out waiting for the other side")
}

/// Polls `condition` until it holds or `seconds` pass; returns whether it held.
private func eventually(within seconds: Double = 10, _ condition: () -> Bool) async -> Bool {
    let deadline = Date().addingTimeInterval(seconds)
    while !condition() {
        if Date() > deadline { return false }
        try? await Task.sleep(for: .milliseconds(2))
    }
    return true
}

/// A thread-safe flag.
private final class Flag: Sendable {
    private let stored = OSAllocatedUnfairLock<Bool>(initialState: false)
    var value: Bool { stored.withLock { $0 } }
    func set(_ value: Bool = true) { stored.withLock { $0 = value } }
}

/// Starts `appWillSuspend()` in its own task. The flag is set when it returns.
private func startAnnouncement() -> (task: Task<Void, Never>, done: Flag) {
    let done = Flag()
    let task = Task {
        await RingStore.appWillSuspend()
        done.set()
    }
    return (task, done)
}

/// How long to keep checking that an announcement has *not* returned while something it must wait
/// for is still running. A fixed bound: it only limits how long the test looks for a failure.
private let stillWaitingWindow = 0.3

/// Whether a connection with no busy timeout can take the write lock at once.
private func plainConnectionCanTakeWriteLock(_ url: URL) throws -> Bool {
    let queue = try DatabaseQueue(path: url.path)
    do {
        try queue.inDatabase { db in
            try db.execute(sql: "BEGIN IMMEDIATE")
            try db.execute(sql: "ROLLBACK")
        }
        return true
    } catch let error as DatabaseError where error.resultCode == .SQLITE_BUSY {
        return false
    }
}

private func insertRing(_ db: Database, _ id: UUID) throws {
    try db.execute(
        sql: "INSERT INTO ring (id, protocol_family, first_seen_at) VALUES (?, 'ycbt', 0)",
        arguments: [id.uuidString])
}

@Suite(.serialized, .timeLimit(.minutes(1))) struct SuspensionTests {
    // MARK: Opening

    @Test func asyncOpenerOpensMigratesAndReopens() async throws {
        try await withDatabaseURL { url in
            let ringID: UUID
            do {
                let store = try await RingStore.open(at: url)
                #expect(store.observesSuspension)
                #expect(try await store.writer.read { db in try RingStore.migrator.hasCompletedMigrations(db) })
                ringID = try await store.registerRing(protocolFamily: "ycbt").id
                #expect(try await store.insert(transfer(), ringID: ringID) == .inserted(id: 1, pruned: 0))
            }
            let reopened = try await RingStore.open(at: url)
            #expect(try await reopened.writer.read { db in try RingStore.migrator.hasCompletedMigrations(db) })
            #expect(try await reopened.ring(id: ringID) != nil)
            #expect(try await reopened.transfers(ringID: ringID).transfers.map(\.transfer.data) == [[1, 2, 3]])
        }
    }

    @Test func asyncOpenerReportsFileErrors() async throws {
        let url = URL(fileURLWithPath: "/nonexistent-directory-for-test/ring.sqlite")
        await #expect(throws: RingStoreError.fileAccess(path: url.path, errno: ENOENT)) {
            _ = try await RingStore.open(at: url)
        }
    }

    /// The review's reproduction: another coordinator (what a widget's opener will be) holds a
    /// coordinated read, so `open(at:)` waits in coordination after its entry check; the app
    /// announces suspension; the read is released. The open must fail, having created nothing.
    @Test func suspensionWhileWaitingForCoordinationRefusesTheOpen() async throws {
        try await withDatabaseURL { url in
            let held = DispatchSemaphore(value: 0), release = DispatchSemaphore(value: 0)
            DispatchQueue.global().async {
                var error: NSError?
                NSFileCoordinator(filePresenter: nil).coordinate(readingItemAt: url, options: [], error: &error) { _ in
                    held.signal()
                    _ = release.wait(timeout: .now() + 10)
                }
            }
            await wait(for: held)
            let opening = Task { try await RingStore.open(at: url) }
            // Fixed delay: let the open reach its wait for coordination.
            try await Task.sleep(for: .milliseconds(200))
            // The open is not counted while it waits, so the announcement does not wait for it.
            #expect(SuspensionState.inFlightCount == 0)
            let announcement = startAnnouncement()
            defer { RingStore.appDidResume() }
            #expect(await eventually(within: 5) { announcement.done.value }, "the announcement waited for the opener")
            #expect(SuspensionState.isSuspended)
            release.signal()
            await #expect(throws: RingStoreError.suspended) { _ = try await opening.value }
            await announcement.task.value
            #expect(!FileManager.default.fileExists(atPath: url.path))
        }
    }

    /// An announcement just before `DatabasePool.init` is not seen by GRDB (its observers are
    /// registered inside it), so the store's second check must refuse to migrate.
    @Test func suspensionJustBeforeThePoolExistsRefusesToMigrate() async throws {
        try await withDatabaseURL { url in
            defer { RingStore.appDidResume() }
            var hooks = RingStore.OpenHooks()
            hooks.beforePoolCreated = { SuspensionState.announce() }
            #expect(throws: RingStoreError.suspended) {
                _ = try RingStore(url: url, observesSuspension: true, hooks: hooks)
            }
            #expect(SuspensionState.inFlightCount == 0)
            let plain = try DatabaseQueue(path: url.path)
            #expect(try plain.read { db in try db.tableExists("grdb_migrations") } == false)
        }
    }

    @Test func refusesASchemaFromANewerBuild() async throws {
        try await withDatabaseURL { url in
            do {
                let store = try await RingStore.open(at: url)
                try await store.writer.write { db in
                    try db.execute(sql: "INSERT INTO grdb_migrations (identifier) VALUES ('v99-from-a-newer-build')")
                }
            }
            await #expect(throws: RingStoreError.schemaTooNew) { _ = try await RingStore.open(at: url) }
        }
    }

    // MARK: Files

    /// Persistent WAL is on for the writer (read back through `sqlite3_file_control`) and the files
    /// survive the last connection. Neither check can tell the store's setting from Apple's platform
    /// default: with the store's call removed, the system SQLite still reads back 1 and keeps the
    /// files (checked on macOS). They do fail if the flag is turned off (0).
    @Test func persistentWALIsSetAndTheFilesSurviveClosing() async throws {
        try await withDatabaseURL { url in
            do {
                let store = try await RingStore.open(at: url)
                let flag = try await store.writer.write { db -> CInt in
                    var flag: CInt = -1  // -1 queries the setting
                    _ = sqlite3_file_control(db.sqliteConnection, nil, SQLITE_FCNTL_PERSIST_WAL, &flag)
                    return flag
                }
                #expect(flag == 1)
                try await store.registerRing(protocolFamily: "ycbt")
            }
            for suffix in ["-wal", "-shm"] {
                #expect(FileManager.default.fileExists(atPath: url.path + suffix), "\(suffix) missing")
            }
        }
    }

    @Test func walSizeIsLimitedOnTheWriter() async throws {
        try await withDatabaseURL { url in
            let store = try await RingStore.open(at: url)
            let limit = try await store.writer.write { db in try Int.fetchOne(db, sql: "PRAGMA journal_size_limit") }
            #expect(limit == RingStore.journalSizeLimit)
            #expect(RingStore.journalSizeLimit == 4 * 1024 * 1024)
        }
    }

    /// The protection step as the real open path calls it (recorded through the injected setter):
    /// once before the pool opens (directory and database file; the `-wal` and `-shm` files do not
    /// exist yet, and will be created inside the protected directory) and once after migrating
    /// (all four). Whether iOS then enforces the attribute is for the device check (#53).
    @Test func openingProtectsTheDirectoryFirstAndThenAllThreeFiles() async throws {
        try await withDatabaseURL { url in
            let recorded = OSAllocatedUnfairLock<[String]>(initialState: [])
            var hooks = RingStore.OpenHooks()
            hooks.protect = { path in recorded.withLock { $0.append(path) } }
            _ = try RingStore(url: url, observesSuspension: true, hooks: hooks)
            let dir = url.deletingLastPathComponent().path
            #expect(recorded.withLock { $0 } == [
                dir, url.path,
                dir, url.path, url.path + "-wal", url.path + "-shm",
            ])
            #if os(iOS)
            _ = try RingStore(url: url)
            for path in [dir, url.path, url.path + "-wal", url.path + "-shm"] {
                let attributes = try FileManager.default.attributesOfItem(atPath: path)
                #expect(attributes[.protectionKey] as? FileProtectionType == .completeUntilFirstUserAuthentication)
            }
            #endif
        }
    }

    // MARK: Refusing calls

    @Test func suspendedStoreRefusesReadsAndWritesThenResumes() async throws {
        try await withDatabaseURL { url in
            let store = try await RingStore.open(at: url)
            let ringID = try await store.registerRing(protocolFamily: "ycbt").id
            try await store.insert(transfer(), ringID: ringID)

            await RingStore.appWillSuspend()
            do {
                defer { RingStore.appDidResume() }
                await #expect(throws: RingStoreError.suspended) { try await store.rings() }
                await #expect(throws: RingStoreError.suspended) { try await store.transfers(ringID: ringID) }
                await #expect(throws: RingStoreError.suspended) { try await store.latestSleepNight(ringID: ringID) }
                await #expect(throws: RingStoreError.suspended) {
                    try await store.registerRing(protocolFamily: "ycbt")
                }
                await #expect(throws: RingStoreError.suspended) {
                    try await store.insert(transfer(data: [1, 2, 3, 4]), ringID: ringID)
                }
                await #expect(throws: RingStoreError.suspended) { try await store.ingest(ringID: ringID, kind: .heartRate) }
                #expect(throws: RingStoreError.suspended) { try blocking(store).rings() }
                #expect(throws: RingStoreError.suspended) { try blocking(store).registerRing(protocolFamily: "ycbt") }
                // A stream created while suspended ends at once.
                await #expect(throws: RingStoreError.suspended) {
                    for try await _ in store.observeLatestSleepNight(ringID: ringID) {}
                }
                // Opening migrates, which takes the write lock.
                await #expect(throws: RingStoreError.suspended) { _ = try await RingStore.open(at: url) }
                // Announcing again with nothing in progress returns at once.
                await RingStore.appWillSuspend()
                await #expect(throws: RingStoreError.suspended) { try await store.rings() }
            }

            // Resumed: reads and writes work, and what was written before is intact.
            #expect(try await store.rings().map(\.id) == [ringID])
            #expect(try await store.transfers(ringID: ringID).transfers.map(\.transfer.data) == [[1, 2, 3]])
            #expect(try await store.insert(transfer(data: [1, 2, 3, 4]), ringID: ringID) == .inserted(id: 2, pruned: 1))
            #expect(try await store.transfers(ringID: ringID).transfers.map(\.transfer.data) == [[1, 2, 3, 4]])
            #expect(try await RingStore.open(at: url).rings().map(\.id) == [ringID])
        }
    }

    /// In-memory stores hold no file lock and ignore suspension.
    @Test func inMemoryStoreIgnoresSuspension() async throws {
        let store = try RingStore()
        await RingStore.appWillSuspend()
        defer { RingStore.appDidResume() }
        let ring = try await store.registerRing(protocolFamily: "ycbt")
        #expect(try await store.rings().map(\.id) == [ring.id])
    }

    /// An interruption is reported as `suspended` only when a suspension can explain it.
    @Test func interruptionIsMappedOnlyWhenSuspended() async throws {
        let busy = DatabaseError(resultCode: .SQLITE_BUSY)
        let interrupted = DatabaseError(resultCode: .SQLITE_INTERRUPT)
        let aborted = DatabaseError(resultCode: .SQLITE_ABORT)
        let epoch = try SuspensionState.enter()
        SuspensionState.leave()
        #expect((RingStore.mapSuspension(interrupted, since: epoch) as? DatabaseError)?.resultCode == .SQLITE_INTERRUPT)
        #expect((RingStore.mapSuspension(aborted, since: epoch) as? DatabaseError)?.resultCode == .SQLITE_ABORT)
        #expect((RingStore.mapSuspension(busy, since: epoch) as? DatabaseError)?.resultCode == .SQLITE_BUSY)
        await RingStore.appWillSuspend()
        RingStore.appDidResume()
        // A suspension happened during the operation, though the app has resumed since.
        #expect(RingStore.mapSuspension(interrupted, since: epoch) as? RingStoreError == .suspended)
        #expect(RingStore.mapSuspension(aborted, since: epoch) as? RingStoreError == .suspended)
        #expect(RingStore.mapSuspension(busy, since: epoch) as? RingStoreError == .suspended)
        let later = try SuspensionState.enter()
        SuspensionState.leave()
        #expect((RingStore.mapSuspension(busy, since: later) as? DatabaseError)?.resultCode == .SQLITE_BUSY)
        #expect(RingStore.mapSuspension(CancellationError(), since: epoch) is CancellationError)
    }

    // MARK: Draining calls in progress

    /// A write with a statement still to run when suspension is announced: the announcement waits
    /// for it, it is rolled back as a whole at that statement, and when the announcement returns a
    /// connection with no busy timeout can take the write lock at once.
    @Test func announcementWaitsForAWriteThatIsThenRolledBack() async throws {
        try await withDatabaseURL { url in
            let store = try await RingStore.open(at: url)
            let first = UUID(), second = UUID()
            let started = DispatchSemaphore(value: 0), proceed = DispatchSemaphore(value: 0)
            let write = Task {
                try await store.write { db in
                    try insertRing(db, first)
                    started.signal()
                    _ = proceed.wait(timeout: .now() + 10)
                    try insertRing(db, second)
                }
            }
            await wait(for: started)
            let announcement = startAnnouncement()
            defer { RingStore.appDidResume() }
            #expect(await eventually { SuspensionState.isSuspended })
            #expect(await !eventually(within: stillWaitingWindow) { announcement.done.value },
                    "returned while the write still held the lock")
            #expect(try !plainConnectionCanTakeWriteLock(url))
            proceed.signal()
            await announcement.task.value
            #expect(try plainConnectionCanTakeWriteLock(url))
            await #expect(throws: RingStoreError.suspended) { try await write.value }
            RingStore.appDidResume()
            #expect(try await store.rings().isEmpty)
            // The connection is usable again after resuming.
            try await store.write { db in try insertRing(db, first) }
            #expect(try await store.rings().map(\.id) == [first])
        }
    }

    /// A write whose statements have all run when suspension is announced completes: GRDB lets
    /// COMMIT through because it releases the lock. The call returns normally and the row is kept.
    @Test func writeThatOnlyHasToCommitCompletes() async throws {
        try await withDatabaseURL { url in
            let store = try await RingStore.open(at: url)
            let id = UUID()
            let started = DispatchSemaphore(value: 0), proceed = DispatchSemaphore(value: 0)
            let write = Task {
                try await store.write { db in
                    try insertRing(db, id)
                    started.signal()
                    _ = proceed.wait(timeout: .now() + 10)
                }
            }
            await wait(for: started)
            let announcement = startAnnouncement()
            defer { RingStore.appDidResume() }
            #expect(await eventually { SuspensionState.isSuspended })
            proceed.signal()
            await announcement.task.value
            try await write.value
            RingStore.appDidResume()
            #expect(try await store.rings().map(\.id) == [id])
        }
    }

    /// A write waiting in the busy timeout when suspension is announced is not woken (measured in
    /// review): it gets the lock later. It must then do no work, and the announcement waits for it.
    @Test func writeWaitingForTheLockDoesNoWorkAndIsWaitedFor() async throws {
        try await withDatabaseURL { url in
            let store = try await RingStore.open(at: url)
            // Another connection holds the write lock.
            let held = DispatchSemaphore(value: 0), release = DispatchSemaphore(value: 0)
            DispatchQueue.global().async {
                do {
                    let other = try DatabaseQueue(path: url.path)
                    try other.inDatabase { db in
                        try db.execute(sql: "BEGIN IMMEDIATE")
                        held.signal()
                        _ = release.wait(timeout: .now() + 10)
                        try db.execute(sql: "ROLLBACK")
                    }
                } catch {
                    held.signal()
                }
            }
            await wait(for: held)
            let ranBody = Flag()
            let write = Task {
                try await store.write { db in
                    ranBody.set()
                    try insertRing(db, UUID())
                }
            }
            #expect(await eventually { SuspensionState.inFlightCount == 1 })
            // Fixed delay: let the write get past GRDB's own check into SQLite's busy wait, the case
            // under test. (If it has not, GRDB refuses its BEGIN instead and the test still passes,
            // without exercising the store's in-transaction check.)
            try await Task.sleep(for: .milliseconds(200))
            let announcement = startAnnouncement()
            defer { RingStore.appDidResume() }
            #expect(await eventually { SuspensionState.isSuspended })
            #expect(await !eventually(within: stillWaitingWindow) { announcement.done.value },
                    "returned while a write was still waiting for the lock")
            release.signal()
            await announcement.task.value
            await #expect(throws: RingStoreError.suspended) { try await write.value }
            #expect(!ranBody.value, "the write ran its body after the announcement")
            #expect(try plainConnectionCanTakeWriteLock(url))
        }
    }

    /// A read in progress when suspension is announced is waited for and completes normally.
    @Test func announcementWaitsForAReadThatCompletes() async throws {
        try await withDatabaseURL { url in
            let store = try await RingStore.open(at: url)
            let ring = try await store.registerRing(protocolFamily: "ycbt")
            let started = DispatchSemaphore(value: 0), proceed = DispatchSemaphore(value: 0)
            let read = Task {
                try await store.read { db in
                    started.signal()
                    _ = proceed.wait(timeout: .now() + 10)
                    return try String.fetchAll(db, sql: "SELECT id FROM ring")
                }
            }
            await wait(for: started)
            let announcement = startAnnouncement()
            defer { RingStore.appDidResume() }
            #expect(await eventually { SuspensionState.isSuspended })
            #expect(await !eventually(within: stillWaitingWindow) { announcement.done.value },
                    "returned while a read was in progress")
            proceed.signal()
            await announcement.task.value
            #expect(try await read.value == [ring.id.uuidString])
        }
    }

    /// An active sleep observation is ended with `suspended`, and has stopped (it no longer counts
    /// as in progress) by the time the announcement returns.
    @Test func announcementEndsAnActiveObservation() async throws {
        try await withDatabaseURL { url in
            let store = try await RingStore.open(at: url)
            let ring = try await store.registerRing(protocolFamily: "ycbt")
            var iterator = store.observeLatestSleepNight(ringID: ring.id).makeAsyncIterator()
            #expect(try await iterator.next() == .some(nil))
            #expect(SuspensionState.inFlightCount == 1)
            let announcement = startAnnouncement()
            defer { RingStore.appDidResume() }
            let drained = await eventually(within: 5) { announcement.done.value }
            #expect(drained, "the announcement did not return: the observation was not ended")
            guard drained else {
                RingStore.appDidResume()  // ends the wait; the stream is left alone so the test cannot hang
                return
            }
            await announcement.task.value
            #expect(SuspensionState.inFlightCount == 0)
            do {
                _ = try await iterator.next()
                Issue.record("expected the stream to end with suspended")
            } catch {
                #expect(error as? RingStoreError == .suspended)
            }
        }
    }

    /// `appDidResume()` ends a wait at once, even with a call still in progress; that call then
    /// carries on normally.
    @Test func resumingEndsTheWait() async throws {
        try await withDatabaseURL { url in
            let store = try await RingStore.open(at: url)
            let first = UUID(), second = UUID()
            let started = DispatchSemaphore(value: 0), proceed = DispatchSemaphore(value: 0)
            let write = Task {
                try await store.write { db in
                    try insertRing(db, first)
                    started.signal()
                    _ = proceed.wait(timeout: .now() + 10)
                    try insertRing(db, second)
                }
            }
            await wait(for: started)
            let announcement = startAnnouncement()
            defer { proceed.signal() }
            #expect(await eventually { SuspensionState.isSuspended })
            RingStore.appDidResume()
            #expect(await eventually(within: 5) { announcement.done.value }, "resuming did not end the wait")
            proceed.signal()
            try await write.value
            #expect(Set(try await store.rings().map(\.id)) == [first, second])
        }
    }

    /// A second announcement while the first is waiting waits too, and both return when the call
    /// in progress ends.
    @Test func secondAnnouncementWhileDrainingWaitsToo() async throws {
        try await withDatabaseURL { url in
            let store = try await RingStore.open(at: url)
            let started = DispatchSemaphore(value: 0), proceed = DispatchSemaphore(value: 0)
            let write = Task {
                try await store.write { db in
                    try insertRing(db, UUID())
                    started.signal()
                    _ = proceed.wait(timeout: .now() + 10)
                    try insertRing(db, UUID())
                }
            }
            await wait(for: started)
            let one = startAnnouncement()
            defer { RingStore.appDidResume() }
            #expect(await eventually { SuspensionState.isSuspended })
            let two = startAnnouncement()
            #expect(await !eventually(within: stillWaitingWindow) { one.done.value || two.done.value })
            proceed.signal()
            await one.task.value
            await two.task.value
            await #expect(throws: RingStoreError.suspended) { try await write.value }
            RingStore.appDidResume()
            #expect(try await store.rings().isEmpty)
        }
    }

    // MARK: Two stores

    /// Two stores on one file: while one holds the write lock, the other's write waits (the busy
    /// timeout) instead of failing with "database is locked", and runs only after the release. A
    /// connection with GRDB's default (no busy timeout) is refused at once meanwhile.
    @Test func twoStoresOnOneFileWriteInTurn() async throws {
        try await withDatabaseURL { url in
            let a = try await RingStore.open(at: url)
            let b = try await RingStore.open(at: url)
            let heldID = UUID(), waitedID = UUID()
            let held = DispatchSemaphore(value: 0), release = DispatchSemaphore(value: 0)
            let released = Flag(), ranAfterRelease = Flag(), ran = Flag()
            let holder = Task {
                try await a.write { db in
                    try insertRing(db, heldID)
                    held.signal()
                    _ = release.wait(timeout: .now() + 10)
                }
            }
            await wait(for: held)
            let waiter = Task {
                try await b.write { db in
                    ran.set()
                    ranAfterRelease.set(released.value)
                    try insertRing(db, waitedID)
                }
            }
            // Both writes are in progress; b cannot have the lock while a holds it.
            #expect(await eventually { SuspensionState.inFlightCount == 2 })
            #expect(try !plainConnectionCanTakeWriteLock(url))
            #expect(!ran.value)
            released.set()
            release.signal()
            try await holder.value
            try await waiter.value
            #expect(ranAfterRelease.value)
            #expect(Set(try await a.rings().map(\.id)) == [heldID, waitedID])
        }
    }

    // MARK: Review fixes (second round)

    /// A cancelled announcer returns promptly with a write still holding the lock; the store stays
    /// suspended, and a later resume works.
    @Test func cancellingTheAnnouncementEndsTheWaitButKeepsTheSuspension() async throws {
        try await withDatabaseURL { url in
            let store = try await RingStore.open(at: url)
            let started = DispatchSemaphore(value: 0), proceed = DispatchSemaphore(value: 0)
            let write = Task {
                try await store.write { db in
                    try insertRing(db, UUID())
                    started.signal()
                    _ = proceed.wait(timeout: .now() + 10)
                    try insertRing(db, UUID())
                }
            }
            await wait(for: started)
            let announcement = startAnnouncement()
            defer { RingStore.appDidResume() }
            #expect(await eventually { SuspensionState.isSuspended })
            #expect(await !eventually(within: stillWaitingWindow) { announcement.done.value })
            announcement.task.cancel()
            #expect(await eventually(within: 5) { announcement.done.value }, "cancelling did not end the wait")
            #expect(SuspensionState.isSuspended)
            #expect(SuspensionState.inFlightCount == 1, "the write should still be running")
            await #expect(throws: RingStoreError.suspended) { try await store.rings() }
            proceed.signal()
            await #expect(throws: RingStoreError.suspended) { try await write.value }
            RingStore.appDidResume()
            #expect(try await store.rings().isEmpty)
        }
    }

    /// Write Y queues behind write X across a quick suspend and resume: the store is no longer
    /// suspended when Y gets the lock, so Y runs and is stored.
    @Test func writeQueuedAcrossSuspendAndResumeSucceeds() async throws {
        try await withDatabaseURL { url in
            let store = try await RingStore.open(at: url)
            let xID = UUID(), yID = UUID()
            let started = DispatchSemaphore(value: 0), proceed = DispatchSemaphore(value: 0)
            let x = Task {
                try await store.write { db in
                    try insertRing(db, xID)
                    started.signal()
                    _ = proceed.wait(timeout: .now() + 10)
                }
            }
            await wait(for: started)
            let y = Task { try await store.write { db in try insertRing(db, yID) } }
            #expect(await eventually { SuspensionState.inFlightCount == 2 })
            try await Task.sleep(for: .milliseconds(100))  // let Y queue behind X
            // X has only its commit left, so it completes; announce and resume while Y waits.
            let announcement = startAnnouncement()
            #expect(await eventually { SuspensionState.isSuspended })
            RingStore.appDidResume()
            await announcement.task.value
            proceed.signal()
            try await x.value
            try await y.value
            #expect(Set(try await store.rings().map(\.id)) == [xID, yID])
        }
    }

    /// A write whose busy timeout runs out while a suspension is in force fails with `suspended`,
    /// not SQLITE_BUSY; without a suspension the busy error passes through unchanged.
    @Test func busyTimeoutDuringSuspensionIsReportedAsSuspended() async throws {
        try await withDatabaseURL { url in
            var hooks = RingStore.OpenHooks()
            hooks.busyTimeout = 0.5
            let store = try RingStore(url: url, observesSuspension: true, hooks: hooks)
            let held = DispatchSemaphore(value: 0), release = DispatchSemaphore(value: 0)
            DispatchQueue.global().async {
                do {
                    let other = try DatabaseQueue(path: url.path)
                    try other.inDatabase { db in
                        try db.execute(sql: "BEGIN IMMEDIATE")
                        held.signal()
                        _ = release.wait(timeout: .now() + 10)
                        try db.execute(sql: "ROLLBACK")
                    }
                } catch { held.signal() }
            }
            await wait(for: held)
            defer { release.signal() }
            // No suspension: plain contention is passed through.
            do {
                try await store.write { db in try insertRing(db, UUID()) }
                Issue.record("expected SQLITE_BUSY")
            } catch {
                #expect((error as? DatabaseError)?.resultCode == .SQLITE_BUSY)
            }
            // Suspension announced while the write waits for the lock.
            let ranBody = Flag()
            let write = Task { try await store.write { db in ranBody.set(); try insertRing(db, UUID()) } }
            #expect(await eventually { SuspensionState.inFlightCount == 1 })
            try await Task.sleep(for: .milliseconds(100))  // let it reach SQLite's busy wait
            let announcement = startAnnouncement()
            defer { RingStore.appDidResume() }
            await #expect(throws: RingStoreError.suspended) { try await write.value }
            await announcement.task.value
            #expect(!ranBody.value)
        }
    }
}
