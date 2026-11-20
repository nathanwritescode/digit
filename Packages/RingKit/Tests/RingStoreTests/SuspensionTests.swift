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
// one serialised suite, and no test anywhere else may call `appWillSuspend()`, `announceSuspension()`,
// `drainSuspension()` or `appDidResume()`.
// Every other file-backed test opens its store with `observesSuspension: false`. Each test that
// suspends resumes in a `defer`, so a failure cannot leave the process suspended.
//
// None of this suspends a real process: it checks the store's side of the contract (what happens
// after the announcement), not that iOS spares the app. Waits use signals, and every wait on a task
// is bounded (`finished(_:)`), because `.timeLimit` does not stop an `await` on an unstructured
// task. Fixed delays that remain: `stillWaitingWindow`, which only bounds how long a test looks for
// something that must not happen (an announcement returning early); and one in
// `busyTimeoutAfterResumeIsNotReportedAsSuspended`, which cannot observe that a write has entered
// SQLite's busy wait and so waits a fixed 300 ms to make it likely, then repeats an attempt in
// which the write was refused instead (documented there).

/// A fresh directory dedicated to one database, removed afterwards.
private func withDatabaseURL(_ body: (URL) async throws -> Void) async throws {
    let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: dir) }
    try await body(dir.appendingPathComponent("ring.sqlite"))
}

/// Expects `task` to finish by throwing `expected`.
private func expectFailure<T: Sendable, E: Error & Equatable>(_ task: Task<T, any Error>, _ expected: E) async {
    guard let result = await finished(task) else { return }
    switch result {
    case .success: Issue.record("expected \(expected), but the task succeeded")
    case .failure(let error): #expect(error as? E == expected, "got \(error)")
    }
}

/// A thread-safe flag.
private final class Flag: Sendable {
    private let stored = OSAllocatedUnfairLock<Bool>(initialState: false)
    var value: Bool { stored.withLock { $0 } }
    func set(_ value: Bool = true) { stored.withLock { $0 = value } }
}

/// Starts `appWillSuspend()` in its own task. The flag is set when it returns.
private func startAnnouncement() -> (task: Task<Void, any Error>, done: Flag) {
    let done = Flag()
    let task = Task {
        try await RingStore.appWillSuspend()
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
    /// coordinated read, so the open waits in coordination; the app announces suspension; the read
    /// is released. The open must fail, having created nothing, and the announcement must not have
    /// waited for it. The open signals just before it asks for coordination (the hook), and the
    /// test's coordinated read is already held, so the open can only get past coordination after the
    /// release. Residual gap: the signal comes before the open is actually blocked inside
    /// `coordinate`, so the test cannot prove it was blocked there; what it does prove is the order
    /// that matters (announce, then release), and that the open is not counted in flight before
    /// coordination is granted.
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
            defer { release.signal() }
            let aboutToWait = DispatchSemaphore(value: 0)
            var mutable = RingStore.OpenHooks()
            mutable.beforeCoordination = { aboutToWait.signal() }
            let hooks = mutable
            let opening = Task<RingStore, any Error> {
                try await withCheckedThrowingContinuation { continuation in
                    DispatchQueue.global().async {
                        continuation.resume(with: Result { try RingStore(url: url, observesSuspension: true, hooks: hooks) })
                    }
                }
            }
            await wait(for: aboutToWait)
            // The open is not counted while it waits, so the announcement does not wait for it.
            #expect(SuspensionState.inFlightCount == 0)
            let announcement = startAnnouncement()
            defer { RingStore.appDidResume() }
            #expect(await eventually(within: 5) { announcement.done.value }, "the announcement waited for the opener")
            #expect(SuspensionState.isSuspended)
            release.signal()
            await expectFailure(opening, RingStoreError.suspended)
            _ = await finished(announcement.task)
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

            try await RingStore.appWillSuspend()
            do {
                defer { RingStore.appDidResume() }
                await #expect(throws: RingStoreError.suspended) { try await store.rings() }
                await #expect(throws: RingStoreError.suspended) { try await store.transfers(ringID: ringID) }
                await #expect(throws: RingStoreError.suspended) { try await store.latestSleepNight(ringID: ringID) }
                let days = (first: RingDay(year: 2027, month: 1, day: 1), last: RingDay(year: 2027, month: 1, day: 30))
                await #expect(throws: RingStoreError.suspended) {
                    try await store.sleepNights(ringID: ringID, from: days.first, through: days.last)
                }
                await #expect(throws: RingStoreError.suspended) {
                    try await store.sleepNightDetail(ringID: ringID, startingAt: Date(timeIntervalSince1970: 0))
                }
                await #expect(throws: RingStoreError.suspended) {
                    try await store.dailySummaries(ringID: ringID, from: days.first, through: days.last)
                }
                await #expect(throws: RingStoreError.suspended) {
                    try await store.sleepNightDetails(ringID: ringID, from: days.first, through: days.last)
                }
                await #expect(throws: RingStoreError.suspended) {
                    try await store.heartRateSummary(
                        ringID: ringID, within: [DateInterval(start: Date(timeIntervalSince1970: 0), duration: 60)])
                }
                // Even with nothing to read, the call is refused before touching SQLite.
                await #expect(throws: RingStoreError.suspended) { try await store.heartRateSummary(ringID: ringID, within: []) }
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
                try await RingStore.appWillSuspend()
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
        try await RingStore.appWillSuspend()
        defer { RingStore.appDidResume() }
        let ring = try await store.registerRing(protocolFamily: "ycbt")
        #expect(try await store.rings().map(\.id) == [ring.id])
    }

    /// An interruption is reported as `suspended` only when a suspension can explain it; SQLITE_BUSY
    /// only while suspended at the moment of mapping.
    @Test func interruptionIsMappedOnlyWhenSuspended() async throws {
        let busy = DatabaseError(resultCode: .SQLITE_BUSY)
        let interrupted = DatabaseError(resultCode: .SQLITE_INTERRUPT)
        let aborted = DatabaseError(resultCode: .SQLITE_ABORT)
        let epoch = try SuspensionState.enter()
        SuspensionState.leave()
        #expect((RingStore.mapSuspension(interrupted, since: epoch) as? DatabaseError)?.resultCode == .SQLITE_INTERRUPT)
        #expect((RingStore.mapSuspension(aborted, since: epoch) as? DatabaseError)?.resultCode == .SQLITE_ABORT)
        #expect((RingStore.mapSuspension(busy, since: epoch) as? DatabaseError)?.resultCode == .SQLITE_BUSY)
        try await RingStore.appWillSuspend()
        RingStore.appDidResume()
        // A suspension happened during the operation, though the app has resumed since.
        #expect(RingStore.mapSuspension(interrupted, since: epoch) as? RingStoreError == .suspended)
        #expect(RingStore.mapSuspension(aborted, since: epoch) as? RingStoreError == .suspended)
        // BUSY is mapped only while suspended now, so after the resume it passes through.
        #expect((RingStore.mapSuspension(busy, since: epoch) as? DatabaseError)?.resultCode == .SQLITE_BUSY)
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
            _ = await finished(announcement.task)
            #expect(try plainConnectionCanTakeWriteLock(url))
            await expectFailure(write, RingStoreError.suspended)
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
            _ = await finished(announcement.task)
            try await finished(write)?.get()
            RingStore.appDidResume()
            #expect(try await store.rings().map(\.id) == [id])
        }
    }

    /// How many times a test may set up a write waiting for the lock before it gives up on getting
    /// the write into SQLite's busy wait ahead of the announcement (see the two tests that use it).
    private static let busyWaitAttempts = 5

    /// A write waiting in the busy timeout when suspension is announced is not woken (measured in
    /// review): it gets the lock later. It must then do no work, and the announcement waits for it.
    ///
    /// Whether the write has reached SQLite's busy wait cannot be observed. If the announcement
    /// lands first, GRDB refuses the write's BEGIN, the write fails with `suspended` at once and the
    /// announcement returns at once: right, but not this scenario. That case is told apart from a
    /// real failure afterwards (the write has already left while the lock is still held, which a
    /// write in the busy wait cannot do before its 5 s busy timeout) and the attempt is repeated.
    /// It used to count as a failure: 1 run in 30 of the store tests beside busy processes (#68).
    @Test func writeWaitingForTheLockDoesNoWorkAndIsWaitedFor() async throws {
        var exercised = false
        for _ in 0..<Self.busyWaitAttempts where !exercised {
            exercised = try await writeWaitingForTheLockAttempt()
        }
        #expect(exercised, "the write never reached the busy wait before the announcement")
    }

    /// One attempt of `writeWaitingForTheLockDoesNoWorkAndIsWaitedFor`. Returns false if GRDB
    /// refused the write before it waited for the lock.
    private func writeWaitingForTheLockAttempt() async throws -> Bool {
        var exercised = true
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
            let announcement = startAnnouncement()
            defer { RingStore.appDidResume() }
            #expect(await eventually { SuspensionState.isSuspended })
            if await eventually(within: stillWaitingWindow, { announcement.done.value }) {
                // The announcement returns only once no call is in flight, unless it is broken.
                if SuspensionState.inFlightCount == 0 {
                    exercised = false  // the write left with the lock still held: it was refused
                } else {
                    Issue.record("returned while a write was still waiting for the lock")
                }
            }
            release.signal()
            _ = await finished(announcement.task)
            await expectFailure(write, RingStoreError.suspended)
            #expect(!ranBody.value, "the write ran its body after the announcement")
            #expect(try plainConnectionCanTakeWriteLock(url))
        }
        return exercised
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
            _ = await finished(announcement.task)
            #expect(try await finished(read)?.get() == [ring.id.uuidString])
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
            _ = await finished(announcement.task)
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
            try await finished(write)?.get()
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
            _ = await finished(one.task)
            _ = await finished(two.task)
            await expectFailure(write, RingStoreError.suspended)
            RingStore.appDidResume()
            #expect(try await store.rings().isEmpty)
        }
    }

    /// `announceSuspension()` refuses new calls before it returns, without waiting for a write in
    /// progress (#53: an expiration handler calls it, then ends its task); `appWillSuspend()`
    /// afterwards announces nothing more and waits for that write as usual.
    @Test func synchronousAnnouncementRefusesAtOnceAndTheDrainStillWaits() async throws {
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
            defer { proceed.signal() }
            RingStore.announceSuspension()
            defer { RingStore.appDidResume() }
            // Synchronous: suspended on return, with the write still in progress.
            #expect(SuspensionState.isSuspended)
            #expect(SuspensionState.inFlightCount == 1)
            await #expect(throws: RingStoreError.suspended) { try await store.rings() }

            let drain = startAnnouncement()
            #expect(await !eventually(within: stillWaitingWindow) { drain.done.value })
            proceed.signal()
            _ = await finished(drain.task)
            await expectFailure(write, RingStoreError.suspended)
            RingStore.appDidResume()
            #expect(try await store.rings().isEmpty)
        }
    }

    /// Announcing while already suspended posts nothing more to GRDB; a resume undoes it once.
    @Test func announcingAgainWhileSuspendedDoesNothing() async throws {
        try await withDatabaseURL { url in
            let store = try await RingStore.open(at: url)
            let posted = OSAllocatedUnfairLock(initialState: 0)
            let observer = NotificationCenter.default.addObserver(
                forName: Database.suspendNotification, object: nil, queue: nil
            ) { _ in posted.withLock { $0 += 1 } }
            defer { NotificationCenter.default.removeObserver(observer) }
            RingStore.announceSuspension()
            RingStore.announceSuspension()
            try await RingStore.appWillSuspend()
            #expect(posted.withLock { $0 } == 1)
            await #expect(throws: RingStoreError.suspended) { try await store.rings() }
            RingStore.appDidResume()
            #expect(try await store.rings().isEmpty)
        }
    }

    /// A drain after a resume finds nothing to wait for and does not suspend again (#53: the app's
    /// drain can run after it has resumed).
    @Test func drainAfterResumeReturnsAtOnceAndLeavesTheStoreResumed() async throws {
        try await withDatabaseURL { url in
            let store = try await RingStore.open(at: url)
            RingStore.announceSuspension()
            RingStore.appDidResume()
            let drained = Flag()
            let drain = Task {
                try await RingStore.drainSuspension()
                drained.set()
            }
            #expect(await eventually(within: 5) { drained.value })
            _ = await finished(drain)
            #expect(!SuspensionState.isSuspended)
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
            try await finished(holder)?.get()
            try await finished(waiter)?.get()
            #expect(ranAfterRelease.value)
            #expect(Set(try await a.rings().map(\.id)) == [heldID, waitedID])
        }
    }

    // MARK: Review fixes (second round)

    /// A cancelled announcer throws `CancellationError` promptly with a write still holding the lock
    /// (so the caller knows a lock may be held); the store stays suspended, and a later resume works.
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
            // The wait ends with CancellationError, not as if the calls had drained.
            guard let outcome = await finished(announcement.task) else { return }
            if case .failure(let error) = outcome { #expect(error is CancellationError, "got \(error)") }
            else { Issue.record("a cancelled wait returned as if the calls had drained") }
            #expect(!announcement.done.value)
            #expect(SuspensionState.isSuspended)
            #expect(SuspensionState.inFlightCount == 1, "the write should still be running")
            await #expect(throws: RingStoreError.suspended) { try await store.rings() }
            proceed.signal()
            await expectFailure(write, RingStoreError.suspended)
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
            // X has only its commit left, so it completes; announce and resume while Y waits.
            let announcement = startAnnouncement()
            #expect(await eventually { SuspensionState.isSuspended })
            RingStore.appDidResume()
            _ = await finished(announcement.task)
            proceed.signal()
            try await finished(x)?.get()
            try await finished(y)?.get()
            #expect(Set(try await store.rings().map(\.id)) == [xID, yID])
        }
    }

    /// Holds the write lock from another connection until `release` is signalled.
    private func holdWriteLock(_ url: URL, held: DispatchSemaphore, release: DispatchSemaphore) {
        DispatchQueue.global().async {
            do {
                let other = try DatabaseQueue(path: url.path)
                try other.inDatabase { db in
                    try db.execute(sql: "BEGIN IMMEDIATE")
                    held.signal()
                    _ = release.wait(timeout: .now() + 20)
                    try db.execute(sql: "ROLLBACK")
                }
            } catch { held.signal() }
        }
    }

    /// A write whose busy timeout runs out while the store is suspended fails with `suspended`,
    /// not SQLITE_BUSY; without a suspension the busy error passes through unchanged. Timing: the
    /// announcement must land before the 1 s timeout runs out (it does within milliseconds). It does
    /// not force which path produces `suspended` (the busy error mapped, or GRDB refusing the write
    /// first because the announcement came before the write's BEGIN), since a custom busy handler is
    /// not available through GRDB's `busyMode` and so entry into SQLite's busy wait cannot be
    /// observed; `busyTimeoutAfterResumeIsNotReportedAsSuspended` is the one that pins the mapping.
    @Test func busyTimeoutDuringSuspensionIsReportedAsSuspended() async throws {
        try await withDatabaseURL { url in
            var hooks = RingStore.OpenHooks()
            hooks.busyTimeout = 1
            let store = try RingStore(url: url, observesSuspension: true, hooks: hooks)
            let held = DispatchSemaphore(value: 0), release = DispatchSemaphore(value: 0)
            holdWriteLock(url, held: held, release: release)
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
            let announcement = startAnnouncement()
            defer { RingStore.appDidResume() }
            await expectFailure(write, RingStoreError.suspended)
            _ = await finished(announcement.task)
            #expect(!ranBody.value)
        }
    }

    /// The reviewed sequence: another connection holds the write lock, a store write waits in the
    /// busy timeout, the app suspends and resumes within the timeout, and the timeout then runs
    /// out. Contention was the only cause, so the raw SQLITE_BUSY must pass through. With the
    /// mapping made epoch-based again it fails.
    ///
    /// Timing: the write must be in SQLite's busy wait before the announcement, or GRDB refuses it
    /// (ABORT, a real `suspended`). That cannot be observed, so the attempt waits a fixed 300 ms
    /// first, which only makes the first attempt likely to get there. A refused write is told apart
    /// afterwards by its duration: one that went through the busy wait cannot fail before the 1.5 s
    /// busy timeout (the lock is held throughout), and one refused fails at once. A refused attempt
    /// is repeated. Before #68 a refusal failed the test: a stall of over 300 ms before the write's
    /// BEGIN failed it if the BEGIN then fell between the announcement and the resume (a window of
    /// milliseconds; never seen failing). Now a spurious failure needs a refused write taking 1.5 s.
    @Test func busyTimeoutAfterResumeIsNotReportedAsSuspended() async throws {
        var exercised = false
        for _ in 0..<Self.busyWaitAttempts where !exercised {
            exercised = true
            try await busyTimeoutAfterResumeAttempt(exercised: &exercised)
        }
        #expect(exercised, "the write never reached the busy wait before the announcement")
    }

    /// One attempt of `busyTimeoutAfterResumeIsNotReportedAsSuspended`. Sets `exercised` to false
    /// if GRDB refused the write before it waited for the lock.
    private func busyTimeoutAfterResumeAttempt(exercised: inout Bool) async throws {
        try await withDatabaseURL { url in
            let busyTimeout = 1.5
            var hooks = RingStore.OpenHooks()
            hooks.busyTimeout = busyTimeout
            let store = try RingStore(url: url, observesSuspension: true, hooks: hooks)
            let held = DispatchSemaphore(value: 0), release = DispatchSemaphore(value: 0)
            holdWriteLock(url, held: held, release: release)
            await wait(for: held)
            defer { release.signal() }
            let ranBody = Flag()
            let started = ContinuousClock.now
            let ended = OSAllocatedUnfairLock<ContinuousClock.Instant?>(initialState: nil)
            let write = Task {
                defer { ended.withLock { $0 = ContinuousClock.now } }
                try await store.write { db in ranBody.set(); try insertRing(db, UUID()) }
            }
            #expect(await eventually { SuspensionState.inFlightCount == 1 })
            try await Task.sleep(for: .milliseconds(300))  // makes reaching SQLite's busy wait likely
            let announcement = startAnnouncement()
            #expect(await eventually { SuspensionState.isSuspended })
            RingStore.appDidResume()
            _ = await finished(announcement.task)
            guard let outcome = await finished(write) else { return }
            let took = try #require(ended.withLock { $0 }) - started
            switch outcome {
            case .success: Issue.record("expected SQLITE_BUSY")
            case .failure(let error as RingStoreError) where error == .suspended && took < .seconds(busyTimeout):
                exercised = false  // refused before it waited for the lock
            case .failure(let error): #expect((error as? DatabaseError)?.resultCode == .SQLITE_BUSY, "got \(error)")
            }
            #expect(!ranBody.value)
        }
    }

    // MARK: Derivation outside the write (#69)

    /// A suspension announced while `ingest` derives: no call is in progress then (the derivation
    /// holds no database access, let alone a lock), so the announcement returns at once, and the
    /// derivation stops at its next checkpoint without reaching the write. Fails if derivation is
    /// moved back inside the write (the announcement would wait for it). The ingest is the blocking
    /// form on a dispatch thread, so the hook that holds the derivation still blocks no Swift
    /// concurrency thread; both forms run the same checkpoint.
    @Test func aSuspensionDuringDerivationStopsItBeforeTheWrite() async throws {
        try await withDatabaseURL { url in
            let store = try await RingStore.open(at: url)
            var history = try VariedHistory(store)
            try history.addDays(0..<2)
            let ringID = history.ringID
            try await store.ingest(ringID: ringID, kind: .heartRate)
            try history.addDays(2..<3)
            let before = try TypedSnapshot(store, ringID: ringID)
            let deriving = DispatchSemaphore(value: 0), proceed = DispatchSemaphore(value: 0)
            let paused = Flag(), reachedApply = Flag()
            let hooks = RingStore.IngestionHooks(
                checkpoint: { _ in
                    guard !paused.value else { return }
                    paused.set()
                    deriving.signal()
                    _ = proceed.wait(timeout: .now() + 10)
                },
                beforeApply: { _ in reachedApply.set() })
            let outcome = OSAllocatedUnfairLock<Result<IngestionResult, any Error>?>(initialState: nil)
            let finished = DispatchSemaphore(value: 0)
            DispatchQueue.global().async {
                let result = Result { try blocking(store).ingest(ringID: ringID, kind: .heartRate, hooks: hooks) }
                outcome.withLock { $0 = result }
                finished.signal()
            }
            await wait(for: deriving)
            defer { RingStore.appDidResume() }
            #expect(SuspensionState.inFlightCount == 0)
            let announcement = startAnnouncement()
            #expect(await eventually { announcement.done.value }, "the announcement waited for the derivation")
            proceed.signal()
            await wait(for: finished)
            switch outcome.withLock({ $0 }) {
            case .failure(let error): #expect(error as? RingStoreError == .suspended, "got \(error)")
            case .success: Issue.record("expected suspended, but the ingest succeeded")
            case nil: Issue.record("the ingest did not finish")
            }
            #expect(!reachedApply.value)
            RingStore.appDidResume()
            #expect(try TypedSnapshot(store, ringID: ringID) == before)
            // After the resume the same call goes through.
            #expect(try await store.ingest(ringID: ringID, kind: .heartRate).inserted > 0)
        }
    }
}
