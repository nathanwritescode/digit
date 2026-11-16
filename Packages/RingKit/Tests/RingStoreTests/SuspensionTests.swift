import Foundation
import GRDB
import Testing
import RingProtocol
@testable import RingStore

// All data here is synthetic.
//
// Suspension is process-wide (GRDB's notifications are), so every test that suspends, or opens a
// store that observes suspension, lives in this one serialised suite. Every other file-backed test
// opens its store with `observesSuspension: false`. Each test that suspends resumes in a `defer`,
// so a failure cannot leave the process suspended.
//
// None of this suspends a real process: it checks the store's side of the contract (what happens
// after the announcement), not that iOS spares the app.

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

/// Raw SQL through the store's guarded write path (suspension check and error mapping included).
private func guardedInsertRing(_ db: Database, _ id: UUID) throws {
    try db.execute(
        sql: "INSERT INTO ring (id, protocol_family, first_seen_at) VALUES (?, 'ycbt', 0)",
        arguments: [id.uuidString])
}

@Suite(.serialized) struct SuspensionTests {
    @Test(.timeLimit(.minutes(1))) func asyncOpenerOpensMigratesAndReopens() async throws {
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

    @Test(.timeLimit(.minutes(1))) func asyncOpenerReportsFileErrors() async throws {
        let url = URL(fileURLWithPath: "/nonexistent-directory-for-test/ring.sqlite")
        await #expect(throws: RingStoreError.fileAccess(path: url.path, errno: ENOENT)) {
            _ = try await RingStore.open(at: url)
        }
    }

    /// Persistent WAL (for the future read-only opener): the `-wal` and `-shm` files outlive the
    /// last connection. Without it SQLite deletes them when the last connection closes.
    @Test(.timeLimit(.minutes(1))) func walFilesSurviveClosing() async throws {
        try await withDatabaseURL { url in
            do {
                let store = try await RingStore.open(at: url)
                try await store.registerRing(protocolFamily: "ycbt")
            }
            for suffix in ["-wal", "-shm"] {
                #expect(FileManager.default.fileExists(atPath: url.path + suffix), "\(suffix) missing")
            }
        }
    }

    /// The protection attribute is set on iOS only, and the simulator does not enforce it; the
    /// device check is #53. What can be checked here is that the directory and all three files
    /// exist after opening and are each handed to the protection step.
    @Test(.timeLimit(.minutes(1))) func protectionCoversTheDirectoryAndThreeFiles() async throws {
        try await withDatabaseURL { url in
            let store = try await RingStore.open(at: url)
            _ = store
            var protected: [String] = []
            RingStore.applyProtection(at: url) { protected.append($0) }
            #expect(protected == [
                url.deletingLastPathComponent().path, url.path, url.path + "-wal", url.path + "-shm",
            ])
            #if os(iOS)
            for path in protected {
                let attributes = try FileManager.default.attributesOfItem(atPath: path)
                #expect(attributes[.protectionKey] as? FileProtectionType == .completeUntilFirstUserAuthentication)
            }
            #endif
        }
    }

    @Test(.timeLimit(.minutes(1))) func suspendedStoreRefusesReadsAndWritesThenResumes() async throws {
        try await withDatabaseURL { url in
            let store = try await RingStore.open(at: url)
            let ringID = try await store.registerRing(protocolFamily: "ycbt").id
            try await store.insert(transfer(), ringID: ringID)

            RingStore.appWillSuspend()
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
                await #expect(throws: RingStoreError.suspended) {
                    for try await _ in store.observeLatestSleepNight(ringID: ringID) {}
                }
                // Opening migrates, which takes the write lock.
                await #expect(throws: RingStoreError.suspended) { _ = try await RingStore.open(at: url) }
                // Announcing twice is harmless.
                RingStore.appWillSuspend()
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

    /// A write that still has a statement to run when suspension is announced is rolled back as a
    /// whole: GRDB refuses that statement and rolls the transaction back, and the store reports
    /// `suspended`. Nothing from the first statement survives.
    @Test(.timeLimit(.minutes(1))) func writeInterruptedBySuspensionIsRolledBack() async throws {
        try await withDatabaseURL { url in
            let store = try await RingStore.open(at: url)
            let first = UUID(), second = UUID()
            let started = DispatchSemaphore(value: 0), proceed = DispatchSemaphore(value: 0)
            let write = Task {
                try await store.write { db in
                    try guardedInsertRing(db, first)
                    started.signal()
                    _ = proceed.wait(timeout: .now() + 10)
                    try guardedInsertRing(db, second)
                }
            }
            await wait(for: started)
            RingStore.appWillSuspend()
            do {
                defer { RingStore.appDidResume() }
                proceed.signal()
                await #expect(throws: RingStoreError.suspended) { try await write.value }
            }
            #expect(try await store.rings().isEmpty)
            // The connection is usable again after resuming.
            try await store.write { db in try guardedInsertRing(db, first) }
            #expect(try await store.rings().map(\.id) == [first])
        }
    }

    /// A write whose statements have all run when suspension is announced completes: GRDB lets
    /// COMMIT through because it releases the lock. The call returns normally and the row is kept.
    @Test(.timeLimit(.minutes(1))) func writeThatOnlyHasToCommitCompletes() async throws {
        try await withDatabaseURL { url in
            let store = try await RingStore.open(at: url)
            let id = UUID()
            let started = DispatchSemaphore(value: 0), proceed = DispatchSemaphore(value: 0)
            let write = Task {
                try await store.write { db in
                    try guardedInsertRing(db, id)
                    started.signal()
                    _ = proceed.wait(timeout: .now() + 10)
                }
            }
            await wait(for: started)
            RingStore.appWillSuspend()
            do {
                defer { RingStore.appDidResume() }
                proceed.signal()
                try await write.value
            }
            #expect(try await store.rings().map(\.id) == [id])
        }
    }

    /// In-memory stores hold no file lock and ignore suspension.
    @Test(.timeLimit(.minutes(1))) func inMemoryStoreIgnoresSuspension() async throws {
        let store = try RingStore()
        RingStore.appWillSuspend()
        defer { RingStore.appDidResume() }
        let ring = try await store.registerRing(protocolFamily: "ycbt")
        #expect(try await store.rings().map(\.id) == [ring.id])
    }

    /// An interruption is reported as `suspended` only when a suspension can explain it.
    @Test(.timeLimit(.minutes(1))) func interruptionIsMappedOnlyWhenSuspended() throws {
        let interrupted = DatabaseError(resultCode: .SQLITE_INTERRUPT)
        let aborted = DatabaseError(resultCode: .SQLITE_ABORT)
        let busy = DatabaseError(resultCode: .SQLITE_BUSY)
        let epoch = try SuspensionState.enter()
        #expect((RingStore.mapSuspension(interrupted, since: epoch) as? DatabaseError)?.resultCode == .SQLITE_INTERRUPT)
        #expect((RingStore.mapSuspension(aborted, since: epoch) as? DatabaseError)?.resultCode == .SQLITE_ABORT)
        RingStore.appWillSuspend()
        RingStore.appDidResume()
        // A suspension happened during the operation, though the app has resumed since.
        #expect(RingStore.mapSuspension(interrupted, since: epoch) as? RingStoreError == .suspended)
        #expect(RingStore.mapSuspension(aborted, since: epoch) as? RingStoreError == .suspended)
        #expect((RingStore.mapSuspension(busy, since: epoch) as? DatabaseError)?.resultCode == .SQLITE_BUSY)
        #expect(RingStore.mapSuspension(CancellationError(), since: epoch) is CancellationError)
    }

    /// Two stores on one file: while one holds the write lock, the other's write waits (the busy
    /// timeout) instead of failing at once with "database is locked". A connection without a busy
    /// timeout shows what would happen otherwise.
    @Test(.timeLimit(.minutes(1))) func twoStoresOnOneFileWriteInTurn() async throws {
        try await withDatabaseURL { url in
            let a = try await RingStore.open(at: url)
            let b = try await RingStore.open(at: url)
            let held = UUID()
            let started = DispatchSemaphore(value: 0)
            let holder = Task {
                try await a.write { db in
                    try guardedInsertRing(db, held)
                    started.signal()
                    Thread.sleep(forTimeInterval: 1)
                }
            }
            await wait(for: started)
            // Without a busy timeout (GRDB's default), the same write fails immediately.
            let plain = try DatabaseQueue(path: url.path)
            let refused = #expect(throws: DatabaseError.self) {
                try plain.write { db in try guardedInsertRing(db, UUID()) }
            }
            #expect(refused?.resultCode == .SQLITE_BUSY)
            let waited = try await b.registerRing(protocolFamily: "ycbt")
            try await holder.value
            #expect(Set(try await a.rings().map(\.id)) == [held, waited.id])
            #expect(Set(try await b.rings().map(\.id)) == [held, waited.id])
        }
    }
}
