import CryptoKit
import Foundation
import GRDB
import RingProtocol
// Only for `sqlite3_file_control` (persistent WAL). GRDB links the system SQLite library, so this
// is the same library GRDB's connections use; importing GRDB's own `GRDBSQLite` module instead
// would need a new product dependency in Package.swift.
import SQLite3

/// The app's on-device history: raw ring data exactly as received. Anything derived can be rebuilt
/// from it. The caller chooses the file location (the app passes a URL inside an App Group
/// container so a future widget can read it); the store never picks one itself.
///
/// No health data is ever logged from here. Errors thrown by SQLite itself surface as GRDB's
/// `DatabaseError`; the store's own failures are `RingStoreError`.
///
/// **The public database API is `async` only, except the synchronous initialisers.** Every function
/// that touches an open database is `async throws` and runs off the caller's thread. The app opens
/// its store with `open(at:)`, which does the work off the caller's thread; `init(url:)` (and
/// `init()`) are synchronous: they open the file, run every migration and set file protection on
/// the calling thread. The synchronous same-name forms (`internal`,
/// used by tests through `@testable import`) block the calling thread; they are not public because
/// a same-name synchronous overload is chosen silently in any synchronous context and in any
/// closure without an `await`, where it would block. Each pair calls one `static` implementation,
/// so the two cannot drift. (The sleep queries in `SleepQueries.swift` were already async only.)
///
/// **Cancellation.** An `async` call can throw `CancellationError` when its task is cancelled. A
/// write that is cancelled is rolled back, so nothing it did is kept: cancelling `ingest` or
/// `rebuild` discards the work done so far and leaves the typed tables as they were. Callers that
/// need the result must call again.
///
/// **Errors.** A call naming a ring that is not stored throws `RingStoreError.unknownRing`, except
/// the reads (`ring(id:)`, `transfer(id:)`), which return `nil`, and the typed and sleep reads,
/// which return an empty list.
///
/// **Suspension.** A file-backed store observes `appWillSuspend()` and `appDidResume()`: while
/// suspended every new call throws `RingStoreError.suspended` without touching the database, and
/// `appWillSuspend()` returns only once the calls in progress have finished (see those two
/// functions and `Suspension.swift`). In-memory stores ignore suspension.
public final class RingStore: Sendable {
    // Internal, not private, so tests can inspect the schema and issue raw SQL.
    let writer: any DatabaseWriter
    /// Whether this store follows `appWillSuspend()`/`appDidResume()`. True for every file-backed
    /// store the public API opens; false for in-memory stores, which hold no file lock.
    let observesSuspension: Bool

    /// Opens (creating if needed) the database at `url` and migrates it, off the caller's thread.
    /// **This is the opener the app uses.** It does what `init(url:)` does on a background
    /// dispatch queue, so neither the main actor nor a Swift concurrency thread is blocked by file
    /// coordination, migration or file protection. It cannot be cancelled once started: a
    /// cancelled caller still waits for the open to finish.
    ///
    /// Throws what `init(url:)` throws, including `RingStoreError.suspended` if called between
    /// `appWillSuspend()` and `appDidResume()`.
    public static func open(at url: URL) async throws -> RingStore {
        try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global(qos: .userInitiated).async {
                continuation.resume(with: Result { try RingStore(url: url) })
            }
        }
    }

    /// Opens (creating if needed) the database at `url` and migrates it. The database runs in
    /// persistent WAL mode, so SQLite also creates `-wal` and `-shm` files beside it and keeps them.
    ///
    /// `url` must be inside a directory dedicated to this database (for the app, a subdirectory
    /// of the App Group container). On iOS the directory itself is given file protection
    /// `completeUntilFirstUserAuthentication`, because SQLite can recreate the `-wal` and `-shm`
    /// files and only a protected directory covers the new ones.
    ///
    /// Synchronous: opening, migrating and setting file protection all happen on the calling
    /// thread. The app uses `open(at:)` instead. Throws `RingStoreError.suspended` while the app is
    /// suspended (see `appWillSuspend()`), since migrating takes a write lock, and
    /// `RingStoreError.schemaTooNew` if a newer build has migrated the file.
    public convenience init(url: URL) throws {
        try self.init(url: url, observesSuspension: true)
    }

    /// Points in opening that tests can observe or act at. Production uses the defaults.
    struct OpenHooks: Sendable {
        /// Sets file protection on one path. The default sets it on iOS and does nothing elsewhere.
        var protect: @Sendable (String) throws -> Void = RingStore.setFileProtection
        /// Runs after the first suspension check inside coordination and just before
        /// `DatabasePool.init`: the window in which GRDB would not yet see an announcement.
        var beforePoolCreated: @Sendable () -> Void = {}
    }

    /// `observesSuspension: false` is for tests only: suspension is process-wide, so the tests that
    /// suspend run in one serialised suite and every other file-backed test opens its store this
    /// way, so a suspension in one test cannot fail another running in parallel.
    init(url: URL, observesSuspension: Bool, hooks: OpenHooks = OpenHooks()) throws {
        if observesSuspension {
            // Opening migrates, which takes the write lock, so it is refused while suspended, counts
            // as a call in progress for `appWillSuspend()`'s wait, and a suspension during it
            // surfaces as `suspended` like any other write.
            let epoch = try SuspensionState.enter()
            defer { SuspensionState.leave() }
            do {
                writer = try Self.openPool(at: url, observesSuspension: true, epoch: epoch, hooks: hooks)
            } catch {
                throw Self.mapSuspension(error, since: epoch)
            }
        } else {
            writer = try Self.openPool(at: url, observesSuspension: false, epoch: nil, hooks: hooks)
        }
        self.observesSuspension = observesSuspension
    }

    /// An empty in-memory database, for tests and previews. It ignores suspension.
    public init() throws {
        let queue = try DatabaseQueue(configuration: Self.configuration())
        try Self.migrator.migrate(queue)
        writer = queue
        observesSuspension = false
    }

    /// GRDB's "Sharing a Database" guide (section "Use the WAL mode") opens a shared database
    /// inside an `NSFileCoordinator` write coordination, because several processes (the app and,
    /// later, a widget) may open it at the same time, and one of them may be creating or
    /// migrating it. Coordination takes no SQLite lock and writes no file. Its wait has no
    /// timeout: another process holding a coordinated access to the file (a widget opening it)
    /// delays this open for as long as it holds it, which is one reason migrations must stay quick.
    ///
    /// `epoch` is `nil` for a store that ignores suspension.
    private static func openPool(
        at url: URL, observesSuspension: Bool, epoch: UInt64?, hooks: OpenHooks
    ) throws -> DatabasePool {
        let coordinator = NSFileCoordinator(filePresenter: nil)
        var coordinatorError: NSError?
        var result: Result<DatabasePool, any Error>?
        coordinator.coordinate(writingItemAt: url, options: .forMerging, error: &coordinatorError) { url in
            result = Result {
                // The wait for coordination can be long, and an announcement made during it is not
                // seen by GRDB, which registers its suspension observers only inside
                // `DatabasePool.init`. So check again before creating anything.
                try SuspensionState.check(since: epoch)
                try Self.prepareFile(at: url)
                // Before opening as well as after, so the `-wal` and `-shm` files SQLite creates on a
                // first open are created inside a directory that is already protected.
                try Self.protectFiles(at: url, hooks.protect)
                hooks.beforePoolCreated()
                let pool = try DatabasePool(
                    path: url.path, configuration: Self.fileConfiguration(observesSuspension: observesSuspension))
                // And again now that GRDB observes suspension: one announced between the check
                // above and `DatabasePool.init` would otherwise go unseen, and migrating takes the
                // write lock. Throwing here releases the pool, which closes it, before any migration.
                try SuspensionState.check(since: epoch)
                // Guide, "Use the WAL mode" (the writer sample): never write into a schema a newer
                // build created; an older build would not know its tables or rules.
                if try pool.read({ db in try Self.migrator.hasBeenSuperseded(db) }) {
                    throw RingStoreError.schemaTooNew
                }
                try Self.migrator.migrate(pool)
                // After migrating, so the -wal and -shm files exist and get the same treatment.
                try Self.protectFiles(at: url, hooks.protect)
                return pool
            }
        }
        if let result { return try result.get() }
        throw coordinatorError ?? CocoaError(.fileWriteUnknown)
    }

    private static func configuration() -> Configuration {
        var configuration = Configuration()
        // Foreign keys are on by default in GRDB; the schema relies on them, so not set again.
        // Keep SQLite's temporary files (sorts, indexes) in memory so health data is never
        // written outside the database's own directory.
        configuration.prepareDatabase { db in
            try db.execute(sql: "PRAGMA temp_store = MEMORY")
        }
        return configuration
    }

    /// The settings a database in an App Group container needs, from GRDB's "Sharing a Database"
    /// guide (GRDB 7.12, `GRDB/Documentation.docc/DatabaseSharing.md`). Only file-backed stores
    /// use them; the in-memory store has no file to share.
    static func fileConfiguration(observesSuspension: Bool) -> Configuration {
        var configuration = configuration()
        // Guide, "How to limit the SQLITE_BUSY error": without a busy timeout a write fails at once
        // with SQLITE_BUSY ("database is locked") whenever another connection to the file holds
        // the write lock: another process (a future widget or extension), or a second `RingStore`
        // on the same file in this process. With it the write waits up to this long. Five seconds
        // covers a normal write transaction and stays well inside an iOS background window; a
        // longer wait still fails with SQLITE_BUSY.
        configuration.busyMode = .timeout(5)
        // Guide, "How to limit the 0xDEAD10CC exception": the store must stop taking database locks
        // once the app announces it is about to be suspended, or iOS kills the app. See
        // `Suspension.swift`.
        configuration.observesSuspensionNotifications = observesSuspension
        configuration.prepareDatabase { db in
            guard !db.configuration.readonly else { return }
            // Guide, "Use the WAL mode" and "The Specific Case of Read-Only Connections": a
            // read-only connection (the widget's, later) fails unless the `-wal` and `-shm` files
            // exist. The guide says SQLite may delete them when the last connection closes,
            // depending on the SQLite and OS versions. On Apple platforms this overstates it: the
            // system SQLite has persistent WAL on by default (with this call removed the flag still
            // reads back as 1 and the files are kept; seen on macOS in review of #64). That is a
            // platform default, not a promise, so the flag is set explicitly. Set on writable
            // connections only, as the guide does.
            var flag: CInt = 1
            let code = withUnsafeMutablePointer(to: &flag) { pointer in
                sqlite3_file_control(db.sqliteConnection, nil, SQLITE_FCNTL_PERSIST_WAL, pointer)
            }
            guard code == SQLITE_OK else { throw DatabaseError(resultCode: ResultCode(rawValue: code)) }
            // With persistent WAL the `-wal` file is never deleted, and SQLite does not shrink it
            // on its own: it would stay at the size of the largest transaction ever written,
            // holding old page images (rows since deleted included) on disk and in device backups.
            // This limit has SQLite truncate it to 4 MB whenever it is reset after a checkpoint.
            try db.execute(sql: "PRAGMA journal_size_limit = \(Self.journalSizeLimit)")
        }
        return configuration
    }

    /// Bytes the `-wal` file is truncated to after a checkpoint (`PRAGMA journal_size_limit`).
    static let journalSizeLimit = 4 * 1024 * 1024

    // MARK: Rings

    /// Blocking form of `registerRing(protocolFamily:model:firmware:now:)` (internal, for tests).
    @discardableResult
    func registerRing(
        protocolFamily: String, model: String? = nil, firmware: String? = nil, now: Date = Date()
    ) throws -> StoredRing {
        let ring = Self.newRing(protocolFamily: protocolFamily, model: model, firmware: firmware, now: now)
        try writeBlocking { db in try Self.insertRing(db, ring) }
        return ring
    }

    /// Registers a new ring under a freshly generated identifier and returns it. `firstSeenAt` is
    /// `now` rounded down to whole milliseconds, which is also what is stored, so the returned value
    /// equals what `ring(id:)` reads back. `peripheralIdentifier` starts `nil`. Never throws
    /// `unknownRing`; can throw `CancellationError` (nothing is registered then).
    @discardableResult
    public func registerRing(
        protocolFamily: String, model: String? = nil, firmware: String? = nil, now: Date = Date()
    ) async throws -> StoredRing {
        let ring = Self.newRing(protocolFamily: protocolFamily, model: model, firmware: firmware, now: now)
        try await write { db in try Self.insertRing(db, ring) }
        return ring
    }

    private static func newRing(protocolFamily: String, model: String?, firmware: String?, now: Date) -> StoredRing {
        StoredRing(
            id: UUID(), protocolFamily: protocolFamily, model: model, firmware: firmware,
            firstSeenAt: HistoryTransfer.date(millisecondsSince1970: HistoryTransfer.millisecondsSince1970(now)))
    }

    private static func insertRing(_ db: Database, _ ring: StoredRing) throws {
        try db.execute(
            sql: "INSERT INTO ring (id, protocol_family, model, firmware, first_seen_at) VALUES (?, ?, ?, ?, ?)",
            arguments: [
                ring.id.uuidString, ring.protocolFamily, ring.model, ring.firmware,
                HistoryTransfer.millisecondsSince1970(ring.firstSeenAt),
            ])
    }

    /// Blocking form of `ring(id:)` (internal, for tests).
    func ring(id: UUID) throws -> StoredRing? {
        try readBlocking { db in try Self.ring(db, id: id) }
    }

    /// The ring stored under `id`, or `nil` if there is none (not an error). A stored
    /// `peripheral_identifier` that is not a UUID reads as `nil` rather than failing; a corrupt ring
    /// id still throws `corruptRow`. Can throw `CancellationError`.
    public func ring(id: UUID) async throws -> StoredRing? {
        try await read { db in try Self.ring(db, id: id) }
    }

    private static func ring(_ db: Database, id: UUID) throws -> StoredRing? {
        try Row.fetchOne(db, sql: "SELECT * FROM ring WHERE id = ?", arguments: [id.uuidString]).map(Self.ring)
    }

    /// Blocking form of `rings()` (internal, for tests).
    func rings() throws -> [StoredRing] {
        try readBlocking { db in try Self.rings(db) }
    }

    /// All rings, oldest first (by first-seen time, then id); empty when none. A malformed stored
    /// peripheral identifier reads as `nil` and never hides the other rings. Can throw
    /// `CancellationError`.
    public func rings() async throws -> [StoredRing] {
        try await read { db in try Self.rings(db) }
    }

    private static func rings(_ db: Database) throws -> [StoredRing] {
        try Row.fetchAll(db, sql: "SELECT * FROM ring ORDER BY first_seen_at, id").map(Self.ring)
    }

    /// Blocking form of `updateRing(id:model:firmware:)` (internal, for tests).
    func updateRing(id: UUID, model: String?, firmware: String?) throws {
        try writeBlocking { db in try Self.updateRing(db, id: id, model: model, firmware: firmware) }
    }

    /// Replaces the model and firmware the ring reported (for example after a firmware update),
    /// both always overwritten, so `nil` clears one. Throws `unknownRing(id)` if no such ring is
    /// stored. One write transaction; a `CancellationError` rolls it back.
    public func updateRing(id: UUID, model: String?, firmware: String?) async throws {
        try await write { db in try Self.updateRing(db, id: id, model: model, firmware: firmware) }
    }

    private static func updateRing(_ db: Database, id: UUID, model: String?, firmware: String?) throws {
        try db.execute(
            sql: "UPDATE ring SET model = ?, firmware = ? WHERE id = ?",
            arguments: [model, firmware, id.uuidString])
        if db.changesCount == 0 { throw RingStoreError.unknownRing(id) }
    }

    /// Blocking form of `ring(peripheralIdentifier:)` (internal, for tests).
    func ring(peripheralIdentifier: UUID) throws -> StoredRing? {
        try readBlocking { db in try Self.ring(db, peripheralIdentifier: peripheralIdentifier) }
    }

    /// The stored ring that holds this CoreBluetooth identifier, or `nil` (not an error). The
    /// match ignores letter case, like the unique index. **The sync engine must look a connected
    /// peripheral up here before registering a new ring, and reuse the ring it finds** (#51);
    /// registering a second ring for a ring the store already knows would store and ingest the
    /// same records twice. Can throw `CancellationError`.
    public func ring(peripheralIdentifier: UUID) async throws -> StoredRing? {
        try await read { db in try Self.ring(db, peripheralIdentifier: peripheralIdentifier) }
    }

    private static func ring(_ db: Database, peripheralIdentifier: UUID) throws -> StoredRing? {
        try Row.fetchOne(
            db, sql: "SELECT * FROM ring WHERE peripheral_identifier = ? COLLATE NOCASE",
            arguments: [peripheralIdentifier.uuidString]
        ).map(Self.ring)
    }

    /// Blocking form of `setPeripheralIdentifier(_:forRing:)` (internal, for tests).
    func setPeripheralIdentifier(_ identifier: UUID?, forRing id: UUID) throws {
        try writeBlocking { db in try Self.setPeripheralIdentifier(db, identifier, forRing: id) }
    }

    /// Remembers the identifier CoreBluetooth gave this phone for the ring
    /// (`CBPeripheral.identifier`), so the app can reconnect without scanning; `nil` clears it. It
    /// replaces any earlier value of this ring.
    ///
    /// **One identifier, one ring.** An identifier names one physical ring on one phone, so if
    /// another ring already holds it, the two are almost certainly the same ring and the caller has
    /// made a mistake (moving it would orphan that ring's history and have this one store and
    /// ingest the same records again). The call then throws `peripheralIdentifierInUse(holder)`,
    /// naming the holding ring by its app id (never the identifier), and changes nothing. Setting
    /// the value this ring already holds succeeds, and `nil` always does. A unique index (case
    /// insensitive) backs this up. **The sync engine must look a connected peripheral up with
    /// `ring(peripheralIdentifier:)` before registering a new ring, and reuse the ring it finds**
    /// (#51).
    ///
    /// This is a per-phone device identifier assigned by CoreBluetooth. It is not the Bluetooth
    /// address and not the ring's serial number, and it can change (for example after a
    /// network-settings reset), so a lookup that finds nothing means rescan and overwrite it. Treat
    /// it as a device identifier: never in exports, logs, error messages, crash or analytics
    /// payloads, test fixtures, issues or pull requests. This is the only place it is stored, and
    /// the column holds nothing else, never an advertised name or address.
    ///
    /// Throws `unknownRing(id)` first if no such ring is stored; either error leaves every row
    /// unchanged. One write transaction; a `CancellationError` rolls it back.
    public func setPeripheralIdentifier(_ identifier: UUID?, forRing id: UUID) async throws {
        try await write { db in try Self.setPeripheralIdentifier(db, identifier, forRing: id) }
    }

    private static func setPeripheralIdentifier(_ db: Database, _ identifier: UUID?, forRing id: UUID) throws {
        try Self.requireRing(db, id)
        if let identifier {
            // Case-insensitive, matching the index: a lower-case copy written outside the API counts.
            let holder = try String.fetchOne(
                db, sql: "SELECT id FROM ring WHERE peripheral_identifier = ? COLLATE NOCASE AND id <> ?",
                arguments: [identifier.uuidString, id.uuidString])
            if let holder { throw RingStoreError.peripheralIdentifierInUse(try uuid(holder)) }
        }
        try db.execute(
            sql: "UPDATE ring SET peripheral_identifier = ? WHERE id = ?",
            arguments: [identifier?.uuidString, id.uuidString])
    }

    // MARK: Transfers

    /// The insert statement, kept as a constant so a test can run exactly this SQL. `ON CONFLICT`
    /// names the hash on purpose: `INSERT OR IGNORE` would also swallow NOT NULL and CHECK failures.
    static let insertTransferSQL = """
        INSERT INTO history_transfer
        (ring_id, kind, header, data, fetched_at, time_zone_identifier, content_hash)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(content_hash) DO NOTHING
        """

    /// Finds a stored transfer for the same ring and kind that is longer than the new data and
    /// starts with it. Compared in SQL, so SQLite still reads each candidate blob but nothing is copied
    /// into Swift; `substr` on a blob works in bytes and the comparison is a byte comparison, so
    /// zero bytes are fine. An empty new transfer
    /// (`length(data) > 0`, empty prefix) matches any non-empty row.
    private static let findSupersederSQL = """
        SELECT id FROM history_transfer
        WHERE ring_id = ? AND kind = ? AND length(data) > ? AND substr(data, 1, ?) = ?
        ORDER BY id LIMIT 1
        """

    /// Deletes the transfers the new one supersedes: same ring and kind, strictly shorter, and a
    /// prefix of the new data. The new row is longer than all of them, so it never matches itself.
    /// An existing empty transfer is a prefix of everything and goes when any non-empty one arrives.
    private static let pruneSupersededSQL = """
        DELETE FROM history_transfer
        WHERE ring_id = ? AND kind = ? AND length(data) < ? AND substr(?, 1, length(data)) = data
        """

    /// The fetch log entry written for every `insert`. Naming the unique columns means re-importing
    /// the same capture adds nothing, while any other constraint failure still throws.
    static let insertObservationSQL = """
        INSERT INTO transfer_observation
        (ring_id, kind, data_length, header, fetched_at, time_zone_identifier, content_hash)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ring_id, kind, content_hash, fetched_at) DO NOTHING
        """

    /// Blocking form of `insert(_:ringID:)` (internal, for tests).
    @discardableResult
    func insert(_ transfer: HistoryTransfer, ringID: UUID) throws -> InsertOutcome {
        try writeBlocking { db in try Self.insert(db, transfer, ringID: ringID) }
    }

    /// Stores `transfer` for `ringID`, in one write transaction with any pruning.
    ///
    /// Every call records one `transfer_observation` of the incoming transfer (ring, kind, data
    /// length, header, fetch time, zone, content hash), whatever the outcome, so the observations
    /// are the complete fetch log. The same transfer fetched at the same instant is logged once.
    /// That log is what tells ingestion which bytes existed before each sync, including that a
    /// buffer was unchanged at a sync, and an empty fetch after a non-empty one is the only trace
    /// of a buffer reset. Read it with `observations`.
    ///
    /// - Identical bytes (same ring, kind and data) already stored are a no-op that keeps the first
    ///   row (`.duplicate`); the header, fetch time and zone of the new fetch live only in its
    ///   observation.
    /// - If an existing transfer for that ring and kind is longer and starts with the new data,
    ///   the new one adds nothing (an older capture imported late, or a truncated fetch) and is
    ///   not stored (`.supersededByExisting`).
    /// - Otherwise it is stored, and every earlier transfer for that ring and kind whose data is a
    ///   strict prefix of it is deleted, since every record in it is in the new one. The count is
    ///   in `.inserted(id:pruned:)`. A transfer that is not a prefix (the ring's buffer rolled over or
    ///   was reset) is kept alongside.
    ///
    /// Empty data is a prefix of everything: it is never stored when any transfer for that ring and
    /// kind exists (it is a `.duplicate` of an existing empty one, else superseded by a non-empty
    /// one), and an existing empty transfer is pruned when a non-empty one arrives.
    ///
    /// A pruned row's data is gone because the superseding row holds all of it, and its own
    /// observation (made when it was inserted) stays. Anything that references a transfer by row id
    /// must survive its deletion.
    ///
    /// Only a clash on the content hash counts as a duplicate; any other constraint failure throws.
    ///
    /// Throws `unknownRing(ringID)` if the ring is not stored (nothing is written, not even an
    /// observation). A `CancellationError` rolls the whole transaction back, observation included.
    @discardableResult
    public func insert(_ transfer: HistoryTransfer, ringID: UUID) async throws -> InsertOutcome {
        try await write { db in try Self.insert(db, transfer, ringID: ringID) }
    }

    private static func insert(_ db: Database, _ transfer: HistoryTransfer, ringID: UUID) throws -> InsertOutcome {
        let hash = Self.contentHash(ringID: ringID, kind: transfer.kind, data: transfer.data)
        let kindCode = KindCode.code(for: transfer.kind)
        let data = Data(transfer.data)
        try Self.requireRing(db, ringID)
        try db.execute(
            sql: Self.insertObservationSQL,
            arguments: [
                ringID.uuidString, kindCode, data.count, Data(transfer.header),
                HistoryTransfer.millisecondsSince1970(transfer.fetchedAt), transfer.timeZoneIdentifier, Data(hash),
            ])
        if let existing = try Int64.fetchOne(
            db, sql: "SELECT id FROM history_transfer WHERE content_hash = ?", arguments: [Data(hash)])
        {
            return .duplicate(existingID: existing)
        }
        if let superseder = try Int64.fetchOne(
            db, sql: Self.findSupersederSQL,
            arguments: [ringID.uuidString, kindCode, data.count, data.count, data])
        {
            return .supersededByExisting(existingID: superseder)
        }
        try db.execute(
            sql: Self.insertTransferSQL,
            arguments: [
                ringID.uuidString, kindCode, Data(transfer.header), data,
                HistoryTransfer.millisecondsSince1970(transfer.fetchedAt), transfer.timeZoneIdentifier, Data(hash),
            ])
        // Unreachable in practice: the hash lookup above already returned for a duplicate, and
        // writes are serialised. The ON CONFLICT clause stays so a conflict can never turn into
        // a thrown constraint error or a silent overwrite.
        guard db.changesCount > 0 else {
            throw RingStoreError.corruptRow("content hash conflict that the duplicate check did not see")
        }
        let id = db.lastInsertedRowID
        try db.execute(sql: Self.pruneSupersededSQL, arguments: [ringID.uuidString, kindCode, data.count, data])
        return .inserted(id: id, pruned: db.changesCount)
    }

    /// Blocking form of `transfer(id:)` (internal, for tests).
    func transfer(id: Int64) throws -> StoredTransfer? {
        try readBlocking { db in try Self.transfer(db, id: id) }
    }

    /// One transfer by row id, or `nil` if there is none (for example it was pruned). Throws
    /// `corruptRow` if the row's kind code is not one this version knows (for example written by a
    /// newer version). Can throw `CancellationError`.
    public func transfer(id: Int64) async throws -> StoredTransfer? {
        try await read { db in try Self.transfer(db, id: id) }
    }

    private static func transfer(_ db: Database, id: Int64) throws -> StoredTransfer? {
        guard let row = try Row.fetchOne(db, sql: "SELECT * FROM history_transfer WHERE id = ?", arguments: [id])
        else { return nil }
        guard let stored = try Self.storedTransfer(row) else { throw RingStoreError.corruptRow("unknown history kind") }
        return stored
    }

    /// Blocking form of `observations(ringID:kind:)` (internal, for tests).
    func observations(ringID: UUID, kind: HistoryKind? = nil) throws -> ObservationListing {
        try readBlocking { db in try Self.observations(db, ringID: ringID, kind: kind) }
    }

    /// The fetch log for a ring, optionally of one kind: one entry per distinct fetch (same data
    /// at the same instant counts once) whether or not its transfer row still exists, ordered by
    /// fetch time then row id. Rows with a kind code this version does not know are left out and
    /// counted in `ObservationListing.skippedUnknownKind`, so a skipped row is not read as "no
    /// fetch". (With `kind` given, no unknown row can match, so the count is 0.)
    /// Empty for a ring that is not stored (no error). Can throw `CancellationError`.
    public func observations(ringID: UUID, kind: HistoryKind? = nil) async throws -> ObservationListing {
        try await read { db in try Self.observations(db, ringID: ringID, kind: kind) }
    }

    static func observations(_ db: Database, ringID: UUID, kind: HistoryKind?) throws -> ObservationListing {
        var sql = "SELECT * FROM transfer_observation WHERE ring_id = ?"
        var arguments: [any DatabaseValueConvertible] = [ringID.uuidString]
        if let kind {
            sql += " AND kind = ?"
            arguments.append(KindCode.code(for: kind))
        }
        sql += " ORDER BY fetched_at, id"
        var listing = ObservationListing(observations: [], skippedUnknownKind: 0)
        for row in try Row.fetchAll(db, sql: sql, arguments: StatementArguments(arguments)) {
            guard let kind = KindCode.kind(forCode: try Self.column(row, "kind")) else {
                listing.skippedUnknownKind += 1
                continue
            }
            let header: Data = try Self.column(row, "header")
            listing.observations.append(TransferObservation(
                id: try Self.column(row, "id"), ringID: try Self.uuid(try Self.column(row, "ring_id")),
                kind: kind, dataLength: try Self.column(row, "data_length"), header: [UInt8](header),
                contentHash: [UInt8](try Self.column(row, "content_hash") as Data),
                fetchedAt: HistoryTransfer.date(millisecondsSince1970: try Self.column(row, "fetched_at")),
                timeZoneIdentifier: try Self.column(row, "time_zone_identifier")))
        }
        return listing
    }

    /// Blocking form of `transfers(ringID:kind:fetchedWithin:)` (internal, for tests).
    func transfers(
        ringID: UUID, kind: HistoryKind? = nil, fetchedWithin range: ClosedRange<Date>? = nil
    ) throws -> TransferListing {
        try readBlocking { db in try Self.transfers(db, ringID: ringID, kind: kind, fetchedWithin: range) }
    }

    /// Transfers for a ring, optionally of one kind and within a fetched-at range, oldest first
    /// (ties by row id). The range is inclusive at both ends and compared in whole milliseconds,
    /// each bound rounded down the same way stored times are.
    ///
    /// A row's `fetched_at` is only the time of the fetch that produced that row. After pruning or
    /// duplicates, a range in the past may return nothing although its data lives in a later row;
    /// every fetch time is in `observations`.
    ///
    /// A row whose kind code this version does not know is skipped and counted in
    /// `TransferListing.skippedUnknownKind` instead of failing the whole listing.
    /// Empty for a ring that is not stored (no error). Can throw `CancellationError`.
    public func transfers(
        ringID: UUID, kind: HistoryKind? = nil, fetchedWithin range: ClosedRange<Date>? = nil
    ) async throws -> TransferListing {
        try await read { db in try Self.transfers(db, ringID: ringID, kind: kind, fetchedWithin: range) }
    }

    static func transfers(
        _ db: Database, ringID: UUID, kind: HistoryKind?, fetchedWithin range: ClosedRange<Date>?
    ) throws -> TransferListing {
        var clauses = ["ring_id = ?"]
        var arguments: [any DatabaseValueConvertible] = [ringID.uuidString]
        if let kind {
            clauses.append("kind = ?")
            arguments.append(KindCode.code(for: kind))
        }
        if let range {
            clauses.append("fetched_at >= ? AND fetched_at <= ?")
            arguments.append(HistoryTransfer.millisecondsSince1970(range.lowerBound))
            arguments.append(HistoryTransfer.millisecondsSince1970(range.upperBound))
        }
        let sql = "SELECT * FROM history_transfer WHERE \(clauses.joined(separator: " AND ")) ORDER BY fetched_at, id"
        var listing = TransferListing(transfers: [], skippedUnknownKind: 0)
        for row in try Row.fetchAll(db, sql: sql, arguments: StatementArguments(arguments)) {
            if let stored = try Self.storedTransfer(row) {
                listing.transfers.append(stored)
            } else {
                listing.skippedUnknownKind += 1
            }
        }
        return listing
    }

    // MARK: Clock sets

    /// Blocking form of `recordClockSet(_:)` (internal, for tests).
    func recordClockSet(_ event: ClockSetEvent) throws {
        try writeBlocking { db in try Self.recordClockSet(db, event) }
    }

    /// Logs that the app set the ring's clock, with the zone and UTC offset it sent. Every
    /// clock-set must be logged, because time resolution reads them. Events are appended, never
    /// merged. Throws `unknownRing(event.ringID)` if the ring is not stored. A `CancellationError`
    /// rolls the write back, so the caller cannot assume it was logged.
    public func recordClockSet(_ event: ClockSetEvent) async throws {
        try await write { db in try Self.recordClockSet(db, event) }
    }

    private static func recordClockSet(_ db: Database, _ event: ClockSetEvent) throws {
        try Self.requireRing(db, event.ringID)
        try db.execute(
            sql: "INSERT INTO clock_set (ring_id, instant, time_zone_identifier, utc_offset_seconds) VALUES (?, ?, ?, ?)",
            arguments: [
                event.ringID.uuidString, HistoryTransfer.millisecondsSince1970(event.instant),
                event.timeZoneIdentifier, event.utcOffsetSeconds,
            ])
    }

    /// Blocking form of `clockSets(ringID:)` (internal, for tests).
    func clockSets(ringID: UUID) throws -> [ClockSetEvent] {
        try readBlocking { db in try Self.clockSets(db, ringID: ringID) }
    }

    /// Clock-set events for a ring in time order (ties in the order recorded); empty for a ring that
    /// is not stored. Can throw `CancellationError`.
    public func clockSets(ringID: UUID) async throws -> [ClockSetEvent] {
        try await read { db in try Self.clockSets(db, ringID: ringID) }
    }

    static func clockSets(_ db: Database, ringID: UUID) throws -> [ClockSetEvent] {
        try Row.fetchAll(
            db, sql: "SELECT * FROM clock_set WHERE ring_id = ? ORDER BY instant, id", arguments: [ringID.uuidString]
        ).map { row in
            ClockSetEvent(
                ringID: try Self.uuid(try Self.column(row, "ring_id")),
                instant: HistoryTransfer.date(millisecondsSince1970: try Self.column(row, "instant")),
                timeZoneIdentifier: try Self.column(row, "time_zone_identifier"),
                utcOffsetSeconds: try Self.column(row, "utc_offset_seconds"))
        }
    }

    // MARK: Kind codes and hash

    /// The strings stored in `history_transfer.kind` and fed to the content hash. They are
    /// deliberately not `HistoryKind.name`: that may be renamed, and changing these would orphan
    /// existing rows and break duplicate detection. NEVER CHANGE an existing code. The switch has
    /// no default, so adding a `HistoryKind` case fails to compile until it gets a new code here.
    enum KindCode {
        static func code(for kind: HistoryKind) -> String {
            switch kind {
            case .activity: "activity"
            case .sleep: "sleep"
            case .heartRate: "heartRate"
            case .combined: "combined"
            }
        }

        static func kind(forCode code: String) -> HistoryKind? {
            HistoryKind.allCases.first { self.code(for: $0) == code }
        }
    }

    /// SHA-256 over ring id (16 bytes), kind code (one length byte, then UTF-8) and data. Not the
    /// header (its stability between identical fetches is unverified), fetch time or zone.
    static func contentHash(ringID: UUID, kind: HistoryKind, data: [UInt8]) -> [UInt8] {
        var hasher = SHA256()
        withUnsafeBytes(of: ringID.uuid) { hasher.update(bufferPointer: $0) }
        let code = Array(KindCode.code(for: kind).utf8)
        hasher.update(data: [UInt8(code.count)])
        hasher.update(data: code)
        hasher.update(data: data)
        return Array(hasher.finalize())
    }

    // MARK: Schema

    /// This database is the owner's source-of-truth history. `eraseDatabaseOnSchemaChange` must
    /// never be enabled here, not even in debug builds: it would silently delete everything.
    static var migrator: DatabaseMigrator {
        var migrator = DatabaseMigrator()
        migrator.registerMigration("v1") { db in
            try db.execute(sql: """
                CREATE TABLE ring (
                    id TEXT PRIMARY KEY NOT NULL,
                    protocol_family TEXT NOT NULL,
                    model TEXT,
                    firmware TEXT,
                    first_seen_at INTEGER NOT NULL
                ) STRICT;

                CREATE TABLE history_transfer (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ring_id TEXT NOT NULL REFERENCES ring(id),
                    kind TEXT NOT NULL,
                    header BLOB NOT NULL,
                    data BLOB NOT NULL,
                    fetched_at INTEGER NOT NULL,
                    time_zone_identifier TEXT NOT NULL,
                    content_hash BLOB NOT NULL UNIQUE
                ) STRICT;
                CREATE INDEX history_transfer_ring_kind_fetched
                    ON history_transfer (ring_id, kind, fetched_at);

                CREATE TABLE clock_set (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ring_id TEXT NOT NULL REFERENCES ring(id),
                    instant INTEGER NOT NULL,
                    time_zone_identifier TEXT NOT NULL,
                    utc_offset_seconds INTEGER NOT NULL
                ) STRICT;
                CREATE INDEX clock_set_ring_instant ON clock_set (ring_id, instant);
                """)
        }
        // Not edited into v1: v1 may already exist on devices. No CHECK on kind, matching v1.
        migrator.registerMigration("v2") { db in
            try db.execute(sql: """
                CREATE TABLE transfer_observation (
                    id INTEGER PRIMARY KEY,
                    ring_id TEXT NOT NULL REFERENCES ring(id),
                    kind TEXT NOT NULL,
                    data_length INTEGER NOT NULL CHECK (data_length >= 0),
                    header BLOB NOT NULL,
                    fetched_at INTEGER NOT NULL,
                    time_zone_identifier TEXT NOT NULL,
                    content_hash BLOB NOT NULL,
                    UNIQUE (ring_id, kind, content_hash, fetched_at)
                ) STRICT;
                -- For the ordered listing; the unique index cannot serve it.
                CREATE INDEX transfer_observation_ring_kind_fetched
                    ON transfer_observation (ring_id, kind, fetched_at);

                -- Transfers stored under v1 are in the log before any of them can be pruned.
                INSERT INTO transfer_observation
                    (ring_id, kind, data_length, header, fetched_at, time_zone_identifier, content_hash)
                SELECT ring_id, kind, length(data), header, fetched_at, time_zone_identifier, content_hash
                FROM history_transfer ORDER BY id;
                """)
        }
        // Typed rows derived from the raw transfers by `ingest`. Deliberately no reference to
        // `history_transfer` (pruning deletes transfer rows, and a transfer's hash changes whenever
        // its buffer grows) and no provenance columns: rows are always re-derived. `kind` and
        // `time_source` hold frozen text codes (see `StageCode`, `TimeSourceCode`), unchecked like
        // `history_transfer.kind` so a later code needs no table rebuild.
        migrator.registerMigration("v3") { db in
            try db.execute(sql: """
                CREATE TABLE heart_rate_reading (
                    id INTEGER PRIMARY KEY,
                    ring_id TEXT NOT NULL REFERENCES ring(id),
                    time INTEGER NOT NULL,
                    bpm INTEGER NOT NULL,
                    mode INTEGER NOT NULL,
                    utc_offset_seconds INTEGER NOT NULL,
                    ring_local_seconds INTEGER NOT NULL,
                    time_source TEXT NOT NULL,
                    UNIQUE (ring_id, time)
                ) STRICT;

                CREATE TABLE activity_interval (
                    id INTEGER PRIMARY KEY,
                    ring_id TEXT NOT NULL REFERENCES ring(id),
                    start INTEGER NOT NULL,
                    "end" INTEGER NOT NULL,
                    steps INTEGER NOT NULL,
                    distance_meters INTEGER NOT NULL,
                    kilocalories INTEGER NOT NULL,
                    utc_offset_seconds INTEGER NOT NULL,
                    ring_local_seconds INTEGER NOT NULL,
                    time_source TEXT NOT NULL,
                    UNIQUE (ring_id, start)
                ) STRICT;

                CREATE TABLE sleep_session (
                    id INTEGER PRIMARY KEY,
                    ring_id TEXT NOT NULL REFERENCES ring(id),
                    start INTEGER NOT NULL,
                    "end" INTEGER NOT NULL,
                    utc_offset_seconds INTEGER NOT NULL,
                    ring_local_seconds INTEGER NOT NULL,
                    time_source TEXT NOT NULL,
                    UNIQUE (ring_id, start)
                ) STRICT;

                CREATE TABLE sleep_stage (
                    id INTEGER PRIMARY KEY,
                    session_id INTEGER NOT NULL REFERENCES sleep_session(id) ON DELETE CASCADE,
                    ordinal INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    start INTEGER NOT NULL,
                    duration_seconds INTEGER NOT NULL,
                    UNIQUE (session_id, ordinal)
                ) STRICT;
                """)
        }
        // SpO2, respiration, HRV and CVRR from the combined record (#33). Only these four values are
        // stored (nullable: an absent value is NULL, never 0 or 255); no heart rate, steps, blood
        // pressure, temperature, body fat, glucose or raw bytes. No reference to `history_transfer`,
        // like the other typed tables.
        migrator.registerMigration("v4-vitals") { db in
            try db.execute(sql: """
                CREATE TABLE vitals_reading (
                    id INTEGER PRIMARY KEY,
                    ring_id TEXT NOT NULL REFERENCES ring(id),
                    time INTEGER NOT NULL,
                    spo2 INTEGER,
                    respiration_rate INTEGER,
                    hrv INTEGER,
                    cvrr INTEGER,
                    utc_offset_seconds INTEGER NOT NULL,
                    ring_local_seconds INTEGER NOT NULL,
                    time_source TEXT NOT NULL,
                    UNIQUE (ring_id, time)
                ) STRICT;
                """)
        }
        // After v4-vitals, which is the released order. GRDB tracks applied migrations by name.
        // The CoreBluetooth peripheral identifier is a per-phone device identifier and the only link
        // from a connected peripheral to a stored ring, so it is unique among rings (partial: many
        // rings have none) and case-insensitive, since reads accept lower case.
        migrator.registerMigration("v5-ring-peripheral") { db in
            try db.execute(sql: """
                ALTER TABLE ring ADD COLUMN peripheral_identifier TEXT;
                CREATE UNIQUE INDEX ring_peripheral_identifier ON ring(peripheral_identifier COLLATE NOCASE)
                    WHERE peripheral_identifier IS NOT NULL;
                """)
        }
        return migrator
    }

    // MARK: File protection

    /// Creates the file owner-only if missing and forces mode 0600 on every open, so a file that
    /// was ever looser is tightened. `O_CREAT` without `O_EXCL` covers both cases in one call and
    /// is created with 0600 from the start, so it is never briefly readable by others.
    private static func prepareFile(at url: URL) throws {
        // Qualified: the store's own `open(at:)` would otherwise shadow the system call.
        let fd = Darwin.open(url.path, O_WRONLY | O_CREAT, 0o600)
        guard fd >= 0 else { throw RingStoreError.fileAccess(path: url.path, errno: errno) }
        defer { close(fd) }
        guard fchmod(fd, 0o600) == 0 else { throw RingStoreError.fileAccess(path: url.path, errno: errno) }
    }

    /// Owner-only on the database's `-wal` and `-shm` files too (SQLite normally gives them the
    /// main file's mode; this makes it explicit for files that already exist). On iOS, also
    /// `completeUntilFirstUserAuthentication` on the files and their directory: background sync
    /// can then write while the phone is locked after the first unlock (the stricter `complete`
    /// would make that fail), and the data stays encrypted before first unlock. The directory
    /// setting is what covers `-wal`/`-shm` files SQLite creates later. Called with
    /// `setFileProtection` in production; tests inject a recorder through `OpenHooks`. Not verified
    /// on a device; the simulator does not enforce data protection.
    private static func protectFiles(at url: URL, _ protect: (String) throws -> Void) throws {
        for suffix in ["-wal", "-shm"] {
            let path = url.path + suffix
            if chmod(path, 0o600) != 0, errno != ENOENT {
                throw RingStoreError.fileAccess(path: path, errno: errno)
            }
        }
        // The directory first, then the database, `-wal` and `-shm` files that exist.
        var paths = [url.deletingLastPathComponent().path]
        for suffix in ["", "-wal", "-shm"] { paths.append(url.path + suffix) }
        for path in paths where FileManager.default.fileExists(atPath: path) {
            try protect(path)
        }
    }

    /// `completeUntilFirstUserAuthentication` on one path, on iOS only (macOS, where the tests run,
    /// is left alone).
    @Sendable static func setFileProtection(_ path: String) throws {
        #if os(iOS)
        try FileManager.default.setAttributes(
            [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication], ofItemAtPath: path)
        #endif
    }

    // MARK: Row conversion

    /// Decodes a column, turning any failure into `corruptRow` (GRDB's subscript would trap).
    static func column<T: DatabaseValueConvertible>(_ row: Row, _ name: String) throws -> T {
        do {
            return try row.decode(T.self, forColumn: name)
        } catch {
            throw RingStoreError.corruptRow(name)
        }
    }

    /// `column` names what failed in `corruptRow`; it defaults to the ring id, which every
    /// identifier column except the peripheral identifier is.
    static func uuid(_ text: String, column: String = "ring id") throws -> UUID {
        guard let id = UUID(uuidString: text) else { throw RingStoreError.corruptRow(column) }
        return id
    }

    static func requireRing(_ db: Database, _ id: UUID) throws {
        let exists = try Bool.fetchOne(db, sql: "SELECT EXISTS (SELECT 1 FROM ring WHERE id = ?)", arguments: [id.uuidString])
        if exists != true { throw RingStoreError.unknownRing(id) }
    }

    private static func ring(_ row: Row) throws -> StoredRing {
        StoredRing(
            id: try uuid(try column(row, "id")), protocolFamily: try column(row, "protocol_family"),
            model: try column(row, "model"), firmware: try column(row, "firmware"),
            firstSeenAt: HistoryTransfer.date(millisecondsSince1970: try column(row, "first_seen_at")),
            // A stored value that is not a UUID reads as nil instead of throwing: the identifier only
            // saves a rescan, and one bad value must not make `ring(id:)` and `rings()` fail for
            // every ring. The error would also be the wrong place to carry the value.
            peripheralIdentifier: ((try? column(row, "peripheral_identifier") as String?) ?? nil)
                .flatMap { try? uuid($0, column: "peripheral identifier") })
    }

    /// Nil when the row's kind code is not known to this version.
    private static func storedTransfer(_ row: Row) throws -> StoredTransfer? {
        guard let kind = KindCode.kind(forCode: try column(row, "kind")) else { return nil }
        let header: Data = try column(row, "header")
        let data: Data = try column(row, "data")
        return StoredTransfer(
            id: try column(row, "id"), ringID: try uuid(try column(row, "ring_id")),
            transfer: HistoryTransfer(
                kind: kind, header: [UInt8](header), data: [UInt8](data),
                fetchedAt: HistoryTransfer.date(millisecondsSince1970: try column(row, "fetched_at")),
                timeZoneIdentifier: try column(row, "time_zone_identifier")))
    }
}
