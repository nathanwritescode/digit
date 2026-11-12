import CryptoKit
import Foundation
import GRDB
import RingProtocol

/// The app's on-device history: raw ring data exactly as received. Anything derived can be rebuilt
/// from it. The caller chooses the file location (the app passes a URL inside an App Group
/// container so a future widget can read it); the store never picks one itself.
///
/// No health data is ever logged from here. Errors thrown by SQLite itself surface as GRDB's
/// `DatabaseError`; the store's own failures are `RingStoreError`.
public final class RingStore: Sendable {
    // Internal, not private, so tests can inspect the schema and issue raw SQL.
    let writer: any DatabaseWriter

    /// Opens (creating if needed) the database at `url` and migrates it. The database runs in WAL
    /// mode, so SQLite also creates and recreates `-wal` and `-shm` files beside it.
    ///
    /// `url` must be inside a directory dedicated to this database (for the app, a subdirectory
    /// of the App Group container). On iOS the directory itself is given file protection
    /// `completeUntilFirstUserAuthentication`, because SQLite recreates the `-wal` and `-shm`
    /// files and only a protected directory covers the new ones.
    public init(url: URL) throws {
        try Self.prepareFile(at: url)
        let pool = try DatabasePool(path: url.path, configuration: Self.configuration())
        try Self.migrator.migrate(pool)
        // After migrating, so the -wal and -shm files exist and get the same treatment.
        try Self.protectFiles(at: url)
        writer = pool
    }

    /// An empty in-memory database, for tests and previews.
    public init() throws {
        let queue = try DatabaseQueue(configuration: Self.configuration())
        try Self.migrator.migrate(queue)
        writer = queue
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

    // MARK: Rings

    /// Registers a new ring under a freshly generated identifier.
    @discardableResult
    public func registerRing(
        protocolFamily: String, model: String? = nil, firmware: String? = nil, now: Date = Date()
    ) throws -> StoredRing {
        let firstSeen = HistoryTransfer.millisecondsSince1970(now)
        let ring = StoredRing(
            id: UUID(), protocolFamily: protocolFamily, model: model, firmware: firmware,
            firstSeenAt: HistoryTransfer.date(millisecondsSince1970: firstSeen))
        try writer.write { db in
            try db.execute(
                sql: "INSERT INTO ring (id, protocol_family, model, firmware, first_seen_at) VALUES (?, ?, ?, ?, ?)",
                arguments: [ring.id.uuidString, ring.protocolFamily, ring.model, ring.firmware, firstSeen])
        }
        return ring
    }

    public func ring(id: UUID) throws -> StoredRing? {
        try writer.read { db in
            try Row.fetchOne(db, sql: "SELECT * FROM ring WHERE id = ?", arguments: [id.uuidString]).map(Self.ring)
        }
    }

    /// All rings, oldest first.
    public func rings() throws -> [StoredRing] {
        try writer.read { db in
            try Row.fetchAll(db, sql: "SELECT * FROM ring ORDER BY first_seen_at, id").map(Self.ring)
        }
    }

    /// Updates the model and firmware the ring reported (for example after a firmware update).
    public func updateRing(id: UUID, model: String?, firmware: String?) throws {
        try writer.write { db in
            try db.execute(
                sql: "UPDATE ring SET model = ?, firmware = ? WHERE id = ?",
                arguments: [model, firmware, id.uuidString])
            if db.changesCount == 0 { throw RingStoreError.unknownRing(id) }
        }
    }

    // MARK: Transfers

    /// Stores `transfer` for `ringID`. Identical bytes (same ring, kind and data) that are already
    /// stored are a no-op that keeps the first row; the header, fetch time and zone do not count.
    /// Only a clash on the content hash counts as a duplicate; any other constraint failure throws.
    @discardableResult
    public func insert(_ transfer: HistoryTransfer, ringID: UUID) throws -> InsertOutcome {
        let hash = Self.contentHash(ringID: ringID, kind: transfer.kind, data: transfer.data)
        return try writer.write { db in
            try Self.requireRing(db, ringID)
            try db.execute(
                sql: """
                    INSERT INTO history_transfer
                    (ring_id, kind, header, data, fetched_at, time_zone_identifier, content_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(content_hash) DO NOTHING
                    """,
                arguments: [
                    ringID.uuidString, KindCode.code(for: transfer.kind), Data(transfer.header), Data(transfer.data),
                    HistoryTransfer.millisecondsSince1970(transfer.fetchedAt), transfer.timeZoneIdentifier, Data(hash),
                ])
            if db.changesCount > 0 { return .inserted(id: db.lastInsertedRowID) }
            let existing = try Int64.fetchOne(
                db, sql: "SELECT id FROM history_transfer WHERE content_hash = ?", arguments: [Data(hash)])
            guard let existing else { throw RingStoreError.corruptRow("duplicate without an existing row") }
            return .duplicate(existingID: existing)
        }
    }

    /// One transfer by row id. Throws `corruptRow` if the row's kind code is not one this version
    /// knows (for example written by a newer version).
    public func transfer(id: Int64) throws -> StoredTransfer? {
        try writer.read { db in
            guard let row = try Row.fetchOne(db, sql: "SELECT * FROM history_transfer WHERE id = ?", arguments: [id])
            else { return nil }
            guard let stored = try Self.storedTransfer(row) else { throw RingStoreError.corruptRow("unknown history kind") }
            return stored
        }
    }

    /// Transfers for a ring, optionally of one kind and within a fetched-at range, oldest first
    /// (ties by row id). The range is inclusive at both ends and compared in whole milliseconds,
    /// each bound rounded down the same way stored times are.
    ///
    /// A row whose kind code this version does not know is skipped and counted in
    /// `TransferListing.skippedUnknownKind` instead of failing the whole listing.
    public func transfers(
        ringID: UUID, kind: HistoryKind? = nil, fetchedWithin range: ClosedRange<Date>? = nil
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
        return try writer.read { db in
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
    }

    // MARK: Clock sets

    public func recordClockSet(_ event: ClockSetEvent) throws {
        try writer.write { db in
            try Self.requireRing(db, event.ringID)
            try db.execute(
                sql: "INSERT INTO clock_set (ring_id, instant, time_zone_identifier, utc_offset_seconds) VALUES (?, ?, ?, ?)",
                arguments: [
                    event.ringID.uuidString, HistoryTransfer.millisecondsSince1970(event.instant),
                    event.timeZoneIdentifier, event.utcOffsetSeconds,
                ])
        }
    }

    /// Clock-set events for a ring in time order (ties in the order recorded).
    public func clockSets(ringID: UUID) throws -> [ClockSetEvent] {
        try writer.read { db in
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
        return migrator
    }

    // MARK: File protection

    /// Creates the file owner-only if missing and forces mode 0600 on every open, so a file that
    /// was ever looser is tightened. `O_CREAT` without `O_EXCL` covers both cases in one call and
    /// is created with 0600 from the start, so it is never briefly readable by others.
    private static func prepareFile(at url: URL) throws {
        let fd = open(url.path, O_WRONLY | O_CREAT, 0o600)
        guard fd >= 0 else { throw RingStoreError.fileAccess(path: url.path, errno: errno) }
        defer { close(fd) }
        guard fchmod(fd, 0o600) == 0 else { throw RingStoreError.fileAccess(path: url.path, errno: errno) }
    }

    /// Owner-only on the database's `-wal` and `-shm` files too (SQLite normally gives them the
    /// main file's mode; this makes it explicit for files that already exist). On iOS, also
    /// `completeUntilFirstUserAuthentication` on the files and their directory: background sync
    /// can then write while the phone is locked after the first unlock (the stricter `complete`
    /// would make that fail), and the data stays encrypted before first unlock. The directory
    /// setting is what covers `-wal`/`-shm` files SQLite creates later. Not verified on a device;
    /// the simulator does not enforce data protection.
    private static func protectFiles(at url: URL) throws {
        for suffix in ["-wal", "-shm"] {
            let path = url.path + suffix
            if chmod(path, 0o600) != 0, errno != ENOENT {
                throw RingStoreError.fileAccess(path: path, errno: errno)
            }
        }
        #if os(iOS)
        let protection: [FileAttributeKey: Any] = [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication]
        var paths = [url.deletingLastPathComponent().path]
        for suffix in ["", "-wal", "-shm"] { paths.append(url.path + suffix) }
        for path in paths where FileManager.default.fileExists(atPath: path) {
            try FileManager.default.setAttributes(protection, ofItemAtPath: path)
        }
        #endif
    }

    // MARK: Row conversion

    /// Decodes a column, turning any failure into `corruptRow` (GRDB's subscript would trap).
    private static func column<T: DatabaseValueConvertible>(_ row: Row, _ name: String) throws -> T {
        do {
            return try row.decode(T.self, forColumn: name)
        } catch {
            throw RingStoreError.corruptRow(name)
        }
    }

    private static func uuid(_ text: String) throws -> UUID {
        guard let id = UUID(uuidString: text) else { throw RingStoreError.corruptRow("ring id") }
        return id
    }

    private static func requireRing(_ db: Database, _ id: UUID) throws {
        let exists = try Bool.fetchOne(db, sql: "SELECT EXISTS (SELECT 1 FROM ring WHERE id = ?)", arguments: [id.uuidString])
        if exists != true { throw RingStoreError.unknownRing(id) }
    }

    private static func ring(_ row: Row) throws -> StoredRing {
        StoredRing(
            id: try uuid(try column(row, "id")), protocolFamily: try column(row, "protocol_family"),
            model: try column(row, "model"), firmware: try column(row, "firmware"),
            firstSeenAt: HistoryTransfer.date(millisecondsSince1970: try column(row, "first_seen_at")))
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
