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
    private let writer: any DatabaseWriter

    /// Opens (creating if needed) the database at `url` and migrates it.
    public init(url: URL) throws {
        try Self.prepareFile(at: url)
        var configuration = Configuration()
        configuration.foreignKeysEnabled = true
        let pool = try DatabasePool(path: url.path, configuration: configuration)
        try Self.migrator.migrate(pool)
        // After migrating, so the -wal and -shm files exist too.
        try Self.protectFiles(at: url)
        writer = pool
    }

    /// An empty in-memory database, for tests.
    public init() throws {
        let queue = try DatabaseQueue()
        try Self.migrator.migrate(queue)
        writer = queue
    }

    // MARK: Rings

    /// Registers a new ring under a freshly generated identifier.
    @discardableResult
    public func registerRing(
        protocolFamily: String, model: String? = nil, firmware: String? = nil, now: Date = Date()
    ) throws -> StoredRing {
        let ring = StoredRing(
            id: UUID(), protocolFamily: protocolFamily, model: model, firmware: firmware,
            firstSeenAt: HistoryTransfer.truncatedToMilliseconds(now))
        try writer.write { db in
            try db.execute(
                sql: "INSERT INTO ring (id, protocol_family, model, firmware, first_seen_at) VALUES (?, ?, ?, ?, ?)",
                arguments: [ring.id.uuidString, ring.protocolFamily, ring.model, ring.firmware, Self.millis(ring.firstSeenAt)])
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
    @discardableResult
    public func insert(_ transfer: HistoryTransfer, ringID: UUID) throws -> InsertOutcome {
        let hash = Self.contentHash(ringID: ringID, kind: transfer.kind, data: transfer.data)
        return try writer.write { db in
            try Self.requireRing(db, ringID)
            try db.execute(
                sql: """
                    INSERT OR IGNORE INTO history_transfer
                    (ring_id, kind, header, data, fetched_at, time_zone_identifier, content_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                arguments: [
                    ringID.uuidString, transfer.kind.name, Data(transfer.header), Data(transfer.data),
                    Self.millis(transfer.fetchedAt), transfer.timeZoneIdentifier, Data(hash),
                ])
            if db.changesCount > 0 { return .inserted(id: db.lastInsertedRowID) }
            let existing = try Int64.fetchOne(
                db, sql: "SELECT id FROM history_transfer WHERE content_hash = ?", arguments: [Data(hash)])
            guard let existing else { throw RingStoreError.corruptRow("duplicate without an existing row") }
            return .duplicate(existingID: existing)
        }
    }

    /// Transfers for a ring, optionally of one kind and within a fetched-at range (inclusive),
    /// oldest first.
    public func transfers(
        ringID: UUID, kind: HistoryKind? = nil, fetchedWithin range: ClosedRange<Date>? = nil
    ) throws -> [StoredTransfer] {
        var clauses = ["ring_id = ?"]
        var arguments: [any DatabaseValueConvertible] = [ringID.uuidString]
        if let kind {
            clauses.append("kind = ?")
            arguments.append(kind.name)
        }
        if let range {
            clauses.append("fetched_at >= ? AND fetched_at <= ?")
            arguments.append(Self.millis(range.lowerBound))
            arguments.append(Self.millis(range.upperBound))
        }
        let sql = "SELECT * FROM history_transfer WHERE \(clauses.joined(separator: " AND ")) ORDER BY fetched_at, id"
        return try writer.read { db in
            try Row.fetchAll(db, sql: sql, arguments: StatementArguments(arguments)).map(Self.storedTransfer)
        }
    }

    // MARK: Clock sets

    public func recordClockSet(_ event: ClockSetEvent) throws {
        try writer.write { db in
            try Self.requireRing(db, event.ringID)
            try db.execute(
                sql: "INSERT INTO clock_set (ring_id, instant, time_zone_identifier, utc_offset_seconds) VALUES (?, ?, ?, ?)",
                arguments: [event.ringID.uuidString, Self.millis(event.instant), event.timeZoneIdentifier, event.utcOffsetSeconds])
        }
    }

    /// Clock-set events for a ring in time order (ties in the order recorded).
    public func clockSets(ringID: UUID) throws -> [ClockSetEvent] {
        try writer.read { db in
            try Row.fetchAll(
                db, sql: "SELECT * FROM clock_set WHERE ring_id = ? ORDER BY instant, id", arguments: [ringID.uuidString]
            ).map { row in
                ClockSetEvent(
                    ringID: try Self.uuid(row["ring_id"]), instant: Self.date(row["instant"]),
                    timeZoneIdentifier: row["time_zone_identifier"], utcOffsetSeconds: row["utc_offset_seconds"])
            }
        }
    }

    // MARK: Hash

    /// SHA-256 over ring id (16 bytes), kind name (one length byte, then UTF-8) and data. Not the
    /// header (its stability between identical fetches is unverified), fetch time or zone.
    static func contentHash(ringID: UUID, kind: HistoryKind, data: [UInt8]) -> [UInt8] {
        var hasher = SHA256()
        withUnsafeBytes(of: ringID.uuid) { hasher.update(bufferPointer: $0) }
        let name = Array(kind.name.utf8)
        hasher.update(data: [UInt8(name.count)])
        hasher.update(data: name)
        hasher.update(data: data)
        return Array(hasher.finalize())
    }

    // MARK: Schema

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

    /// The file holds health data. iOS: `completeUntilFirstUserAuthentication`, so background sync
    /// can write while the phone is locked after the first unlock (the stricter `complete` would
    /// make that fail) while the file stays encrypted before first unlock. macOS has no such
    /// class, so the file is created owner-only (0600); SQLite gives the -wal and -shm files the
    /// same mode. Created up front so it is never briefly world-readable under the umask.
    private static func prepareFile(at url: URL) throws {
        if !FileManager.default.fileExists(atPath: url.path) {
            let fd = open(url.path, O_WRONLY | O_CREAT | O_EXCL, 0o600)
            if fd >= 0 { close(fd) } else if errno != EEXIST {
                throw CocoaError(.fileWriteUnknown)
            }
        }
    }

    private static func protectFiles(at url: URL) throws {
        #if os(iOS)
        // Not verified on a device: the iOS simulator does not enforce data protection.
        for suffix in ["", "-wal", "-shm"] {
            let path = url.path + suffix
            if FileManager.default.fileExists(atPath: path) {
                try FileManager.default.setAttributes(
                    [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication], ofItemAtPath: path)
            }
        }
        #endif
    }

    // MARK: Row conversion

    private static func millis(_ date: Date) -> Int64 {
        Int64((HistoryTransfer.truncatedToMilliseconds(date).timeIntervalSince1970 * 1000).rounded())
    }

    private static func date(_ millis: Int64) -> Date {
        Date(timeIntervalSince1970: Double(millis) / 1000)
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
            id: try uuid(row["id"]), protocolFamily: row["protocol_family"], model: row["model"],
            firmware: row["firmware"], firstSeenAt: date(row["first_seen_at"]))
    }

    private static func storedTransfer(_ row: Row) throws -> StoredTransfer {
        let name: String = row["kind"]
        guard let kind = HistoryKind.allCases.first(where: { $0.name == name }) else {
            throw RingStoreError.corruptRow("unknown history kind")
        }
        let header: Data = row["header"]
        let data: Data = row["data"]
        return StoredTransfer(
            id: row["id"], ringID: try uuid(row["ring_id"]),
            transfer: HistoryTransfer(
                kind: kind, header: [UInt8](header), data: [UInt8](data),
                fetchedAt: date(row["fetched_at"]), timeZoneIdentifier: row["time_zone_identifier"]))
    }

    /// Column names of the `ring` table, so a test can assert no identifier columns creep in.
    func ringColumnNames() throws -> [String] {
        try writer.read { db in try db.columns(in: "ring").map(\.name) }
    }
}
