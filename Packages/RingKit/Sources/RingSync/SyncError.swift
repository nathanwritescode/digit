import Foundation
import GRDB
import RingProtocol
import RingStore
import RingTransport

/// An error as the sync result keeps it: built where the error is caught, holding only what is
/// safe to show or dump.
///
/// The errors the sync catches can carry health data where no description shows it: GRDB's
/// `DatabaseError` keeps its statement's arguments (for a history insert, the transfer's raw bytes
/// and header; for ingestion, typed readings), and `RingError` cases can hold a `Frame`. A
/// debugger, `dump` or any `Mirror`-based tool prints those. So no raw error is ever stored in
/// `SyncResult`; this value replaces it, and no case can hold a `Frame`, a payload, a byte buffer
/// or a database argument.
public enum SyncError: Error, Sendable, Equatable, CustomStringConvertible {
    /// A transport failure or an error reply from the ring.
    case ring(RingFailure)
    /// The ring refused a monitoring interval.
    case monitoringSetting(MonitoringSettingError)
    /// The store's own error.
    case store(StoreFailure)
    /// An SQLite error: its result codes only (never its statement or arguments).
    case database(resultCode: Int32, extendedResultCode: Int32)
    /// The calling task was cancelled (cancellation is not handled by the sync yet, #52).
    case cancelled
    /// The ring side returned data without calling `persist`, so it was not stored before the ring
    /// was told it was received. A contract violation by a `SyncableRing` conformer; `RingSession`
    /// does not do this.
    case persistNotCalled
    /// Any other error: its type name only.
    case other(typeName: String)

    /// Converts a caught error. Every error kept in a `SyncResult` goes through here.
    public init(_ error: any Error) {
        switch error {
        case let error as SyncError: self = error
        case let error as RingError: self = .ring(RingFailure(error))
        case let error as MonitoringSettingError: self = .monitoringSetting(error)
        case let error as RingStoreError: self = .store(StoreFailure(error))
        case let error as DatabaseError:
            self = .database(resultCode: error.resultCode.rawValue, extendedResultCode: error.extendedResultCode.rawValue)
        case is CancellationError: self = .cancelled
        default: self = .other(typeName: String(reflecting: type(of: error)))
        }
    }

    public var description: String {
        switch self {
        case .ring(let failure): "ring: \(failure)"
        case .monitoringSetting(let error): "monitoring setting: \(error)"
        case .store(let failure): "store: \(failure)"
        case .database(let code, let extended): "database error (SQLite code \(code), extended \(extended))"
        case .cancelled: "cancelled"
        case .persistNotCalled: "the ring returned data without storing it before acknowledging it"
        case .other(let name): "error of type \(name)"
        }
    }
}

/// A `RingError` without anything it carries that could hold ring data: command types and reply
/// codes are kept, frames and payloads are not.
public enum RingFailure: Sendable, Equatable, CustomStringConvertible {
    case bluetoothUnavailable
    case notFound
    case unknownIdentifier
    /// The underlying reason text is dropped: it comes from the system and is not needed here.
    case connectionFailed
    case missingService
    case notConnected
    /// What was waited for, as the session names it (built from constants and command types).
    case timeout(String)
    /// The ring answered `command` with an error reply carrying `code`.
    case rejected(command: UInt16, code: UInt8?)
    case checksumMismatch
    case refusedDestructive(command: UInt16)
    case busy
    case stopped
    case tooManyFrames(limit: Int)
    case refusedUploadGroup(command: UInt16)

    init(_ error: RingError) {
        // Every case named, so a new `RingError` case forces a decision here.
        switch error {
        case .bluetoothUnavailable: self = .bluetoothUnavailable
        case .notFound: self = .notFound
        case .unknownIdentifier: self = .unknownIdentifier
        case .connectionFailed: self = .connectionFailed
        case .missingService: self = .missingService
        case .notConnected: self = .notConnected
        case .timeout(let what): self = .timeout(what)
        case .rejected(let frame): self = .rejected(command: frame.type, code: frame.payload.count == 1 ? frame.payload[0] : nil)
        case .checksumMismatch: self = .checksumMismatch
        case .refusedDestructive(let frame): self = .refusedDestructive(command: frame.type)
        case .busy: self = .busy
        case .stopped: self = .stopped
        case .tooManyFrames(let limit): self = .tooManyFrames(limit: limit)
        case .refusedUploadGroup(let frame): self = .refusedUploadGroup(command: frame.type)
        }
    }

    public var description: String {
        switch self {
        case .bluetoothUnavailable: "Bluetooth is unavailable"
        case .notFound: "no matching ring was found in range"
        case .unknownIdentifier: "the ring's identifier is not known to this device"
        case .connectionFailed: "connection failed"
        case .missingService: "the device does not expose the YCBT service"
        case .notConnected: "not connected"
        case .timeout(let what): "timed out waiting for \(what)"
        case .rejected(let command, let code):
            String(format: "ring rejected command %04X", command) + (code.map { String(format: " (code %02X)", $0) } ?? "")
        case .checksumMismatch: "history transfer failed its checksum"
        case .refusedDestructive(let command): String(format: "refused to send %04X: it erases history from the ring", command)
        case .busy: "busy"
        case .stopped: "the session was stopped"
        case .tooManyFrames(let limit): "more than \(limit) frames arrived while the conversation was not reading"
        case .refusedUploadGroup(let command): String(format: "refused to send %04X: its reply would be taken as an upload", command)
        }
    }
}

/// A `RingStoreError` reduced to what is safe to show. The store's own ring ids are kept (the
/// result already carries the ring id); a file path is not.
public enum StoreFailure: Sendable, Equatable, CustomStringConvertible {
    case unknownRing(UUID)
    case peripheralIdentifierInUse(UUID)
    case fileAccess(errno: Int32)
    /// The store names what could not be read with a constant (a column or a field), never a value.
    case corruptRow(String)
    case notIngestible(HistoryKind)

    init(_ error: RingStoreError) {
        switch error {
        case .unknownRing(let id): self = .unknownRing(id)
        case .peripheralIdentifierInUse(let id): self = .peripheralIdentifierInUse(id)
        case .fileAccess(_, let code): self = .fileAccess(errno: code)
        case .corruptRow(let what): self = .corruptRow(what)
        case .notIngestible(let kind): self = .notIngestible(kind)
        }
    }

    public var description: String {
        switch self {
        case .unknownRing: "the ring is not in the store"
        case .peripheralIdentifierInUse: "the peripheral identifier belongs to another ring"
        case .fileAccess(let code): "the database file could not be accessed (errno \(code))"
        case .corruptRow(let what): "a stored value could not be read (\(what))"
        case .notIngestible(let kind): "\(kind.name) history cannot be ingested"
        }
    }
}
