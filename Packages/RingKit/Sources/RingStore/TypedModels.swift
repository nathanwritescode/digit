import Foundation
import RingProtocol

// Typed rows derived from raw transfers. Instants are real instants (the ring's wall-clock
// reading minus `utcOffsetSeconds`), in whole milliseconds in the database. Nothing here comes
// from the combined record.

/// A typed row with its database id. Ids stay the same across repeated ingestion while the row's
/// key is still derived; compare `value`s, not ids, when comparing two databases.
public struct StoredRow<Value: Equatable & Sendable>: Equatable, Sendable {
    public var id: Int64
    public var value: Value
}

public struct HeartRateReading: Equatable, Sendable {
    public var time: Date
    public var bpm: Int
    /// Raw mode byte; it does not tell scheduled and on-demand readings apart on the R99.
    public var mode: Int
    public var utcOffsetSeconds: Int
    public var timeSource: TimeSource
}

public struct ActivityInterval: Equatable, Sendable {
    public var start: Date
    public var end: Date
    public var steps: Int
    public var distanceMeters: Int
    public var kilocalories: Int
    public var utcOffsetSeconds: Int
    public var timeSource: TimeSource
}

public struct SleepStageEntry: Equatable, Sendable {
    public var kind: SleepStageKind
    public var start: Date
    public var durationSeconds: Int
}

public struct SleepSessionRecord: Equatable, Sendable {
    public var start: Date
    public var end: Date
    public var utcOffsetSeconds: Int
    public var timeSource: TimeSource
    /// In the order the decoder returned them; the table keeps that order as `ordinal`.
    public var stages: [SleepStageEntry]
}

/// What one `ingest` call did for one ring and kind.
public struct IngestionResult: Equatable, Sendable {
    public var inserted = 0
    public var updated = 0
    public var deleted = 0
    public var unchanged = 0
    /// Records (counted per occurrence across stored transfers) whose ring-local time is before
    /// 2020-01-01, which means the ring's clock had never been set.
    public var rejectedBefore2020 = 0
    /// Rows that make up the desired set, by how their offset was chosen.
    public var rowsByTimeSource: [TimeSource: Int] = [:]
    /// Bytes in stored transfers that did not form a whole record (trailing or malformed bytes),
    /// summed over transfers.
    public var unusableBytes = 0
}

/// The strings stored in `sleep_stage.kind`. NEVER CHANGE an existing code; no default, so a new
/// `SleepStageKind` case fails to compile until it gets a new code.
enum StageCode {
    static let all: [SleepStageKind] = [.deep, .light, .rem, .awake, .nap]

    static func code(for kind: SleepStageKind) -> String {
        switch kind {
        case .deep: "deep"
        case .light: "light"
        case .rem: "rem"
        case .awake: "awake"
        case .nap: "nap"
        }
    }

    static func kind(forCode code: String) -> SleepStageKind? {
        all.first { self.code(for: $0) == code }
    }
}
