import Foundation
import RingProtocol

// Typed rows derived from raw transfers. Instants are real instants (the ring's wall-clock
// reading minus `utcOffsetSeconds`), in whole milliseconds in the database. Nothing here comes
// from the combined record.

/// A typed row with its database id. Ids stay the same across repeated ingestion while the row's
/// key is still derived; compare `value`s, not ids, when comparing two databases.
public struct StoredRow<Value: Equatable & Sendable>: Equatable, Sendable {
    public let id: Int64
    public let value: Value
}

public struct HeartRateReading: Equatable, Sendable {
    public var time: Date
    public var bpm: Int
    /// Raw mode byte; it does not tell scheduled and on-demand readings apart on the R99.
    public var mode: Int
    public var utcOffsetSeconds: Int
    /// The record's raw ring time (seconds since 2000-01-01 by the ring's clock). Unlike the
    /// resolved instant it never changes, but it is not unique either: the hour repeated after the
    /// clock goes back, or two readings in one ring second, share it. Part of what identifies a
    /// record, not all of it (#12, #14 decide the exact key).
    public var ringLocalSeconds: Int
    public var timeSource: TimeSource
}

public struct ActivityInterval: Equatable, Sendable {
    public var start: Date
    public var end: Date
    public var steps: Int
    public var distanceMeters: Int
    public var kilocalories: Int
    public var utcOffsetSeconds: Int
    /// The start's raw ring time (seconds since 2000-01-01 by the ring's clock); see `HeartRateReading.ringLocalSeconds`.
    public var ringLocalSeconds: Int
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
    /// The start's raw ring time (seconds since 2000-01-01 by the ring's clock); see `HeartRateReading.ringLocalSeconds`.
    public var ringLocalSeconds: Int
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
    /// Distinct records (an identical record in several transfers counts once) rejected as
    /// implausible: a ring-local time before 2020-01-01 (the ring's clock was never set); an activity
    /// bucket that starts after it ends or is longer than 24 hours; a sleep session that starts after
    /// it ends, is longer than 48 hours, or has a stage that starts outside the session or ends more
    /// than 60 seconds after it; a heart-rate reading of 0 or 255 bpm. Records dated far in the
    /// future are counted separately in `rejectedFuture`.
    public var rejectedImplausible = 0
    /// Rows that make up the desired set, by how their offset was chosen.
    public var rowsByTimeSource: [TimeSource: Int] = [:]
    /// Bytes in stored transfers that did not form a whole record (trailing or malformed bytes),
    /// summed over transfers.
    public var unusableBytes = 0
    /// Distinct records that resolved to the same unique key (two readings in one ring second, an
    /// ambiguous repeated hour). The one later in the buffer is kept and the other is counted here:
    /// a distinct record was dropped, never silently.
    public var collisions = 0
    /// Sleep sessions the ring rewrote (the same ring-local start in an earlier copy), counted when
    /// only the copy latest in the buffer is kept. Not a loss: the later copy replaces the earlier.
    public var superseded = 0
    /// Records dated more than 24 hours after the fetch that first showed them (a record cannot be
    /// written after it was fetched; this also catches bytes such as FF FF FF FF).
    public var rejectedFuture = 0
}

/// The strings stored in `sleep_stage.kind`. NEVER CHANGE an existing code; no default, so a new
/// `SleepStageKind` case fails to compile until it gets a new code.
enum StageCode {
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
        SleepStageKind.allCases.first { self.code(for: $0) == code }
    }
}
