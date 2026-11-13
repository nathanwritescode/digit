import Foundation
import RingProtocol

/// How the UTC offset of a typed row was chosen. Stored as a frozen text code.
public enum TimeSource: Equatable, Hashable, Sendable, CaseIterable {
    /// The logs single out the clock-set that was in force when the record was written.
    case resolved
    /// Several clock-sets fit; the latest was used.
    case ambiguous
    /// Not confirmed by the logs: no clock-set fits (or none was recorded), so the latest
    /// candidate, or the zone the first-seeing fetch recorded, was used. This is not a claim that
    /// the offset is wrong, only that the logs cannot confirm it.
    case assumed
}

/// The strings stored in `time_source`. NEVER CHANGE an existing code; the switch has no default so
/// a new case fails to compile until it gets a new code.
enum TimeSourceCode {
    static func code(for source: TimeSource) -> String {
        switch source {
        case .resolved: "resolved"
        case .ambiguous: "ambiguous"
        case .assumed: "assumed"
        }
    }

    static func source(forCode code: String) -> TimeSource? {
        TimeSource.allCases.first { self.code(for: $0) == code }
    }
}

struct ResolvedOffset: Equatable, Sendable {
    var seconds: Int
    var source: TimeSource
}

/// Pure logic (no database): chooses the UTC offset for a record in a stored transfer.
///
/// The ring stamps records with wall-clock time by whatever clock was last set and does not follow
/// travel or daylight saving. The app fetches history before it sets the clock, so the fetch log
/// (observations) bounds when a record was written and the clock-set log says which offset was in
/// force. The rules are summarised in AGENTS.md (Decisions, Store).
///
/// Not handled: a clock-set in the middle of a record's own time span (a sleep session that
/// straddles one). The session's end is the reference and every stage gets the same offset.
struct TimeResolver {
    /// Slack on the period bounds, for the seconds between a record's real time and what the ring
    /// stamped (clock drift, rounding) and between fetch and clock-set instants.
    static let tolerance: TimeInterval = 120
    /// A lone clock-set this soon after the last fetch that lacked the record was made in the
    /// same sync as that fetch, so it was in force for the whole window.
    static let sameSyncWindow: TimeInterval = 600

    let ringID: UUID
    let kind: HistoryKind
    /// Sorted by fetch time then row id.
    let observations: [TransferObservation]
    /// Sorted by instant; equal instants keep the order given (the store returns them in the order
    /// recorded). The tie-break is explicit rather than relying on sort stability.
    let clockSets: [ClockSetEvent]

    init(ringID: UUID, kind: HistoryKind, observations: [TransferObservation], clockSets: [ClockSetEvent]) {
        self.ringID = ringID
        self.kind = kind
        self.observations = observations.sorted { ($0.fetchedAt, $0.id) < ($1.fetchedAt, $1.id) }
        self.clockSets = clockSets.enumerated()
            .sorted { ($0.element.instant, $0.offset) < ($1.element.instant, $1.offset) }
            .map(\.element)
    }

    /// One usable observation of a transfer's data.
    struct Sighting: Equatable, Sendable {
        var dataLength: Int
        var fetchedAt: Date
        var timeZoneIdentifier: String
    }

    /// When a record was written, as bounds from the fetch log.
    struct Window: Equatable, Sendable {
        /// The earliest fetch that contained the record.
        var firstSeen: Date
        var firstSeenZone: String
        /// The latest earlier fetch that did not contain it, if any.
        var lastAbsent: Date?
    }

    /// The observations whose bytes are a prefix of `data` (the stored transfer). Every fetch counts,
    /// including repeats of identical data: an unchanged fetch is exactly what proves a later record
    /// did not exist yet. Hashes are computed once per distinct length.
    func sightings(in data: [UInt8]) -> [Sighting] {
        var hashByLength: [Int: [UInt8]] = [:]
        var result: [Sighting] = []
        for o in observations where o.ringID == ringID && o.kind == kind && o.dataLength <= data.count {
            let expected: [UInt8]
            if let cached = hashByLength[o.dataLength] {
                expected = cached
            } else {
                expected = RingStore.contentHash(ringID: ringID, kind: kind, data: Array(data.prefix(o.dataLength)))
                hashByLength[o.dataLength] = expected
            }
            if o.contentHash == expected {
                result.append(Sighting(
                    dataLength: o.dataLength, fetchedAt: o.fetchedAt, timeZoneIdentifier: o.timeZoneIdentifier))
            }
        }
        return result
    }

    /// The record occupies bytes `[offset, offset + length)` of the data `sightings` came from.
    /// Every stored transfer has an observation of its own full data (written in the same
    /// transaction as the transfer, and backfilled by migration v2), so a record is always covered;
    /// if not, the log is damaged and this throws rather than guess an offset.
    func window(sightings: [Sighting], offset: Int, length: Int) throws -> Window {
        let end = offset + length
        guard let first = sightings.first(where: { $0.dataLength >= end }) else {
            throw RingStoreError.corruptRow("no fetch log entry covers a stored transfer")
        }
        let lastAbsent = sightings.last { $0.dataLength <= offset && $0.fetchedAt < first.fetchedAt }?.fetchedAt
        return Window(firstSeen: first.fetchedAt, firstSeenZone: first.timeZoneIdentifier, lastAbsent: lastAbsent)
    }

    func resolve(sightings: [Sighting], offset: Int, length: Int, reference: Date) throws -> ResolvedOffset {
        resolve(window: try window(sightings: sightings, offset: offset, length: length), reference: reference)
    }

    /// `reference` is the record's ring-local end (its wall-clock reading decoded as if UTC).
    func resolve(window w: Window, reference: Date) -> ResolvedOffset {
        let tol = Self.tolerance
        let eligible = clockSets.indices.filter { clockSets[$0].instant < w.firstSeen }
        guard !eligible.isEmpty else {
            // One offset for the whole sighting: the ring's clock does not follow daylight saving.
            let zone = TimeZone(identifier: w.firstSeenZone) ?? .gmt
            return ResolvedOffset(seconds: zone.secondsFromGMT(for: w.firstSeen), source: .assumed)
        }

        var candidates = eligible
        if let lastAbsent = w.lastAbsent {
            let atOrBefore = eligible.last { clockSets[$0].instant <= lastAbsent }
            let inWindow = eligible.filter { clockSets[$0].instant > lastAbsent }
            if inWindow.isEmpty, let k = atOrBefore {
                return ResolvedOffset(seconds: clockSets[k].utcOffsetSeconds, source: .resolved)
            }
            // The record's own time is deliberately not checked here: the log bounds when it was
            // written, and a sleep session can be written after its end time.
            if inWindow.count == 1,
               clockSets[inWindow[0]].instant.timeIntervalSince(lastAbsent) <= Self.sameSyncWindow
            {
                return ResolvedOffset(seconds: clockSets[inWindow[0]].utcOffsetSeconds, source: .resolved)
            }
            candidates = (atOrBefore.map { [$0] } ?? []) + inWindow
        }

        let fitting = candidates.filter { k in
            let clock = clockSets[k]
            let proposed = reference.addingTimeInterval(-TimeInterval(clock.utcOffsetSeconds))
            let periodEnd = k + 1 < clockSets.count ? clockSets[k + 1].instant : w.firstSeen
            return proposed >= clock.instant.addingTimeInterval(-tol)
                && proposed <= periodEnd.addingTimeInterval(tol)
                && proposed <= w.firstSeen.addingTimeInterval(tol)
        }
        if fitting.count == 1 { return ResolvedOffset(seconds: clockSets[fitting[0]].utcOffsetSeconds, source: .resolved) }
        if let latest = fitting.last { return ResolvedOffset(seconds: clockSets[latest].utcOffsetSeconds, source: .ambiguous) }
        // Never empty: every eligible clock-set is either at or before `lastAbsent` or in the window.
        let latest = candidates.last ?? eligible[eligible.count - 1]
        return ResolvedOffset(seconds: clockSets[latest].utcOffsetSeconds, source: .assumed)
    }
}
