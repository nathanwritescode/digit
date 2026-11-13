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

    /// `reference` is the record's ring-local time (its wall-clock reading decoded as if UTC): the
    /// end for activity and sleep, the time for heart rate.
    ///
    /// Candidates are the latest clock-set at or before `lastAbsent` plus every clock-set between
    /// `lastAbsent` and `firstSeen` (every clock-set before `firstSeen` when there is no
    /// `lastAbsent`). If they all share one offset, that offset is certain. Otherwise each candidate
    /// is tested: its proposed instant must lie in its own period and not after `firstSeen`
    /// (both widened by the tolerance), and, for records written at about their reference time
    /// (heart rate and activity), not before `lastAbsent` (widened). A sleep session is written
    /// some time after it ends, so that last bound does not apply to it.
    func resolve(window w: Window, reference: Date) -> ResolvedOffset {
        let tol = Self.tolerance
        let eligible = clockSets.indices.filter { clockSets[$0].instant < w.firstSeen }
        var candidates = eligible
        // A sleep session is stamped during the night and written later, so the clock in force at its
        // end can be older than the latest clock-set at or before `lastAbsent`: for sleep every
        // clock-set before the first sighting is a candidate and the period test narrows them.
        if let lastAbsent = w.lastAbsent, kind != .sleep {
            // One split point: clock-sets after the last-absent fetch are the window, and the latest
            // one at or before it (one made at that very instant included) was in force at its start.
            let split = eligible.firstIndex { clockSets[$0].instant > lastAbsent } ?? eligible.count
            candidates = Array(eligible[max(split - 1, 0)...])
        }
        guard let latest = candidates.last else {
            // One offset for the whole sighting: the ring's clock does not follow daylight saving.
            let zone = TimeZone(identifier: w.firstSeenZone) ?? .gmt
            return ResolvedOffset(seconds: zone.secondsFromGMT(for: w.firstSeen), source: .assumed)
        }
        // Includes a retried clock-set (same offset a few seconds apart) and any sync where the
        // offset did not change. The record must still postdate the earliest candidate: one written
        // before the first clock-set the app ever logged (history from before the app, or after a
        // reinstall) is not confirmed by it. Otherwise the fit test below decides (with one offset it
        // is either `resolved` or `assumed`).
        if Set(candidates.map { clockSets[$0].utcOffsetSeconds }).count == 1 {
            let offset = clockSets[latest].utcOffsetSeconds
            let proposed = reference.addingTimeInterval(-TimeInterval(offset))
            let earliest = candidates.map { clockSets[$0].instant }.min() ?? w.firstSeen
            if proposed >= earliest.addingTimeInterval(-tol) && proposed <= w.firstSeen.addingTimeInterval(tol) {
                return ResolvedOffset(seconds: offset, source: .resolved)
            }
        }

        let fitting = candidates.filter { k in
            let clock = clockSets[k]
            let proposed = reference.addingTimeInterval(-TimeInterval(clock.utcOffsetSeconds))
            let periodEnd = k + 1 < clockSets.count ? clockSets[k + 1].instant : w.firstSeen
            var fits = proposed >= clock.instant.addingTimeInterval(-tol)
                && proposed <= periodEnd.addingTimeInterval(tol)
                && proposed <= w.firstSeen.addingTimeInterval(tol)
            if fits, kind != .sleep, let lastAbsent = w.lastAbsent {
                fits = proposed >= lastAbsent.addingTimeInterval(-tol)
            }
            return fits
        }
        // Fitting candidates are grouped by offset: two clock-sets with the same offset do not make
        // a record ambiguous.
        if Set(fitting.map { clockSets[$0].utcOffsetSeconds }).count == 1, let only = fitting.last {
            return ResolvedOffset(seconds: clockSets[only].utcOffsetSeconds, source: .resolved)
        }
        if let best = fitting.last { return ResolvedOffset(seconds: clockSets[best].utcOffsetSeconds, source: .ambiguous) }
        return ResolvedOffset(seconds: clockSets[latest].utcOffsetSeconds, source: .assumed)
    }
}
