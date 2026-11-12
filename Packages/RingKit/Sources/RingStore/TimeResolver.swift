import Foundation
import RingProtocol

/// How the UTC offset of a typed row was chosen. Stored as a frozen text code.
public enum TimeSource: Equatable, Hashable, Sendable, CaseIterable {
    /// Exactly one clock-set explained the record's wall-clock time within its fetch window.
    case resolved
    /// Several clock-sets fit; the latest was used.
    case ambiguous
    /// Nothing fit (or nothing was recorded): the latest clock-set before the record was first
    /// seen, or the phone's zone at that fetch when the ring's clock was never set by the app.
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
/// travel or daylight saving. The app fetches history before it sets the clock, so a record that
/// first appeared in one fetch and was absent from the previous one was written under the clock
/// set at or before that previous fetch. The fetch log (observations) bounds when each record
/// was written; the clock-set log says which offset was in force. See the issue for #10.
///
/// Not handled: a clock-set in the middle of a record's own time span (a sleep session that
/// straddles one). The session's end is the reference and every stage gets the same offset.
struct TimeResolver {
    /// Slack on each bound, for the seconds between a record's real time and what the ring
    /// stamped (clock drift, the ring's own rounding) and between fetch and clock-set instants.
    static let tolerance: TimeInterval = 120

    let ringID: UUID
    let kind: HistoryKind
    /// Sorted by fetch time then row id.
    let observations: [TransferObservation]
    /// Sorted by instant.
    let clockSets: [ClockSetEvent]

    init(ringID: UUID, kind: HistoryKind, observations: [TransferObservation], clockSets: [ClockSetEvent]) {
        self.ringID = ringID
        self.kind = kind
        self.observations = observations.sorted { ($0.fetchedAt, $0.id) < ($1.fetchedAt, $1.id) }
        self.clockSets = clockSets.sorted { $0.instant < $1.instant }
    }

    /// One usable observation of a transfer's data.
    struct Sighting: Equatable, Sendable {
        var dataLength: Int
        var fetchedAt: Date
        var timeZoneIdentifier: String
    }

    /// The observations whose bytes are a prefix of `data` (the stored transfer). Every fetch counts,
    /// including repeats of identical data: an unchanged fetch is exactly what proves a later record
    /// did not exist yet. If none covers the whole transfer (it should always exist), the
    /// transfer's own fetch stands in so every record still has a first-seen time.
    func sightings(in data: [UInt8], fetchedAt: Date, timeZoneIdentifier: String) -> [Sighting] {
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
        if !result.contains(where: { $0.dataLength == data.count }) {
            result.append(Sighting(dataLength: data.count, fetchedAt: fetchedAt, timeZoneIdentifier: timeZoneIdentifier))
            result.sort { $0.fetchedAt < $1.fetchedAt }
        }
        return result
    }

    /// `reference` is the record's ring-local end (its wall-clock reading decoded as if UTC).
    /// The record occupies bytes `[offset, offset + length)` of the data `sightings` came from.
    func resolve(sightings: [Sighting], offset: Int, length: Int, reference: Date) -> ResolvedOffset {
        let end = offset + length
        guard let first = sightings.first(where: { $0.dataLength >= end }) else {
            return ResolvedOffset(seconds: 0, source: .assumed)
        }
        let firstSeen = first.fetchedAt
        let lastAbsent = sightings.last { $0.dataLength <= offset && $0.fetchedAt < firstSeen }?.fetchedAt

        let candidates = clockSets.enumerated().filter { $0.element.instant < firstSeen }
        let tol = Self.tolerance
        let valid = candidates.filter { index, clock in
            let proposed = reference.addingTimeInterval(-TimeInterval(clock.utcOffsetSeconds))
            let periodEnd = index + 1 < clockSets.count ? clockSets[index + 1].instant : firstSeen
            let inPeriod = proposed >= clock.instant.addingTimeInterval(-tol)
                && proposed <= periodEnd.addingTimeInterval(tol)
            let inWindow = proposed <= firstSeen.addingTimeInterval(tol)
                && (lastAbsent.map { proposed >= $0.addingTimeInterval(-tol) } ?? true)
            return inPeriod && inWindow
        }
        if valid.count == 1 { return ResolvedOffset(seconds: valid[0].element.utcOffsetSeconds, source: .resolved) }
        if valid.count > 1 { return ResolvedOffset(seconds: valid[valid.count - 1].element.utcOffsetSeconds, source: .ambiguous) }
        if let latest = candidates.last {
            return ResolvedOffset(seconds: latest.element.utcOffsetSeconds, source: .assumed)
        }
        return ResolvedOffset(
            seconds: Self.offset(inZone: first.timeZoneIdentifier, atWallClock: reference), source: .assumed)
    }

    /// The offset a zone has at a wall-clock reading (given as if UTC), taken at the real instant
    /// as `RingTime.date` does. An identifier this system does not know counts as UTC.
    static func offset(inZone identifier: String, atWallClock reference: Date) -> Int {
        let zone = TimeZone(identifier: identifier) ?? TimeZone(secondsFromGMT: 0)!
        let guess = reference.addingTimeInterval(-TimeInterval(zone.secondsFromGMT(for: reference)))
        return zone.secondsFromGMT(for: guess)
    }
}
