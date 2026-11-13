import Foundation
import GRDB
import RingProtocol

// Read-only sleep queries over `sleep_session` and `sleep_stage` (typed rows from ingestion).
// Nothing here writes, migrates or changes an existing function.
//
// Grouping is computed over every session of the ring on each call, never over a window, so a
// night is the same whichever query asks for it (rule 6). A ring has on the order of a thousand
// sessions a year and the query reads only the session columns, not the stages, so this stays
// cheap; stages are read only for the nights whose detail is asked for.

/// The longest gap between one session's end and the next one's start for them to be one night:
/// 60 minutes, inclusive. The ring reports no awake stages, so a brief wake-up is a gap between
/// two sessions.
public let sleepNightMaximumGapSeconds = 3600

/// A calendar day on the ring's own clock. Not a `Date`: it carries no time zone.
public struct SleepDay: Comparable, Hashable, Sendable {
    public var year: Int
    public var month: Int
    public var day: Int

    public init(year: Int, month: Int, day: Int) {
        self.year = year
        self.month = month
        self.day = day
    }

    public static func < (a: SleepDay, b: SleepDay) -> Bool {
        (a.year, a.month, a.day) < (b.year, b.month, b.day)
    }

    /// The Gregorian day of `instant` on a clock `utcOffsetSeconds` ahead of UTC, read in GMT.
    /// Never uses the device's time zone.
    public init(instant: Date, utcOffsetSeconds: Int) {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(identifier: "GMT")!
        let parts = calendar.dateComponents(
            [.year, .month, .day], from: instant.addingTimeInterval(TimeInterval(utcOffsetSeconds)))
        self.init(year: parts.year!, month: parts.month!, day: parts.day!)
    }
}

/// A gap between two consecutive sessions of one night.
public struct SleepInterruption: Equatable, Sendable {
    public var start: Date
    public var end: Date
    public var seconds: Int { Int(end.timeIntervalSince(start).rounded(.down)) }
}

/// One night: a maximal run of sessions, in start order, each starting at most
/// `sleepNightMaximumGapSeconds` after the latest end seen so far in the night. See `RingStore.sleepNights` for the rules.
public struct SleepNightSummary: Equatable, Sendable {
    /// The calendar day the night ends on, by the own offset of the session with the latest end (the later in start order on a tie).
    public var day: SleepDay
    /// First session's start.
    public var start: Date
    /// Latest end among the night's sessions.
    public var end: Date
    /// Sum of stage durations as stored (not clipped), for every kind, including those at zero.
    public var secondsByKind: [SleepStageKind: Int]
    /// Sum over `deep`, `light`, `rem` and `nap` stages. `awake` is not counted.
    public var secondsAsleep: Int
    /// Stretches inside the night covered by no session; overlapping or contained sessions add none.
    public var interruptions: [SleepInterruption]
    public var interruptionSeconds: Int
    /// The least certain `timeSource` among the night's sessions (resolved, ambiguous, assumed).
    /// No session is left out because of its source; the caller decides how to show it.
    public var timeSource: TimeSource
    /// True for the night with the most time asleep among those of its day (the later-ending one
    /// on a tie); every other night of the day is a nap. Independent of `SleepStageKind.nap`.
    public var isMainSleep: Bool
}

/// A night with its stages: all sessions' stages in time order.
public struct SleepNightDetail: Equatable, Sendable {
    public var summary: SleepNightSummary
    public var stages: [SleepStageEntry]
}

extension RingStore {
    /// The main sleep of the latest day that has one, with its stages; `nil` if the ring has no
    /// sessions.
    public func latestSleepNight(ringID: UUID) async throws -> SleepNightDetail? {
        try await writer.read { db in try Self.latestSleepNight(db, ringID: ringID) }
    }

    /// Every night whose day is in `first...last` (inclusive), naps included and flagged, ordered
    /// by start. Days are read on the ring's clock (the latest session end plus that session's own offset,
    /// in GMT). A night is never cut by the range: it has the contents and main-sleep flag any
    /// other query gives it.
    public func sleepNights(ringID: UUID, from first: SleepDay, through last: SleepDay) async throws
        -> [SleepNightSummary]
    {
        try await writer.read { db in
            try Self.sleepNightGroups(db, ringID: ringID)
                .map(\.summary)
                .filter { first <= $0.day && $0.day <= last }
        }
    }

    /// The stages of the night `summary` describes, found by its start and recomputed from the
    /// current rows, so the summary inside is fresh; `nil` if no night starts there any more.
    public func sleepNightDetail(ringID: UUID, for summary: SleepNightSummary) async throws -> SleepNightDetail? {
        try await writer.read { db in
            guard let night = try Self.sleepNightGroups(db, ringID: ringID).first(where: { $0.summary.start == summary.start })
            else { return nil }
            return try Self.detail(db, night)
        }
    }

    /// Yields `latestSleepNight` now and again after every change to the sleep tables. Observing
    /// stops when the consumer stops iterating or its task is cancelled. Errors end the stream.
    public func observeLatestSleepNight(ringID: UUID) -> AsyncThrowingStream<SleepNightDetail?, Error> {
        let writer = self.writer
        return AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    let observation = ValueObservation.tracking { db in
                        try Self.latestSleepNight(db, ringID: ringID)
                    }
                    for try await value in observation.values(in: writer) {
                        continuation.yield(value)
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    // MARK: Computation

    /// A night before its stages are loaded.
    struct NightGroup {
        var summary: SleepNightSummary
        var sessionIDs: [Int64]
    }

    private struct SessionRow {
        var id: Int64
        var start: Int64  // milliseconds since 1970
        var end: Int64
        var offset: Int
        var source: TimeSource
    }

    static func latestSleepNight(_ db: Database, ringID: UUID) throws -> SleepNightDetail? {
        let nights = try sleepNightGroups(db, ringID: ringID)
        guard let latestDay = nights.map(\.summary.day).max(),
            let main = nights.first(where: { $0.summary.day == latestDay && $0.summary.isMainSleep })
        else { return nil }
        return try detail(db, main)
    }

    static func detail(_ db: Database, _ night: NightGroup) throws -> SleepNightDetail {
        let marks = night.sessionIDs.map { _ in "?" }.joined(separator: ",")
        let rows = try Row.fetchAll(
            db,
            sql: "SELECT * FROM sleep_stage WHERE session_id IN (\(marks)) ORDER BY start, session_id, ordinal",
            arguments: StatementArguments(night.sessionIDs))
        let stages = try rows.map { row -> SleepStageEntry in
            guard let kind = StageCode.kind(forCode: try column(row, "kind")) else {
                throw RingStoreError.corruptRow("sleep stage kind")
            }
            return SleepStageEntry(
                kind: kind, start: HistoryTransfer.date(millisecondsSince1970: try column(row, "start")),
                durationSeconds: try column(row, "duration_seconds"))
        }
        return SleepNightDetail(summary: night.summary, stages: stages)
    }

    /// Rules 1 to 5, over all of the ring's sessions, ordered by night start.
    static func sleepNightGroups(_ db: Database, ringID: UUID) throws -> [NightGroup] {
        let ring = ringID.uuidString
        let sessions = try Row.fetchAll(
            db,
            sql: """
                SELECT id, start, "end", utc_offset_seconds, time_source FROM sleep_session
                WHERE ring_id = ? ORDER BY start, id
                """,
            arguments: [ring]
        ).map { row -> SessionRow in
            guard let source = TimeSourceCode.source(forCode: try column(row, "time_source")) else {
                throw RingStoreError.corruptRow("time source")
            }
            return SessionRow(
                id: try column(row, "id"), start: try column(row, "start"), end: try column(row, "end"),
                offset: try column(row, "utc_offset_seconds"), source: source)
        }

        // Stage seconds per session and kind, summed as stored.
        var seconds: [Int64: [SleepStageKind: Int]] = [:]
        for row in try Row.fetchAll(
            db,
            sql: """
                SELECT s.session_id, s.kind, SUM(s.duration_seconds) AS total
                FROM sleep_stage s JOIN sleep_session p ON p.id = s.session_id
                WHERE p.ring_id = ? GROUP BY s.session_id, s.kind
                """,
            arguments: [ring])
        {
            guard let kind = StageCode.kind(forCode: try column(row, "kind")) else {
                throw RingStoreError.corruptRow("sleep stage kind")
            }
            seconds[try column(row, "session_id"), default: [:]][kind] = try column(row, "total")
        }

        // Rule 1: a session joins the current night when it starts within the gap of the latest end
        // seen so far in that night (not of the previous session's end, which a contained session
        // would make too early).
        var runs: [[SessionRow]] = []
        var latestEnd: Int64 = 0
        for session in sessions {
            if !runs.isEmpty, session.start - latestEnd <= Int64(sleepNightMaximumGapSeconds) * 1000 {
                runs[runs.count - 1].append(session)
                latestEnd = max(latestEnd, session.end)
            } else {
                runs.append([session])
                latestEnd = session.end
            }
        }

        func date(_ ms: Int64) -> Date { HistoryTransfer.date(millisecondsSince1970: ms) }
        var nights: [NightGroup] = runs.map { run in
            var byKind = Dictionary(uniqueKeysWithValues: SleepStageKind.allCases.map { ($0, 0) })
            for session in run { for (kind, total) in seconds[session.id] ?? [:] { byKind[kind, default: 0] += total } }
            // Interruptions: stretches inside the night covered by no session.
            var interruptions: [SleepInterruption] = []
            var covered = run[0].end
            // The session with the latest end, the later in start order on a tie, decides end and day.
            var last = run[0]
            for session in run.dropFirst() {
                if session.start > covered {
                    interruptions.append(SleepInterruption(start: date(covered), end: date(session.start)))
                }
                covered = max(covered, session.end)
                if session.end >= last.end { last = session }
            }
            let summary = SleepNightSummary(
                day: SleepDay(instant: date(last.end), utcOffsetSeconds: last.offset),  // rule 2
                start: date(run[0].start), end: date(last.end), secondsByKind: byKind,
                secondsAsleep: [SleepStageKind.deep, .light, .rem, .nap].reduce(0) { $0 + (byKind[$1] ?? 0) },
                interruptions: interruptions, interruptionSeconds: interruptions.reduce(0) { $0 + $1.seconds },
                // Rule 4: the least certain source; `allCases` runs from most to least certain.
                timeSource: run.map(\.source).max { rank($0) < rank($1) } ?? .resolved,
                isMainSleep: false)
            return NightGroup(summary: summary, sessionIDs: run.map(\.id))
        }

        // Rule 3: per day, the most time asleep, the later end on a tie.
        var mainIndex: [SleepDay: Int] = [:]
        for (index, night) in nights.enumerated() {
            if let current = mainIndex[night.summary.day] {
                let other = nights[current].summary
                let s = night.summary
                if (s.secondsAsleep, s.end) > (other.secondsAsleep, other.end) { mainIndex[s.day] = index }
            } else {
                mainIndex[night.summary.day] = index
            }
        }
        for index in mainIndex.values { nights[index].summary.isMainSleep = true }
        return nights
    }

    private static func rank(_ source: TimeSource) -> Int {
        TimeSource.allCases.firstIndex(of: source) ?? 0
    }
}
