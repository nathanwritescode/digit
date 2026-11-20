import Foundation
import GRDB
import RingProtocol

// Read-only daily summaries over `activity_interval` and `heart_rate_reading` (typed rows from
// ingestion). Nothing here writes, migrates or changes an existing function.
//
// SQLite does the summing: each table is read once, restricted to the range, and grouped by day
// and time source, so a call returns at most a few rows per day whatever the history's length.
// The range test is on the row's ring-local time (instant plus its own offset), which no index
// covers, so a coarse bound on the instant itself (the range widened by a day each side, more than
// any UTC offset) is added for the `(ring_id, time)` and `(ring_id, start)` unique indexes.

/// Heart rate over one ring-local day, from heart-rate records only.
public struct HeartRateDaySummary: Equatable, Sendable {
    public var readingCount: Int
    public var minimumBPM: Int
    public var maximumBPM: Int
    /// The mean of the day's readings, rounded to the nearest whole beat (halves away from zero).
    public var meanBPM: Int

    public init(readingCount: Int, minimumBPM: Int, maximumBPM: Int, meanBPM: Int) {
        self.readingCount = readingCount
        self.minimumBPM = minimumBPM
        self.maximumBPM = maximumBPM
        self.meanBPM = meanBPM
    }
}

/// Heart rate over any stretch of time, from heart-rate records only (see
/// `RingStore.heartRateSummary(ringID:within:)`). The same fields as a day's.
public typealias HeartRateSummary = HeartRateDaySummary

/// Steps and heart rate for one day on the ring's own clock. See
/// `RingStore.dailySummaries(ringID:from:through:)` for the rules.
public struct DailySummary: Equatable, Sendable {
    public var day: RingDay
    /// Sums over the activity buckets that start on this day. A bucket that crosses midnight counts
    /// whole on its start's day.
    public var steps: Int
    public var distanceMeters: Int
    public var kilocalories: Int
    /// How many activity buckets start on this day. Zero on a day with heart rate only.
    public var activityIntervalCount: Int
    /// `nil` on a day with no heart-rate readings.
    public var heartRate: HeartRateDaySummary?
    /// The least certain `timeSource` among the day's activity buckets and heart-rate readings
    /// (resolved, ambiguous, assumed). No row is left out because of its source.
    public var timeSource: TimeSource

    public init(
        day: RingDay, steps: Int, distanceMeters: Int, kilocalories: Int, activityIntervalCount: Int,
        heartRate: HeartRateDaySummary?, timeSource: TimeSource
    ) {
        self.day = day
        self.steps = steps
        self.distanceMeters = distanceMeters
        self.kilocalories = kilocalories
        self.activityIntervalCount = activityIntervalCount
        self.heartRate = heartRate
        self.timeSource = timeSource
    }
}

extension RingStore {
    /// One summary per ring-local day in `first...last` (inclusive) that has at least one activity
    /// bucket or heart-rate reading, oldest first.
    ///
    /// - **Day:** a row's instant plus its own `utc_offset_seconds`, read as a Gregorian date in GMT
    ///   (the `RingDay` rule, as for sleep nights), never the device's zone. For an activity bucket
    ///   the instant is its start, so a bucket that crosses midnight counts whole on its start's day.
    /// - **Steps, distance, kilocalories:** sums over the day's activity buckets.
    /// - **Heart rate:** from `heart_rate_reading` only, never the combined record (each scheduled
    ///   reading produces both, and the heart-rate record is the one used for heart rate).
    /// - **Time source:** the least certain among the day's rows.
    ///
    /// Rows are never dated before 2020 (ingestion rejects them), which the day arithmetic relies on.
    public func dailySummaries(ringID: UUID, from first: RingDay, through last: RingDay) async throws -> [DailySummary] {
        try await read { db in try Self.dailySummaries(db, ringID: ringID, from: first, through: last) }
    }

    static func dailySummaries(_ db: Database, ringID: UUID, from first: RingDay, through last: RingDay) throws
        -> [DailySummary]
    {
        guard first <= last else { return [] }
        let dayMS: Int64 = 86_400_000
        let lowLocal = HistoryTransfer.millisecondsSince1970(first.gmtMidnight)
        let highLocal = HistoryTransfer.millisecondsSince1970(last.adding(days: 1).gmtMidnight)  // exclusive
        func arguments() -> StatementArguments {
            [ringID.uuidString, lowLocal - dayMS, highLocal + dayMS, lowLocal, highLocal]
        }

        struct Partial {
            var steps = 0
            var distance = 0
            var kilocalories = 0
            var buckets = 0
            var readings = 0
            var bpmSum = 0
            var minimum = Int.max
            var maximum = Int.min
            var rank = 0
        }
        var days: [Int64: Partial] = [:]

        func source(_ row: Row) throws -> TimeSource {
            guard let source = TimeSourceCode.source(forCode: try column(row, "time_source")) else {
                throw RingStoreError.corruptRow("time source")
            }
            return source
        }

        for row in try Row.fetchAll(
            db,
            sql: """
                SELECT (start + utc_offset_seconds * 1000) / \(dayMS) AS day_number, time_source,
                    SUM(steps) AS steps, SUM(distance_meters) AS distance, SUM(kilocalories) AS kcal,
                    COUNT(*) AS buckets
                FROM activity_interval
                WHERE ring_id = ? AND start >= ? AND start < ?
                    AND start + utc_offset_seconds * 1000 >= ? AND start + utc_offset_seconds * 1000 < ?
                GROUP BY day_number, time_source
                """,
            arguments: arguments())
        {
            let rank = Self.rank(try source(row))
            let number: Int64 = try column(row, "day_number")
            var partial = days[number] ?? Partial()
            partial.steps += try column(row, "steps") as Int
            partial.distance += try column(row, "distance") as Int
            partial.kilocalories += try column(row, "kcal") as Int
            partial.buckets += try column(row, "buckets") as Int
            partial.rank = max(partial.rank, rank)
            days[number] = partial
        }

        for row in try Row.fetchAll(
            db,
            sql: """
                SELECT (time + utc_offset_seconds * 1000) / \(dayMS) AS day_number, time_source,
                    COUNT(*) AS readings, SUM(bpm) AS bpm_sum, MIN(bpm) AS minimum, MAX(bpm) AS maximum
                FROM heart_rate_reading
                WHERE ring_id = ? AND time >= ? AND time < ?
                    AND time + utc_offset_seconds * 1000 >= ? AND time + utc_offset_seconds * 1000 < ?
                GROUP BY day_number, time_source
                """,
            arguments: arguments())
        {
            let rank = Self.rank(try source(row))
            let number: Int64 = try column(row, "day_number")
            var partial = days[number] ?? Partial()
            partial.readings += try column(row, "readings") as Int
            partial.bpmSum += try column(row, "bpm_sum") as Int
            partial.minimum = min(partial.minimum, try column(row, "minimum"))
            partial.maximum = max(partial.maximum, try column(row, "maximum"))
            partial.rank = max(partial.rank, rank)
            days[number] = partial
        }

        let sources = TimeSource.allCases.sorted { rank($0) < rank($1) }
        return days.sorted { $0.key < $1.key }.map { number, partial in
            DailySummary(
                day: RingDay(
                    instant: Date(timeIntervalSince1970: TimeInterval(number) * 86400), utcOffsetSeconds: 0),
                steps: partial.steps, distanceMeters: partial.distance, kilocalories: partial.kilocalories,
                activityIntervalCount: partial.buckets,
                heartRate: partial.readings == 0
                    ? nil
                    : HeartRateDaySummary(
                        readingCount: partial.readings, minimumBPM: partial.minimum, maximumBPM: partial.maximum,
                        meanBPM: Int((Double(partial.bpmSum) / Double(partial.readings)).rounded())),
                timeSource: sources[partial.rank])
        }
    }
}

extension RingStore {
    /// Heart rate over the readings whose instant lies in any of `intervals`, from
    /// `heart_rate_reading` only, never the combined record (as for `dailySummaries`). Both ends of
    /// each interval are inclusive, at whole milliseconds (the stored precision; an end is
    /// truncated to its millisecond); a reading inside two overlapping intervals counts once.
    /// Instants only: a reading's `utc_offset_seconds` plays no part. `nil` when no reading falls
    /// inside, or `intervals` is empty (still a guarded read, so it is refused while suspended).
    ///
    /// Used for a night's sleeping heart rate, with one interval per stretch its sessions cover.
    public func heartRateSummary(ringID: UUID, within intervals: [DateInterval]) async throws -> HeartRateSummary? {
        try await read { db in try Self.heartRateSummary(db, ringID: ringID, within: intervals) }
    }

    static func heartRateSummary(_ db: Database, ringID: UUID, within intervals: [DateInterval]) throws
        -> HeartRateSummary?
    {
        guard !intervals.isEmpty else { return nil }
        var arguments: StatementArguments = [ringID.uuidString]
        var conditions: [String] = []
        for interval in intervals {
            conditions.append("(time >= ? AND time <= ?)")
            arguments += [
                HistoryTransfer.millisecondsSince1970(interval.start), HistoryTransfer.millisecondsSince1970(interval.end),
            ]
        }
        guard
            let row = try Row.fetchOne(
                db,
                sql: """
                    SELECT COUNT(*) AS readings, SUM(bpm) AS bpm_sum, MIN(bpm) AS minimum, MAX(bpm) AS maximum
                    FROM heart_rate_reading WHERE ring_id = ? AND (\(conditions.joined(separator: " OR ")))
                    """,
                arguments: arguments)
        else { return nil }
        let readings: Int = try column(row, "readings")
        guard readings > 0 else { return nil }
        let sum: Int = try column(row, "bpm_sum")
        return HeartRateSummary(
            readingCount: readings, minimumBPM: try column(row, "minimum"), maximumBPM: try column(row, "maximum"),
            meanBPM: Int((Double(sum) / Double(readings)).rounded()))
    }
}
