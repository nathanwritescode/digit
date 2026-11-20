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
        let lowLocal = HistoryTransfer.millisecondsSince1970(first.midnight)
        let highLocal = HistoryTransfer.millisecondsSince1970(last.adding(days: 1).midnight)  // exclusive
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
            days[number, default: Partial()].steps += try column(row, "steps") as Int
            days[number]!.distance += try column(row, "distance") as Int
            days[number]!.kilocalories += try column(row, "kcal") as Int
            days[number]!.buckets += try column(row, "buckets") as Int
            days[number]!.rank = max(days[number]!.rank, rank)
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
            days[number, default: Partial()].readings += try column(row, "readings") as Int
            days[number]!.bpmSum += try column(row, "bpm_sum") as Int
            days[number]!.minimum = min(days[number]!.minimum, try column(row, "minimum"))
            days[number]!.maximum = max(days[number]!.maximum, try column(row, "maximum"))
            days[number]!.rank = max(days[number]!.rank, rank)
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
