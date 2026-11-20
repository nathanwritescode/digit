import Foundation

/// A calendar day on the ring's own clock: a row's instant plus its own `utc_offset_seconds`, read
/// as a Gregorian date in GMT. Never the device's time zone. Not a `Date`: it carries no time zone.
///
/// Shared by the sleep queries (a night's day) and the daily summaries (a row's day).
public struct RingDay: Comparable, Hashable, Sendable {
    public var year: Int
    public var month: Int
    public var day: Int

    public init(year: Int, month: Int, day: Int) {
        self.year = year
        self.month = month
        self.day = day
    }

    public static func < (a: RingDay, b: RingDay) -> Bool {
        (a.year, a.month, a.day) < (b.year, b.month, b.day)
    }

    /// The Gregorian day of `instant` on a clock `utcOffsetSeconds` ahead of UTC, read in GMT.
    /// Never uses the device's time zone.
    public init(instant: Date, utcOffsetSeconds: Int) {
        let parts = Self.calendar.dateComponents(
            [.year, .month, .day], from: instant.addingTimeInterval(TimeInterval(utcOffsetSeconds)))
        self.init(year: parts.year!, month: parts.month!, day: parts.day!)
    }

    /// The day `count` days later (earlier when negative).
    public func adding(days count: Int) -> RingDay {
        RingDay(instant: gmtMidnight.addingTimeInterval(TimeInterval(count) * 86400), utcOffsetSeconds: 0)
    }

    /// The start of this day read as GMT: the instant whose GMT wall clock is this day at 00:00.
    /// Adding a row's offset to its instant and comparing with this gives the row's day.
    public var gmtMidnight: Date {
        Self.calendar.date(from: DateComponents(year: year, month: month, day: day))!
    }

    private static let calendar: Calendar = {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone.gmt
        return calendar
    }()
}

/// The sleep queries' name for `RingDay`, kept so the sleep API reads as before.
public typealias SleepDay = RingDay
