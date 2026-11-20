import Foundation
import RingProtocol
import RingStore

/// Text for the data screens. Times of day are always shown on a row's own clock (its
/// `utcOffsetSeconds`), never in the device's time zone; days are `RingDay`s, which carry no zone.
///
/// Durations, distances, numbers, times and dates come from Foundation's format styles in
/// `locale`. The surrounding words ("asleep", "steps", "mean") are English, like the rest of the app.
public struct HealthFormatter: Sendable {
    public var locale: Locale

    public init(locale: Locale = .autoupdatingCurrent) {
        self.locale = locale
    }

    // MARK: Durations

    /// Rounded to the nearest whole minute (half a minute rounds up) before formatting, so the
    /// style never has to show or round seconds.
    private static func wholeMinutes(_ seconds: Int) -> Duration {
        .seconds(Int((Double(max(seconds, 0)) / 60).rounded()) * 60)
    }

    /// The style for `duration(seconds:)` and `spokenDuration(seconds:)`.
    public func durationStyle(width: Duration.UnitsFormatStyle.UnitWidth) -> Duration.UnitsFormatStyle {
        Duration.UnitsFormatStyle(allowedUnits: [.hours, .minutes], width: width).locale(locale)
    }

    /// Hours and minutes, abbreviated: "7 hrs, 12 min" in en_GB.
    public func duration(seconds: Int) -> String {
        Self.wholeMinutes(seconds).formatted(durationStyle(width: .abbreviated))
    }

    /// For VoiceOver, in full: "7 hours, 12 minutes" in en_GB.
    public func spokenDuration(seconds: Int) -> String {
        Self.wholeMinutes(seconds).formatted(durationStyle(width: .wide))
    }

    /// Compact, for chart labels: "1h 10m" in en_GB.
    public func compactDuration(seconds: Int) -> String {
        Self.wholeMinutes(seconds).formatted(durationStyle(width: .narrow))
    }

    // MARK: Times and days

    /// The zone a row's times are shown in: a fixed offset, never the device's zone.
    public static func zone(utcOffsetSeconds: Int) -> TimeZone {
        TimeZone(secondsFromGMT: utcOffsetSeconds) ?? .gmt
    }

    private func calendar(in zone: TimeZone) -> Calendar {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = zone
        calendar.locale = locale
        return calendar
    }

    /// A time of day on the clock `utcOffsetSeconds` ahead of UTC, in the locale's short style.
    public func time(_ date: Date, utcOffsetSeconds: Int) -> String {
        let zone = Self.zone(utcOffsetSeconds: utcOffsetSeconds)
        return date.formatted(
            Date.FormatStyle(date: .omitted, time: .shortened, locale: locale, calendar: calendar(in: zone), timeZone: zone))
    }

    /// "Saturday 16 January" (in the locale's order). Read in GMT, since a `RingDay` is.
    public func day(_ day: RingDay) -> String {
        // Noon, so no rounding can move it to a neighbouring day.
        day.gmtMidnight.addingTimeInterval(12 * 3600).formatted(
            Date.FormatStyle(locale: locale, calendar: calendar(in: .gmt), timeZone: .gmt)
                .weekday(.wide).day().month(.wide))
    }

    /// The day's weekday as one letter ("F"), for a chart's day axis. Read in GMT, since a
    /// `RingDay` is.
    public func weekdayLetter(_ day: RingDay) -> String {
        day.gmtMidnight.addingTimeInterval(12 * 3600).formatted(
            Date.FormatStyle(locale: locale, calendar: calendar(in: .gmt), timeZone: .gmt).weekday(.narrow))
    }

    /// The day's weekday in full ("Friday"), for VoiceOver beside `weekdayLetter`.
    public func weekday(_ day: RingDay) -> String {
        day.gmtMidnight.addingTimeInterval(12 * 3600).formatted(
            Date.FormatStyle(locale: locale, calendar: calendar(in: .gmt), timeZone: .gmt).weekday(.wide))
    }

    /// A time of day given as seconds after midnight, with no date or zone involved ("23:10").
    public func timeOfDay(secondsAfterMidnight seconds: Int) -> String {
        time(Date(timeIntervalSince1970: TimeInterval(seconds.modulo(86_400))), utcOffsetSeconds: 0)
    }

    // MARK: Values

    /// "87%", from a fraction.
    public func percent(_ fraction: Double) -> String {
        fraction.formatted(.percent.precision(.fractionLength(0)).locale(locale))
    }

    /// "58 bpm".
    public func bpm(_ value: Int) -> String {
        "\(number(value)) bpm"
    }

    public func number(_ value: Int) -> String {
        value.formatted(.number.locale(locale))
    }

    /// "8,412 steps", "1 step".
    public func steps(_ count: Int) -> String {
        count == 1 ? "1 step" : "\(number(count)) steps"
    }

    /// The style for `distance(meters:)` and `spokenDistance(meters:)`: the locale's road units
    /// (kilometres, or miles in the US and UK), with at most one decimal.
    public func distanceStyle(width: Measurement<UnitLength>.FormatStyle.UnitWidth) -> Measurement<UnitLength>.FormatStyle {
        Measurement<UnitLength>.FormatStyle(
            width: width, locale: locale, usage: .road, numberFormatStyle: .number.precision(.fractionLength(0...1)))
    }

    /// "6.1 km", "3.8 mi".
    public func distance(meters: Int) -> String {
        Measurement(value: Double(meters), unit: UnitLength.meters).formatted(distanceStyle(width: .abbreviated))
    }

    /// "6.1 kilometres", "3.8 miles".
    public func spokenDistance(meters: Int) -> String {
        Measurement(value: Double(meters), unit: UnitLength.meters).formatted(distanceStyle(width: .wide))
    }

    /// "52–118 bpm, mean 64".
    public func heartRate(_ summary: HeartRateDaySummary) -> String {
        "\(number(summary.minimumBPM))–\(number(summary.maximumBPM)) bpm, mean \(number(summary.meanBPM))"
    }

    /// "heart rate 52 to 118 beats per minute, average 64".
    public func spokenHeartRate(_ summary: HeartRateDaySummary) -> String {
        "heart rate \(number(summary.minimumBPM)) to \(number(summary.maximumBPM)) beats per minute, "
            + "average \(number(summary.meanBPM))"
    }

    // MARK: Sleep stages

    /// A stage's name, as a text label beside its colour.
    public static func name(of kind: SleepStageKind) -> String {
        switch kind {
        case .deep: "Deep"
        case .light: "Light"
        case .rem: "REM"
        case .awake: "Awake"
        case .nap: "Nap"
        }
    }

    // MARK: Certainty

    /// A plain marker for rows whose times the logs could not confirm; `nil` when `resolved`.
    public static func uncertaintyMarker(for source: TimeSource) -> String? {
        source == .resolved ? nil : "Times uncertain"
    }

    /// One line on why the marker is shown; `nil` when `resolved`.
    public static func uncertaintyExplanation(for source: TimeSource) -> String? {
        switch source {
        case .resolved: nil
        case .ambiguous:
            "More than one of the ring's clock settings could apply here, so these times may be shifted by a time-zone difference."
        case .assumed:
            "Digit couldn't confirm which clock setting the ring used here, so these times may be shifted by a time-zone difference."
        }
    }
}
