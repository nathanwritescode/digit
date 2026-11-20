import Foundation
import RingProtocol
import RingStore

/// Text for the data screens. Times of day are always shown on a row's own clock (its
/// `utcOffsetSeconds`), never in the device's time zone; days are `RingDay`s, which carry no zone.
///
/// Durations and the spoken forms are English, like the rest of the app; numbers, times and dates
/// follow `locale`.
public struct HealthFormatter: Sendable {
    public var locale: Locale

    public init(locale: Locale = .autoupdatingCurrent) {
        self.locale = locale
    }

    // MARK: Durations

    /// Whole minutes, rounded to the nearest (half a minute rounds up).
    private func hoursAndMinutes(_ seconds: Int) -> (hours: Int, minutes: Int) {
        let minutes = Int((Double(max(seconds, 0)) / 60).rounded())
        return (minutes / 60, minutes % 60)
    }

    /// "7 h 12 min", "45 min", "8 h", "0 min".
    public func duration(seconds: Int) -> String {
        let (h, m) = hoursAndMinutes(seconds)
        switch (h, m) {
        case (0, _): return "\(m) min"
        case (_, 0): return "\(h) h"
        default: return "\(h) h \(m) min"
        }
    }

    /// For VoiceOver: "7 hours 12 minutes", "1 hour", "1 minute", "0 minutes".
    public func spokenDuration(seconds: Int) -> String {
        let (h, m) = hoursAndMinutes(seconds)
        let hours = h == 1 ? "1 hour" : "\(h) hours"
        let minutes = m == 1 ? "1 minute" : "\(m) minutes"
        switch (h, m) {
        case (0, _): return minutes
        case (_, 0): return hours
        default: return "\(hours) \(minutes)"
        }
    }

    // MARK: Times and days

    /// The zone a row's times are shown in: a fixed offset, never the device's zone.
    static func zone(utcOffsetSeconds: Int) -> TimeZone {
        TimeZone(secondsFromGMT: utcOffsetSeconds) ?? .gmt
    }

    private var gmtCalendar: Calendar {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = .gmt
        calendar.locale = locale
        return calendar
    }

    /// A time of day on the clock `utcOffsetSeconds` ahead of UTC, in the locale's short style.
    public func time(_ date: Date, utcOffsetSeconds: Int) -> String {
        var calendar = gmtCalendar
        let zone = Self.zone(utcOffsetSeconds: utcOffsetSeconds)
        calendar.timeZone = zone
        return date.formatted(
            Date.FormatStyle(date: .omitted, time: .shortened, locale: locale, calendar: calendar, timeZone: zone))
    }

    /// "Saturday 16 January" (in the locale's order). Read in GMT, since a `RingDay` is.
    public func day(_ day: RingDay) -> String {
        // Noon, so no rounding can move it to a neighbouring day.
        day.midnight.addingTimeInterval(12 * 3600).formatted(
            Date.FormatStyle(locale: locale, calendar: gmtCalendar, timeZone: .gmt)
                .weekday(.wide).day().month(.wide))
    }

    // MARK: Values

    public func number(_ value: Int) -> String {
        value.formatted(.number.locale(locale))
    }

    /// "8,412 steps", "1 step".
    public func steps(_ count: Int) -> String {
        count == 1 ? "1 step" : "\(number(count)) steps"
    }

    /// Kilometres, or miles where the locale uses US units, with one decimal: "6.1 km", "3.8 mi".
    public func distance(meters: Int) -> String {
        let (value, unit) = distanceParts(meters: meters)
        return "\(value.formatted(.number.precision(.fractionLength(1)).locale(locale))) \(unit.short)"
    }

    /// "6.1 kilometres", "3.8 miles".
    public func spokenDistance(meters: Int) -> String {
        let (value, unit) = distanceParts(meters: meters)
        return "\(value.formatted(.number.precision(.fractionLength(1)).locale(locale))) \(unit.long)"
    }

    private func distanceParts(meters: Int) -> (Double, (short: String, long: String)) {
        if locale.measurementSystem == .us {
            return (Double(meters) / 1609.344, ("mi", "miles"))
        }
        return (Double(meters) / 1000, ("km", "kilometres"))
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
