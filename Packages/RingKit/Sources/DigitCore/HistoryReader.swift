import Foundation
import RingStore
import RingSync

/// The store reads the data screens make. `RingStore` is the production conformer; tests and
/// previews use fakes with synthetic data.
public protocol RingHistoryReader: Sendable {
    func sleepNights(ringID: UUID, from first: RingDay, through last: RingDay) async throws -> [SleepNightSummary]
    func sleepNightDetail(ringID: UUID, startingAt start: Date) async throws -> SleepNightDetail?
    /// Every night of the range with its stages, in one read (the Sleep tab and the night screen
    /// need a month of them; never one read per night).
    func sleepNightDetails(ringID: UUID, from first: RingDay, through last: RingDay) async throws -> [SleepNightDetail]
    /// Heart rate from heart-rate records only, over readings inside any of `intervals` (ends inclusive).
    func heartRateSummary(ringID: UUID, within intervals: [DateInterval]) async throws -> HeartRateSummary?
    func dailySummaries(ringID: UUID, from first: RingDay, through last: RingDay) async throws -> [DailySummary]
}

extension RingStore: RingHistoryReader {}

/// What a data screen shows.
public enum DataState<Value: Sendable & Equatable>: Sendable, Equatable {
    /// Nothing read yet (or the paired ring has not been read from the store yet).
    case loading
    /// The store holds no ring.
    case notPaired
    /// A ring is stored but nothing is in range yet.
    case noData
    case loaded(Value)
    /// A load failed and there was nothing to keep showing. Plain words only: never a raw error.
    case failed(String)

    public var value: Value? {
        if case .loaded(let value) = self { value } else { nil }
    }
}

/// The days a data screen covers.
public enum RecentDays {
    /// How many ring-local days the screens show, today included.
    public static let count = 30

    /// The last `count` days ending today, plus tomorrow. Today is read on `timeZone` (the
    /// device's), the only place a device zone is used: it says which days are recent, never what
    /// day a row falls on (each row's own offset does). Tomorrow is included because the ring's
    /// clock can be ahead of the device's zone after travel west (the ring keeps the earlier,
    /// eastern zone's clock until the next sync sets it).
    public static func window(now: Date, timeZone: TimeZone) -> (first: RingDay, last: RingDay) {
        let today = RingDay(instant: now, utcOffsetSeconds: timeZone.secondsFromGMT(for: now))
        return (today.adding(days: 1 - count), today.adding(days: 1))
    }
}

/// How a failed load is shown. The words never include the error's own text: a raw store error
/// can hold statement arguments (health values), so only `SyncError`'s description is used.
enum DataLoadFailure {
    static let interrupted =
        "Digit was interrupted while reading your data. It tries again when you return to the app."

    static func isSuspension(_ error: any Error) -> Bool {
        (error as? RingStoreError) == .suspended
    }

    /// The words for a failed load with nothing kept on screen.
    static func message(for error: any Error) -> String {
        if isSuspension(error) { return interrupted }
        return "Digit couldn't read your data (\(SyncError(error).description)). Tap Try Again."
    }

    /// The state after a failed load. A load refused because the store was suspended keeps what
    /// was shown (it is retried when the app becomes active); with nothing to keep it says so.
    static func state<Value>(after error: any Error, previous: DataState<Value>) -> DataState<Value> {
        if isSuspension(error) {
            switch previous {
            case .loaded, .noData: return previous
            case .loading, .notPaired, .failed: break
            }
        }
        return .failed(message(for: error))
    }
}
