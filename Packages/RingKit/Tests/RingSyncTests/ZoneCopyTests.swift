import Foundation
import Testing
import RingProtocol
import RingStore
import RingSync
import RingTransport

// The sync must use one fixed zone for the clock-set command and its log entry. A zone the caller
// passes can change between uses (an auto-updating zone when the user travels or the system zone
// changes); this fake makes that happen on every use, so the hazard is real at this level.

/// A zone named "America/New_York" whose offset alternates between UTC+1 and UTC+2 on every
/// query, standing in for an auto-updating zone that changes while the clock-set is built.
/// Neither value is New York's real offset, so any offset read from this object rather than from
/// the fixed copy differs from the copy's, whichever query it happens to be.
/// `@unchecked Sendable`: the counter is guarded by `lock`.
private final class ShiftingZone: NSTimeZone, @unchecked Sendable {
    private let lock = NSLock()
    private var queries = 0
    var offsetQueries: Int { lock.withLock { queries } }

    override var name: String { "America/New_York" }
    override var data: Data { Data() }
    override func secondsFromGMT(for aDate: Date) -> Int {
        lock.withLock {
            queries += 1
            return queries % 2 == 1 ? 3600 : 7200
        }
    }
    override func abbreviation(for aDate: Date) -> String? { nil }
    override func isDaylightSavingTime(for aDate: Date) -> Bool { false }
    override func daylightSavingTimeOffset(for aDate: Date) -> TimeInterval { 0 }
    override func nextDaylightSavingTimeTransition(after aDate: Date) -> Date? { nil }
}

@Test(.timeLimit(.minutes(1))) func clockSetUsesAFixedCopyOfAZoneThatChangesBetweenUses() async throws {
    let shifting = ShiftingZone()
    let given = shifting as TimeZone
    // Precondition: the fake really changes between uses, so without a fixed copy the command
    // (built through a calendar) and the logged offset could disagree.
    let probe = Date(timeIntervalSince1970: 1_814_529_600)
    #expect(given.secondsFromGMT(for: probe) != given.secondsFromGMT(for: probe))

    let (store, id) = try await makeStore()
    let ring = FakeRing(store: store, ringID: id)
    let date = Date(timeIntervalSince1970: 1_814_529_600.25)  // 2027-07-01 12:00:00.25 UTC
    let result = await RingSyncer(
        ring: ring, store: store, ringID: id, monitoringInterval: nil, budget: .seconds(60),
        clock: { date }, timeZone: { given }
    ).run()

    guard case .acknowledgedAndLogged(let event) = result.clockSet else {
        Issue.record("clock-set not acknowledged and logged")
        return
    }
    let sent = try #require(await ring.clockSent)
    // The frame built from the zone the ring was given must show the wall-clock time that the
    // logged offset says was sent, and that zone must not change on later use.
    let frame = try #require(await ring.clockFrame)
    #expect(Command.setTime(sent.date, timeZone: sent.zone) == frame, "the zone sent to the ring changes between uses")
    var gmt = Calendar(identifier: .gregorian)
    gmt.timeZone = TimeZone(identifier: "GMT")!
    let wall = gmt.dateComponents(
        [.hour, .minute], from: date.addingTimeInterval(TimeInterval(event.utcOffsetSeconds)))
    #expect(frame.payload[4] == UInt8(wall.hour!) && frame.payload[5] == UInt8(wall.minute!))
    #expect(event.timeZoneIdentifier == "America/New_York")
    #expect(sent.zone.identifier == event.timeZoneIdentifier)
    #expect(event.utcOffsetSeconds == sent.zone.secondsFromGMT(for: date))
}
