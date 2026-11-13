import Foundation
import Testing

@testable import RingStore

// All bytes here are synthetic (see `sleepBytes`).

private func at(_ seconds: Double) -> Date { t0.addingTimeInterval(seconds) }

struct IngestionSkippedStagesTests {
    /// A valid light stage, an unknown kind (0x01), a zero-duration stage and a valid REM stage:
    /// two entries are skipped.
    private func skippingSession(_ s0: UInt32) -> [UInt8] {
        sleepBytes(start: s0, end: s0 + 1000, stages: [(242, s0, 400), (1, s0 + 401, 100), (241, s0 + 502, 0), (243, s0 + 503, 400)])
    }

    private func cleanSession(_ s0: UInt32) -> [UInt8] {
        sleepBytes(start: s0, end: s0 + 1000, stages: [(242, s0, 500)])
    }

    @Test func aSessionWithSkippedStagesIsStoredAndCounted() throws {
        try onEachStore { store in
            let f = try clockedFixture(store, kinds: [.sleep])
            try f.fetch(.sleep, skippingSession(wall(at(100), offset: plus1)), at: at(3600))
            let r = try store.ingest(ringID: f.ring.id, kind: .sleep)
            #expect(r.inserted == 1 && r.skippedSleepStages == 2)
            #expect(try store.sleepSessions(ringID: f.ring.id).first?.value.stages.count == 2)
        }
    }

    @Test func aCleanSessionAndOtherKindsReportZero() throws {
        try onEachStore { store in
            let f = try clockedFixture(store, kinds: [.sleep, .heartRate])
            try f.fetch(.sleep, cleanSession(wall(at(100), offset: plus1)), at: at(3600))
            #expect(try store.ingest(ringID: f.ring.id, kind: .sleep).skippedSleepStages == 0)
            #expect(try store.ingest(ringID: f.ring.id, kind: .heartRate).skippedSleepStages == 0)
        }
    }

    @Test func aSessionInTwoStoredTransfersIsCountedOnce() throws {
        try onEachStore { store in
            let f = try clockedFixture(store, kinds: [.sleep])
            let a = skippingSession(wall(at(100), offset: plus1))
            let b = cleanSession(wall(at(5000), offset: plus1))
            try f.fetch(.sleep, a, at: at(3600))
            // Reordered, so it is not an extension of the first transfer and both are kept.
            try f.fetch(.sleep, b + a, at: at(9000))
            #expect(try store.transfers(ringID: f.ring.id, kind: .sleep).transfers.count == 2)
            let r = try store.ingest(ringID: f.ring.id, kind: .sleep)
            #expect(r.inserted == 2 && r.skippedSleepStages == 2)
        }
    }

    @Test func aCleanCopySupersededByOneWithSkippedStagesReportsTheLaterCount() throws {
        try onEachStore { store in
            let f = try clockedFixture(store, kinds: [.sleep])
            let s0 = wall(at(100), offset: plus1)
            try f.fetch(.sleep, sleepBytes(start: s0, end: s0 + 500, stages: [(242, s0, 400)]), at: at(3600))
            let rewritten = sleepBytes(
                start: s0, end: s0 + 1200,
                stages: [(242, s0, 500), (1, s0 + 501, 100), (243, s0 + 602, 0), (243, s0 + 603, 500)])
            try f.fetch(.sleep, rewritten, at: at(7200))
            let r = try store.ingest(ringID: f.ring.id, kind: .sleep)
            #expect(r.superseded == 1 && r.skippedSleepStages == 2)
        }
    }

    @Test func aRejectedSessionIsNotCounted() throws {
        try onEachStore { store in
            let f = try clockedFixture(store, kinds: [.sleep])
            let s0 = wall(at(100), offset: plus1)
            // Starts after it ends, so it is implausible; it also has an unknown-kind and a zero-length stage.
            let bad = sleepBytes(start: s0 + 1000, end: s0, stages: [(242, s0, 400), (1, s0 + 401, 100), (241, s0 + 502, 0)])
            try f.fetch(.sleep, bad, at: at(3600))
            let r = try store.ingest(ringID: f.ring.id, kind: .sleep)
            #expect(r.rejectedImplausible == 1 && r.inserted == 0 && r.skippedSleepStages == 0)
        }
    }

    /// A ended at real 3000 under +1 h (resolved +1 h, first seen after the clock went back at 7210);
    /// B is a longer session whose ring-local start is an hour earlier and which ends under the new
    /// clock (offset 0). Both land on the instant real 2100: one row, one collision, the later copy in
    /// the buffer (B) wins, and the count is B's (3), not A's (1) and not their sum (4).
    @Test func onACollisionTheCountIsThatOfTheSessionThatIsStored() throws {
        try onEachStore { store in
            let f = try Fixture(store)
            try f.fetch(.sleep, [], at: at(-10))
            try f.setEarlierAndFirstClocks(at: at(0), offset: plus1)
            try f.fetch(.sleep, [], at: at(7200))
            try f.setClock(at: at(7210), offset: 0)
            let aStart = wall(at(2100), offset: plus1)
            let a = sleepBytes(
                start: aStart, end: wall(at(3000), offset: plus1),
                stages: [(242, aStart, 900), (1, aStart + 901, 5)])
            let bStart = wall(at(2100), offset: 0)
            let b = sleepBytes(
                start: bStart, end: wall(at(9000), offset: 0),
                stages: [(242, bStart, 6000), (1, bStart + 6001, 5), (243, bStart + 6010, 0), (1, bStart + 6020, 5)])
            try f.fetch(.sleep, a + b, at: at(14400))
            let r = try store.ingest(ringID: f.ring.id, kind: .sleep)
            #expect(r.collisions == 1 && r.inserted == 1)
            let row = try #require(try store.sleepSessions(ringID: f.ring.id).first).value
            #expect(row.ringLocalSeconds == Int(bStart) && row.stages.count == 1)
            #expect(r.skippedSleepStages == 3)
        }
    }

    @Test func aSupersededCopyIsNotCounted() throws {
        try onEachStore { store in
            let f = try clockedFixture(store, kinds: [.sleep])
            let s0 = wall(at(100), offset: plus1)
            try f.fetch(.sleep, skippingSession(s0), at: at(3600))
            // The ring rewrote the session (same start, not a prefix) with no skipped stages.
            let rewritten = sleepBytes(start: s0, end: s0 + 1200, stages: [(242, s0, 500), (243, s0 + 501, 600)])
            try f.fetch(.sleep, rewritten, at: at(7200))
            let r = try store.ingest(ringID: f.ring.id, kind: .sleep)
            #expect(r.superseded == 1 && r.skippedSleepStages == 0)
        }
    }
}
