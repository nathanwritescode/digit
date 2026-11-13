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
