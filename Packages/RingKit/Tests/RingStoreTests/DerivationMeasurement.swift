import Foundation
import Testing
@testable import RingStore

// The measurement asked for in #69, not a check: off unless DIGIT_MEASURE=1, and it prints timings
// only (no health data; the history is the SYNTHETIC `VariedHistory`, 30 days at one heart-rate
// reading a minute). Run it with
//   DIGIT_MEASURE=1 swift test -c release --filter DerivationMeasurement
// Two file-backed stores get the same history: one ingests through the new path, the other
// through the one-transaction reference (the pre-#69 shape). For the reference the write lock is
// held for the whole call. For the new path "lock" is the time from the end of derivation to the
// call's return, an upper bound on the apply step's write transaction (it includes queueing for
// the writer).

@Suite(.timeLimit(.minutes(5))) struct DerivationMeasurement {
    @Test(.enabled(if: ProcessInfo.processInfo.environment["DIGIT_MEASURE"] == "1"))
    func writeLockTimeBeforeAndAfter() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let new = try RingStore(url: dir.appendingPathComponent("new.sqlite"), observesSuspension: false)
        let old = try RingStore(url: dir.appendingPathComponent("old.sqlite"), observesSuspension: false)
        var newHistory = try VariedHistory(new, readingSpacing: 60)
        var oldHistory = try VariedHistory(old, readingSpacing: 60)
        try newHistory.addDays(0..<30)
        try oldHistory.addDays(0..<30)
        let clock = ContinuousClock()
        let derived = Locked<ContinuousClock.Instant?>(nil)
        let hooks = RingStore.IngestionHooks(beforeApply: { _ in derived.update { $0 = clock.now } })
        func ms(_ d: Duration) -> String { String(format: "%.1f ms", Double(d.components.attoseconds) / 1e15 + Double(d.components.seconds) * 1000) }
        for label in ["first ingest (inserts)", "repeat ingest (unchanged)"] {
            for kind in RingStore.ingestibleKinds {
                var start = clock.now
                let result = try await new.ingest(ringID: newHistory.ringID, kind: kind, hooks: hooks)
                var end = clock.now
                print("MEASURE new \(label) \(kind.name) (\(result.inserted + result.unchanged) rows): total \(ms(end - start)), lock \(ms(end - derived.value!))")
                start = clock.now
                _ = try referenceIngest(old, ringID: oldHistory.ringID, kind: kind)
                end = clock.now
                print("MEASURE old \(label) \(kind.name): total = lock \(ms(end - start))")
            }
        }
        var start = clock.now
        _ = try await new.rebuild(ringID: newHistory.ringID, hooks: hooks)
        var end = clock.now
        print("MEASURE new rebuild: total \(ms(end - start)), lock \(ms(end - derived.value!))")
        start = clock.now
        _ = try referenceRebuild(old, ringID: oldHistory.ringID)
        end = clock.now
        print("MEASURE old rebuild: total = lock \(ms(end - start))")
    }
}
