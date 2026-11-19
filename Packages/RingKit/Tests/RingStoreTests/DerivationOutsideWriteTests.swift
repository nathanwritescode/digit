import Foundation
import GRDB
import os
import Testing
import RingProtocol
@testable import RingStore

// `ingest` and `rebuild` derive outside the write transaction and apply in a short one (#69). All
// data here is SYNTHETIC (`VariedHistory` and hand-built records). The suspension case lives in
// `SuspensionTests`, the one suite allowed to suspend.

private func at(_ seconds: Double) -> Date { t0.addingTimeInterval(seconds) }

/// A thread-safe value for hooks to write and tests to read.
final class Locked<Value: Sendable>: Sendable {
    private let stored: OSAllocatedUnfairLock<Value>
    init(_ value: Value) { stored = OSAllocatedUnfairLock(initialState: value) }
    var value: Value { stored.withLock { $0 } }
    func update(_ change: @Sendable (inout Value) -> Void) { stored.withLock { change(&$0) } }
}

extension BlockingView {
    @discardableResult
    func ingest(ringID: UUID, kind: HistoryKind, hooks: RingStore.IngestionHooks) throws -> IngestionResult {
        try store.ingest(ringID: ringID, kind: kind, hooks: hooks)
    }
    @discardableResult
    func rebuild(ringID: UUID, hooks: RingStore.IngestionHooks) throws -> [HistoryKind: IngestionResult] {
        try store.rebuild(ringID: ringID, hooks: hooks)
    }
}

/// A file-backed store (not observing suspension) in its own directory, with its URL.
private func withFileStore(_ body: (RingStore, URL) async throws -> Void) async throws {
    let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: dir) }
    let url = dir.appendingPathComponent("ring.sqlite")
    try await body(try RingStore(url: url, observesSuspension: false), url)
}

/// Whether a separate connection with no busy timeout can take the write lock at once.
private func otherConnectionCanTakeWriteLock(_ url: URL) throws -> Bool {
    let queue = try DatabaseQueue(path: url.path)
    do {
        try queue.inDatabase { db in
            try db.execute(sql: "BEGIN IMMEDIATE")
            try db.execute(sql: "ROLLBACK")
        }
        return true
    } catch let error as DatabaseError where error.resultCode == .SQLITE_BUSY {
        return false
    }
}

/// A history of two days, ingested, then a third day stored but not ingested, so a call that
/// completes would change rows. Returns the rows as they are.
private func ingestedThenGrown(_ store: RingStore) throws -> (VariedHistory, TypedSnapshot) {
    var history = try VariedHistory(store)
    try history.addDays(0..<2)
    for kind in RingStore.ingestibleKinds { try blocking(store).ingest(ringID: history.ringID, kind: kind) }
    try history.addDays(2..<3)
    return (history, try TypedSnapshot(store, ringID: history.ringID))
}

private func expectNoChange(_ result: IngestionResult, rows: Int) {
    #expect(result.inserted == 0 && result.updated == 0 && result.deleted == 0, "\(result.canonical)")
    #expect(result.unchanged == rows)
}

@Suite(.timeLimit(.minutes(1))) struct DerivationOutsideWriteTests {
    // MARK: The write lock

    /// Fails if derivation runs inside the write: another connection could not take the lock.
    @Test(arguments: [false, true]) func anotherConnectionCanWriteWhileDeriving(blockingForms: Bool) async throws {
        try await withFileStore { store, url in
            var history = try VariedHistory(store)
            try history.addDays(0..<2)
            let checked = Locked<[String]>([])
            let hooks = RingStore.IngestionHooks(checkpoint: { kind in
                let free = try otherConnectionCanTakeWriteLock(url)
                checked.update { $0.append("\(kind.name) \(free)") }
            })
            for kind in RingStore.ingestibleKinds {
                if blockingForms {
                    try blocking(store).ingest(ringID: history.ringID, kind: kind, hooks: hooks)
                } else {
                    try await store.ingest(ringID: history.ringID, kind: kind, hooks: hooks)
                }
            }
            if blockingForms {
                try blocking(store).rebuild(ringID: history.ringID, hooks: hooks)
            } else {
                try await store.rebuild(ringID: history.ringID, hooks: hooks)
            }
            let results = checked.value
            #expect(results.allSatisfy { $0.hasSuffix(" true") }, "\(results.filter { $0.hasSuffix(" false") })")
            // Each kind reached checkpoints, in ingest and again in rebuild.
            for kind in RingStore.ingestibleKinds {
                #expect(results.filter { $0.hasPrefix(kind.name + " ") }.count >= 4, "\(kind.name)")
            }
        }
    }

    // MARK: Writes landing between the read and the apply step

    @Test func aTransferStoredMeanwhileIsIngestedByARederivation() async throws {
        try await onEachStoreAsync { store in
            var history = try VariedHistory(store)
            try history.addDays(0..<2)
            let ringID = history.ringID
            try await store.ingest(ringID: ringID, kind: .heartRate)
            // The ring's buffer one reading longer, fetched on day 2 under day 2's clock.
            let latest = try await store.transfers(ringID: ringID, kind: .heartRate).transfers.last!.transfer.data
            let readingAt = VariedHistory.dayStart(2).addingTimeInterval(600)
            let grown = latest + hrBytes(wall(readingAt, offset: variedOffset(day: 2)), bpm: 77)
            let attempts = Locked<[Int]>([])
            let hooks = RingStore.IngestionHooks(beforeApply: { attempt in
                attempts.update { $0.append(attempt) }
                if attempt == 1 {
                    try blocking(store).insert(
                        transfer(.heartRate, data: grown, at: VariedHistory.dayStart(2).addingTimeInterval(3600)),
                        ringID: ringID)
                }
            })
            let raced = try await store.ingest(ringID: ringID, kind: .heartRate, hooks: hooks)
            #expect(attempts.value == [1, 2])
            #expect(raced.inserted == 1 && raced.updated == 0 && raced.deleted == 0, "\(raced.canonical)")
            let rows = try TypedSnapshot(store, ringID: ringID)
            #expect(rows.heartRate.contains { $0.value.time == readingAt && $0.value.bpm == 77 })
            // A single up-to-date ingest agrees with what the raced one left.
            let again = try await store.ingest(ringID: ringID, kind: .heartRate)
            expectNoChange(again, rows: rows.heartRate.count)
            #expect(try TypedSnapshot(store, ringID: ringID) == rows)
            var counts = again
            (counts.inserted, counts.unchanged) = (raced.inserted, raced.unchanged)
            #expect(counts == raced, "the derivation counts differ from an up-to-date ingest")
        }
    }

    @Test func aClockSetLoggedMeanwhileIsUsedByARederivation() async throws {
        try await onEachStoreAsync { store in
            let f = try clockedFixture(store)
            try f.fetch(.heartRate, hrBytes(wall(at(1200), offset: plus1), bpm: 70), at: at(3600))
            try await store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(try await store.heartRateReadings(ringID: f.ring.id).map(\.value.timeSource) == [.resolved])
            let attempts = Locked<[Int]>([])
            let hooks = RingStore.IngestionHooks(beforeApply: { attempt in
                attempts.update { $0.append(attempt) }
                // A clock-set to +2 h before the reading was first seen: no candidate offset fits it
                // any more, so the row changes.
                if attempt == 1 { try f.setClock(at: at(500), offset: 7200) }
            })
            let raced = try await store.ingest(ringID: f.ring.id, kind: .heartRate, hooks: hooks)
            #expect(attempts.value == [1, 2])
            let rows = try TypedSnapshot(store, ringID: f.ring.id)
            #expect(rows.heartRate.map(\.value.timeSource) == [.assumed])
            #expect(raced.rowsByTimeSource[.assumed] == 1)
            expectNoChange(try await store.ingest(ringID: f.ring.id, kind: .heartRate), rows: 1)
            #expect(try TypedSnapshot(store, ringID: f.ring.id) == rows)
        }
    }

    /// A fetch that stores no transfer (the same bytes, an older copy imported late) only adds to
    /// the fetch log, and that alone can change a resolution: it must count as a change too.
    @Test func aFetchLoggedMeanwhileWithoutANewTransferIsUsedByARederivation() async throws {
        try await onEachStoreAsync { store in
            let f = try clockedFixture(store)
            // Stamped at ring wall time 6000 s after t0: under +1 h (clock at t0) or under +0 h (clock
            // at t0 + 5000 s) it fits either clock, so with only the fetch at 8000 s it is ambiguous.
            try f.setClock(at: at(5000), offset: 0)
            let data = hrBytes(wall(at(6000), offset: 0), bpm: 70)
            try f.fetch(.heartRate, data, at: at(8000))
            try await store.ingest(ringID: f.ring.id, kind: .heartRate)
            #expect(try await store.heartRateReadings(ringID: f.ring.id).map(\.value.timeSource) == [.ambiguous])
            let attempts = Locked<[Int]>([])
            let outcome = Locked<InsertOutcome?>(nil)
            let hooks = RingStore.IngestionHooks(beforeApply: { attempt in
                attempts.update { $0.append(attempt) }
                // The same bytes seen at 4000 s, before the clock went to +0 h: a duplicate, so only
                // a fetch-log entry is written.
                if attempt == 1 {
                    let inserted = try f.fetch(.heartRate, data, at: at(4000))
                    outcome.update { $0 = inserted }
                }
            })
            try await store.ingest(ringID: f.ring.id, kind: .heartRate, hooks: hooks)
            guard case .duplicate = outcome.value else { Issue.record("not a duplicate: \(String(describing: outcome.value))"); return }
            #expect(attempts.value == [1, 2])
            let rows = try TypedSnapshot(store, ringID: f.ring.id)
            #expect(rows.heartRate.map(\.value.timeSource) == [.resolved])
            #expect(rows.heartRate.map(\.value.time) == [at(2400)])
            expectNoChange(try await store.ingest(ringID: f.ring.id, kind: .heartRate), rows: 1)
        }
    }

    /// Through the API every change to the transfers also logs a fetch, so the transfer ids in the
    /// fingerprint are a second line of defence; this removes a transfer behind the API's back to
    /// show they are checked on their own.
    @Test func aTransferRemovedMeanwhileIsNoticedOnItsOwn() async throws {
        try await onEachStoreAsync { store in
            var history = try VariedHistory(store)
            try history.addDays(0..<5)
            let ringID = history.ringID
            let transfers = try await store.transfers(ringID: ringID, kind: .heartRate).transfers
            #expect(transfers.count >= 2)
            let attempts = Locked<[Int]>([])
            let hooks = RingStore.IngestionHooks(beforeApply: { attempt in
                attempts.update { $0.append(attempt) }
                if attempt == 1 {
                    try store.writer.writeWithoutTransaction { db in
                        try db.execute(sql: "DELETE FROM history_transfer WHERE id = ?", arguments: [transfers[0].id])
                    }
                }
            })
            try await store.ingest(ringID: ringID, kind: .heartRate, hooks: hooks)
            #expect(attempts.value == [1, 2])
            expectNoChange(
                try await store.ingest(ringID: ringID, kind: .heartRate),
                rows: try TypedSnapshot(store, ringID: ringID).heartRate.count)
        }
    }

    /// Another ingest of the same kind writes typed rows, not inputs: not stale, and the apply step
    /// finds its rows already right and keeps their ids.
    @Test func aCompetingIngestMeanwhileLeavesTheSameRowsAndIDs() async throws {
        try await onEachStoreAsync { store in
            let (history, _) = try ingestedThenGrown(store)
            let ringID = history.ringID
            let competing = Locked<(IngestionResult, TypedSnapshot)?>(nil)
            let attempts = Locked<[Int]>([])
            let hooks = RingStore.IngestionHooks(beforeApply: { attempt in
                attempts.update { $0.append(attempt) }
                let result = try blocking(store).ingest(ringID: ringID, kind: .heartRate)
                let rows = try TypedSnapshot(store, ringID: ringID)
                competing.update { $0 = (result, rows) }
            })
            let raced = try await store.ingest(ringID: ringID, kind: .heartRate, hooks: hooks)
            #expect(attempts.value == [1])
            guard let (theirs, theirRows) = competing.value else { Issue.record("the hook did not run"); return }
            #expect(theirs.inserted > 0)
            expectNoChange(raced, rows: theirRows.heartRate.count)
            #expect(try TypedSnapshot(store, ringID: ringID) == theirRows)
            expectNoChange(try await store.ingest(ringID: ringID, kind: .heartRate), rows: theirRows.heartRate.count)
        }
    }

    /// In a rebuild only the stale kind is derived again; the result is still a full rebuild.
    @Test func aRebuildRederivesTheStaleKindOnly() async throws {
        try await onEachStoreAsync { store in
            let (history, _) = try ingestedThenGrown(store)
            let ringID = history.ringID
            let latest = try await store.transfers(ringID: ringID, kind: .activity).transfers.last!.transfer.data
            let start = VariedHistory.dayStart(3)
            let grown = latest + activityBytes(
                start: wall(start, offset: variedOffset(day: 3)),
                end: wall(start.addingTimeInterval(1800), offset: variedOffset(day: 3)), steps: 1234)
            let derived = Locked<[String]>([])
            let hooks = RingStore.IngestionHooks(
                checkpoint: { kind in derived.update { if $0.last != kind.name { $0.append(kind.name) } } },
                beforeApply: { attempt in
                    derived.update { $0.append("apply \(attempt)") }
                    if attempt == 1 {
                        try blocking(store).insert(transfer(.activity, data: grown, at: start.addingTimeInterval(3600)), ringID: ringID)
                    }
                })
            let results = try await store.rebuild(ringID: ringID, hooks: hooks)
            #expect(derived.value == ["activity", "sleep", "heart", "combined", "apply 1", "activity", "apply 2"])
            #expect(try TypedSnapshot(store, ringID: ringID).activity.contains { $0.value.steps == 1234 })
            for kind in RingStore.ingestibleKinds {
                let again = try await store.ingest(ringID: ringID, kind: kind)
                #expect(again.inserted == 0 && again.updated == 0 && again.deleted == 0, "\(kind.name)")
                #expect(results[kind]!.inserted == again.unchanged, "\(kind.name)")
            }
        }
    }

    // MARK: The bound

    @Test(arguments: [false, true]) func inputsThatKeepChangingEndInTheDocumentedError(rebuild: Bool) async throws {
        try await onEachStoreAsync { store in
            let (history, before) = try ingestedThenGrown(store)
            let ringID = history.ringID
            let attempts = Locked<[Int]>([])
            let hooks = RingStore.IngestionHooks(beforeApply: { attempt in
                attempts.update { $0.append(attempt) }
                try history.fixture.setClock(at: at(Double(attempt)), offset: plus1)
            })
            await #expect(throws: RingStoreError.ingestionInputsKeptChanging) {
                if rebuild {
                    try await store.rebuild(ringID: ringID, hooks: hooks)
                } else {
                    try await store.ingest(ringID: ringID, kind: .heartRate, hooks: hooks)
                }
            }
            #expect(attempts.value == Array(1...RingStore.maximumDerivationAttempts))
            #expect(try TypedSnapshot(store, ringID: ringID) == before)
        }
    }

    // MARK: Cancellation and failure

    @Test(arguments: [false, true]) func cancellingDuringDerivationNeverReachesTheWrite(rebuild: Bool) async throws {
        try await onEachStoreAsync { store in
            let (history, before) = try ingestedThenGrown(store)
            let ringID = history.ringID
            let reachedApply = Locked(false)
            let checkpointsAfterCancel = Locked(0)
            // The last kind a rebuild derives, so every earlier kind's derivation is thrown away.
            let cancelAt: HistoryKind = rebuild ? .combined : .heartRate
            let cancelled = Locked(false)
            let hooks = RingStore.IngestionHooks(
                checkpoint: { kind in
                    if cancelled.value { checkpointsAfterCancel.update { $0 += 1 } }
                    if kind == cancelAt, !cancelled.value {
                        cancelled.update { $0 = true }
                        withUnsafeCurrentTask { $0?.cancel() }
                    }
                },
                beforeApply: { _ in reachedApply.update { $0 = true } })
            let task = Task {
                if rebuild {
                    _ = try await store.rebuild(ringID: ringID, hooks: hooks)
                } else {
                    _ = try await store.ingest(ringID: ringID, kind: .heartRate, hooks: hooks)
                }
            }
            let outcome = await task.result
            if case .failure(let error) = outcome { #expect(error is CancellationError, "got \(error)") }
            else { Issue.record("a cancelled call succeeded") }
            #expect(cancelled.value && !reachedApply.value)
            #expect(checkpointsAfterCancel.value == 0, "derivation went on after the cancellation")
            #expect(try TypedSnapshot(store, ringID: ringID) == before)
        }
    }

    @Test(arguments: [false, true]) func cancellingDuringTheApplyStepRollsItBack(rebuild: Bool) async throws {
        try await onEachStoreAsync { store in
            let (history, before) = try ingestedThenGrown(store)
            let ringID = history.ringID
            let hooks = RingStore.IngestionHooks(duringApply: { withUnsafeCurrentTask { $0?.cancel() } })
            let task = Task {
                if rebuild {
                    _ = try await store.rebuild(ringID: ringID, hooks: hooks)
                } else {
                    _ = try await store.ingest(ringID: ringID, kind: .heartRate, hooks: hooks)
                }
            }
            let outcome = await task.result
            if case .failure(let error) = outcome { #expect(error is CancellationError, "got \(error)") }
            else { Issue.record("a cancelled call succeeded") }
            #expect(try TypedSnapshot(store, ringID: ringID) == before)
        }
    }

    /// A rebuild that fails after it has deleted and re-inserted some kinds leaves every table as
    /// it was. The failure is a trigger that refuses sleep rows, applied after activity.
    @Test func aRebuildThatFailsPartWayLeavesEveryTableAsItWas() async throws {
        try await onEachStoreAsync { store in
            let (history, before) = try ingestedThenGrown(store)
            let ringID = history.ringID
            #expect(!before.activity.isEmpty && !before.sleep.isEmpty && !before.heartRate.isEmpty)
            try await store.writer.write { db in
                try db.execute(sql: """
                    CREATE TRIGGER refuse_sleep BEFORE INSERT ON sleep_session
                    BEGIN SELECT RAISE(ABORT, 'refused by test'); END
                    """)
            }
            let reachedApply = Locked(false)
            let hooks = RingStore.IngestionHooks(duringApply: { reachedApply.update { $0 = true } })
            await #expect(throws: DatabaseError.self) { try await store.rebuild(ringID: ringID, hooks: hooks) }
            #expect(reachedApply.value)
            #expect(try TypedSnapshot(store, ringID: ringID) == before)
        }
    }

    @Test func anUnknownRingIsReportedBeforeAnyDerivation() async throws {
        try await onEachStoreAsync { store in
            let missing = UUID()
            let derived = Locked(false)
            let hooks = RingStore.IngestionHooks(checkpoint: { _ in derived.update { $0 = true } })
            await #expect(throws: RingStoreError.unknownRing(missing)) {
                try await store.ingest(ringID: missing, kind: .sleep, hooks: hooks)
            }
            await #expect(throws: RingStoreError.unknownRing(missing)) {
                try await store.rebuild(ringID: missing, hooks: hooks)
            }
            #expect(!derived.value)
        }
    }
}
