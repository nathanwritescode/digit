import Foundation
import GRDB
import Testing
import RingProtocol
@testable import RingStore

// Pruning of superseded transfers. All data is synthetic. Helpers (`transfer`, `t0`,
// `onEachStore`) live in RingStoreTests.swift.

struct PruningTests {
    private func rows(_ store: RingStore, _ ring: UUID, _ kind: HistoryKind = .heartRate) throws -> [[UInt8]] {
        try store.transfers(ringID: ring, kind: kind).transfers.map(\.transfer.data)
    }

    @Test func growingBufferLeavesOneRowWithTheLongestData() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            var pruned: [Int] = []
            for n in 1...5 {
                let outcome = try store.insert(
                    transfer(data: Array(1...UInt8(n * 4)), at: t0.addingTimeInterval(Double(n))), ringID: ring.id)
                guard case .inserted(_, let p) = outcome else { Issue.record("expected insert"); return }
                pruned.append(p)
            }
            #expect(pruned == [0, 1, 1, 1, 1])
            #expect(try rows(store, ring.id) == [Array(1...20)])
        }
    }

    @Test func nonPrefixTransferIsKeptAlongside() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            try store.insert(transfer(data: [1, 2, 3]), ringID: ring.id)
            let outcome = try store.insert(transfer(data: [9, 2, 3, 4]), ringID: ring.id)
            guard case .inserted(_, 0) = outcome else { Issue.record("expected insert, none pruned"); return }
            #expect(try rows(store, ring.id).count == 2)
            // A longer one extending only the first prunes only the first.
            try store.insert(transfer(data: [1, 2, 3, 4, 5]), ringID: ring.id)
            #expect(Set(try rows(store, ring.id)) == [[9, 2, 3, 4], [1, 2, 3, 4, 5]])
        }
    }

    @Test func prefixInsertedAfterLongerIsNotStored() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            guard case .inserted(let id, _) = try store.insert(transfer(data: [1, 2, 3, 4]), ringID: ring.id)
            else { Issue.record("expected insert"); return }
            #expect(try store.insert(transfer(data: [1, 2]), ringID: ring.id) == .supersededByExisting(existingID: id))
            #expect(try store.insert(transfer(data: [1, 2, 3, 4]), ringID: ring.id) == .duplicate(existingID: id))
            #expect(try rows(store, ring.id) == [[1, 2, 3, 4]])
        }
    }

    @Test func prefixRelationsNeverCrossRingsOrKinds() throws {
        try onEachStore { store in
            let a = try store.registerRing(protocolFamily: "ycbt")
            let b = try store.registerRing(protocolFamily: "ycbt")
            try store.insert(transfer(.heartRate, data: [1, 2]), ringID: a.id)
            guard case .inserted(_, 0) = try store.insert(transfer(.heartRate, data: [1, 2, 3]), ringID: b.id),
                  case .inserted(_, 0) = try store.insert(transfer(.sleep, data: [1, 2, 3]), ringID: a.id),
                  case .inserted(_, 0) = try store.insert(transfer(.sleep, data: [1]), ringID: b.id)
            else { Issue.record("expected plain inserts"); return }
            #expect(try rows(store, a.id, .heartRate) == [[1, 2]])
            #expect(try rows(store, a.id, .sleep) == [[1, 2, 3]])
            #expect(try rows(store, b.id, .heartRate) == [[1, 2, 3]])
            #expect(try rows(store, b.id, .sleep) == [[1]])
        }
    }

    @Test func emptyTransfers() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            // Empty into nothing is stored once; again is a duplicate.
            guard case .inserted(let emptyID, 0) = try store.insert(transfer(data: []), ringID: ring.id)
            else { Issue.record("expected insert"); return }
            #expect(try store.insert(transfer(data: []), ringID: ring.id) == .duplicate(existingID: emptyID))
            // A non-empty one supersedes the empty one.
            guard case .inserted(let id, 1) = try store.insert(transfer(data: [0]), ringID: ring.id)
            else { Issue.record("expected insert pruning the empty row"); return }
            #expect(try rows(store, ring.id) == [[0]])
            // Empty now adds nothing.
            #expect(try store.insert(transfer(data: []), ringID: ring.id) == .supersededByExisting(existingID: id))
            // Another kind is unaffected.
            guard case .inserted(_, 0) = try store.insert(transfer(.sleep, data: []), ringID: ring.id)
            else { Issue.record("expected insert"); return }
        }
    }

    @Test func zeroBytesAreComparedAsBytes() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            try store.insert(transfer(data: [0, 0]), ringID: ring.id)
            try store.insert(transfer(data: [0]), ringID: ring.id)  // prefix of the stored one
            try store.insert(transfer(data: [0, 1, 0]), ringID: ring.id)  // [0,0] is not its prefix
            #expect(Set(try rows(store, ring.id)) == [[0, 0], [0, 1, 0]])
            try store.insert(transfer(data: [0, 0, 0, 7]), ringID: ring.id)
            #expect(Set(try rows(store, ring.id)) == [[0, 1, 0], [0, 0, 0, 7]])
            // A trailing zero is a real byte, not a terminator.
            try store.insert(transfer(data: [5, 0, 0]), ringID: ring.id)
            guard case .supersededByExisting = try store.insert(transfer(data: [5, 0]), ringID: ring.id)
            else { Issue.record("expected superseded"); return }
            #expect(Set(try rows(store, ring.id)) == [[0, 1, 0], [0, 0, 0, 7], [5, 0, 0]])
        }
    }

    @Test func insertAndPruneAreAtomic() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            try store.insert(transfer(data: [1, 2]), ringID: ring.id)
            // Make the prune's DELETE fail after the INSERT has run.
            try store.writer.write { db in
                try db.execute(sql: """
                    CREATE TRIGGER block_delete BEFORE DELETE ON history_transfer
                    BEGIN SELECT RAISE(ABORT, 'blocked'); END
                    """)
            }
            #expect(throws: DatabaseError.self) { try store.insert(transfer(data: [1, 2, 3]), ringID: ring.id) }
            #expect(try rows(store, ring.id) == [[1, 2]])
            // The incoming transfer's observation was written first; it must have rolled back too.
            #expect(try store.observations(ringID: ring.id).observations.map(\.dataLength) == [2])
            try store.writer.write { db in try db.execute(sql: "DROP TRIGGER block_delete") }
            guard case .inserted(_, 1) = try store.insert(transfer(data: [1, 2, 3]), ringID: ring.id)
            else { Issue.record("expected insert pruning one"); return }
            #expect(try rows(store, ring.id) == [[1, 2, 3]])
            #expect(try store.observations(ringID: ring.id).observations.map(\.dataLength) == [2, 3])
        }
    }

    @Test func growingBufferLogsEveryFetch() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            try store.insert(transfer(data: [1], header: [0xA], at: t0, zone: "Europe/London"), ringID: ring.id)
            try store.insert(
                transfer(data: [1, 2], header: [0xB], at: t0.addingTimeInterval(10), zone: "Asia/Tokyo"), ringID: ring.id)
            try store.insert(
                transfer(data: [1, 2, 3], header: [0xC], at: t0.addingTimeInterval(20), zone: "UTC"), ringID: ring.id)
            #expect(try rows(store, ring.id) == [[1, 2, 3]])
            let obs = try store.observations(ringID: ring.id).observations
            #expect(obs.map(\.dataLength) == [1, 2, 3])
            #expect(obs.map(\.header) == [[0xA], [0xB], [0xC]])
            #expect(obs.map(\.fetchedAt) == [t0, t0.addingTimeInterval(10), t0.addingTimeInterval(20)])
            #expect(obs.map(\.timeZoneIdentifier) == ["Europe/London", "Asia/Tokyo", "UTC"])
            #expect(obs.allSatisfy { $0.ringID == ring.id && $0.kind == .heartRate })
            // Each observation's hash is that of the first dataLength bytes of the surviving transfer.
            let survivor = [UInt8]([1, 2, 3])
            for o in obs {
                let prefix = Array(survivor.prefix(o.dataLength))
                #expect(o.contentHash == RingStore.contentHash(ringID: ring.id, kind: .heartRate, data: prefix))
            }
        }
    }

    @Test func duplicateAtAnotherTimeIsLoggedAndSameFetchTwiceOnce() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            try store.insert(transfer(data: [1, 2], at: t0), ringID: ring.id)
            try store.insert(transfer(data: [1, 2], at: t0), ringID: ring.id)  // exactly the same fetch
            #expect(try store.observations(ringID: ring.id).observations.count == 1)
            let outcome = try store.insert(transfer(data: [1, 2], header: [4], at: t0.addingTimeInterval(60)), ringID: ring.id)
            guard case .duplicate = outcome else { Issue.record("expected duplicate"); return }
            #expect(try store.observations(ringID: ring.id).observations.map(\.fetchedAt) == [t0, t0.addingTimeInterval(60)])
            #expect(try rows(store, ring.id) == [[1, 2]])
        }
    }

    @Test func identicalImportOutOfOrderLogsBothFetchTimes() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            let t2 = t0.addingTimeInterval(1000)
            guard case .inserted(let id, _) = try store.insert(transfer(data: [1, 2], at: t2), ringID: ring.id)
            else { Issue.record("expected insert"); return }
            #expect(try store.insert(transfer(data: [1, 2], at: t0), ringID: ring.id) == .duplicate(existingID: id))
            #expect(try store.observations(ringID: ring.id).observations.map(\.fetchedAt) == [t0, t2])
        }
    }

    @Test func observationsComeBackInFetchTimeOrderNotInsertionOrder() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            try store.insert(transfer(data: [1], at: t0.addingTimeInterval(300)), ringID: ring.id)
            try store.insert(transfer(data: [2], at: t0.addingTimeInterval(100)), ringID: ring.id)
            try store.insert(transfer(data: [3], at: t0.addingTimeInterval(200)), ringID: ring.id)
            #expect(try store.observations(ringID: ring.id).observations.map(\.dataLength) == [1, 1, 1])
            #expect(
                try store.observations(ringID: ring.id).observations.map(\.fetchedAt)
                    == [100, 200, 300].map { t0.addingTimeInterval($0) })
            #expect(try store.observations(ringID: ring.id).observations.first?.contentHash
                == RingStore.contentHash(ringID: ring.id, kind: .heartRate, data: [2]))
        }
    }

    @Test func supersededByExistingIsLoggedAndStoresNothing() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            try store.insert(transfer(data: [1, 2, 3, 4]), ringID: ring.id)
            let later = t0.addingTimeInterval(99)
            try store.insert(transfer(data: [1, 2], header: [5], at: later, zone: "Asia/Tokyo"), ringID: ring.id)
            #expect(try rows(store, ring.id) == [[1, 2, 3, 4]])
            let obs = try store.observations(ringID: ring.id).observations
            #expect(obs.count == 2)
            #expect(obs.last?.dataLength == 2)
            #expect(obs.last?.header == [5])
            #expect(obs.last?.fetchedAt == later)
            #expect(obs.last?.timeZoneIdentifier == "Asia/Tokyo")
        }
    }

    @Test func emptyAfterNonEmptyLogsAZeroLengthObservation() throws {
        try onEachStore { store in
            let ring = try store.registerRing(protocolFamily: "ycbt")
            try store.insert(transfer(data: [1]), ringID: ring.id)
            try store.insert(transfer(data: [], at: t0.addingTimeInterval(5)), ringID: ring.id)
            #expect(try store.observations(ringID: ring.id).observations.map(\.dataLength) == [1, 0])
            #expect(try rows(store, ring.id) == [[1]])
        }
    }

    @Test func observationsDoNotCrossRingsOrKinds() throws {
        try onEachStore { store in
            let a = try store.registerRing(protocolFamily: "ycbt")
            let b = try store.registerRing(protocolFamily: "ycbt")
            try store.insert(transfer(.heartRate, data: [1]), ringID: a.id)
            try store.insert(transfer(.heartRate, data: [1, 2]), ringID: a.id)
            try store.insert(transfer(.sleep, data: [1]), ringID: a.id)
            try store.insert(transfer(.heartRate, data: [1]), ringID: b.id)
            #expect(try store.observations(ringID: a.id).observations.count == 3)
            #expect(try store.observations(ringID: a.id, kind: .heartRate).observations.count == 2)
            #expect(try store.observations(ringID: a.id, kind: .sleep).observations.count == 1)
            #expect(try store.observations(ringID: b.id).observations.count == 1)
        }
    }

    /// A database created by v1 (a prefix chain of three rows) is upgraded by opening it with
    /// `RingStore`; the migration's backfill puts the three rows in the log, then a longer transfer
    /// prunes all three at once.
    @Test func v1DatabaseUpgradesBackfillsTheLogAndPrunesSeveralRows() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let url = dir.appendingPathComponent("ring.sqlite")
        let ringID = UUID()
        let chain: [[UInt8]] = [[1], [1, 2], [1, 2, 3]]
        do {
            let pool = try DatabasePool(path: url.path)
            try RingStore.migrator.migrate(pool, upTo: "v1")
            try pool.write { db in
                try db.execute(
                    sql: "INSERT INTO ring (id, protocol_family, first_seen_at) VALUES (?, 'ycbt', 0)",
                    arguments: [ringID.uuidString])
                for (i, data) in chain.enumerated() {
                    let hash = RingStore.contentHash(ringID: ringID, kind: .heartRate, data: data)
                    try db.execute(
                        sql: RingStore.insertTransferSQL,
                        arguments: [
                            ringID.uuidString, "heartRate", Data([UInt8(i)]), Data(data),
                            1_000 * (i + 1), "UTC", Data(hash),
                        ])
                }
            }
        }
        let store = try RingStore(url: url)
        let backfilled = try store.observations(ringID: ringID).observations
        #expect(backfilled.map(\.dataLength) == [1, 2, 3])
        #expect(backfilled.map(\.fetchedAt) == [1, 2, 3].map { Date(timeIntervalSince1970: Double($0)) })
        #expect(backfilled.map(\.contentHash)
            == chain.map { RingStore.contentHash(ringID: ringID, kind: .heartRate, data: $0) })
        guard case .inserted(_, let pruned) = try store.insert(
            transfer(data: [1, 2, 3, 4], at: t0), ringID: ringID)
        else { Issue.record("expected insert"); return }
        #expect(pruned == 3)
        #expect(try rows(store, ringID) == [[1, 2, 3, 4]])
        #expect(try store.observations(ringID: ringID).observations.count == 4)
    }

    @Test func concurrentGrowingInsertsEndWithTheLongest() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let store = try RingStore(url: dir.appendingPathComponent("ring.sqlite"))
        let ringID = try sync(store).registerRing(protocolFamily: "ycbt").id
        try await withThrowingTaskGroup(of: Void.self) { group in
            for n in 1...40 {
                group.addTask {
                    try store.insert(transfer(data: Array(repeating: 7, count: n)), ringID: ringID)
                }
            }
            try await group.waitForAll()
        }
        let data = try sync(store).transfers(ringID: ringID).transfers.map(\.transfer.data)
        #expect(data == [Array(repeating: 7, count: 40)])
    }

    @Test func everyInsertedSequenceIsAPrefixOfSomeStoredTransfer() throws {
        func isPrefix(_ a: [UInt8], of b: [UInt8]) -> Bool { a.count <= b.count && Array(b.prefix(a.count)) == a }
        let seed: UInt64 = 0x5EED_0030
        try onEachStore { store in
            var rng = SplitMix64(seed: seed)  // same sequence for each store kind
            let rings = [try store.registerRing(protocolFamily: "ycbt").id, try store.registerRing(protocolFamily: "ycbt").id]
            let kinds: [HistoryKind] = [.heartRate, .sleep]
            var inserted: [String: [[UInt8]]] = [:]
            for _ in 0..<300 {
                let ring = rings.randomElement(using: &rng)!
                let kind = kinds.randomElement(using: &rng)!
                // Tiny alphabet and lengths so prefixes, duplicates and zeros are common.
                let data = (0..<Int.random(in: 0...6, using: &rng)).map { _ in UInt8.random(in: 0...1, using: &rng) }
                try store.insert(transfer(kind, data: data), ringID: ring)
                inserted["\(ring)\(kind)", default: []].append(data)
            }
            for ring in rings {
                for kind in kinds {
                    let stored = try rows(store, ring, kind)
                    for data in inserted["\(ring)\(kind)"] ?? [] {
                        #expect(stored.contains { isPrefix(data, of: $0) }, "seed \(seed)")
                    }
                    // No stored row is redundant: none is a prefix of another.
                    for (i, a) in stored.enumerated() {
                        for (j, b) in stored.enumerated() where i != j { #expect(!isPrefix(a, of: b), "seed \(seed)") }
                    }
                }
            }
        }
    }
}
