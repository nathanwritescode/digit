import CryptoKit
import Foundation
import Testing
import RingProtocol
@testable import RingStore

// Results must not change when derivation moves out of the write transaction (#69). The data is the
// SYNTHETIC `VariedHistory`.
//
// The real equivalence check is `recordedDigest`, taken from the implementation before the change
// (ingest and rebuild in one write transaction, commit 132126e): `VariedHistory.swift` and
// `variedTranscript` below were copied unchanged into a scratch worktree of 132126e, run there
// through its blocking `ingest` and `rebuild`, and the printed digest pasted here. It covers rows,
// ids and every `IngestionResult` field, including deletes and `assumed` rows (the "retro" phase).
//
// The test-only one-transaction reference is built from the new pieces (`loadInputs`, `plan`,
// `apply`), so comparing it line by line with the new path only shows that moving the transaction
// boundary changes nothing; it does not compare against the old code.

/// SHA-256 of `variedTranscript` as the pre-#69 implementation produced it (6779 lines; the same on
/// an in-memory and a file-backed store).
private let recordedDigest = "2769ccfdcafd7b762bfc7d5bcc26743cb926a2df36cd7068ecaced5385e46cbb"

/// Two phases of history with ingests after each (so row ids must survive new data arriving), a
/// repeat ingest that must change nothing, an ingest after a retroactive clock-set (rows deleted,
/// re-keyed and `assumed`), then a rebuild: every result and every row, with its id.
func variedTranscript(
    _ store: RingStore,
    ingest: (UUID, HistoryKind) async throws -> IngestionResult,
    rebuild: (UUID) async throws -> [HistoryKind: IngestionResult]
) async throws -> String {
    var history = try VariedHistory(store)
    var lines: [String] = []
    try history.addDays(0..<4)
    for label in ["first", "second", "again", "retro"] {
        if label == "second" { try history.addDays(4..<6) }
        if label == "retro" { try history.addRetroactiveClockSet() }
        for kind in RingStore.ingestibleKinds {
            lines.append("\(label) \(kind.name) \(try await ingest(history.ringID, kind).canonical)")
        }
        lines.append(try TypedSnapshot(store, ringID: history.ringID).canonical)
    }
    let rebuilt = try await rebuild(history.ringID)
    for kind in RingStore.ingestibleKinds { lines.append("rebuild \(kind.name) \(rebuilt[kind]!.canonical)") }
    lines.append(try TypedSnapshot(store, ringID: history.ringID).canonical)
    return lines.joined(separator: "\n")
}

func digest(_ text: String) -> String {
    SHA256.hash(data: Data(text.utf8)).map { String(format: "%02x", $0) }.joined()
}

/// The pre-#69 shape, for tests only: inputs read, derived and applied inside one write
/// transaction, built from the same pieces as the new path (so not the old code itself).
func referenceIngest(_ store: RingStore, ringID: UUID, kind: HistoryKind) throws -> IngestionResult {
    try store.writeBlocking { db in
        let inputs = try RingStore.loadInputs(db, ringID: ringID, kinds: [kind])
        return try RingStore.apply(db, ringID: ringID, RingStore.plan(inputs[0], ringID: ringID) {})
    }
}

/// `rebuild` in the pre-#69 shape: per kind, delete then ingest, all in one write.
func referenceRebuild(_ store: RingStore, ringID: UUID) throws -> [HistoryKind: IngestionResult] {
    try store.writeBlocking { db in
        var results: [HistoryKind: IngestionResult] = [:]
        for input in try RingStore.loadInputs(db, ringID: ringID, kinds: RingStore.ingestibleKinds) {
            try RingStore.deleteTypedRows(db, ringID: ringID, kind: input.kind)
            results[input.kind] = try RingStore.apply(db, ringID: ringID, RingStore.plan(input, ringID: ringID) {})
        }
        return results
    }
}

@Suite(.timeLimit(.minutes(1))) struct DerivationEquivalenceTests {
    @Test func blockingFormsMatchTheRecordedOldOutput() async throws {
        try await onEachStoreAsync { store in
            let view = blocking(store)
            let transcript = try await variedTranscript(
                store, ingest: { try view.ingest(ringID: $0, kind: $1) }, rebuild: { try view.rebuild(ringID: $0) })
            #expect(digest(transcript) == recordedDigest)
        }
    }

    @Test func asyncFormsMatchTheRecordedOldOutput() async throws {
        try await onEachStoreAsync { store in
            let transcript = try await variedTranscript(
                store, ingest: { try await store.ingest(ringID: $0, kind: $1) },
                rebuild: { try await store.rebuild(ringID: $0) })
            #expect(digest(transcript) == recordedDigest)
        }
    }

    @Test func newPathMatchesTheOneTransactionReferenceLineByLine() async throws {
        // Two fresh in-memory stores, so both start from an empty database and get the same ids.
        let oldStore = try RingStore()
        let reference = try await variedTranscript(
            oldStore, ingest: { try referenceIngest(oldStore, ringID: $0, kind: $1) },
            rebuild: { try referenceRebuild(oldStore, ringID: $0) })
        #expect(digest(reference) == recordedDigest)
        let newStore = try RingStore()
        let new = try await variedTranscript(
            newStore, ingest: { try await newStore.ingest(ringID: $0, kind: $1) },
            rebuild: { try await newStore.rebuild(ringID: $0) })
        let (newLines, oldLines) = (new.split(separator: "\n"), reference.split(separator: "\n"))
        #expect(newLines.count == oldLines.count)
        if let index = zip(newLines, oldLines).enumerated().first(where: { $0.element.0 != $0.element.1 })?.offset {
            Issue.record("first difference at line \(index): new \(newLines[index]) | reference \(oldLines[index])")
        }
    }
}
