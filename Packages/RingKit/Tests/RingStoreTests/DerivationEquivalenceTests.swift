import CryptoKit
import Foundation
import Testing
import RingProtocol
@testable import RingStore

// Results must not change when derivation moves out of the write transaction (#69). The data is the
// SYNTHETIC `VariedHistory`. The digest below was recorded from the implementation before the change
// (ingest and rebuild in one write transaction, at commit 132126e), so the new path is checked
// against the old one's actual output, row ids included. A test-only reference that does the old
// thing (read, derive and apply inside one write) is compared line by line, so a mismatch shows
// where it is.

/// SHA-256 of `variedTranscript` as the pre-#69 implementation produced it (5328 lines; the same on
/// an in-memory and a file-backed store).
private let recordedDigest = "874b216cf6428429ac1715457e7138681702f1b90f96011e4f752ba4778dbec3"

/// Two phases of history with ingests after each (so row ids must survive new data arriving), a
/// repeat ingest that must change nothing, then a rebuild: every result and every row, with its id.
func variedTranscript(
    _ store: RingStore,
    ingest: (UUID, HistoryKind) async throws -> IngestionResult,
    rebuild: (UUID) async throws -> [HistoryKind: IngestionResult]
) async throws -> String {
    var history = try VariedHistory(store)
    var lines: [String] = []
    try history.addDays(0..<4)
    for label in ["first", "second", "again"] {
        if label == "second" { try history.addDays(4..<6) }
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

/// The pre-#69 shape, kept for tests only: inputs read, derived and applied inside one write
/// transaction. Built from the same pieces as the real path.
func referenceIngest(_ store: RingStore, ringID: UUID, kind: HistoryKind) throws -> IngestionResult {
    try store.writeBlocking { db in
        let inputs = try RingStore.loadInputs(db, ringID: ringID, kinds: [kind])
        return try RingStore.apply(db, ringID: ringID, RingStore.plan(inputs[kind]!, ringID: ringID, kind: kind) {})
    }
}

/// `rebuild` in the pre-#69 shape: per kind, delete then ingest, all in one write.
func referenceRebuild(_ store: RingStore, ringID: UUID) throws -> [HistoryKind: IngestionResult] {
    try store.writeBlocking { db in
        let inputs = try RingStore.loadInputs(db, ringID: ringID, kinds: RingStore.ingestibleKinds)
        var results: [HistoryKind: IngestionResult] = [:]
        for kind in RingStore.ingestibleKinds {
            try RingStore.deleteTypedRows(db, ringID: ringID, kind: kind)
            results[kind] = try RingStore.apply(
                db, ringID: ringID, RingStore.plan(inputs[kind]!, ringID: ringID, kind: kind) {})
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
