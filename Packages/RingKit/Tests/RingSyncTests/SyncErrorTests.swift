import CoreBluetooth
import Foundation
import Testing
import RingProtocol
import RingStore
import RingSync
import RingTransport

// The result must not carry health data even when dumped through a Mirror (a debugger, `dump`).
// The values below are SYNTHETIC and chosen to be recognisable in a dump.

private func dumped(_ value: some Any) -> String {
    var out = ""
    dump(value, to: &out)
    return out
}

/// A dump with the store's ring id removed, so a digit run inside a random UUID cannot match.
private func dumped(_ result: SyncResult, without ringID: UUID) -> String {
    dumped(result).replacingOccurrences(of: ringID.uuidString, with: "<ring>")
}

/// Text that only a raw error (statement arguments, frames, byte buffers) would put in a dump.
private let rawErrorMarkers = ["DatabaseError", "StatementArguments", "arguments", "RingProtocol.Frame", "payload", "bytes"]

/// Failing the `history_transfer` insert puts the transfer's data blob among the failing
/// statement's arguments; failing the `transfer_observation` insert (which runs first) puts its
/// header, length and fetch time there. Neither may reach the result.
@Test(.timeLimit(.minutes(1)), arguments: ["transfer_observation", "history_transfer"])
func resultOfAStoreFailureInsidePersistHoldsNoRawData(_ table: String) async throws {
    let (store, id) = try await makeStore()
    try failInserts(into: table, when: "NEW.kind = 'heartRate'", store)
    // Readings of 173 and 187 bpm (0xAD, 0xBB) at ring times ending in recognisable bytes.
    let data = heartBytes(ring(-3600) | 0xC3, bpm: 173) + heartBytes(ring(-3000), bpm: 187)
    var fetches = syntheticHistory.mapValues { FakeRing.Fetch.data($0) }
    fetches[.heartRate] = .data(data)
    let result = await RingSyncer(
        ring: FakeRing(store: store, ringID: id, fetches: fetches), store: store, ringID: id,
        monitoringInterval: nil, budget: .seconds(60), clock: { t0 }, timeZone: { london }
    ).run()

    guard case .failed(.store, .database) = result.fetches[.heartRate] else {
        Issue.record("heart rate not reported as a database failure")
        return
    }
    let text = dumped(result, without: id)
    for marker in rawErrorMarkers + ["173", "187", Hex.encode(data), Hex.encode(data).uppercased()] {
        #expect(!text.contains(marker), "dump contains \(marker)")
    }
}

@Test(.timeLimit(.minutes(1))) func resultOfAnIngestionFailureHoldsNoReadings() async throws {
    let (store, id) = try await makeStore()
    try failInserts(into: "heart_rate_reading", store)
    var fetches = syntheticHistory.mapValues { FakeRing.Fetch.data($0) }
    fetches[.heartRate] = .data(heartBytes(ring(-3600), bpm: 173) + heartBytes(ring(-3000), bpm: 187))
    let result = await RingSyncer(
        ring: FakeRing(store: store, ringID: id, fetches: fetches), store: store, ringID: id,
        monitoringInterval: nil, budget: .seconds(60), clock: { t0 }, timeZone: { london }
    ).run()

    guard case .failed(.database) = result.ingestions[.heartRate] else {
        Issue.record("heart-rate ingestion did not fail with a database error")
        return
    }
    let text = dumped(result, without: id)
    for marker in rawErrorMarkers + ["173", "187"] {
        #expect(!text.contains(marker), "dump contains \(marker)")
    }
}

/// Every `RingError` case, with frames whose payloads are recognisable.
private let everyRingError: [RingError] = {
    let frame = Frame(group: 0x05, key: 0x40, payload: [0xE7, 0x9B, 0x51])
    return [
        .bluetoothUnavailable(.poweredOff), .notFound, .unknownIdentifier, .connectionFailed("synthetic"),
        .missingService, .notConnected, .timeout("synthetic"), .rejected(frame), .checksumMismatch,
        .refusedDestructive(frame), .busy("synthetic"), .stopped, .tooManyFrames(1024), .refusedUploadGroup(frame),
    ]
}()

/// Whether any value reachable through `value`'s mirror is a `Frame`, a byte array or `Data`.
private func holdsRawBytes(_ value: Any) -> Bool {
    if value is Frame || value is [UInt8] || value is Data { return true }
    return Mirror(reflecting: value).children.contains { holdsRawBytes($0.value) }
}

@Test func noSyncErrorHoldsAFrameOrPayload() {
    let errors: [SyncError] = everyRingError.map(SyncError.init)
        + [SyncError(RingStoreError.fileAccess(path: "/synthetic/path", errno: 13)), SyncError(MonitoringSettingError.rejected(0xFD))]
    for error in errors {
        #expect(!holdsRawBytes(error), "\(error)")
        let text = dumped(error) + error.description
        for marker in ["RingProtocol.Frame", "payload", "E79B51", "e79b51", "231", "155", "/synthetic/path"] {
            #expect(!text.contains(marker), "\(error) shows \(marker)")
        }
    }
    // The command type and reply code are kept.
    #expect(SyncError(RingError.rejected(Frame(group: 0x01, key: 0x00, payload: [0xFC])))
        == .ring(.rejected(command: 0x0100, code: 0xFC)))
}

@Test func otherErrorsKeepOnlyTheirTypeName() {
    struct CarriesReading: Error { let bpm = 173 }
    let error = SyncError(CarriesReading())
    guard case .other(let name) = error else {
        Issue.record("not reduced to its type name")
        return
    }
    #expect(name.hasSuffix("CarriesReading"))
    #expect(!dumped(error).contains("173"))
}
