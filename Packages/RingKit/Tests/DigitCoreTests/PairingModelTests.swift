import DigitCore
import Foundation
import RingProtocol
import RingStore
import Testing

// Pairing and forgetting against the scripted ring side and an in-memory store. Synthetic data only.

@MainActor
@Suite(.timeLimit(.minutes(1)))
struct PairingModelTests {
    @Test func nothingFound() async throws {
        let f = try Fixture()
        try await f.load()
        await f.pairing.findRings()
        #expect(f.pairing.state == .nothingFound)
        #expect(f.ringEvents == ["discover"])
        #expect(f.pairedRing.activity == nil)
    }

    @Test func oneCandidate() async throws {
        let f = try Fixture()
        try await f.load()
        let only = RingCandidate(identifier: UUID(), name: syntheticName, signal: .fair)
        f.access.candidates = [only]
        await f.pairing.findRings()
        #expect(f.pairing.state == .found([only]))
    }

    @Test func severalCandidatesStrongestFirst() async throws {
        let f = try Fixture()
        try await f.load()
        let weak = RingCandidate(identifier: UUID(), name: "R99 A", signal: .weak)
        let strong = RingCandidate(identifier: UUID(), name: "R99 B", signal: .strong)
        let unknown = RingCandidate(identifier: UUID(), name: "R99 C", signal: .unknown)
        let fairFirst = RingCandidate(identifier: UUID(), name: "R99 D", signal: .fair)
        let fairSecond = RingCandidate(identifier: UUID(), name: "R99 E", signal: .fair)
        f.access.candidates = [weak, fairFirst, unknown, strong, fairSecond]
        await f.pairing.findRings()
        // Equal levels keep the transport's order.
        #expect(f.pairing.state == .found([strong, fairFirst, fairSecond, weak, unknown]))
    }

    @Test func signalStrengthFromRSSI() {
        #expect(SignalStrength(rssi: 127) == .unknown)
        #expect(SignalStrength(rssi: -50) == .strong)
        #expect(SignalStrength(rssi: -70) == .fair)
        #expect(SignalStrength(rssi: -90) == .weak)
    }

    @Test func discoveryProblemIsShown() async throws {
        let f = try Fixture()
        try await f.load()
        f.access.discoveryProblem = .bluetoothNotPermitted
        await f.pairing.findRings()
        #expect(f.pairing.state == .failed(.ring(.bluetoothNotPermitted)))
        #expect(RingAccessProblem.bluetoothNotPermitted.needsSettings)
    }

    @Test func pickRegistersTheRingWithModelFirmwareAndIdentifier() async throws {
        let f = try Fixture()
        try await f.load()
        f.access.firmware = "2.40"
        let pick = RingCandidate(identifier: syntheticIdentifier, name: syntheticName, signal: .strong)
        #expect(!f.pairing.replacesStoredRing)
        await f.pairing.pair(pick)

        #expect(f.pairing.state == .paired)
        #expect(f.ringEvents == ["connect", "deviceDetails", "disconnect"])
        #expect(f.access.connectedIdentifier == syntheticIdentifier)
        let rings = try await f.store.rings()
        #expect(rings.count == 1)
        let ring = try #require(rings.first)
        #expect(ring.protocolFamily == "ycbt")
        #expect(ring.model == "R99")
        #expect(ring.firmware == "2.40")
        #expect(ring.peripheralIdentifier == syntheticIdentifier)
        #expect(f.pairedRing.ring == ring)
        #expect(f.pairedRing.canSync)
        #expect(f.pairedRing.activity == nil)
    }

    @Test func connectFailing() async throws {
        let f = try Fixture()
        try await f.load()
        f.access.connectProblem = .ringBusy
        await f.pairing.pair(RingCandidate(identifier: syntheticIdentifier, name: syntheticName, signal: .strong))
        #expect(f.pairing.state == .failed(.ring(.ringBusy)))
        #expect(f.ringEvents == ["connect"])
        #expect(try await f.store.rings().isEmpty)
    }

    @Test func deviceInfoFailingRegistersNothing() async throws {
        let f = try Fixture()
        try await f.load()
        f.access.detailsProblem = .timedOut
        await f.pairing.pair(RingCandidate(identifier: syntheticIdentifier, name: syntheticName, signal: .strong))
        #expect(f.pairing.state == .failed(.ring(.timedOut)))
        #expect(f.ringEvents == ["connect", "deviceDetails", "disconnect"])
        #expect(try await f.store.rings().isEmpty)
        #expect(f.pairedRing.ring == nil)
    }

    @Test func pairingAgainReplacesTheIdentifierOnTheSameRing() async throws {
        let f = try Fixture()
        let old = try await f.storePairedRing()
        // History already stored for that ring.
        let transfer = HistoryTransfer(
            kind: .heartRate, header: Array(repeating: 1, count: 12), data: syntheticHistory[.heartRate]!,
            fetchedAt: t0, timeZoneIdentifier: "Europe/London")
        try await f.store.insert(transfer, ringID: old.id)

        #expect(f.pairing.replacesStoredRing)
        #expect(f.pairing.confirmationMessage == "This replaces the ring Digit is paired with; its history is kept.")
        let newIdentifier = UUID()
        await f.pairing.pair(RingCandidate(identifier: newIdentifier, name: syntheticName, signal: .strong))

        #expect(f.pairing.state == .paired)
        let rings = try await f.store.rings()
        #expect(rings.count == 1)
        #expect(rings.first?.id == old.id)
        #expect(rings.first?.peripheralIdentifier == newIdentifier)
        #expect(try await f.store.transfers(ringID: old.id).transfers.count == 1)
        #expect(f.pairedRing.ring?.peripheralIdentifier == newIdentifier)
    }

    @Test func pairingAfterForgettingAttachesToTheStoredRing() async throws {
        let f = try Fixture()
        let old = try await f.storePairedRing()
        await f.pairing.forget()
        #expect(!f.pairedRing.canSync)
        await f.pairing.pair(RingCandidate(identifier: syntheticIdentifier, name: syntheticName, signal: .strong))
        let rings = try await f.store.rings()
        #expect(rings.map(\.id) == [old.id])
        #expect(rings.first?.peripheralIdentifier == syntheticIdentifier)
    }

    @Test func forgetClearsTheIdentifierOnlyAndSendsNothing() async throws {
        let f = try Fixture()
        let ring = try await f.storePairedRing()
        let transfer = HistoryTransfer(
            kind: .activity, header: Array(repeating: 1, count: 12), data: syntheticHistory[.activity]!,
            fetchedAt: t0, timeZoneIdentifier: "Europe/London")
        try await f.store.insert(transfer, ringID: ring.id)

        await f.pairing.forget()

        #expect(f.ringEvents.isEmpty)
        #expect(f.pairing.forgetFailure == nil)
        let stored = try #require(try await f.store.ring(id: ring.id))
        #expect(stored.peripheralIdentifier == nil)
        #expect(stored.firmware == ring.firmware)
        #expect(try await f.store.transfers(ringID: ring.id).transfers.count == 1)
        #expect(f.pairedRing.ring == stored)
        #expect(!f.pairedRing.canSync)
    }

    @Test func pairingInterruptedBySuspension() async throws {
        let f = try Fixture()
        try await f.load()
        f.registry.suspendedCalls = ["registerRing"]
        await f.pairing.pair(RingCandidate(identifier: syntheticIdentifier, name: syntheticName, signal: .strong))
        #expect(f.pairing.state == .failed(.interrupted))
        #expect(f.ringEvents == ["connect", "deviceDetails", "disconnect"])
        #expect(try await f.store.rings().isEmpty)
    }

    @Test func forgetInterruptedBySuspension() async throws {
        let f = try Fixture()
        try await f.storePairedRing()
        f.registry.suspendedCalls = ["setPeripheralIdentifier"]
        await f.pairing.forget()
        #expect(f.pairing.forgetFailure == .interrupted)
        #expect(f.pairedRing.canSync)
    }

    @Test func identifierHeldByAnotherRingIsAFailureNotACrash() async throws {
        let f = try Fixture()
        // Two rings can only exist if something outside pairing made them; the second holds the
        // identifier the owner picks, and pairing attaches to the first (oldest).
        _ = try await f.store.registerRing(protocolFamily: "ycbt", model: "R99", firmware: "2.32", now: t0)
        let other = try await f.store.registerRing(protocolFamily: "ycbt", model: "R99", firmware: "2.32", now: t0.addingTimeInterval(1))
        try await f.store.setPeripheralIdentifier(syntheticIdentifier, forRing: other.id)
        try await f.load()
        await f.pairing.pair(RingCandidate(identifier: syntheticIdentifier, name: syntheticName, signal: .strong))
        #expect(f.pairing.state == .failed(.identifierHeldByAnotherRing))
        #expect(f.ringEvents.last == "disconnect")
    }

    @Test func nothingStartsBeforeTheStoreIsRead() async throws {
        let f = try Fixture()
        await f.pairing.findRings()
        #expect(f.pairing.state == .idle)
        #expect(f.ringEvents.isEmpty)
    }
}
