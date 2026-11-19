import DigitCore
import Foundation
import RingProtocol
import RingStore
import RingSync
import RingTransport
import Testing

// "Sync now" against the scripted ring side, the real RingSyncer and an in-memory store.
// Synthetic data only.

private let completeEvents = [
    "connect", "deviceDetails",
    "fetch activity", "fetch sleep", "fetch heart", "fetch combined", "setTime", "interval 10",
    "disconnect", "ingest",
]

@MainActor
@Suite(.timeLimit(.minutes(1)))
struct SyncModelTests {
    @Test func completeSync() async throws {
        let f = try Fixture()
        try await f.storePairedRing()
        #expect(f.sync.canSync)
        await f.sync.sync()

        // The ring phase, then the disconnect, and only then the derive phase.
        #expect(f.ringEvents == completeEvents)
        guard case .done(let summary) = f.sync.state else {
            Issue.record("sync did not complete")
            return
        }
        #expect(summary.kinds.map(\.kind) == RingSyncer.fetchOrder)
        #expect(summary.kinds.map(\.fetch) == [.stored, .empty, .stored, .empty])
        #expect(summary.kinds.first { $0.kind == .activity }?.ingestion == .counts(inserted: 2, updated: 0, rejected: 0))
        #expect(summary.kinds.first { $0.kind == .heartRate }?.ingestion == .counts(inserted: 2, updated: 0, rejected: 0))
        #expect(summary.clockText == "The ring's clock was set.")
        #expect(summary.intervalSent)
        #expect(summary.finishedAt == t0.addingTimeInterval(120))
        #expect(f.sync.statusText == "Sync complete.")
        #expect(f.pairedRing.activity == nil)
        #expect(!f.sync.isRunning)
    }

    @Test func connectFails() async throws {
        let f = try Fixture()
        try await f.storePairedRing()
        f.access.connectProblem = .ringNotFound
        await f.sync.sync()
        #expect(f.sync.state == .failed(SyncFailure(reason: .ring(.ringNotFound))))
        // Never connected, so nothing to disconnect.
        #expect(f.ringEvents == ["connect"])
        #expect(f.pairedRing.activity == nil)
    }

    @Test func deviceInfoFailsAndDisconnects() async throws {
        let f = try Fixture()
        try await f.storePairedRing()
        f.access.detailsProblem = .unreadableReply
        await f.sync.sync()
        #expect(f.sync.state == .failed(SyncFailure(reason: .ring(.unreadableReply))))
        #expect(f.ringEvents == ["connect", "deviceDetails", "disconnect"])
    }

    @Test func aKindFails() async throws {
        let f = try Fixture()
        try await f.storePairedRing()
        f.access.ring = ScriptedRing(log: f.log, fetches: [
            .activity: .data(syntheticHistory[.activity]!),
            .sleep: .fail(RingError.timeout("synthetic")),
            .heartRate: .data(syntheticHistory[.heartRate]!),
        ])
        await f.sync.sync()
        guard case .failed(let failure) = f.sync.state, let summary = failure.summary else {
            Issue.record("expected a failure with counts")
            return
        }
        #expect(failure.reason == .incomplete)
        #expect(summary.kinds.first { $0.kind == .sleep }?.fetch == .failed("ring: timed out waiting for synthetic"))
        // The clock is not set over incomplete history.
        #expect(summary.clockSet == .skipped(.historyIncomplete))
        #expect(!summary.intervalSent)
        #expect(f.ringEvents.suffix(2) == ["disconnect", "ingest"])
    }

    @Test func clockSetUnknown() async throws {
        let f = try Fixture()
        try await f.storePairedRing()
        f.access.ring = ScriptedRing(log: f.log, clockError: RingError.timeout("synthetic"))
        await f.sync.sync()
        guard case .failed(let failure) = f.sync.state, let summary = failure.summary else {
            Issue.record("expected a failure with counts")
            return
        }
        #expect(failure.reason == .incomplete)
        guard case .unknownAndLogged = summary.clockSet else {
            Issue.record("expected an unknown clock-set")
            return
        }
        #expect(summary.clockText.contains("the next sync sets it again"))
        #expect(!summary.intervalSent)
        #expect(f.ringEvents.contains("disconnect"))
    }

    @Test func unknownIdentifierLeadsBackToPairing() async throws {
        let f = try Fixture()
        try await f.storePairedRing()
        f.access.connectProblem = .unknownIdentifier
        await f.sync.sync()
        guard case .failed(let failure) = f.sync.state else {
            Issue.record("expected a failure")
            return
        }
        #expect(failure.needsPairing)
        #expect(failure.message == "Digit can no longer find this ring by its saved identity; pair it again.")
    }

    @Test func notPairedDoesNothing() async throws {
        let f = try Fixture()
        try await f.load()
        #expect(!f.sync.canSync)
        await f.sync.sync()
        #expect(f.sync.state == .idle)
        #expect(f.ringEvents.isEmpty)
    }

    @Test func firmwareIsUpdatedWhenTheRingReportsANewOne() async throws {
        let f = try Fixture()
        let ring = try await f.storePairedRing(firmware: "2.32")
        f.access.firmware = "2.33"
        await f.sync.sync()
        #expect(try await f.store.ring(id: ring.id)?.firmware == "2.33")
        #expect(f.pairedRing.ring?.firmware == "2.33")
        #expect(f.log.count("store updateRing") == 1)
    }

    @Test func unchangedFirmwareIsNotWritten() async throws {
        let f = try Fixture()
        try await f.storePairedRing(firmware: "2.32")
        await f.sync.sync()
        #expect(f.log.count("store updateRing") == 0)
    }

    // MARK: Suspension

    @Test func ringPhaseEndedBySuspensionRunsNoDerivePhase() async throws {
        let f = try Fixture()
        try await f.storePairedRing()
        // What `persist` rethrows when the store refuses during a suspension.
        f.access.ring = ScriptedRing(log: f.log, fetches: [.activity: .fail(RingStoreError.suspended)])
        await f.sync.sync()
        guard case .failed(let failure) = f.sync.state else {
            Issue.record("expected a failure")
            return
        }
        #expect(failure.reason == .interrupted)
        #expect(failure.summary?.kinds.allSatisfy { $0.ingestion == .notRun } == true)
        #expect(f.ringEvents == ["connect", "deviceDetails", "fetch activity", "disconnect"])
    }

    @Test func storeCallRefusedBySuspensionEndsInterrupted() async throws {
        let f = try Fixture()
        try await f.storePairedRing(firmware: "2.32")
        f.access.firmware = "2.33"
        f.registry.suspendedCalls = ["updateRing"]
        await f.sync.sync()
        guard case .failed(let failure) = f.sync.state else {
            Issue.record("expected a failure")
            return
        }
        #expect(failure.reason == .interrupted)
        #expect(!f.ringEvents.contains("ingest"))
        #expect(f.ringEvents.contains("disconnect"))
    }

    // MARK: One flow at a time

    @Test func secondTapWhileSyncingIsIgnoredAndPairingIsRefused() async throws {
        let f = try Fixture()
        try await f.storePairedRing()
        let gate = Gate()
        f.access.connectGate = gate
        let finished = Flag()
        let model = f.sync
        Task { await model.sync(); finished.value = true }
        #expect(await eventually { f.log.count("connect") == 1 })
        #expect(f.sync.isRunning)
        #expect(!f.sync.canSync)

        await f.sync.sync()
        await f.pairing.findRings()
        await f.pairing.pair(RingCandidate(identifier: UUID(), name: syntheticName, signal: .strong))
        await f.pairing.forget()
        #expect(f.log.count("connect") == 1)
        #expect(f.log.count("discover") == 0)
        #expect(f.pairing.state == .idle)

        gate.open()
        #expect(await eventually { finished.value })
        #expect(f.ringEvents == completeEvents)
        #expect(f.pairedRing.ring?.peripheralIdentifier == syntheticIdentifier)
    }

    @Test func syncIsRefusedWhilePairing() async throws {
        let f = try Fixture()
        try await f.storePairedRing()
        let gate = Gate()
        f.access.connectGate = gate
        let finished = Flag()
        let pairing = f.pairing
        let pick = RingCandidate(identifier: syntheticIdentifier, name: syntheticName, signal: .strong)
        Task { await pairing.pair(pick); finished.value = true }
        #expect(await eventually { f.log.count("connect") == 1 })
        #expect(!f.sync.canSync)

        await f.sync.sync()
        #expect(f.sync.state == .idle)
        #expect(f.log.count("connect") == 1)

        gate.open()
        #expect(await eventually { finished.value })
        #expect(f.pairing.state == .paired)
    }

    // MARK: Texts

    @Test func everyClockSetOutcomeHasText() {
        let event = ClockSetEvent(ringID: UUID(), instant: t0, timeZoneIdentifier: "Europe/London", utcOffsetSeconds: 0)
        let error = SyncError.ring(.timeout("synthetic"))
        let skips: [SyncSkipReason] = [
            .budgetExhausted, .sessionUnavailable, .historyIncomplete, .clockNotAcknowledged, .storeSuspended, .clockNotSent,
        ]
        let outcomes: [ClockSetOutcome] = [
            .acknowledgedAndLogged(event), .unknownAndLogged(event, error: error),
            .notAppliedButLogged(event, error: error), .notLogged(error),
            .notSentLogTooSlow(event, elapsed: .seconds(3)),
        ] + skips.map { .skipped($0) }
        for outcome in outcomes {
            #expect(!SyncSummary.text(for: outcome).isEmpty)
        }
        // Not errors the owner can fix: the next sync sets it again.
        for outcome in [ClockSetOutcome.unknownAndLogged(event, error: error), .notAppliedButLogged(event, error: error)] {
            let text = SyncSummary.text(for: outcome)
            #expect(text.contains("Nothing to do"))
            #expect(text.contains("the next sync sets it again"))
        }
    }

    @Test func noTextCarriesANameOrIdentifier() async throws {
        var texts: [String] = []
        let candidate = RingCandidate(identifier: syntheticIdentifier, name: syntheticName, signal: .strong)
        texts.append(String(describing: candidate))
        texts.append(String(reflecting: candidate))
        var dumped = ""
        dump(candidate, to: &dumped)
        texts.append(dumped)

        // Pairing through every state it can reach here.
        let f = try Fixture()
        try await f.load()
        texts.append(f.pairing.statusText)
        f.access.candidates = [candidate]
        await f.pairing.findRings()
        texts.append(f.pairing.statusText)
        texts.append(f.pairing.confirmationMessage)
        for problem in [RingAccessProblem.ringBusy, .unknownIdentifier, .bluetoothOff, .timedOut] {
            f.access.connectProblem = problem
            await f.pairing.pair(candidate)
            texts.append(f.pairing.statusText)
        }
        f.access.connectProblem = nil
        await f.pairing.pair(candidate)
        texts.append(f.pairing.statusText)
        texts.append(f.pairing.confirmationMessage)

        // Sync: complete, then each failure.
        await f.sync.sync()
        texts += syncTexts(f.sync)
        f.access.ring = ScriptedRing(log: f.log, fetches: [.sleep: .fail(RingError.notConnected)], clockError: RingError.timeout("x"))
        await f.sync.sync()
        texts += syncTexts(f.sync)
        f.access.connectProblem = .unknownIdentifier
        await f.sync.sync()
        texts += syncTexts(f.sync)
        texts.append(PairingFailure.identifierHeldByAnotherRing.message)
        texts.append(PairingFailure.store(.store(.peripheralIdentifierInUse(UUID()))).message)

        for text in texts {
            #expect(!text.contains(syntheticName))
            #expect(!text.contains("ZQXJ"))
            #expect(!text.localizedCaseInsensitiveContains(syntheticIdentifier.uuidString))
        }
    }

    private func syncTexts(_ model: SyncModel) -> [String] {
        var texts = [model.statusText]
        let summary: SyncSummary? = switch model.state {
        case .done(let summary): summary
        case .failed(let failure): failure.summary
        default: nil
        }
        if let summary {
            texts += [summary.clockText, summary.intervalText]
            for line in summary.kinds { texts += [line.title, line.fetch.text, line.ingestion.text] }
        }
        return texts
    }
}
