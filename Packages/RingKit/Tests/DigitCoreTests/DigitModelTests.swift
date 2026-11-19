import DigitCore
import Foundation
import RingStore
import Testing

// The root model's load at launch, against an in-memory store whose calls can be refused as if
// suspended. Synthetic data only.

@MainActor
@Suite(.timeLimit(.minutes(1)))
struct DigitModelTests {
    private func model(_ registry: SuspendingRegistry, _ access: FakeRingAccess) -> DigitModel {
        DigitModel(
            services: DigitServices(
                ringAccess: access, registry: registry,
                makeSync: { _, _ in fatalError("no sync in these tests") }),
            backgroundTasks: FakeBackgroundTasks(), announce: {}, suspend: {}, resume: {})
    }

    @Test func aLoadRefusedBySuspensionHasItsOwnWordingAndIsRetriedOnActivation() async throws {
        let log = EventLog()
        let store = try RingStore()
        let ring = try await store.registerRing(protocolFamily: "ycbt", model: "R99", firmware: "2.32")
        let registry = SuspendingRegistry(store, log: log)
        registry.suspendedCalls = ["rings"]
        let model = model(registry, FakeRingAccess(log: log))

        await model.load()
        #expect(model.loadFailure == "Digit was interrupted while reading its data. It tries again when you return to the app.")
        #expect(!model.pairedRing.isLoaded)

        registry.suspendedCalls = []
        model.sceneDidBecomeActive()
        #expect(await eventually { model.loadFailure == nil && model.pairedRing.isLoaded })
        #expect(model.pairedRing.ring?.id == ring.id)
    }

    @Test func aSuccessfulLoadIsNotRepeatedOnActivation() async throws {
        let log = EventLog()
        let registry = SuspendingRegistry(try RingStore(), log: log)
        let model = model(registry, FakeRingAccess(log: log))
        await model.load()
        #expect(model.loadFailure == nil)
        model.sceneDidBecomeActive()
        try? await Task.sleep(for: .milliseconds(20))
        #expect(log.count("store rings") == 1)
    }
}
