#if DEBUG
import DigitCore
import Foundation
import RingStore
import RingSync
import SwiftUI

/// A ring side for previews: one made-up ring is "found", and connecting fails as it does in the
/// simulator, where Bluetooth is unavailable. No Bluetooth, no real data.
private struct PreviewRingAccess: RingAccess {
    func discoverRings(for duration: Duration) async throws(RingAccessProblem) -> [RingCandidate] {
        [RingCandidate(identifier: UUID(), name: "R99 Preview", signal: .strong)]
    }

    func connect(identifier: UUID) async throws(RingAccessProblem) -> any ConnectedRing {
        throw .bluetoothUnsupported
    }
}

/// Previews do nothing with background tasks.
private final class PreviewBackgroundTasks: BackgroundTaskRunner {
    func begin(expirationHandler: @escaping @MainActor () -> Void) -> BackgroundTaskToken? { nil }
    func end(_ token: BackgroundTaskToken) {}
}

@MainActor
private func previewModel(paired: Bool) async -> DigitModel {
    // In memory: previews never touch the App Group container.
    let store = try! RingStore()
    if paired {
        let ring = try! await store.registerRing(protocolFamily: "ycbt", model: "R99", firmware: "2.32")
        try! await store.setPeripheralIdentifier(UUID(), forRing: ring.id)
    }
    let model = DigitModel(
        services: DigitServices(ringAccess: PreviewRingAccess(), store: store),
        backgroundTasks: PreviewBackgroundTasks(), suspend: {}, resume: {})
    await model.load()
    return model
}

private struct PreviewHost: View {
    let paired: Bool
    @State private var model: DigitModel?

    var body: some View {
        Group {
            if let model { ReadyView(model: model) } else { ProgressView() }
        }
        .task { model = await previewModel(paired: paired) }
    }
}

#Preview("Not paired") { PreviewHost(paired: false) }
#Preview("Paired") { PreviewHost(paired: true) }
#endif
