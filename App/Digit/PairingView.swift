import DigitCore
import SwiftUI

/// Not paired: an explanation, "Find my ring", the rings found, a confirmation and the result.
/// The ring's advertised name is shown here only, never stored or logged.
struct PairingView: View {
    let pairing: PairingModel
    var onPaired: (() -> Void)?

    @State private var pendingPick: RingCandidate?

    var body: some View {
        List {
            Section {
                Text("Digit copies your ring's history to this iPhone. Pair your ring once, then sync it whenever you like.")
                Text(pairing.statusText)
                    .foregroundStyle(.secondary)
                    .accessibilityLabel("Status: \(pairing.statusText)")
                if case .failed(.ring(let problem)) = pairing.state, problem.needsSettings {
                    OpenSettingsButton()
                }
            }

            Section {
                Button {
                    Task { await pairing.findRings() }
                } label: {
                    HStack {
                        Text("Find My Ring")
                        if pairing.state == .scanning || pairing.state == .connecting {
                            Spacer()
                            ProgressView()
                                .accessibilityLabel(pairing.state == .scanning ? "Looking for rings" : "Connecting")
                        }
                    }
                }
                .disabled(!pairing.canStart)
                .accessibilityHint("Looks for rings near this iPhone for a few seconds.")
            }

            if case .found(let candidates) = pairing.state {
                Section("Rings found") {
                    ForEach(candidates) { candidate in
                        Button {
                            pendingPick = candidate
                        } label: {
                            VStack(alignment: .leading) {
                                Text(candidate.name)
                                Text(candidate.signal.label)
                                    .font(.footnote)
                                    .foregroundStyle(.secondary)
                            }
                        }
                        .disabled(!pairing.canStart)
                        .accessibilityElement(children: .combine)
                        .accessibilityLabel("\(candidate.name), \(candidate.signal.label)")
                        .accessibilityHint("Pairs this ring with Digit.")
                    }
                }
            }
        }
        .confirmationDialog(
            "Pair this ring?",
            isPresented: Binding(get: { pendingPick != nil }, set: { if !$0 { pendingPick = nil } }),
            titleVisibility: .visible,
            presenting: pendingPick
        ) { candidate in
            Button("Pair") {
                Task {
                    await pairing.pair(candidate)
                    if pairing.state == .paired { onPaired?() }
                }
            }
            Button("Cancel", role: .cancel) {}
        } message: { _ in
            Text(pairing.confirmationMessage)
        }
    }
}
