import DigitCore
import RingStore
import SwiftUI

/// Paired: the ring's model and firmware, "Sync now", what the last sync in this run of the app
/// did (counts only), and "Forget this ring".
struct PairedRingView: View {
    let model: DigitModel

    @State private var confirmingForget = false
    @State private var showingPairing = false

    private var sync: SyncModel { model.sync }

    var body: some View {
        List {
            Section("Ring") {
                LabeledContent("Model", value: model.pairedRing.ring?.model ?? "Unknown")
                LabeledContent("Firmware", value: model.pairedRing.ring?.firmware ?? "Unknown")
            }

            Section {
                Button {
                    Task { await sync.sync() }
                } label: {
                    HStack {
                        Text("Sync Now")
                        if sync.isRunning {
                            Spacer()
                            ProgressView()
                                .accessibilityLabel("Syncing")
                        }
                    }
                }
                .disabled(!sync.canSync)
                .accessibilityHint("Copies the ring's history to this iPhone and sets the ring's clock.")

                Text(sync.statusText)
                    .foregroundStyle(.secondary)
                    .accessibilityLabel("Sync status: \(sync.statusText)")

                if case .failed(let failure) = sync.state {
                    if failure.needsPairing {
                        Button("Pair Again") { showingPairing = true }
                    }
                    if case .ring(let problem) = failure.reason, problem.needsSettings {
                        OpenSettingsButton()
                    }
                }
            }

            if let summary {
                SyncSummarySection(summary: summary)
            }

            Section {
                Button("Forget This Ring", role: .destructive) { confirmingForget = true }
                    .disabled(model.pairedRing.activity != nil)
                    .accessibilityHint("Digit stops connecting to this ring. Nothing is deleted.")
                if let failure = model.pairing.forgetFailure {
                    Text(failure.message)
                        .foregroundStyle(.secondary)
                }
            }
        }
        .confirmationDialog("Forget this ring?", isPresented: $confirmingForget, titleVisibility: .visible) {
            Button("Forget This Ring", role: .destructive) {
                Task { await model.pairing.forget() }
            }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text("Digit stops connecting to this ring. Its history stays on this iPhone, and nothing is deleted from the ring.")
        }
        .sheet(isPresented: $showingPairing) {
            NavigationStack {
                PairingView(pairing: model.pairing, onPaired: { showingPairing = false })
                    .navigationTitle("Pair Again")
                    .toolbar {
                        ToolbarItem(placement: .cancellationAction) {
                            Button("Close") { showingPairing = false }
                        }
                    }
            }
        }
    }

    private var summary: SyncSummary? {
        switch sync.state {
        case .done(let summary): summary
        case .failed(let failure): failure.summary
        case .idle, .connecting, .syncing: nil
        }
    }
}

/// Counts and outcomes of one sync. Never a health value.
struct SyncSummarySection: View {
    let summary: SyncSummary

    var body: some View {
        Section("Last sync") {
            LabeledContent("Finished", value: summary.finishedAt.formatted(date: .omitted, time: .shortened))
            ForEach(summary.kinds) { line in
                VStack(alignment: .leading, spacing: 2) {
                    Text(line.title)
                    Text("From the ring: \(line.fetch.text)")
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                    Text("Records: \(line.ingestion.text)")
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                }
                .accessibilityElement(children: .combine)
            }
            Text(summary.clockText)
            Text(summary.intervalText)
            if summary.firmwareNotSaved {
                Text("The ring's new firmware version couldn't be saved; the next sync tries again.")
                    .foregroundStyle(.secondary)
            }
        }
    }
}
