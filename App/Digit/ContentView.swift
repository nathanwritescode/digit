import DigitCore
import SwiftUI

/// The root screen: opening the store, then pairing or the paired ring.
struct ContentView: View {
    let root: AppRoot

    var body: some View {
        switch root.state {
        case .opening:
            ProgressView("Opening…")
        case .failed(let message):
            ErrorScreen(message: message)
        case .ready(let model):
            ReadyView(model: model)
        }
    }
}

/// Once the paired ring has been read: Sleep, Days and Ring tabs.
struct ReadyView: View {
    let model: DigitModel

    var body: some View {
        if let failure = model.loadFailure {
            NavigationStack {
                ErrorScreen(message: failure) {
                    Task { await model.retryLoad() }
                }
                .navigationTitle("Digit")
            }
        } else if !model.pairedRing.isLoaded {
            ProgressView("Loading…")
        } else {
            AppTabs(model: model)
        }
    }
}

enum AppTab: Hashable {
    case sleep, days, ring
}

struct AppTabs: View {
    let model: DigitModel
    /// Starts on the Ring tab when no ring is paired, so pairing is the first thing seen.
    @State private var selection: AppTab

    init(model: DigitModel) {
        self.model = model
        _selection = State(initialValue: model.pairedRing.canSync ? .sleep : .ring)
    }

    var body: some View {
        TabView(selection: $selection) {
            Tab("Sleep", systemImage: "bed.double", value: AppTab.sleep) {
                NavigationStack { SleepListView(model: model) }
            }
            Tab("Days", systemImage: "figure.walk", value: AppTab.days) {
                NavigationStack { DaysView(days: model.days) }
            }
            Tab("Ring", systemImage: "circle.circle", value: AppTab.ring) {
                NavigationStack {
                    Group {
                        if model.pairedRing.canSync {
                            PairedRingView(model: model)
                        } else {
                            PairingView(pairing: model.pairing)
                        }
                    }
                    .navigationTitle("Ring")
                }
            }
        }
    }
}

/// The empty, not-paired and failed states shared by the data screens.
struct DataStateView<Value: Sendable & Equatable, Content: View>: View {
    let state: DataState<Value>
    /// What the screen holds, for "Sync your ring to see your …".
    let subject: String
    /// Reloads: "Try Again" after a failure, "Refresh" when there is no data yet.
    let retry: () async -> Void
    @ViewBuilder let content: (Value) -> Content

    var body: some View {
        switch state {
        case .loading:
            ProgressView("Loading…")
        case .notPaired:
            ContentUnavailableView(
                "No Ring Paired", systemImage: "circle.dashed",
                description: Text("Pair your ring in the Ring tab to see your \(subject)."))
        case .noData:
            ContentUnavailableView {
                Label("Nothing Yet", systemImage: "arrow.triangle.2.circlepath")
            } description: {
                Text("Sync your ring to see your \(subject).")
            } actions: {
                Button("Refresh") { Task { await retry() } }
            }
        case .failed(let message):
            ContentUnavailableView {
                Label("Couldn't Load", systemImage: "exclamationmark.triangle")
            } description: {
                Text(message)
            } actions: {
                Button("Try Again") { Task { await retry() } }
            }
        case .loaded(let value):
            content(value)
        }
    }
}

/// "Times uncertain", with its one-line explanation when asked.
struct UncertaintyLabel: View {
    let marker: String
    let explanation: String?
    var showsExplanation = false

    var body: some View {
        VStack(alignment: .leading, spacing: 2) {
            Label(marker, systemImage: "questionmark.circle")
                .font(.footnote)
                .foregroundStyle(.secondary)
            if showsExplanation, let explanation {
                Text(explanation)
                    .font(.footnote)
                    .foregroundStyle(.secondary)
            }
        }
    }
}

/// A plain error, with an optional retry.
struct ErrorScreen: View {
    let message: String
    var retry: (() -> Void)?

    var body: some View {
        VStack(spacing: 16) {
            Image(systemName: "exclamationmark.triangle")
                .font(.largeTitle)
                .accessibilityHidden(true)
            Text(message)
                .multilineTextAlignment(.center)
            if let retry {
                Button("Try Again", action: retry)
                    .buttonStyle(.borderedProminent)
            }
        }
        .padding()
    }
}

/// Opens this app's page in Settings, where Bluetooth access is granted.
struct OpenSettingsButton: View {
    @Environment(\.openURL) private var openURL

    var body: some View {
        Button("Open Settings") {
            if let url = URL(string: UIApplication.openSettingsURLString) { openURL(url) }
        }
        .accessibilityHint("Opens Digit's settings, where you can allow Bluetooth.")
    }
}
