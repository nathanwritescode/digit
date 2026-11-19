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

struct ReadyView: View {
    let model: DigitModel

    var body: some View {
        NavigationStack {
            Group {
                if let failure = model.loadFailure {
                    ErrorScreen(message: failure) {
                        Task { await model.load() }
                    }
                } else if !model.pairedRing.isLoaded {
                    ProgressView("Loading…")
                } else if model.pairedRing.canSync {
                    PairedRingView(model: model)
                } else {
                    PairingView(pairing: model.pairing)
                }
            }
            .navigationTitle("Digit")
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
