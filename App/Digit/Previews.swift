#if DEBUG
import DigitCore
import Foundation
import RingProtocol
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

/// SYNTHETIC sleep, steps and heart rate for previews, made up in code relative to now: a main
/// sleep each night (the second-latest with uncertain times), one afternoon nap, and a day of steps
/// and heart rate for each of the last week. Nothing comes from a ring or a capture.
private struct PreviewHistory: RingHistoryReader {
    let empty: Bool

    private static let offset = 0
    private static var today: RingDay { RingDay(instant: Date(), utcOffsetSeconds: offset) }

    private static func nights() -> [SleepNightDetail] {
        (0..<7).flatMap { back -> [SleepNightDetail] in
            let day = today.adding(days: -back)
            // 23:10 the evening before to about 06:40.
            let start = day.gmtMidnight.addingTimeInterval(-50 * 60 + Double(back * 7 * 60))
            let pattern: [(SleepStageKind, Int)] = [
                (.light, 25), (.deep, 50), (.light, 40), (.rem, 20), (.light, 45), (.deep, 35),
                (.light, 50), (.rem, 30), (.light, 55), (.rem, 35), (.light, 25),
            ]
            var stages: [SleepStageEntry] = []
            var t = start
            for (kind, minutes) in pattern {
                stages.append(SleepStageEntry(kind: kind, start: t, durationSeconds: minutes * 60))
                t = t.addingTimeInterval(Double(minutes * 60 + 1))
            }
            var main = summary(day: day, stages: stages, main: true)
            if back == 1 { main.timeSource = .ambiguous }
            guard back == 2 else { return [SleepNightDetail(summary: main, stages: stages)] }
            let napStart = day.gmtMidnight.addingTimeInterval(14 * 3600 + 20 * 60)
            let nap = [SleepStageEntry(kind: .light, start: napStart, durationSeconds: 25 * 60)]
            return [
                SleepNightDetail(summary: main, stages: stages),
                SleepNightDetail(summary: summary(day: day, stages: nap, main: false), stages: nap),
            ]
        }
    }

    private static func summary(day: RingDay, stages: [SleepStageEntry], main: Bool) -> SleepNightSummary {
        var byKind = Dictionary(uniqueKeysWithValues: SleepStageKind.allCases.map { ($0, 0) })
        for stage in stages { byKind[stage.kind, default: 0] += stage.durationSeconds }
        let last = stages[stages.count - 1]
        return SleepNightSummary(
            day: day, utcOffsetSeconds: offset, start: stages[0].start,
            end: last.start.addingTimeInterval(Double(last.durationSeconds)), secondsByKind: byKind,
            secondsAsleep: byKind.filter { $0.key != .awake }.values.reduce(0, +), interruptions: [],
            timeSource: .resolved, isMainSleep: main)
    }

    func sleepNights(ringID: UUID, from first: RingDay, through last: RingDay) async throws -> [SleepNightSummary] {
        empty ? [] : Self.nights().map(\.summary)
    }

    func sleepNightDetail(ringID: UUID, startingAt start: Date) async throws -> SleepNightDetail? {
        Self.nights().first { $0.summary.start == start }
    }

    func dailySummaries(ringID: UUID, from first: RingDay, through last: RingDay) async throws -> [DailySummary] {
        if empty { return [] }
        return (0..<7).reversed().map { back in
            let steps = 4000 + back * 1250
            return DailySummary(
                day: Self.today.adding(days: -back), steps: steps, distanceMeters: steps * 3 / 4,
                kilocalories: steps / 25, activityIntervalCount: 20,
                heartRate: back == 4
                    ? nil
                    : HeartRateDaySummary(readingCount: 140, minimumBPM: 50 + back, maximumBPM: 112 + back, meanBPM: 63),
                timeSource: back == 1 ? .assumed : .resolved)
        }
    }
}

@MainActor
private func previewModel(paired: Bool, withData: Bool = true) async -> DigitModel {
    // In memory: previews never touch the App Group container.
    let store = try! RingStore()
    if paired {
        let ring = try! await store.registerRing(protocolFamily: "ycbt", model: "R99", firmware: "2.32")
        try! await store.setPeripheralIdentifier(UUID(), forRing: ring.id)
    }
    let model = DigitModel(
        services: DigitServices(ringAccess: PreviewRingAccess(), store: store, history: PreviewHistory(empty: !withData)),
        backgroundTasks: PreviewBackgroundTasks(), announce: {}, suspend: {}, resume: {})
    // As the app does: the first activation after loading reads the data screens.
    await model.load()
    await model.reloadData()
    return model
}

private struct PreviewHost: View {
    let paired: Bool
    var withData = true
    @State private var model: DigitModel?

    var body: some View {
        Group {
            if let model { ReadyView(model: model) } else { ProgressView() }
        }
        .task { model = await previewModel(paired: paired, withData: withData) }
    }
}

/// The night screen for the latest synthetic night.
private struct NightPreviewHost: View {
    @State private var model: DigitModel?

    var body: some View {
        NavigationStack {
            if let model, let start = model.sleep.state.value?.first?.nights.first?.summary.start {
                NightScreen(model: model, start: start)
            } else {
                ProgressView()
            }
        }
        .task { model = await previewModel(paired: true) }
    }
}

#Preview("Not paired") { PreviewHost(paired: false) }
#Preview("Paired") { PreviewHost(paired: true) }
#Preview("Paired, no data yet") { PreviewHost(paired: true, withData: false) }
#Preview("Night") { NightPreviewHost() }
#endif
