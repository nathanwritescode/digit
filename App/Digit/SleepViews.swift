import Charts
import DigitCore
import RingProtocol
import RingStore
import SwiftUI

/// The nights of the last 30 days, grouped by day, main sleep first. Times are on each night's own
/// clock (the model formats them); nothing here formats a date.
struct SleepListView: View {
    let model: DigitModel

    private var sleep: SleepListModel { model.sleep }

    var body: some View {
        DataStateView(state: sleep.state, subject: "sleep", retry: { await sleep.reload() }) { groups in
            List(groups) { group in
                Section(group.title) {
                    ForEach(group.nights) { row in
                        NavigationLink(value: NightRoute(start: row.summary.start)) {
                            SleepNightRowView(row: row)
                        }
                        .accessibilityLabel(row.accessibilityLabel)
                        .accessibilityHint(row.uncertaintyExplanation ?? "Shows the night's stages.")
                    }
                }
            }
            .refreshable { await sleep.reload() }
        }
        .navigationTitle("Sleep")
        .navigationDestination(for: NightRoute.self) { route in
            NightScreen(model: model, start: route.start)
        }
    }
}

/// The sleep list's navigation value: a night, found again by its start.
struct NightRoute: Hashable {
    let start: Date
}

struct SleepNightRowView: View {
    let row: SleepNightRow

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            // Side by side while they fit, one above the other at large text sizes.
            ViewThatFits(in: .horizontal) {
                HStack(alignment: .firstTextBaseline) {
                    kind
                    Spacer()
                    asleep
                }
                VStack(alignment: .leading, spacing: 4) {
                    kind
                    asleep
                }
            }
            Text("\(row.bedtimeText) – \(row.wakeText)")
                .foregroundStyle(.secondary)
                .monospacedDigit()
            if let marker = row.uncertainty {
                UncertaintyLabel(marker: marker, explanation: row.uncertaintyExplanation)
            }
        }
        .padding(.vertical, 2)
    }

    private var kind: some View { Text(row.kindText).font(.headline) }
    private var asleep: some View { Text("\(row.asleepText) asleep").monospacedDigit() }
}

/// One night: bedtime and wake time, the stage timeline and the totals. Returns to the list when
/// the night has moved (ingestion changed its start since the list was read).
struct NightScreen: View {
    let model: DigitModel
    let start: Date

    @State private var detail: NightDetailModel?
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        Group {
            if let detail {
                switch detail.state {
                case .loading, .moved:
                    ProgressView()
                case .failed(let message):
                    ContentUnavailableView {
                        Label("Couldn't Load", systemImage: "exclamationmark.triangle")
                    } description: {
                        Text(message)
                    } actions: {
                        Button("Try Again") { Task { await detail.reload() } }
                    }
                case .loaded(let page):
                    NightPageView(page: page)
                        .refreshable { await detail.reload() }
                }
            } else {
                ProgressView()
            }
        }
        .navigationBarTitleDisplayMode(.inline)
        .task {
            if detail == nil { detail = model.nightDetail(startingAt: start) }
            if detail == nil {
                dismiss()
                return
            }
            await detail?.reload()
        }
        .onChange(of: detail?.state == .moved) { _, moved in
            if moved { dismiss() }
        }
    }
}

struct NightPageView: View {
    let page: SleepNightPage

    var body: some View {
        List {
            Section {
                LabeledContent("Bedtime", value: page.bedtimeText)
                LabeledContent("Wake time", value: page.wakeText)
                if let marker = page.uncertainty {
                    UncertaintyLabel(marker: marker, explanation: page.uncertaintyExplanation, showsExplanation: true)
                        .accessibilityElement(children: .combine)
                }
            } header: {
                Text(page.kindText)
            }

            Section("Stages") {
                if page.stages.isEmpty {
                    Text("No stages recorded.")
                        .foregroundStyle(.secondary)
                } else {
                    StageTimeline(page: page)
                }
            }

            Section("Totals") {
                ForEach(page.totals) { total in
                    LabeledContent(total.title, value: total.value)
                        .monospacedDigit()
                        .accessibilityElement(children: .ignore)
                        .accessibilityLabel(total.accessibilityLabel)
                }
            }
        }
        .navigationTitle(page.title)
    }
}

/// The stages as bars on a time axis, one row per kind with its name, coloured by kind with a
/// legend, so colour is never the only cue. Axis labels come from `HealthFormatter` on the night's
/// own clock, never the device's zone.
struct StageTimeline: View {
    let page: SleepNightPage
    private let formatter = HealthFormatter()
    /// Grows with the text size, so the stage names on the axis keep their rows.
    @ScaledMetric(relativeTo: .body) private var height: CGFloat = 180

    /// Top to bottom, the usual order for a sleep chart.
    private static let order: [SleepStageKind] = [.awake, .rem, .light, .deep, .nap]

    private var kinds: [SleepStageKind] {
        Self.order.filter { kind in page.stages.contains { $0.kind == kind } }
    }

    var body: some View {
        Chart(page.stages) { stage in
            BarMark(
                xStart: .value("Start", stage.start),
                xEnd: .value("End", stage.end),
                y: .value("Stage", stage.name))
                .foregroundStyle(by: .value("Stage", stage.name))
                .accessibilityLabel(stage.accessibilityLabel)
        }
        .chartForegroundStyleScale(
            domain: kinds.map(HealthFormatter.name(of:)), range: kinds.map(Self.color(for:)))
        .chartYScale(domain: kinds.map(HealthFormatter.name(of:)))
        .chartXAxis {
            // Whole hours of the night's own clock, computed in DigitCore, not the device zone's.
            AxisMarks(values: page.hourTicks) { value in
                AxisGridLine()
                AxisTick()
                AxisValueLabel {
                    if let date = value.as(Date.self) {
                        Text(formatter.time(date, utcOffsetSeconds: page.summary.utcOffsetSeconds))
                    }
                }
            }
        }
        .chartLegend(position: .bottom)
        // Anything Charts formats itself is on the night's clock too.
        .environment(\.timeZone, page.timeZone)
        .frame(minHeight: height)
        .padding(.vertical, 8)
    }

    static func color(for kind: SleepStageKind) -> Color {
        switch kind {
        case .deep: .indigo
        case .light: .cyan
        case .rem: .purple
        case .awake: .orange
        case .nap: .teal
        }
    }
}
