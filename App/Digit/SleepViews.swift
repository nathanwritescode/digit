import DigitCore
import RingStore
import SwiftUI

// The Sleep tab (cards for the latest main sleep), the list of every night behind "See all
// nights", and a night's screen. Every value and time is computed and formatted by DigitCore on
// each night's own clock; nothing here formats a date or does arithmetic on health data.

/// The Sleep tab: the score and duration, last night's timeline, the key metrics, and a link to
/// every night of the last 30 days.
struct SleepTabView: View {
    let model: DigitModel

    private var overview: SleepOverviewModel { model.overview }

    var body: some View {
        DataStateView(state: overview.state, subject: "sleep", retry: reload) { overview in
            ScrollView {
                SleepOverviewContent(overview: overview)
                    .padding()
            }
            .background(Color(.systemGroupedBackground))
            .refreshable { await reload() }
        }
        .navigationTitle("Sleep")
        .navigationDestination(for: NightRoute.self) { route in
            NightScreen(model: model, start: route.start)
        }
        .navigationDestination(for: AllNightsRoute.self) { _ in
            SleepListView(model: model)
        }
    }

    private func reload() async {
        await overview.reload()
        await model.sleep.reload()
    }
}

/// The cards themselves.
struct SleepOverviewContent: View {
    let overview: SleepOverview

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            ReflowingPair {
                Card {
                    Text("Sleep score").font(.subheadline).foregroundStyle(.secondary)
                    HStack(alignment: .firstTextBaseline, spacing: 6) {
                        Text(overview.scoreValueText).font(.largeTitle.bold()).monospacedDigit()
                        Text(overview.scoreLabelText).font(.headline)
                    }
                    TrendLineChart(points: overview.scorePoints, yDomain: 0...100)
                }
                .accessibilityElement(children: .ignore)
                .accessibilityLabel(overview.scoreAccessibilityLabel)
            } second: {
                MetricCardView(card: overview.duration, style: .line)
            }

            NavigationLink(value: NightRoute(start: overview.night.start)) {
                Card {
                    HStack {
                        Text("Last night").font(.headline)
                        Spacer()
                        Image(systemName: "chevron.right").foregroundStyle(.tertiary).accessibilityHidden(true)
                    }
                    Text(overview.dayTitle).font(.subheadline).foregroundStyle(.secondary)
                    LaneTimelineView(timeline: overview.timeline)
                    TimelineTimes(timeline: overview.timeline, showsMidpoint: false)
                    if let marker = overview.uncertainty {
                        UncertaintyLabel(marker: marker, explanation: nil)
                    }
                }
                .accessibilityElement(children: .combine)
            }
            .buttonStyle(.plain)
            .accessibilityHint("Shows the night's score, stages and sleep quality.")

            Text("Key metrics").font(.title3.bold())
                .accessibilityAddTraits(.isHeader)
            MetricCardView(card: overview.asleep, style: .bars)
            SleepWindowCardView(card: overview.window)
            MetricCardView(card: overview.rem, style: .bars)
            MetricCardView(card: overview.deep, style: .bars)
            MetricCardView(card: overview.efficiency, style: .bars)
            Text(overview.legend).font(.footnote).foregroundStyle(.secondary)
            Text(overview.scoreDisclaimer).font(.footnote).foregroundStyle(.secondary)

            NavigationLink(value: AllNightsRoute()) {
                Card {
                    HStack {
                        Text("See all nights")
                        Spacer()
                        Image(systemName: "chevron.right").foregroundStyle(.tertiary).accessibilityHidden(true)
                    }
                }
            }
            .buttonStyle(.plain)
            .accessibilityHint("Lists every night and nap of the last 30 days.")
        }
    }
}

/// A key metric: title, the night's value, its tag and a 7-night chart.
struct MetricCardView: View {
    enum Style { case line, bars }
    let card: MetricCard
    let style: Style

    var body: some View {
        Card {
            Text(card.title).font(.subheadline).foregroundStyle(.secondary)
            Text(card.valueText).font(.title2.bold()).monospacedDigit()
            if let detail = card.detailText {
                Text(detail).font(.caption).foregroundStyle(.secondary)
            }
            if let comparison = card.comparison { ComparisonTag(comparison: comparison) }
            switch style {
            case .line: TrendLineChart(points: card.points, band: card.band, yDomain: card.yDomain)
            case .bars: TrendBarChart(points: card.points, band: card.band, yDomain: card.yDomain)
            }
        }
        .accessibilityElement(children: .ignore)
        .accessibilityLabel(card.accessibilityLabel)
    }
}

struct SleepWindowCardView: View {
    let card: SleepWindowCard

    var body: some View {
        Card {
            Text(card.title).font(.subheadline).foregroundStyle(.secondary)
            Text(card.valueText).font(.title2.bold()).monospacedDigit()
            if let comparison = card.comparison { ComparisonTag(comparison: comparison) }
            SleepWindowChart(card: card)
        }
        .accessibilityElement(children: .ignore)
        .accessibilityLabel(card.accessibilityLabel)
    }
}

/// The list of every night's navigation value.
struct AllNightsRoute: Hashable {}

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
        .navigationTitle("All Nights")
    }
}

/// A night's navigation value: found again by its start.
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

/// One night: its score, stages, quality and more. Previous and next move between main sleeps.
/// Returns to the list when the night has moved (ingestion changed its start since the list was
/// read).
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
                    ScrollView {
                        NightPageView(page: page, detail: detail)
                            .padding()
                    }
                    .background(Color(.systemGroupedBackground))
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
    /// For previous and next.
    let detail: NightDetailModel

    @State private var selected: LaneTimeline.Block?
    @State private var showsFormula = false

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            header
            scoreCard
            stagesCard
            if !page.quality.isEmpty { qualityCard }
            moreCard
        }
        .navigationTitle(page.kindText)
        .onChange(of: page.summary.start) { selected = nil }
        .sheet(isPresented: $showsFormula) { formula }
    }

    private var header: some View {
        HStack {
            Button {
                Task { await detail.showPrevious() }
            } label: {
                Image(systemName: "chevron.left")
            }
            .disabled(detail.previousStart == nil)
            .accessibilityLabel("Previous night")
            Spacer()
            Text(page.title)
                .font(.headline)
                .multilineTextAlignment(.center)
                .accessibilityAddTraits(.isHeader)
            Spacer()
            Button {
                Task { await detail.showNext() }
            } label: {
                Image(systemName: "chevron.right")
            }
            .disabled(detail.nextStart == nil)
            .accessibilityLabel("Next night")
        }
        .buttonStyle(.bordered)
        .buttonBorderShape(.circle)
    }

    private var scoreCard: some View {
        Card {
            HStack(alignment: .firstTextBaseline) {
                Text("Sleep score").font(.headline)
                Spacer()
                Button {
                    showsFormula = true
                } label: {
                    Image(systemName: "info.circle")
                }
                .accessibilityLabel("How the score is worked out")
            }
            if let score = page.score {
                HStack(alignment: .firstTextBaseline, spacing: 8) {
                    Text(score.totalText).font(.system(.largeTitle, design: .rounded).bold()).monospacedDigit()
                    Text(score.labelText).font(.title3)
                }
                .accessibilityElement(children: .ignore)
                .accessibilityLabel(score.accessibilityLabel)
                LazyVGrid(columns: [GridItem(.adaptive(minimum: 130), alignment: .leading)], alignment: .leading, spacing: 8) {
                    ForEach(score.chips) { chip in
                        VStack(alignment: .leading, spacing: 2) {
                            Text(chip.title).font(.caption).foregroundStyle(.secondary)
                            Text(chip.pointsText).font(.subheadline.weight(.semibold)).monospacedDigit()
                        }
                        .padding(.horizontal, 12)
                        .padding(.vertical, 6)
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .background(Color(.tertiarySystemFill), in: RoundedRectangle(cornerRadius: 12, style: .continuous))
                        .accessibilityElement(children: .ignore)
                        .accessibilityLabel(chip.accessibilityLabel)
                    }
                }
                if let note = score.consistencyNote {
                    Text(note).font(.footnote).foregroundStyle(.secondary)
                }
                Text(score.disclaimer).font(.footnote).foregroundStyle(.secondary)
            } else if let text = page.noScoreText {
                Text(text).font(.subheadline).foregroundStyle(.secondary)
            }
        }
    }

    private var stagesCard: some View {
        Card {
            Text("Sleep stages").font(.headline)
            if page.timeline.blocks.isEmpty {
                Text("No stages recorded.").foregroundStyle(.secondary)
            } else {
                Text(selected?.headline ?? "Drag across the timeline to see each stage.")
                    .font(.subheadline)
                    .foregroundStyle(selected == nil ? .secondary : .primary)
                    .monospacedDigit()
                    .accessibilityHidden(true)
                LaneTimelineView(timeline: page.timeline, selection: $selected, large: true)
                TimelineTimes(timeline: page.timeline)
            }
        }
    }

    private var qualityCard: some View {
        Card {
            Text("Sleep quality").font(.headline)
            ForEach(page.quality) { bar in
                RangeBarView(bar: bar)
                    .padding(.vertical, 2)
            }
            Text(page.qualityLegend).font(.footnote).foregroundStyle(.secondary)
        }
    }

    private var moreCard: some View {
        Card {
            Text("More about this night").font(.headline)
            LabeledContent("Sleeping heart rate", value: page.sleepingHeartRateText)
                .monospacedDigit()
                .accessibilityElement(children: .ignore)
                .accessibilityLabel(page.sleepingHeartRateAccessibilityLabel)
            ForEach(page.totals) { total in
                LabeledContent(total.title, value: total.value)
                    .monospacedDigit()
                    .accessibilityElement(children: .ignore)
                    .accessibilityLabel(total.accessibilityLabel)
            }
            if let marker = page.uncertainty {
                UncertaintyLabel(marker: marker, explanation: page.uncertaintyExplanation, showsExplanation: true)
                    .accessibilityElement(children: .combine)
            }
        }
    }

    private var formula: some View {
        NavigationStack {
            List {
                Section {
                    ForEach(SleepScore.explanation, id: \.self) { Text($0) }
                } footer: {
                    Text(SleepScore.disclaimer)
                }
            }
            .navigationTitle("Digit's Sleep Score")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Done") { showsFormula = false }
                }
            }
        }
    }
}
