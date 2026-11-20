import Charts
import DigitCore
import SwiftUI
import UIKit

// Shared pieces of the Sleep tab and the night screen: cards, tags, the lane timeline, the small
// 7-night charts and the range bars. They only lay out values DigitCore has computed and
// formatted; nothing here does arithmetic on health data or formats a date.

extension LaneTimeline.LaneKind {
    /// The lane's colour from `StagePalette`, the one place the stage colours are defined.
    var color: Color {
        let pair = StagePalette.colors(for: self)
        return Color(
            uiColor: UIColor { traits in
                let rgb = traits.userInterfaceStyle == .dark ? pair.dark : pair.light
                return UIColor(red: rgb.red, green: rgb.green, blue: rgb.blue, alpha: 1)
            })
    }
}

/// A rounded card on the grouped background, in light and dark mode.
struct Card<Content: View>: View {
    @ViewBuilder let content: Content

    var body: some View {
        VStack(alignment: .leading, spacing: 8) { content }
            .padding()
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(Color(.secondarySystemGroupedBackground), in: RoundedRectangle(cornerRadius: 16, style: .continuous))
    }
}

/// Side by side while there is room, one column at accessibility text sizes.
struct ReflowingPair<First: View, Second: View>: View {
    @Environment(\.dynamicTypeSize) private var size
    @ViewBuilder let first: First
    @ViewBuilder let second: Second

    var body: some View {
        if size.isAccessibilitySize {
            VStack(spacing: 12) { first; second }
        } else {
            HStack(alignment: .top, spacing: 12) { first; second }
        }
    }
}

/// "Usual", "Above your usual", "Below your usual" or "Building your usual range": neutral for
/// usual and building, the one accent colour outside the range. Words, so colour is never the only
/// cue.
struct ComparisonTag: View {
    let comparison: PersonalRange.Comparison

    var body: some View {
        Text(comparison.text)
            .font(.caption.weight(.medium))
            .foregroundStyle(comparison.isOutside ? AnyShapeStyle(.tint) : AnyShapeStyle(.secondary))
    }
}

// MARK: Lane timeline

/// The night as lanes: a rounded track per stage with its total above it, blocks where the stage
/// occurs, and thin connectors between consecutive blocks so the path reads like a hypnogram.
/// With `selection`, dragging (or VoiceOver's adjust action) picks the block under a point.
struct LaneTimelineView: View {
    let timeline: LaneTimeline
    /// The block picked by the scrubber; `nil` for a timeline that cannot be scrubbed.
    var selection: Binding<LaneTimeline.Block?>?
    var large = false

    @ScaledMetric(relativeTo: .caption) private var labelHeight: CGFloat = 18
    @ScaledMetric(relativeTo: .caption) private var compactTrack: CGFloat = 12
    @ScaledMetric(relativeTo: .caption) private var largeTrack: CGFloat = 22
    @Environment(\.colorScheme) private var colorScheme

    private var trackHeight: CGFloat { large ? largeTrack : compactTrack }
    private var rowHeight: CGFloat { labelHeight + trackHeight + 6 }

    var body: some View {
        let canvas = Canvas { context, size in draw(in: &context, size: size) }
            .frame(height: rowHeight * CGFloat(max(timeline.lanes.count, 1)))
            .overlay(alignment: .topLeading) { scrubLine }
        if let selection {
            canvas
                .contentShape(Rectangle())
                .gesture(
                    DragGesture(minimumDistance: 0).onChanged { value in
                        // The fraction of the width; DigitCore finds the block there.
                        selection.wrappedValue = timeline.block(atFraction: value.location.x / max(width, 1))
                    }
                )
                .accessibilityElement(children: .ignore)
                .accessibilityLabel(timeline.accessibilityLabel)
                .accessibilityValue(selection.wrappedValue?.accessibilityLabel ?? "")
                .accessibilityHint("Swipe up or down to move between stages.")
                .accessibilityAdjustableAction { direction in
                    switch direction {
                    case .increment: selection.wrappedValue = timeline.block(after: selection.wrappedValue, step: 1)
                    case .decrement: selection.wrappedValue = timeline.block(after: selection.wrappedValue, step: -1)
                    @unknown default: break
                    }
                }
                .onGeometryChange(for: CGFloat.self, of: { $0.size.width }) { width = $0 }
        } else {
            canvas
                .accessibilityElement(children: .ignore)
                .accessibilityLabel(timeline.accessibilityLabel)
        }
    }

    @State private var width: CGFloat = 0

    @ViewBuilder private var scrubLine: some View {
        if let block = selection?.wrappedValue {
            GeometryReader { proxy in
                let x = proxy.size.width * (block.startFraction + block.endFraction) / 2
                Rectangle()
                    .fill(.primary.opacity(0.6))
                    .frame(width: 1.5)
                    .offset(x: x)
            }
            .allowsHitTesting(false)
            .accessibilityHidden(true)
        }
    }

    private func trackRect(_ index: Int, width: CGFloat) -> CGRect {
        CGRect(x: 0, y: CGFloat(index) * rowHeight + labelHeight, width: width, height: trackHeight)
    }

    private func draw(in context: inout GraphicsContext, size: CGSize) {
        let track = Color(.tertiarySystemFill)
        for (index, lane) in timeline.lanes.enumerated() {
            context.draw(
                Text(lane.title).font(.caption).foregroundStyle(.secondary),
                at: CGPoint(x: 0, y: CGFloat(index) * rowHeight + labelHeight / 2), anchor: .leading)
            let rect = trackRect(index, width: size.width)
            context.fill(Path(roundedRect: rect, cornerRadius: rect.height / 2), with: .color(track))
        }
        // Connectors first, so the blocks sit on top of them.
        for (a, b) in zip(timeline.blocks, timeline.blocks.dropFirst()) where a.laneIndex != b.laneIndex {
            let x = size.width * a.endFraction
            let from = trackRect(a.laneIndex, width: size.width).midY
            let to = trackRect(b.laneIndex, width: size.width).midY
            var path = Path()
            path.move(to: CGPoint(x: x, y: from))
            path.addLine(to: CGPoint(x: x, y: to))
            context.stroke(path, with: .color(.secondary.opacity(0.5)), lineWidth: 1)
        }
        for block in timeline.blocks {
            let lane = trackRect(block.laneIndex, width: size.width)
            let x = size.width * block.startFraction
            let w = max(size.width * (block.endFraction - block.startFraction), 2)
            let rect = CGRect(x: x, y: lane.minY, width: w, height: lane.height)
            var color = block.lane.color
            if let selected = selection?.wrappedValue, selected.id != block.id { color = color.opacity(0.45) }
            context.fill(Path(roundedRect: rect, cornerRadius: min(lane.height / 2, w / 2)), with: .color(color))
        }
    }
}

/// Bedtime, midpoint and wake time under a timeline.
struct TimelineTimes: View {
    let timeline: LaneTimeline
    var showsMidpoint = true

    var body: some View {
        ViewThatFits(in: .horizontal) {
            HStack {
                item("Bedtime", timeline.bedtimeText, alignment: .leading)
                Spacer()
                if showsMidpoint { item("Midpoint", timeline.midpointText, alignment: .center) }
                Spacer()
                item("Wake time", timeline.wakeText, alignment: .trailing)
            }
            VStack(alignment: .leading, spacing: 6) {
                item("Bedtime", timeline.bedtimeText, alignment: .leading)
                if showsMidpoint { item("Midpoint", timeline.midpointText, alignment: .leading) }
                item("Wake time", timeline.wakeText, alignment: .leading)
            }
        }
    }

    private func item(_ title: String, _ value: String, alignment: HorizontalAlignment) -> some View {
        VStack(alignment: alignment, spacing: 2) {
            Text(title).font(.caption).foregroundStyle(.secondary)
            Text(value).font(.subheadline.weight(.semibold)).monospacedDigit()
        }
        .accessibilityElement(children: .combine)
    }
}

// MARK: 7-night charts

extension View {
    /// Day letters on the x axis, positions 0 to 6, the highlighted day in bold.
    fileprivate func dayAxis(_ letters: [(letter: String, highlighted: Bool)]) -> some View {
        chartXScale(domain: -0.5...(Double(letters.count) - 0.5))
            .chartXAxis {
                AxisMarks(values: Array(letters.indices)) { value in
                    AxisValueLabel(centered: false) {
                        if let index = value.as(Int.self), letters.indices.contains(index) {
                            Text(letters[index].letter)
                                .fontWeight(letters[index].highlighted ? .bold : .regular)
                                .foregroundStyle(letters[index].highlighted ? AnyShapeStyle(.primary) : AnyShapeStyle(.secondary))
                        }
                    }
                }
            }
            .chartYAxis(.hidden)
    }
}

/// A 7-night line: a gap on days without a main sleep, the usual range shaded when there is one.
struct TrendLineChart: View {
    let points: [TrendPoint]
    var band: ClosedRange<Double>?
    let yDomain: ClosedRange<Double>
    @ScaledMetric(relativeTo: .caption) private var height: CGFloat = 64

    var body: some View {
        Chart {
            if let band {
                RectangleMark(
                    xStart: .value("Start", -0.5), xEnd: .value("End", Double(points.count) - 0.5),
                    yStart: .value("Usual low", band.lowerBound), yEnd: .value("Usual high", band.upperBound))
                    .foregroundStyle(.secondary.opacity(0.18))
            }
            ForEach(Array(points.enumerated()), id: \.element.id) { index, point in
                if let value = point.value {
                    LineMark(x: .value("Day", index), y: .value("Value", value), series: .value("Run", point.segment))
                        .foregroundStyle(.tint)
                    PointMark(x: .value("Day", index), y: .value("Value", value))
                        .symbolSize(point.isHighlighted ? 60 : 20)
                        .foregroundStyle(.tint)
                }
            }
        }
        .chartYScale(domain: yDomain)
        .dayAxis(points.map { ($0.letter, $0.isHighlighted) })
        .frame(height: height)
        .accessibilityHidden(true)
    }
}

/// 7-night bars: a gap on days without a main sleep, the usual range shaded when there is one.
struct TrendBarChart: View {
    let points: [TrendPoint]
    var band: ClosedRange<Double>?
    let yDomain: ClosedRange<Double>
    @ScaledMetric(relativeTo: .caption) private var height: CGFloat = 64
    @ScaledMetric(relativeTo: .caption) private var barWidth: CGFloat = 12

    var body: some View {
        Chart {
            if let band {
                RectangleMark(
                    xStart: .value("Start", -0.5), xEnd: .value("End", Double(points.count) - 0.5),
                    yStart: .value("Usual low", band.lowerBound), yEnd: .value("Usual high", band.upperBound))
                    .foregroundStyle(.secondary.opacity(0.18))
            }
            ForEach(Array(points.enumerated()), id: \.element.id) { index, point in
                if let value = point.value {
                    BarMark(x: .value("Day", index), y: .value("Value", value), width: .fixed(barWidth))
                        .foregroundStyle(point.isHighlighted ? AnyShapeStyle(.tint) : AnyShapeStyle(.tint.opacity(0.4)))
                        .clipShape(Capsule())
                }
            }
        }
        .chartYScale(domain: yDomain)
        .dayAxis(points.map { ($0.letter, $0.isHighlighted) })
        .frame(height: height)
        .accessibilityHidden(true)
    }
}

/// Bedtime-to-wake bars for seven nights, evening at the top.
struct SleepWindowChart: View {
    let card: SleepWindowCard
    @ScaledMetric(relativeTo: .caption) private var height: CGFloat = 96
    @ScaledMetric(relativeTo: .caption) private var barWidth: CGFloat = 12

    var body: some View {
        Chart {
            ForEach(Array(card.bars.enumerated()), id: \.element.id) { index, bar in
                if let from = bar.from, let to = bar.to {
                    // Negated so that time runs down the chart.
                    BarMark(x: .value("Day", index), yStart: .value("Bedtime", -from), yEnd: .value("Wake", -to), width: .fixed(barWidth))
                        .foregroundStyle(bar.isHighlighted ? AnyShapeStyle(.tint) : AnyShapeStyle(.tint.opacity(0.4)))
                        .clipShape(Capsule())
                }
            }
        }
        .chartYScale(domain: -card.yDomain.upperBound...(-card.yDomain.lowerBound))
        .dayAxis(card.bars.map { ($0.letter, $0.isHighlighted) })
        .chartYAxis {
            AxisMarks(position: .trailing, values: card.ticks.map { -$0.hours }) { value in
                AxisGridLine()
                AxisValueLabel {
                    if let y = value.as(Double.self), let tick = card.ticks.first(where: { -$0.hours == y }) {
                        Text(tick.label).font(.caption2)
                    }
                }
            }
        }
        .frame(height: height)
        .accessibilityHidden(true)
    }
}

// MARK: Range bars

/// A track with the usual range as a dashed band and a marker for the night.
struct RangeBarView: View {
    let bar: RangeBar
    @ScaledMetric(relativeTo: .caption) private var trackHeight: CGFloat = 10

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            ViewThatFits(in: .horizontal) {
                HStack(alignment: .firstTextBaseline) {
                    Text(bar.title).font(.subheadline)
                    Spacer()
                    Text(bar.valueText).font(.subheadline.weight(.semibold)).monospacedDigit()
                }
                VStack(alignment: .leading, spacing: 2) {
                    Text(bar.title).font(.subheadline)
                    Text(bar.valueText).font(.subheadline.weight(.semibold)).monospacedDigit()
                }
            }
            GeometryReader { proxy in
                let w = proxy.size.width
                ZStack(alignment: .leading) {
                    Capsule().fill(Color(.tertiarySystemFill)).frame(height: trackHeight)
                    if let band = bar.band {
                        RoundedRectangle(cornerRadius: trackHeight / 2)
                            .strokeBorder(style: StrokeStyle(lineWidth: 1.5, dash: [4, 3]))
                            .foregroundStyle(.secondary)
                            .background(RoundedRectangle(cornerRadius: trackHeight / 2).fill(.secondary.opacity(0.15)))
                            .frame(width: max(w * (band.upperBound - band.lowerBound), 4), height: trackHeight)
                            .offset(x: w * band.lowerBound)
                    }
                    if let marker = bar.marker {
                        Circle()
                            .fill(bar.comparison?.isOutside == true ? AnyShapeStyle(.tint) : AnyShapeStyle(.primary))
                            .frame(width: trackHeight * 1.6, height: trackHeight * 1.6)
                            .offset(x: w * marker - trackHeight * 0.8)
                    }
                }
            }
            .frame(height: trackHeight * 1.6)
            if let comparison = bar.comparison { ComparisonTag(comparison: comparison) }
        }
        .accessibilityElement(children: .ignore)
        .accessibilityLabel(bar.accessibilityLabel)
    }
}
