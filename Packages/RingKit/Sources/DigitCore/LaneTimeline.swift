import Foundation
import RingProtocol
import RingStore

/// A night drawn as lanes, one per stage present: where each stage occurs, in time order, so the
/// path through the night reads like a hypnogram. Positions are fractions of the night (0 at its
/// start, 1 at its end); the view only scales them. Times are on the night's own clock.
///
/// The R99 reports no awake stages, so the Awake lane shows the night's interruptions (stretches
/// no session covers), plus any awake stage a ring does report. There is no restlessness data, so
/// there is no restlessness lane.
public struct LaneTimeline: Sendable, Equatable {
    public enum LaneKind: Sendable, Equatable, Hashable, CaseIterable {
        case awake, rem, light, deep, nap

        init(_ kind: SleepStageKind) {
            switch kind {
            case .awake: self = .awake
            case .rem: self = .rem
            case .light: self = .light
            case .deep: self = .deep
            case .nap: self = .nap
            }
        }

        public var name: String {
            switch self {
            case .awake: "Awake"
            case .rem: "REM"
            case .light: "Light"
            case .deep: "Deep"
            case .nap: "Nap"
            }
        }
    }

    public struct Lane: Sendable, Equatable, Identifiable {
        public let kind: LaneKind
        /// "REM · 1h 10m": the stage's total over the night.
        public let title: String
        public let accessibilityLabel: String
        public var id: LaneKind { kind }
    }

    public struct Block: Sendable, Equatable, Identifiable {
        public let lane: LaneKind
        /// The lane's row, 0 at the top.
        public let laneIndex: Int
        public let start: Date
        public let end: Date
        public let startFraction: Double
        public let endFraction: Double
        /// "REM, 23:10 to 23:55" for the scrubber's header.
        public let headline: String
        /// "REM, 23:10 to 23:55, 45 minutes", for VoiceOver.
        public let accessibilityLabel: String
        /// Blocks are in time order; the position keeps two with one start apart.
        public let id: Int
    }

    /// Top to bottom.
    public static let order: [LaneKind] = [.awake, .rem, .light, .deep, .nap]

    public let start: Date
    public let end: Date
    public let lanes: [Lane]
    /// In time order (start, then lane order).
    public let blocks: [Block]
    public let bedtimeText: String
    public let midpointText: String
    public let wakeText: String

    public init(_ detail: SleepNightDetail, formatter: HealthFormatter) {
        let summary = detail.summary
        let offset = summary.utcOffsetSeconds
        start = summary.start
        end = summary.end
        let span = end.timeIntervalSince(start)
        func fraction(_ date: Date) -> Double {
            span > 0 ? min(max(date.timeIntervalSince(summary.start) / span, 0), 1) : 0
        }

        var raw: [(lane: LaneKind, start: Date, end: Date)] = detail.stages.map {
            (LaneKind($0.kind), $0.start, $0.start.addingTimeInterval(TimeInterval($0.durationSeconds)))
        }
        raw += summary.interruptions.map { (LaneKind.awake, $0.start, $0.end) }
        raw.sort { a, b in
            a.start != b.start ? a.start < b.start
                : Self.order.firstIndex(of: a.lane)! < Self.order.firstIndex(of: b.lane)!
        }

        var totals: [LaneKind: Int] = [:]
        for item in raw { totals[item.lane, default: 0] += max(Int(item.end.timeIntervalSince(item.start).rounded()), 0) }
        let present = Self.order.filter { totals[$0] != nil }
        lanes = present.map { kind in
            Lane(
                kind: kind, title: "\(kind.name) · \(formatter.compactDuration(seconds: totals[kind] ?? 0))",
                accessibilityLabel: "\(kind.name), \(formatter.spokenDuration(seconds: totals[kind] ?? 0)) in total")
        }
        blocks = raw.enumerated().map { index, item in
            let times = "\(formatter.time(item.start, utcOffsetSeconds: offset)) to \(formatter.time(item.end, utcOffsetSeconds: offset))"
            let seconds = max(Int(item.end.timeIntervalSince(item.start).rounded()), 0)
            return Block(
                lane: item.lane, laneIndex: present.firstIndex(of: item.lane)!, start: item.start, end: item.end,
                startFraction: fraction(item.start), endFraction: fraction(item.end),
                headline: "\(item.lane.name), \(times)",
                accessibilityLabel: "\(item.lane.name), \(times), \(formatter.spokenDuration(seconds: seconds))", id: index)
        }
        bedtimeText = formatter.time(summary.start, utcOffsetSeconds: offset)
        midpointText = formatter.time(summary.start.addingTimeInterval(span / 2), utcOffsetSeconds: offset)
        wakeText = formatter.time(summary.end, utcOffsetSeconds: offset)
    }

    /// The block under `fraction` of the night (the latest-starting one when blocks overlap), or
    /// the nearest block when none covers it (stages are a second or so apart); `nil` only for a
    /// night with no blocks.
    public func block(atFraction fraction: Double) -> Block? {
        if let covering = blocks.last(where: { $0.startFraction <= fraction && fraction <= $0.endFraction }) {
            return covering
        }
        return blocks.min { a, b in distance(a, fraction) < distance(b, fraction) }
    }

    private func distance(_ block: Block, _ fraction: Double) -> Double {
        fraction < block.startFraction ? block.startFraction - fraction : fraction - block.endFraction
    }

    /// The block after (`step` 1) or before (`step` -1) `block` in time order, for VoiceOver's
    /// adjust action; the first block when `block` is `nil`. Stays put at the ends.
    public func block(after block: Block?, step: Int) -> Block? {
        guard let block else { return blocks.first }
        let index = min(max(block.id + step, 0), blocks.count - 1)
        return blocks.indices.contains(index) ? blocks[index] : nil
    }

    /// "Bedtime 23:10, midpoint 03:05, wake time 07:00", for VoiceOver on the whole timeline.
    public var accessibilityLabel: String {
        let lanesText = lanes.map(\.accessibilityLabel).joined(separator: "; ")
        return "Sleep stages. Bedtime \(bedtimeText), midpoint \(midpointText), wake time \(wakeText). \(lanesText)"
    }
}
