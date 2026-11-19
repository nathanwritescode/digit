import Foundation
import RingProtocol
@testable import RingStore

// A varied, deterministic SYNTHETIC history for the derivation tests (#69): every byte is built here
// from the documented record layouts with made-up values from a seeded generator; none come from a
// real ring. It covers the cases ingestion distinguishes: offset changes (clock-sets at the end of
// each day, +1 h then +2 h on days 2 and 3), a heart-rate buffer reset (a transfer that is not a
// prefix of the next), a fetch with trailing garbage, duplicate seconds (collisions), 0 and 255 bpm,
// zero-length and inverted activity buckets, a rewritten sleep session (superseded), unknown sleep
// stage kinds (skipped), a sleep session from a ring whose clock was never set (implausible),
// combined records with nothing measured, and one dated FF FF FF FF (future).

/// The UTC offset the ring's clock is set to for day `day`.
func variedOffset(day: Int) -> Int { (2...3).contains(day) ? 7200 : 3600 }

struct VariedHistory {
    let fixture: Fixture
    /// Seconds between heart-rate readings (and twice that between combined records).
    var readingSpacing: TimeInterval = 600
    private var rng: SplitMix64
    private var buffers: [HistoryKind: [UInt8]] = [:]
    /// The latest sleep session written, so a later day can rewrite it.
    private var lastSession: (start: UInt32, end: UInt32, stages: [(kind: UInt8, start: UInt32, seconds: Int)])?

    var store: RingStore { fixture.store }
    var ringID: UUID { fixture.ring.id }

    static func dayStart(_ day: Int) -> Date { t0.addingTimeInterval(TimeInterval(day) * 86_400) }

    /// Registers a ring, logs empty first fetches and the first clock-sets (as `clockedFixture`).
    init(_ store: RingStore, seed: UInt64 = 69, readingSpacing: TimeInterval = 600) throws {
        fixture = try Fixture(store)
        rng = SplitMix64(seed: seed)
        self.readingSpacing = readingSpacing
        for kind in RingStore.ingestibleKinds {
            try fixture.fetch(kind, [], at: t0.addingTimeInterval(-200))
            buffers[kind] = []
        }
        try fixture.setEarlierAndFirstClocks(at: t0.addingTimeInterval(-100), offset: variedOffset(day: 0))
    }

    private mutating func roll(_ n: UInt64) -> Int { Int(rng.next() % n) }

    /// Adds one day of records, fetches every kind at the end of it, then sets the clock for the
    /// next day.
    mutating func addDay(_ day: Int) throws {
        let start = Self.dayStart(day)
        let offset = variedOffset(day: day)
        func ring(_ seconds: TimeInterval) -> UInt32 { wall(start.addingTimeInterval(seconds), offset: offset) }

        if day == 3 { buffers[.heartRate] = [] }  // the buffer was reset

        // Heart rate.
        var hr: [UInt8] = []
        var s: TimeInterval = 0
        while s < 86_000 {
            let r = roll(100)
            let bpm: UInt8 = r < 2 ? 0 : r < 3 ? 255 : UInt8(45 + roll(120))
            hr += hrBytes(ring(s), bpm: bpm)
            if r == 99 { hr += hrBytes(ring(s), bpm: UInt8(50 + roll(50))) }  // two in one second
            s += readingSpacing
        }
        buffers[.heartRate]! += hr

        // Activity: 30-minute buckets, some omitted, one zero-length, one inverted.
        var activity: [UInt8] = []
        for bucket in 0..<47 {
            let b = TimeInterval(bucket) * 1800
            if roll(5) == 0 { continue }
            let end: UInt32 = bucket == 7 ? ring(b) : bucket == 9 ? ring(b) - 60 : ring(b + 1800)
            activity += activityBytes(start: ring(b), end: end, steps: roll(2000), distance: roll(1500), kcal: roll(90))
        }
        buffers[.activity]! += activity

        // Sleep: one session, with an unknown stage kind now and then.
        var stages: [(kind: UInt8, start: UInt32, seconds: Int)] = []
        var cursor: TimeInterval = 3600
        for _ in 0..<(4 + roll(5)) {
            let seconds = 600 + roll(3000)
            let kind: UInt8 = roll(10) == 0 ? 0x99 : UInt8(241 + roll(5))
            stages.append((kind, ring(cursor), seconds))
            cursor += TimeInterval(seconds) + 1
        }
        let session = sleepBytes(start: ring(3600), end: ring(cursor - 1), stages: stages)
        buffers[.sleep]! += session
        if day == 1 {
            // The ring rewrote the session: the same start, one more stage, later in the buffer.
            let extra = (kind: UInt8(242), start: ring(cursor), seconds: 900)
            buffers[.sleep]! += sleepBytes(start: ring(3600), end: ring(cursor + 899), stages: stages + [extra])
        }
        if day == 4, let last = lastSession {
            // The previous night rewritten a day later, so an ingest after this fetch updates its row.
            let extra = (kind: UInt8(243), start: last.end + 1, seconds: 600)
            buffers[.sleep]! += sleepBytes(start: last.start, end: last.end + 600, stages: last.stages + [extra])
        }
        lastSession = (ring(3600), ring(cursor - 1), stages)
        if day == 4 {
            // Stamped by a ring whose clock was never set (2000-01-01 plus a few hours).
            buffers[.sleep]! += sleepBytes(start: 10_000, end: 13_600, stages: [(242, 10_000, 3600)])
        }

        // Combined records.
        var combined: [UInt8] = []
        s = 300
        while s < 86_000 {
            if roll(10) == 0 {
                combined += combinedBytes(ring(s), steps: 10, hr: 70)  // nothing measured
            } else {
                combined += combinedBytes(
                    ring(s), spo2: UInt8(roll(4) == 0 ? 0 : 90 + roll(10)), resp: UInt8(12 + roll(8)),
                    hrv: UInt8(20 + roll(60)), cvrr: UInt8(roll(3) == 0 ? 0 : 10 + roll(40)))
            }
            s += readingSpacing * 2
        }
        if day == 2 { combined += combinedBytes(0xFFFF_FFFF, spo2: 97) }
        buffers[.combined]! += combined

        let fetchedAt = start.addingTimeInterval(86_340)
        for kind in RingStore.ingestibleKinds {
            var data = buffers[kind]!
            if day == 2 && kind == .heartRate { data += [0xAB, 0xCD, 0xEF] }  // a fetch cut short
            try fixture.fetch(kind, data, at: fetchedAt)
        }
        try fixture.setClock(at: start.addingTimeInterval(86_350), offset: variedOffset(day: day + 1))
    }

    mutating func addDays(_ days: Range<Int>) throws {
        for day in days { try addDay(day) }
    }
}

/// Every typed row of a ring, with ids, as one comparable value.
struct TypedSnapshot: Equatable {
    var heartRate: [StoredRow<HeartRateReading>]
    var activity: [StoredRow<ActivityInterval>]
    var sleep: [StoredRow<SleepSessionRecord>]
    var vitals: [StoredRow<VitalsReading>]

    init(_ store: RingStore, ringID: UUID) throws {
        heartRate = try store.heartRateReadings(ringID: ringID)
        activity = try store.activityIntervals(ringID: ringID)
        sleep = try store.sleepSessions(ringID: ringID)
        vitals = try store.vitalsReadings(ringID: ringID)
    }

    var rowCount: Int { heartRate.count + activity.count + sleep.count + vitals.count }

    /// One line per row, ids included, in a stable order.
    var canonical: String {
        var lines: [String] = []
        for r in heartRate {
            let v = r.value
            lines.append("hr \(r.id) \(ms(v.time)) \(v.bpm) \(v.mode) \(v.utcOffsetSeconds) \(v.ringLocalSeconds) \(v.timeSource)")
        }
        for r in activity {
            let v = r.value
            lines.append("act \(r.id) \(ms(v.start)) \(ms(v.end)) \(v.steps) \(v.distanceMeters) \(v.kilocalories) \(v.utcOffsetSeconds) \(v.ringLocalSeconds) \(v.timeSource)")
        }
        for r in sleep {
            let v = r.value
            let stages = v.stages.map { "\($0.kind):\(ms($0.start)):\($0.durationSeconds)" }.joined(separator: ",")
            lines.append("sleep \(r.id) \(ms(v.start)) \(ms(v.end)) \(v.utcOffsetSeconds) \(v.ringLocalSeconds) \(v.timeSource) [\(stages)]")
        }
        for r in vitals {
            let v = r.value
            lines.append("vit \(r.id) \(ms(v.time)) \(v.spo2.map(String.init) ?? "-") \(v.respirationRate.map(String.init) ?? "-") \(v.hrv.map(String.init) ?? "-") \(v.cvrr.map(String.init) ?? "-") \(v.utcOffsetSeconds) \(v.ringLocalSeconds) \(v.timeSource)")
        }
        return lines.joined(separator: "\n")
    }

    private func ms(_ date: Date) -> Int64 { HistoryTransfer.millisecondsSince1970(date) }
}

extension IngestionResult {
    /// Every field, in a stable order.
    var canonical: String {
        let sources = TimeSource.allCases.map { "\($0)=\(rowsByTimeSource[$0] ?? 0)" }.joined(separator: ",")
        return "ins=\(inserted) upd=\(updated) del=\(deleted) unch=\(unchanged) impl=\(rejectedImplausible) "
            + "unus=\(unusableBytes) coll=\(collisions) sup=\(superseded) fut=\(rejectedFuture) "
            + "skip=\(skippedSleepStages) novit=\(withoutVitals) [\(sources)]"
    }
}
