import Foundation
import Testing
@testable import RingProtocol

// Whitespace is allowed in test literals for readability; Hex.decode itself is strict.
private func hex(_ s: String) -> [UInt8] {
    Hex.decode(s.filter { !$0.isWhitespace })!
}

private let utc = TimeZone(identifier: "UTC")!

@Test func crcCheckValue() {
    #expect(CRC16.ccittFalse(Array("123456789".utf8)) == 0x29B1)
}

// Command frames exactly as accepted by an R99 ring. They carry no device or health data.
@Test func encodesCommandsAsCapturedOnTheWire() {
    #expect(Command.deviceInfo.encoded() == hex("0200080047436fec"))
    #expect(Command.currentActivity.encoded() == hex("020c06006fb6"))
    #expect(Command.history(.sleep).encoded() == hex("05040600e34e"))
    #expect(Command.history(.activity).encoded() == hex("0502060043fc"))
    #expect(Command.history(.heartRate).encoded() == hex("050606008320"))
    #expect(Command.history(.combined).encoded() == hex("05090600b20c"))
    #expect(Command.historyTransferOK.encoded() == hex("0580070000f36a"))
}

@Test func encodesSetTime() {
    var calendar = Calendar(identifier: .gregorian)
    calendar.timeZone = utc
    // Monday 2026-01-05 08:30:00. The payload layout was accepted by an R99.
    let date = calendar.date(from: DateComponents(year: 2026, month: 1, day: 5, hour: 8, minute: 30, second: 0))!
    let frame = Command.setTime(date, timeZone: utc)
    #expect(frame.type == 0x0100)
    #expect(frame.payload == hex("ea07 01 05 08 1e 00 00"))
    // Monday is 0 and Sunday is 6.
    let sunday = calendar.date(from: DateComponents(year: 2026, month: 11, day: 15))!
    #expect(Command.setTime(sunday, timeZone: utc).payload.last == 6)
    #expect(Command.setTime(sunday.addingTimeInterval(86400), timeZone: utc).payload.last == 0)
}

@Test func deleteRangeIsFlaggedAndNotOnTheReadOnlyList() {
    for key in UInt8(0x40)...0x52 {
        let frame = Frame(group: 0x05, key: key)
        #expect(frame.erasesRingData)
        #expect(!Command.knownReadOnlyTypes.contains(frame.type))
    }
    #expect(!Command.history(.sleep).erasesRingData)
    #expect(!Command.historyTransferOK.erasesRingData)
    #expect(!Frame(group: 0x04, key: 0x40).erasesRingData)
}

@Test func readOnlyAllowlistChecksThePayloadToo() {
    #expect(Command.isKnownReadOnly(Command.deviceInfo))
    #expect(Command.isKnownReadOnly(Command.currentActivity))
    #expect(Command.isKnownReadOnly(Command.history(.sleep)))
    #expect(!Command.isKnownReadOnly(Frame(group: 0x02, key: 0x00)))
    #expect(!Command.isKnownReadOnly(Frame(group: 0x05, key: 0x04, payload: [0x01])))
    #expect(!Command.isKnownReadOnly(Command.historyTransferOK))
    #expect(!Command.isKnownReadOnly(Frame(group: 0x01, key: 0x00)))
}

@Test func assemblerDropsImplausibleLengthsAndRecovers() {
    var assembler = FrameAssembler()
    #expect(assembler.append(hex("0511ffff0000")).isEmpty)
    #expect(assembler.append(hex("05110300")).isEmpty)
    #expect(assembler.append(Command.deviceInfo.encoded()) == [Command.deviceInfo])
}

@Test func assemblerRejoinsSplitFramesAndSplitsJoinedOnes() {
    let reply = Frame(group: 0x02, key: 0x00, payload: hex("a30020020064000000000000000000000000000000000000"))
    let wire = reply.encoded() + Command.historyTransferOK.encoded()
    var assembler = FrameAssembler()
    #expect(assembler.append(Array(wire[..<5])).isEmpty)
    #expect(assembler.append(Array(wire[5...])) == [reply, Command.historyTransferOK])
}

@Test func assemblerDropsFramesWithBadChecksum() {
    var bad = Command.deviceInfo.encoded()
    bad[4] ^= 0xFF
    var assembler = FrameAssembler()
    #expect(assembler.append(bad + Command.currentActivity.encoded()) == [Command.currentActivity])
    #expect(assembler.crcErrors == 1)
}

@Test func errorReply() {
    #expect(Frame(group: 0x02, key: 0x20, payload: [0xFC]).isErrorReply)
    #expect(!Frame(group: 0x01, key: 0x00, payload: [0x00]).isErrorReply)
}

@Test func decodesDeviceInfo() {
    let info = DeviceInfo(payload: hex("a30020020064000000000000000000000000000000000000"))
    #expect(info?.deviceID == 0x00A3)
    #expect(info?.firmwareVersion == "2.32")
    #expect(info?.batteryPercent == 100)
}

// Layout confirmed against an R99 and its pedometer service; the values here are synthetic.
@Test func decodesCurrentActivity() {
    let activity = CurrentActivity(payload: hex("e80300 2800 bc02 00000000000000"))
    #expect(activity == CurrentActivity(payload: hex("e80300 2800 bc02"))!)
    #expect(activity?.steps == 1000)
    #expect(activity?.kilocalories == 40)
    #expect(activity?.distanceMeters == 700)
}

@Test func ringTimeIsLocalWallClock() {
    // 2026-01-01 00:00:00 wall-clock is 9497 days after 2000-01-01.
    let raw: UInt32 = 9497 * 86400
    #expect(RingTime.date(fromRingSeconds: raw, timeZone: utc) == Date(timeIntervalSince1970: 1_767_225_600))
    let sixHoursBehind = TimeZone(secondsFromGMT: -6 * 3600)!
    #expect(RingTime.date(fromRingSeconds: raw, timeZone: sixHoursBehind) == Date(timeIntervalSince1970: 1_767_225_600 + 6 * 3600))
}

// Synthetic records built from the documented layouts; replace with real captures once the ring has history.
@Test func decodesActivityAndHeartRecords() {
    let activity = ActivityRecord.decode(hex("00000000 08070000 e803 bc02 2800"), timeZone: utc)
    #expect(activity.count == 1)
    #expect(activity[0].end.timeIntervalSince(activity[0].start) == 1800)
    #expect(activity[0].steps == 1000)
    #expect(activity[0].distanceMeters == 700)
    #expect(activity[0].kilocalories == 40)

    let heart = HeartRateRecord.decode(hex("3c000000 00 48  78000000 00 4a  ff"), timeZone: utc)
    #expect(heart.map(\.bpm) == [72, 74])
}

@Test func decodesSleepSessions() {
    // Two sessions back to back: light 30 min + deep 60 min, then a single REM stage.
    let first = "affa 2400 00000000 78150000 ffff 0000 100e 0807"
        + "f2 00000000 080700" + "f1 08070000 100e00"
    let second = "affa 1c00 a08c0000 a8930000 ffff 0807 0000 0000" + "f3 a08c0000 080700"
    let sessions = SleepSession.decode(hex(first + second), timeZone: utc)
    #expect(sessions.count == 2)
    #expect(sessions[0].stages.map(\.kind) == [.light, .deep])
    #expect(sessions[0].total(.deep) == 3600)
    #expect(sessions[0].total(.light) == 1800)
    #expect(sessions[0].stages[1].start == sessions[0].stages[0].end)
    #expect(sessions[1].stages.map(\.kind) == [.rem])
    #expect(sessions[1].start.timeIntervalSince(sessions[0].start) == 36000)
}

@Test func sleepDecodingFollowsRecordLengthAndStopsAtGarbage() {
    // A record two bytes longer than its single stage, then a normal record.
    let padded = "affa 1e00 00000000 08070000 ffff 0000 0000 0807" + "f2 00000000 080700" + "0000"
    let normal = "affa 1c00 a08c0000 a8930000 ffff 0807 0000 0000" + "f3 a08c0000 080700"
    let sessions = SleepSession.decode(hex(padded + normal), timeZone: utc)
    #expect(sessions.count == 2)
    #expect(sessions[1].stages.map(\.kind) == [.rem])
    #expect(sessions[1].start.timeIntervalSince(sessions[0].start) == 36000)

    #expect(SleepSession.decode(hex(normal + "0102030405060708090a0b0c0d0e0f1011121314"), timeZone: utc).count == 1)
    #expect(SleepSession.decode(hex("affa 1c00 00000000"), timeZone: utc).isEmpty)
}

@Test func hexRoundTripsAndRejectsBadInput() {
    #expect(Hex.encode([0x00, 0x0A, 0xFF]) == "000aff")
    #expect(Hex.encode([]) == "")
    #expect(Hex.decode("000AfF") == [0x00, 0x0A, 0xFF])
    #expect(Hex.decode("") == [])
    #expect(Hex.decode("abc") == nil)
    #expect(Hex.decode("zz") == nil)
    #expect(Hex.decode("+1") == nil)
    #expect(Hex.decode("00 ff") == nil)
}

// Synthetic bytes. The date has a fractional part to prove the format keeps milliseconds.
private let sampleTransfer = HistoryTransfer(
    kind: .sleep, header: [0x01, 0x02, 0x03], data: [0xDE, 0xAD, 0xBE, 0xEF],
    fetchedAt: Date(timeIntervalSince1970: 1_800_000_000.123), timeZoneIdentifier: "Europe/London")

@Test func historyTransferRoundTripsThroughJSON() throws {
    // Default encoder and decoder: the transfer owns its date format.
    let json = try JSONEncoder().encode(sampleTransfer)
    #expect(try JSONDecoder().decode(HistoryTransfer.self, from: json) == sampleTransfer)

    // A real clock reading, truncated the way the session does, round-trips too.
    var now = sampleTransfer
    now.fetchedAt = HistoryTransfer.truncatedToMilliseconds(Date())
    #expect(try JSONDecoder().decode(HistoryTransfer.self, from: JSONEncoder().encode(now)) == now)

    // Byte fields are hex strings, kind is a name, and the file holds exactly the five documented fields.
    let object = try #require(try JSONSerialization.jsonObject(with: json) as? [String: Any])
    #expect(Set(object.keys) == ["kind", "header", "data", "fetchedAt", "timeZoneIdentifier"])
    #expect(object["header"] as? String == "010203")
    #expect(object["data"] as? String == "deadbeef")
    #expect(object["kind"] as? String == "sleep")
    #expect(object["fetchedAt"] as? String == "2027-01-15T08:00:00.123Z")
}

@Test func millisecondTruncationIsIdempotentAndFloors() {
    var rng = SystemRandomNumberGenerator()
    for _ in 0..<20_000 {
        // About -2000 to +2100, covering pre-1970 values.
        let date = Date(timeIntervalSince1970: Double.random(in: -2_000_000_000_000_000_000...4_000_000_000_000_000_000, using: &rng) / 1e9)
        let ms = HistoryTransfer.millisecondsSince1970(date)
        let once = HistoryTransfer.truncatedToMilliseconds(date)
        #expect(HistoryTransfer.truncatedToMilliseconds(once) == once)
        #expect(HistoryTransfer.millisecondsSince1970(once) == ms)
        #expect(once <= date)
        #expect(date.timeIntervalSince(once) < 0.0011)
    }
    // Exact examples, including a negative one that must round toward negative infinity.
    #expect(HistoryTransfer.millisecondsSince1970(Date(timeIntervalSince1970: 1.0009)) == 1000)
    #expect(HistoryTransfer.millisecondsSince1970(Date(timeIntervalSince1970: -0.0005)) == -1)
}

@Test func historyKindsAreEncodedByNameAndUnknownNamesRejected() throws {
    for kind in HistoryKind.allCases {
        var t = sampleTransfer
        t.kind = kind
        let object = try #require(try JSONSerialization.jsonObject(with: JSONEncoder().encode(t)) as? [String: Any])
        #expect(object["kind"] as? String == kind.name)
    }
    let json = Data(#"{"kind":"steps","header":"","data":"","fetchedAt":"2027-01-15T08:00:00.123Z","timeZoneIdentifier":"UTC"}"#.utf8)
    #expect(throws: DecodingError.self) { try JSONDecoder().decode(HistoryTransfer.self, from: json) }
}

@Test func historyTransferRejectsNonHexBytes() {
    let json = Data(#"{"kind":"sleep","header":"xyz","data":"","fetchedAt":"2027-01-15T08:00:00.123Z","timeZoneIdentifier":"UTC"}"#.utf8)
    #expect(throws: DecodingError.self) { try JSONDecoder().decode(HistoryTransfer.self, from: json) }
}

// All bytes in the tests below are SYNTHETIC: built from the documented layouts, with made-up
// values. Combined layout: time u32, steps u16, hr, systolic, diastolic, SpO2, resp, HRV, CVRR,
// temp (2 bytes), body fat (2 bytes), glucose, 2 trailing bytes.
private let combinedA = "3c000000 e803 48 70 46 61 10 25 1e 2a05 0000 00 aabb"
private let combinedB = "100e0000 0a00 50 00 00 62 11 26 1f 2a05 0000 00 ccdd"

@Test func decodesCombinedRecord() throws {
    let bytes = hex(combinedA)
    #expect(bytes.count == CombinedRecord.size)
    let records = CombinedRecord.decode(bytes, timeZone: utc)
    #expect(records.count == 1)
    let r = try #require(records.first)
    #expect(r.time == RingTime.date(fromRingSeconds: 60, timeZone: utc))
    #expect(r.steps == 1000)
    #expect(r.heartRate == 72)
    #expect(r.spo2 == 97)
    #expect(r.respirationRate == 16)
    #expect(r.hrv == 37)
    #expect(r.cvrr == 30)
    #expect(r.raw == bytes)
}

@Test func combinedZeroFieldsAreNoReading() throws {
    // A record with only HRV set (reported for other rings of the family; unverified on the R99).
    let r = try #require(CombinedRecord.decode(hex("3c000000 0000 00 00 00 00 00 25 00 0000 0000 00 0000"), timeZone: utc).first)
    #expect(r.steps == 0)
    #expect(r.heartRate == nil)
    #expect(r.spo2 == nil)
    #expect(r.respirationRate == nil)
    #expect(r.hrv == 37)
    #expect(r.cvrr == nil)
}

@Test func combinedSpO2OutsideOneToOneHundredIsNotAReading() throws {
    let over = try #require(CombinedRecord.decode(hex("3c000000 0000 48 00 00 65 10 25 1e 0000 0000 00 0000"), timeZone: utc).first)
    #expect(over.spo2 == nil)
    #expect(over.heartRate == 72)
    let exactly100 = try #require(CombinedRecord.decode(hex("3c000000 0000 48 00 00 64 10 25 1e 0000 0000 00 0000"), timeZone: utc).first)
    #expect(exactly100.spo2 == 100)
}

@Test func decodesSeveralCombinedRecordsAndIgnoresTruncation() throws {
    let records = CombinedRecord.decode(hex(combinedA + combinedB + "01020304050607"), timeZone: utc)
    #expect(records.count == 2)
    #expect(records.map(\.steps) == [1000, 10])
    let second = try #require(records.last)
    #expect(second.heartRate == 80)
    #expect(second.raw == hex(combinedB))
    #expect(CombinedRecord.decode(Array(hex(combinedA).dropLast()), timeZone: utc).isEmpty)
    #expect(CombinedRecord.decode([], timeZone: utc).isEmpty)
}

// Exists to stop anyone adding blood-pressure, temperature, body-fat or glucose properties:
// those are vendor estimates or absent on the R99 and must stay out of the typed value.
@Test func combinedEstimatesAreKeptRawButNotExposed() throws {
    let r = try #require(CombinedRecord.decode(hex(combinedA), timeZone: utc).first)
    let exposed = Set(Mirror(reflecting: r).children.compactMap(\.label))
    #expect(exposed == ["time", "steps", "heartRate", "spo2", "respirationRate", "hrv", "cvrr", "raw"])
    #expect(r.raw[7] == 0x70 && r.raw[8] == 0x46)
    #expect(Array(r.raw[13...14]) == [0x2a, 0x05])
    #expect(Array(r.raw[18...19]) == [0xaa, 0xbb])
}

// Light 600 s from t=0, deep 900 s from t=601, REM 300 s from t=1502: each stage starts one
// second after the previous ends, as seen on an R99. The header uses the newer layout:
// FF FF, then REM 300, deep 900, light 600 (u16 seconds), equal to the stage sums.
private let sleepOneSecondGaps = "affa 2c00 00000000 0a070000 ffff 2c01 8403 5802"
    + "f2 00000000 580200" + "f1 59020000 840300" + "f3 de050000 2c0100"

@Test func sleepStagesOneSecondApartAreContiguousAndNotRewritten() throws {
    let stages = try #require(SleepSession.decode(hex(sleepOneSecondGaps), timeZone: utc).first).stages
    #expect(stages.map(\.kind) == [.light, .deep, .rem])
    #expect(stages.map(\.duration) == [600, 900, 300])
    for (a, b) in zip(stages, stages.dropFirst()) {
        #expect(b.start.timeIntervalSince(a.end) == 1)
        #expect(a.isContiguous(with: b, tolerance: 1))
        #expect(!a.isContiguous(with: b, tolerance: 0))
    }
}

@Test func sleepContiguityToleranceEdges() throws {
    let a = SleepStage(kind: .light, start: Date(timeIntervalSince1970: 1000), duration: 600)
    func stage(startingAt offset: TimeInterval) -> SleepStage {
        SleepStage(kind: .deep, start: a.end.addingTimeInterval(offset), duration: 5)
    }
    #expect(a.isContiguous(with: stage(startingAt: 0), tolerance: 1))
    #expect(a.isContiguous(with: stage(startingAt: -1), tolerance: 1))     // overlap of one second
    #expect(!a.isContiguous(with: stage(startingAt: 1.5), tolerance: 1))   // just over the tolerance
    #expect(a.isContiguous(with: stage(startingAt: 2), tolerance: 2))
    #expect(!a.isContiguous(with: stage(startingAt: 300), tolerance: 1))
    // Reversed order: the earlier stage is not contiguous after the later one.
    let later = stage(startingAt: 1)
    #expect(!later.isContiguous(with: a, tolerance: 1))
}

@Test func sleepDecoderIgnoresNewerHeaderTotalsAndStillDecodesStages() throws {
    let bytes = hex(sleepOneSecondGaps)
    #expect(Array(bytes[12...13]) == [0xff, 0xff])
    let session = try #require(SleepSession.decode(bytes, timeZone: utc).first)
    // Totals come from the stages; on the ring they equalled the header's, which the fixture mirrors.
    #expect(Int(bytes.u16(14)) == Int(session.total(.rem)))
    #expect(Int(bytes.u16(16)) == Int(session.total(.deep)))
    #expect(Int(bytes.u16(18)) == Int(session.total(.light)))
    #expect(session.stages.count == 3)
    // A session with no awake stages is normal on this ring.
    #expect(!session.stages.contains { $0.kind == .awake })
}

@Test func sleepNightSplitIntoTwoSessionsKeepsTheGapBetweenThem() throws {
    // Session one: light 600 s from t=0. Session two: deep 600 s from t=900, five minutes later.
    let one = "affa 1c00 00000000 58020000 ffff 0000 0000 5802" + "f2 00000000 580200"
    let two = "affa 1c00 84030000 dc050000 ffff 0000 5802 0000" + "f1 84030000 580200"
    let sessions = SleepSession.decode(hex(one + two), timeZone: utc)
    #expect(sessions.count == 2)
    let firstEnd = try #require(sessions.first).end
    let second = try #require(sessions.last)
    #expect(second.start.timeIntervalSince(firstEnd) == 300)
    let lastOfFirst = try #require(sessions.first?.stages.last)
    let firstOfSecond = try #require(second.stages.first)
    #expect(!lastOfFirst.isContiguous(with: firstOfSecond, tolerance: 1))
    #expect(sessions.allSatisfy { $0.total(.awake) == 0 })
}

@Test func activityRecordsAreThirtyMinuteBuckets() {
    // Two consecutive 30-minute buckets, 00:00-00:30 and 00:30-01:00.
    let records = ActivityRecord.decode(
        hex("00000000 08070000 0a00 0700 0100" + "08070000 100e0000 1400 0f00 0200"), timeZone: utc)
    #expect(records.count == 2)
    #expect(records.allSatisfy { $0.end.timeIntervalSince($0.start) == 1800 })
    #expect(records[1].start == records[0].end)
    #expect(records.map(\.steps) == [10, 20])
}

@Test func heartRateModeZeroRecordsOnTheHour() {
    let records = HeartRateRecord.decode(hex("00000000 00 48  100e0000 00 4a"), timeZone: utc)
    #expect(records.map(\.mode) == [0, 0])
    #expect(records.map(\.bpm) == [72, 74])
    #expect(records[1].time.timeIntervalSince(records[0].time) == 3600)
}
