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

// Synthetic bytes.
@Test func historyTransferRoundTripsThroughJSON() throws {
    let transfer = HistoryTransfer(
        kind: .sleep, header: [0x01, 0x02, 0x03], data: [0xDE, 0xAD, 0xBE, 0xEF],
        fetchedAt: Date(timeIntervalSince1970: 1_800_000_000), timeZoneIdentifier: "Europe/London")
    let encoder = JSONEncoder()
    encoder.dateEncodingStrategy = .iso8601
    let json = try encoder.encode(transfer)
    let decoder = JSONDecoder()
    decoder.dateDecodingStrategy = .iso8601
    #expect(try decoder.decode(HistoryTransfer.self, from: json) == transfer)

    // Byte fields are hex strings, and the file holds exactly the five documented fields.
    let object = try #require(try JSONSerialization.jsonObject(with: json) as? [String: Any])
    #expect(Set(object.keys) == ["kind", "header", "data", "fetchedAt", "timeZoneIdentifier"])
    #expect(object["header"] as? String == "010203")
    #expect(object["data"] as? String == "deadbeef")
}

@Test func historyTransferRejectsNonHexBytes() {
    let json = Data(#"{"kind":2,"header":"xyz","data":"","fetchedAt":0,"timeZoneIdentifier":"UTC"}"#.utf8)
    #expect(throws: DecodingError.self) { try JSONDecoder().decode(HistoryTransfer.self, from: json) }
}
