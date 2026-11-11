import Foundation
import RingProtocol
import RingTransport

let usage = """
ringprobe: talk to a YCBT smart ring from a Mac.

  ringprobe scan [seconds]              list nearby Bluetooth devices
  ringprobe info                        firmware and battery (today's activity with --show)
  ringprobe settime                     set the ring's clock to this Mac's local time
  ringprobe history <kind>|all --save <dir>
                                        fetch stored history: \(HistoryKind.allCases.map(\.name).joined(separator: ", "))
  ringprobe raw <GGKK[:payloadhex]>...  send frames and print the replies

Options:
  --name <text>     match rings whose advertised name contains <text> (default "R99")
  --save <dir>      required with history: write the raw bytes to <dir>/<kind>-<timestamp>.hex.
                    The bytes are written before the transfer is acknowledged to the ring.
  --show            print health data (decoded records, reply payloads). Off by default so that
                    health data stays out of terminal logs and agent transcripts.
  --allow-unknown   with raw: send frame types that are not on the known read-only list.
                    Their effects are unknown and may include resetting the ring.
  --trace           print every raw packet (implies health data is printed)

The history-delete commands (0x0540-0x0552) are always refused. `raw` sends only known
read-only commands, with their documented payloads, unless --allow-unknown is given.
"""

func hex(_ bytes: [UInt8]) -> String { bytes.map { String(format: "%02x", $0) }.joined() }

func parseHex(_ text: String) -> [UInt8]? {
    // UInt8(_:radix:) accepts a leading sign, so check the characters first.
    guard text.count % 2 == 0, text.allSatisfy(\.isHexDigit) else { return nil }
    var bytes: [UInt8] = []
    var index = text.startIndex
    while index < text.endIndex {
        let next = text.index(index, offsetBy: 2)
        guard let byte = UInt8(text[index..<next], radix: 16) else { return nil }
        bytes.append(byte)
        index = next
    }
    return bytes
}

func fail(_ message: String) -> Never {
    FileHandle.standardError.write(Data((message + "\n").utf8))
    exit(1)
}

var arguments = Array(CommandLine.arguments.dropFirst())

func option(_ flag: String) -> String? {
    guard let i = arguments.firstIndex(of: flag) else { return nil }
    guard i + 1 < arguments.count else { fail("\(flag) needs a value") }
    let value = arguments[i + 1]
    arguments.removeSubrange(i...(i + 1))
    return value
}

func flag(_ name: String) -> Bool {
    guard arguments.contains(name) else { return false }
    arguments.removeAll { $0 == name }
    return true
}

let trace = flag("--trace")
let show = flag("--show")
let allowUnknown = flag("--allow-unknown")

let ringName = option("--name") ?? "R99"
let saveDirectory = option("--save")

guard let command = arguments.first else { print(usage); exit(0) }
let operands = Array(arguments.dropFirst())
@Sendable func traceBytes(_ outgoing: Bool, _ bytes: [UInt8]) {
    print("  \(outgoing ? ">>" : "<<") \(bytes.map { String(format: "%02x", $0) }.joined())")
}

let connection = trace ? RingConnection(onRawBytes: traceBytes) : RingConnection()

func connect() async throws -> RingSession {
    try await connection.connect { ($0.name ?? "").localizedCaseInsensitiveContains(ringName) }
    return RingSession(connection: connection)
}

let timestamps: DateFormatter = {
    let formatter = DateFormatter()
    formatter.locale = Locale(identifier: "en_US_POSIX")
    formatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
    return formatter
}()

@Sendable func saveCapture(_ kind: HistoryKind, _ bytes: [UInt8], directory: String, stamp: String) throws {
    let url = URL(fileURLWithPath: directory).appendingPathComponent("\(kind.name)-\(stamp).hex")
    let text = bytes.map { String(format: "%02x", $0) }.joined() + "\n"
    try text.write(to: url, atomically: true, encoding: .utf8)
    // Health data: readable by the owner only.
    try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: url.path)
    print("  saved \(url.path)")
}

func recordCount(_ kind: HistoryKind, _ bytes: [UInt8]) -> String {
    let zone = TimeZone.current
    switch kind {
    case .activity: return "\(ActivityRecord.decode(bytes, timeZone: zone).count) records"
    case .heartRate: return "\(HeartRateRecord.decode(bytes, timeZone: zone).count) records"
    case .sleep:
        let sessions = SleepSession.decode(bytes, timeZone: zone)
        return "\(sessions.count) sessions, \(sessions.reduce(0) { $0 + $1.stages.count }) stages"
    case .combined: return "no decoder yet"
    }
}

func describe(_ kind: HistoryKind, _ bytes: [UInt8]) {
    let zone = TimeZone.current
    switch kind {
    case .activity:
        for r in ActivityRecord.decode(bytes, timeZone: zone) {
            print("  \(timestamps.string(from: r.start)) → \(timestamps.string(from: r.end))  \(r.steps) steps, \(r.distanceMeters) m, \(r.kilocalories) kcal")
        }
    case .heartRate:
        for r in HeartRateRecord.decode(bytes, timeZone: zone) {
            print("  \(timestamps.string(from: r.time))  \(r.bpm) bpm (mode \(r.mode))")
        }
    case .sleep:
        for session in SleepSession.decode(bytes, timeZone: zone) {
            print("  session \(timestamps.string(from: session.start)) → \(timestamps.string(from: session.end))")
            for stage in session.stages {
                print("    \(timestamps.string(from: stage.start))  \(String(describing: stage.kind).padding(toLength: 5, withPad: " ", startingAt: 0))  \(Int(stage.duration / 60)) min")
            }
        }
    case .combined:
        print("  (no decoder yet; raw bytes only)")
    }
}

do {
    switch command {
    case "scan":
        let seconds = operands.first.flatMap(Double.init) ?? 8
        for device in try await connection.scan(for: seconds) {
            let services = device.serviceUUIDs.isEmpty ? "" : "  services: \(device.serviceUUIDs.joined(separator: ","))"
            print("\(String(format: "%4d", device.rssi)) dBm  \(device.name ?? "(unnamed)")\(services)")
        }

    case "info":
        let session = try await connect()
        if let info = try await session.deviceInfo() {
            print("device id \(info.deviceID), firmware \(info.firmwareVersion), battery \(info.batteryPercent)% (state \(info.batteryState))")
        }
        if show, let activity = try await session.currentActivity() {
            print("today: \(activity.steps) steps, \(activity.distanceMeters) m, \(activity.kilocalories) kcal")
        }

    case "settime":
        let session = try await connect()
        try await session.setTime()
        print("ring clock set to \(timestamps.string(from: Date())) local time")

    case "history":
        guard let which = operands.first else { fail(usage) }
        let kinds = which == "all" ? HistoryKind.allCases : HistoryKind.allCases.filter { $0.name == which }
        guard !kinds.isEmpty else { fail("unknown history kind '\(which)'") }
        // Acknowledging a transfer may make the ring discard it, so there must be somewhere
        // durable to put the bytes before anything is requested.
        guard let directory = saveDirectory else { fail("history needs --save <dir>, so the data is kept before the ring is told it was received") }
        do {
            try FileManager.default.createDirectory(atPath: directory, withIntermediateDirectories: true)
            let testFile = URL(fileURLWithPath: directory).appendingPathComponent(".ringprobe-write-test")
            try Data().write(to: testFile)
            try FileManager.default.removeItem(at: testFile)
        } catch {
            fail("cannot write to \(directory): \(error.localizedDescription)")
        }
        let stamp = timestamps.string(from: Date()).replacingOccurrences(of: " ", with: "T").replacingOccurrences(of: ":", with: "")
        let session = try await connect()
        for kind in kinds {
            let bytes = try await session.fetchHistory(kind) { bytes in
                try saveCapture(kind, bytes, directory: directory, stamp: stamp)
            }
            guard !bytes.isEmpty else { print("\(kind.name): empty"); continue }
            print("\(kind.name): \(bytes.count) bytes, \(recordCount(kind, bytes))")
            if show { describe(kind, bytes) }
        }

    case "raw":
        guard !operands.isEmpty else { fail(usage) }
        // Validate every frame before connecting, so nothing is sent if any is refused.
        let frames: [Frame] = operands.map { operand in
            let parts = operand.split(separator: ":", maxSplits: 1, omittingEmptySubsequences: false).map(String.init)
            guard let first = parts.first, let type = parseHex(first), type.count == 2,
                  let payload = parseHex(parts.count > 1 ? parts[1] : "") else { fail("bad frame '\(operand)'") }
            let frame = Frame(group: type[0], key: type[1], payload: payload)
            if frame.erasesRingData {
                fail("refusing to send \(first): that range deletes history from the ring")
            }
            if !Command.isKnownReadOnly(frame), !allowUnknown {
                fail("refusing to send \(operand): not a known read-only command with its documented payload. Its effect is unknown; pass --allow-unknown to send it anyway.")
            }
            return frame
        }
        _ = try await connect()
        for frame in frames {
            print(">> \(hex(frame.encoded()))")
            try await connection.send(frame)
            while let reply = await connection.nextFrame(timeout: 2.5) {
                let body = show ? hex(reply.payload) : "\(reply.payload.count) bytes\(reply.isErrorReply ? " (rejected)" : "")"
                print(String(format: "<< %04X  ", reply.type) + body)
            }
        }

    default:
        fail(usage)
    }
    connection.disconnect()
} catch {
    fail("error: \(error)")
}
