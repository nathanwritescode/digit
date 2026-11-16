import Foundation
import RingProtocol
import RingTransport

let usage = """
ringprobe: talk to a YCBT smart ring from a Mac.

  ringprobe scan [seconds]              list nearby Bluetooth devices, with identifiers for
                                        candidate rings (and devices matching --name) only
  ringprobe scan [seconds] --save-id <file>
                                        save the identifier of the one candidate ring to <file>
                                        (owner-only, replacing it) without printing it.
                                        Suggested: ../../captures/ring-id (git-ignored)
  ringprobe info                        firmware and battery (device id and today's activity with --show)
  ringprobe settime                     set the ring's clock to this Mac's local time
  ringprobe history <kind>|all --save <dir>
                                        fetch stored history: \(HistoryKind.allCases.map(\.name).joined(separator: ", "))
  ringprobe monitor <1|5|10|15|20|30|60|off>
                                        CHANGES A RING SETTING: how often the ring takes scheduled readings
                                        (heart rate, SpO2, respiration, HRV). Asks for confirmation first.
  ringprobe raw <GGKK[:payloadhex]>...  send frames and print the replies

Options:
  --name <text>     match rings whose advertised name contains <text> (default "R99"). Given
                    with scan, it also shows identifiers of matching devices and narrows --save-id.
  --id <uuid>       connect to the device with this identifier (as printed by scan) instead
                    of matching by name, without scanning if this Mac already knows it.
                    Identifiers are specific to this Mac but identify the ring here: never
                    paste them into issues, pull requests, commits or the repository. Prefer
                    --id-file, which keeps them out of command lines and transcripts.
  --id-file <file>  like --id, reading the identifier from <file> (as written by scan --save-id).
                    --id, --id-file and --name cannot be combined, and none of --id and
                    --id-file works with scan.
  --save <dir>      required with history: write each transfer (raw bytes as hex,
                    plus header, fetch time and time zone) to <dir>/<kind>-<timestamp>.json.
                    The bytes are written before the transfer is acknowledged to the ring.
  --show            print health data (decoded records, reply payloads). Off by default so that
                    health data stays out of terminal logs and agent transcripts.
  --allow-unknown   with raw: send frame types that are not on the known read-only list.
                    Their effects are unknown and may include resetting the ring.
  --trace           print every raw packet (implies health data is printed)

The history-delete commands (0x0540-0x0552) are always refused. `raw` sends only known
read-only commands, with their documented payloads, unless --allow-unknown is given.
"""

func fail(_ message: String) -> Never {
    FileHandle.standardError.write(Data((message + "\n").utf8))
    exit(1)
}

var arguments = Array(CommandLine.arguments.dropFirst())

@MainActor func option(_ flag: String) -> String? {
    guard let i = arguments.firstIndex(of: flag) else { return nil }
    guard i + 1 < arguments.count else { fail("\(flag) needs a value") }
    let value = arguments[i + 1]
    arguments.removeSubrange(i...(i + 1))
    return value
}

@MainActor func flag(_ name: String) -> Bool {
    guard arguments.contains(name) else { return false }
    arguments.removeAll { $0 == name }
    return true
}

let trace = flag("--trace")
let show = flag("--show")
let allowUnknown = flag("--allow-unknown")

let nameOption = option("--name")
let ringName = nameOption ?? "R99"
let idOption = option("--id")
let idFile = option("--id-file")
let saveIDFile = option("--save-id")
let saveDirectory = option("--save")

guard let command = arguments.first else { print(usage); exit(0) }

if [idOption != nil, idFile != nil, nameOption != nil].filter({ $0 }).count > 1 {
    fail("use only one of --id, --id-file and --name")
}
if command == "scan", idOption != nil || idFile != nil {
    fail("--id and --id-file choose a ring to connect to; scan does not connect")
}
if command != "scan", saveIDFile != nil { fail("--save-id works only with scan") }

/// Reads an identifier saved by `scan --save-id`. Never echoes the file's contents: they are the
/// identifier, which must stay out of terminal logs and transcripts.
func readIdentifier(from path: String) -> UUID {
    guard let data = FileManager.default.contents(atPath: path) else { fail("cannot read \(path)") }
    guard data.count <= 64, let text = String(data: data, encoding: .utf8),
          let id = UUID(uuidString: text.trimmingCharacters(in: .whitespacesAndNewlines)) else {
        fail("\(path) does not hold a ring identifier; write one with scan --save-id")
    }
    return id
}

// Resolved before anything connects, so a malformed --id or --id-file fails first.
let ringID: UUID? = {
    if let idOption {
        guard let id = UUID(uuidString: idOption) else { fail("--id needs a UUID, as printed by scan") }
        return id
    }
    return idFile.map(readIdentifier)
}()
let operands = Array(arguments.dropFirst())
@Sendable func traceBytes(_ outgoing: Bool, _ bytes: [UInt8]) {
    print("  \(outgoing ? ">>" : "<<") \(bytes.map { String(format: "%02x", $0) }.joined())")
}

// `monitor` changes a ring setting, so it validates and gets the owner's confirmation here, before
// the connection object (and so any Bluetooth manager) exists. A refused run never touches Bluetooth.
let monitorInterval: MonitoringInterval? = command == "monitor" ? confirmMonitor() : nil
func confirmMonitor() -> MonitoringInterval {
    guard operands.count == 1, let interval = MonitoringInterval(argument: operands[0]) else {
        fail("monitor needs one of: 1, 5, 10, 15, 20, 30, 60, off")
    }
    // A piped answer must not be able to confirm.
    guard isatty(STDIN_FILENO) != 0 else { fail("monitor needs a terminal on standard input to ask for confirmation; nothing sent") }
    print("About to send 010C \(Hex.encode(Command.setMonitoringInterval(interval).payload)): \(interval.summary).")
    if !interval.isVerifiedOnHardware { print("This value has not been tried on a ring before.") }
    print("The ring cannot report this setting back. Type yes to send it: ", terminator: "")
    fflush(stdout)
    guard MonitoringInterval.isConfirmation(readLine()) else { fail("not confirmed; nothing sent") }
    return interval
}

let connection = trace ? RingConnection(onRawBytes: traceBytes) : RingConnection()

func connect() async throws -> RingSession {
    if let ringID {
        try await connection.connect(identifier: ringID)
    } else {
        try await connection.connect { ($0.name ?? "").localizedCaseInsensitiveContains(ringName) }
    }
    return RingSession(link: connection)
}

let timestamps: DateFormatter = {
    let formatter = DateFormatter()
    formatter.locale = Locale(identifier: "en_US_POSIX")
    formatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
    return formatter
}()

@Sendable func saveCapture(_ transfer: HistoryTransfer, directory: String, stamp: String) throws {
    let url = URL(fileURLWithPath: directory).appendingPathComponent("\(transfer.kind.name)-\(stamp).json")
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
    try PrivateFile.write(try encoder.encode(transfer), to: url.path)
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
    case .combined: return "\(CombinedRecord.decode(bytes, timeZone: zone).count) records"
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
        func text(_ value: Int?) -> String { value.map(String.init) ?? "-" }
        for r in CombinedRecord.decode(bytes, timeZone: zone) {
            print("  \(timestamps.string(from: r.time))  \(r.steps) steps, hr \(text(r.heartRate)), SpO2 \(text(r.spo2)), resp \(text(r.respirationRate)), HRV \(text(r.hrv)), CVRR \(text(r.cvrr))")
        }
    }
}

do {
    switch command {
    case "scan":
        let seconds = operands.first.flatMap(Double.init) ?? 8
        let devices = try await connection.scan(for: seconds)
        // The same rule as `discoverRings`, so scan and the app cannot disagree on what a ring is.
        func isCandidate(_ device: DiscoveredDevice) -> Bool {
            device.isCandidateRing(namePrefixes: YCBT.knownNamePrefixes)
        }
        func matchesName(_ device: DiscoveredDevice) -> Bool {
            nameOption.map { (device.name ?? "").localizedCaseInsensitiveContains($0) } ?? false
        }
        if let saveIDFile {
            let chosen = devices.filter { isCandidate($0) && (nameOption == nil || matchesName($0)) }
            guard chosen.count == 1 else {
                fail(chosen.isEmpty
                     ? "no candidate ring was heard; nothing saved"
                     : "\(chosen.count) candidate rings were heard; narrow with --name; nothing saved")
            }
            do {
                try PrivateFile.createDirectory(URL(fileURLWithPath: saveIDFile).deletingLastPathComponent().path)
                try PrivateFile.replace(Data((chosen[0].identifier.uuidString + "\n").utf8), at: saveIDFile)
            } catch {
                fail("cannot save the identifier: \(error)")
            }
            // Only that it was saved: the identifier stays out of the terminal.
            print("saved the ring's identifier to \(saveIDFile)")
            break
        }
        for device in devices {
            let services = device.serviceUUIDs.isEmpty ? "" : "  services: \(device.serviceUUIDs.joined(separator: ","))"
            let candidate = isCandidate(device), named = matchesName(device)
            // Advertised names identify devices (the ring's suffix, other people's gadgets), so
            // they are shown only with --show; without it the label says only why a row matters.
            let label = show ? (device.name ?? "(unnamed)")
                : candidate ? "(candidate ring)" : named ? "(matches --name)" : "(other device)"
            // The identifier is what --id takes. It is assigned by this Mac's Bluetooth system but
            // still identifies the device here, so other people's devices get one only with --show.
            // Keep every identifier out of the repository, issues and pull requests.
            let identifier = candidate || named || show
                ? device.identifier.uuidString
                : "(hidden; --show)".padding(toLength: 36, withPad: " ", startingAt: 0)
            print("\(String(format: "%4d", device.rssi)) dBm  \(identifier)  \(label)\(services)")
        }

    case "info":
        let session = try await connect()
        if let info = try await session.deviceInfo() {
            // The device id may identify the ring, so it is health-adjacent data: --show only.
            print("\(show ? "device id \(info.deviceID), " : "")firmware \(info.firmwareVersion), battery \(info.batteryPercent)% (state \(info.batteryState))")
        }
        if show, let activity = try await session.currentActivity() {
            print("today: \(activity.steps) steps, \(activity.distanceMeters) m, \(activity.kilocalories) kcal")
        }

    case "settime":
        let session = try await connect()
        try await session.setTime()
        print("ring clock set to \(timestamps.string(from: Date())) local time")

    case "monitor":
        // Validated and confirmed before the connection existed (see `monitorInterval`).
        guard let interval = monitorInterval else { fail("internal error: monitor was not confirmed") }
        let session = try await connect()
        do {
            try await session.setMonitoringInterval(interval)
        } catch RingError.timeout {
            fail("the ring did not reply in time; it may still have applied the setting")
        }
        print("ring accepted the setting")

    case "history":
        guard let which = operands.first else { fail(usage) }
        let kinds = which == "all" ? HistoryKind.allCases : HistoryKind.allCases.filter { $0.name == which }
        guard !kinds.isEmpty else { fail("unknown history kind '\(which)'") }
        // Acknowledging a transfer may make the ring discard it, so there must be somewhere
        // durable to put the bytes before anything is requested.
        guard let directory = saveDirectory else { fail("history needs --save <dir>, so the data is kept before the ring is told it was received") }
        do {
            try PrivateFile.createDirectory(directory)
            if let mode = (try FileManager.default.attributesOfItem(atPath: directory)[.posixPermissions] as? NSNumber)?.intValue,
               mode & 0o077 != 0 {
                FileHandle.standardError.write(Data("warning: \(directory) is accessible to other accounts on this Mac (mode \(String(mode, radix: 8))); consider chmod 700\n".utf8))
            }
            let testFile = URL(fileURLWithPath: directory).appendingPathComponent(".ringprobe-write-test")
            try? FileManager.default.removeItem(at: testFile)
            try PrivateFile.write(Data(), to: testFile.path)
            try FileManager.default.removeItem(at: testFile)
        } catch {
            fail("cannot write to \(directory): \(error)")
        }
        let stamp = timestamps.string(from: Date()).replacingOccurrences(of: " ", with: "T").replacingOccurrences(of: ":", with: "")
        let session = try await connect()
        for kind in kinds {
            let transfer = try await session.fetchHistory(kind) { transfer in
                try saveCapture(transfer, directory: directory, stamp: stamp)
            }
            let bytes = transfer.data
            guard !bytes.isEmpty else { print("\(kind.name): empty"); continue }
            print("\(kind.name): \(bytes.count) bytes, \(recordCount(kind, bytes))")
            if show { describe(kind, bytes) }
        }

    case "raw":
        guard !operands.isEmpty else { fail(usage) }
        // Validate every frame before connecting, so nothing is sent if any is refused.
        let frames: [Frame] = operands.map { operand in
            let parts = operand.split(separator: ":", maxSplits: 1, omittingEmptySubsequences: false).map(String.init)
            guard let first = parts.first, let type = Hex.decode(first), type.count == 2,
                  let payload = Hex.decode(parts.count > 1 ? parts[1] : "") else { fail("bad frame '\(operand)'") }
            let frame = Frame(group: type[0], key: type[1], payload: payload)
            if frame.erasesRingData {
                fail("refusing to send \(first): that range deletes history from the ring")
            }
            if frame.isMonitoringSetting {
                fail("refusing to send \(first): it changes the ring's monitoring interval. Use `ringprobe monitor`, which validates the value and asks for confirmation.")
            }
            if !Command.isKnownReadOnly(frame), !allowUnknown {
                fail("refusing to send \(operand): not a known read-only command with its documented payload. Its effect is unknown; pass --allow-unknown to send it anyway.")
            }
            return frame
        }
        _ = try await connect()
        for frame in frames {
            print(">> \(Hex.encode(frame.encoded()))")
            try await connection.send(frame)
            while let reply = await connection.nextFrame(timeout: 2.5) {
                let body = show ? Hex.encode(reply.payload) : "\(reply.payload.count) bytes\(reply.isErrorReply ? " (rejected)" : "")"
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
