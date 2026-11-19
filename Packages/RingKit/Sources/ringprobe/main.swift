import Foundation
import RingProtocol
import RingTransport

let usage = """
ringprobe: talk to a YCBT smart ring from a Mac.

  ringprobe scan [seconds]              list nearby Bluetooth devices. Prints no identifiers or
                                        names unless --show; rows are labelled candidate ring,
                                        matches --name, or other device
  ringprobe scan [seconds] --save-id <file>
                                        save the identifier of the one candidate ring to <file>
                                        (owner-only, replacing it) without printing it. Refuses a
                                        file inside a git work tree that git does not ignore.
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
                    with scan, it also labels matching devices and narrows --save-id.
  --id-file <file>  connect to the device whose identifier is in <file> (as written by scan
                    --save-id) instead of matching by name, without scanning if this Mac already
                    knows it. Identifiers are specific to this Mac but identify the ring here:
                    never paste them into issues, pull requests, commits or the repository.
                    There is no --id: an identifier on the command line would stay in shell
                    history. Cannot be combined with --name, and scan does not use it.
  --save <dir>      required with history: write each transfer (raw bytes as hex,
                    plus header, fetch time and time zone) to <dir>/<kind>-<timestamp>.json.
                    The bytes are written before the transfer is acknowledged to the ring.
  --show            print health data (decoded records, reply payloads) and, with scan, the
                    names and identifiers of every device heard. Off by default so that health
                    data and device identities stay out of terminal logs and agent transcripts.
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

/// Parses the command line before anything else happens; every rule is in `parseArguments`.
let options: ProbeOptions = {
    switch parseArguments(Array(CommandLine.arguments.dropFirst())) {
    case .success(let options): return options
    case .failure(let error): fail(error.message)
    }
}()

let trace = options.trace
let show = options.show
let allowUnknown = options.allowUnknown
let nameOption = options.name
let ringName = nameOption ?? "R99"
let saveDirectory = options.saveDirectory

guard let command = options.command else { print(usage); exit(0) }

// Resolved before anything connects (and before `monitor` asks for confirmation), so a bad
// --id-file fails first. The file is read through `readRingIdentifier`, which never echoes it.
let ringID: UUID? = {
    guard let path = options.idFile else { return nil }
    switch readRingIdentifier(path: path) {
    case .success(let id): return id
    case .failure(let error): fail(error.message)
    }
}()
// Refuse a save target that could be committed now, not after an 8 second scan.
// The one resolved path is used for the check and for the write.
let saveIDTarget: String? = options.saveID.map { path in
    switch checkSaveTarget(path: path) {
    case .success(let resolved): return resolved
    case .failure(let refusal): fail(refusal.message)
    }
}
let operands = options.operands
@Sendable func traceBytes(_ outgoing: Bool, _ bytes: [UInt8]) {
    print("  \(outgoing ? ">>" : "<<") \(bytes.map { String(format: "%02x", $0) }.joined())")
}

// `monitor` changes a ring setting. Its argument was validated by the parser; the owner's
// confirmation is asked here, before the connection object (and so any Bluetooth manager) exists,
// and after --id-file was read. A refused run never touches Bluetooth.
let monitorInterval: MonitoringInterval? = options.monitorInterval.map(confirmMonitor)
func confirmMonitor(_ interval: MonitoringInterval) -> MonitoringInterval {
    // A piped answer must not be able to confirm.
    guard isatty(STDIN_FILENO) != 0 else { fail("monitor needs a terminal on standard input to ask for confirmation; nothing sent") }
    print("About to send 010C \(Hex.encode(Command.setMonitoringInterval(interval).payload)): \(interval.summary).")
    if !interval.isSpacingObservedOnHardware { print("On an R99 (firmware 2.32) this value was acknowledged, but readings at this spacing have not been observed.") }
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
        if let saveIDFile = saveIDTarget {
            // The same rule as `discoverRings`, so scan and the app cannot disagree on what a ring is.
            let chosen = devices.filter { device in
                guard scanMatch(for: device, nameOption: nameOption) == .candidate else { return false }
                return nameOption.map { (device.name ?? "").localizedCaseInsensitiveContains($0) } ?? true
            }
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
            print("saved the ring's identifier to \(options.saveID ?? saveIDFile)")
            break
        }
        for device in devices {
            print(scanRow(device, show: show, match: scanMatch(for: device, nameOption: nameOption)))
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
        // Validated by the parser and confirmed before the connection existed (see `monitorInterval`).
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
