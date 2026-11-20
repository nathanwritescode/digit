import Foundation

/// CRC-16/CCITT-FALSE (poly 0x1021, init 0xFFFF), as used by the YCBT protocol.
public enum CRC16 {
    public static func ccittFalse<S: Sequence>(_ bytes: S) -> UInt16 where S.Element == UInt8 {
        var crc: UInt16 = 0xFFFF
        for byte in bytes {
            crc ^= UInt16(byte) << 8
            for _ in 0..<8 {
                crc = (crc & 0x8000) != 0 ? (crc << 1) ^ 0x1021 : crc << 1
            }
        }
        return crc
    }
}

/// One YCBT frame: `[group][key][len lo][len hi][payload…][crc lo][crc hi]`.
/// `len` counts the whole frame, header and CRC included.
public struct Frame: Equatable, Sendable {
    public static let overhead = 6

    public var group: UInt8
    public var key: UInt8
    public var payload: [UInt8]

    public init(group: UInt8, key: UInt8, payload: [UInt8] = []) {
        self.group = group
        self.key = key
        self.payload = payload
    }

    /// Group and key combined, e.g. `0x0504` for the sleep history request.
    public var type: UInt16 { UInt16(group) << 8 | UInt16(key) }

    /// A one-byte payload of `0xFB…0xFF` is the ring rejecting the command.
    public var isErrorReply: Bool { payload.count == 1 && payload[0] >= 0xFB }

    /// True for every known command that deletes stored data, resets the ring, puts it into a
    /// firmware or factory state, or writes health data to it. The transport refuses to send these,
    /// and `ringprobe raw --allow-unknown` does not override that. `isDestructive(type:)` is the one
    /// place the list lives.
    ///
    /// The list is specific to the YCBT family and moves behind the ring abstraction when a second
    /// family arrives.
    public var isDestructive: Bool { Self.isDestructive(type: type) }

    /// Names and numbers come from public YCBT references derived from the vendor SDK (owner
    /// decision, #83; see the research comment on #82): narey83/vitals-smart-ring-app (MIT) and
    /// saksham2001/PulseLoopiOS (CC-BY-4.0). None was sent to this ring; refusing needs no
    /// validation, sending one would. A command missing here is not thereby safe: unknown commands
    /// are still limited by the read-only allowlist.
    static func isDestructive(type: UInt16) -> Bool {
        switch UInt8(type >> 8) {
        // Whole groups with no permitted documentation and factory, update or collection roles.
        case 0x07: return true  // Collect_* raw collection; includes the deletes 0x0730, 0x0731 (narey debugger table)
        case 0x0A: return true  // OTA firmware update (0x0A00-0x0A02; narey debugger table)
        case 0x0C: return true  // SelfInspection (0x0C00/0x0C01; narey debugger table)
        case 0x0E: return true  // FactoryTest (0x0E10/0x0E11; narey debugger table)
        default: break
        }
        if (0x0540...0x0552).contains(type) { return true }  // Health_Delete* (Gadgetbridge PR, PulseLoop)
        switch type {
        case 0x0568: return true  // DeleteHistoryWearingStatus (#82 command list)
        case 0x0575: return true  // DeleteRespiratoryTraining (#82 command list)
        case 0x0578: return true  // DeletePowerOnOff (#82 command list)
        case 0x0730: return true  // raw-collection delete (narey debugger table; also inside group 0x07)
        case 0x0731: return true  // collectDeleteEcgPpg (narey debugger table; also inside group 0x07)
        case 0x010E: return true  // SettingRestoreFactory, payload "RSYS": wipes the record store and clock, reboots (narey, verified on an R99 at 2.32)
        case 0x0157: return true  // ClearUserCache (narey debugger marks it as erasing data)
        case 0x0151: return true  // SelfCheck (narey debugger marks it as a reset or factory state)
        case 0x0316: return true  // AppShutDown (narey debugger marks it; powers the ring off, not a reset)
        case 0x0809: return true  // OpenFactory (narey debugger marks it as starting a factory state)
        case 0x031A: return true  // AppHealthWriteBack: writes health data to the ring, could overwrite history (name only, PulseLoop)
        case 0x031B: return true  // AppSleepWriteBack: writes sleep data to the ring, could overwrite history (name only, PulseLoop)
        default: return false
        }
    }

    /// The largest payload `encoded()` can represent (and the assembler accepts).
    public static let maximumPayload = FrameAssembler.maxFrameLength - overhead

    /// `encoded()` traps on a payload over `maximumPayload`; check this first for untrusted input.
    public var isEncodable: Bool { payload.count <= Self.maximumPayload }

    /// Precondition: `isEncodable`.
    public func encoded() -> [UInt8] {
        let length = Frame.overhead + payload.count
        var bytes: [UInt8] = [group, key, UInt8(length & 0xFF), UInt8(length >> 8)]
        bytes += payload
        let crc = CRC16.ccittFalse(bytes)
        bytes += [UInt8(crc & 0xFF), UInt8(crc >> 8)]
        return bytes
    }
}

/// Rejoins frames that arrive split across Bluetooth notifications.
public struct FrameAssembler: Sendable {
    /// Longest frame accepted. An assumption, not a documented limit: the largest frame seen so far is 30 bytes.
    public static let maxFrameLength = 4096

    public private(set) var crcErrors = 0
    private var buffer: [UInt8] = []

    public init() {}

    public mutating func reset() { buffer.removeAll() }

    /// Feeds one notification's bytes and returns every frame completed by it.
    public mutating func append(_ chunk: [UInt8]) -> [Frame] {
        buffer += chunk
        var frames: [Frame] = []
        while buffer.count >= 4 {
            let length = Int(buffer[2]) | Int(buffer[3]) << 8
            guard length >= Frame.overhead, length <= FrameAssembler.maxFrameLength else {
                // Not a frame header. A length-prefixed stream cannot be resynchronised reliably,
                // so drop what is buffered; the transport also resets the reply channel's
                // assembler before each conversation (`RingLink.discardPartialFrames`).
                buffer.removeAll()
                break
            }
            guard buffer.count >= length else { break }
            let raw = Array(buffer[..<length])
            buffer.removeFirst(length)
            let expected = UInt16(raw[length - 2]) | UInt16(raw[length - 1]) << 8
            guard CRC16.ccittFalse(raw[..<(length - 2)]) == expected else {
                crcErrors += 1
                continue
            }
            frames.append(Frame(group: raw[0], key: raw[1], payload: Array(raw[4..<(length - 2)])))
        }
        return frames
    }
}
