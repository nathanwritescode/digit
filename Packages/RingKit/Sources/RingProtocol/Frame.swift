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

    /// True for every known command that deletes stored data, resets the ring or puts it into a
    /// firmware, factory or raw-collection mode. The transport refuses to send these, and
    /// `ringprobe raw --allow-unknown` does not override that. This is the one place the list lives.
    ///
    /// Names and numbers come from public YCBT references that were themselves derived from the
    /// vendor SDK (owner decision, #83). None was sent to this ring; refusing needs no validation,
    /// sending one would. A command missing from here is not thereby safe: unknown commands are
    /// still limited by the read-only allowlist.
    public var isDestructive: Bool { Self.isDestructive(type: type) }

    public static func isDestructive(type: UInt16) -> Bool {
        let group = UInt8(type >> 8)
        switch group {
        // Whole groups with no permitted documentation and factory, update or collection roles.
        case 0x07: return true  // raw collection (includes deletes 0x0730, 0x0731)
        case 0x0A: return true  // firmware update
        case 0x0C: return true  // self-inspection
        case 0x0E: return true  // factory test
        default: break
        }
        if (0x0540...0x0552).contains(type) { return true }  // Health_Delete*
        switch type {
        case 0x0568: return true  // delete (health data family)
        case 0x0575: return true  // delete (health data family)
        case 0x0578: return true  // delete (health data family)
        case 0x010E: return true  // restore factory settings; reported to wipe the record store (R99, fw 2.32)
        case 0x0151: return true  // reset
        case 0x0157: return true  // reset
        case 0x0316: return true  // reset
        case 0x0809: return true  // reset / factory state
        default: return false
        }
    }

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
