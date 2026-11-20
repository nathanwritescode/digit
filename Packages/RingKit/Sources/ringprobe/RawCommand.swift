import Foundation
import RingProtocol

/// Why `ringprobe raw` must not send this frame, or nil if it may. Checked before connecting.
/// Destructive commands are refused first and `allowUnknown` never overrides them.
func rawRefusal(_ frame: Frame, operand: String, allowUnknown: Bool) -> String? {
    let type = String(format: "%04X", frame.type)
    guard frame.isEncodable else {
        return "refusing to send \(type): the payload is longer than \(Frame.maximumPayload) bytes."
    }
    if frame.isDestructive {
        return "refusing to send \(type): it can erase data or reset the ring. This is never overridden, including by --allow-unknown."
    }
    // A payload that holds a destructive command's header could be an embedded frame; cheap hardening.
    let bytes = frame.payload
    if bytes.count >= 2, (0...(bytes.count - 2)).contains(where: {
        Frame(group: bytes[$0], key: bytes[$0 + 1]).isDestructive
    }) {
        return "refusing to send \(type): its payload contains the header of a command that can erase data or reset the ring. This is never overridden, including by --allow-unknown."
    }
    if frame.isMonitoringSetting {
        return "refusing to send \(type): it changes the ring's monitoring interval. Use `ringprobe monitor`, which validates the value and asks for confirmation."
    }
    if !Command.isKnownReadOnly(frame), !allowUnknown {
        return "refusing to send \(operand): not a known read-only command with its documented payload. Its effect is unknown; pass --allow-unknown to send it anyway."
    }
    return nil
}
