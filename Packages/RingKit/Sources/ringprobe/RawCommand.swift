import Foundation
import RingProtocol

/// Why `ringprobe raw` must not send this frame, or nil if it may. Checked before connecting.
/// Destructive commands are refused first and `allowUnknown` never overrides them.
func rawRefusal(_ frame: Frame, operand: String, allowUnknown: Bool) -> String? {
    let type = String(format: "%04X", frame.type)
    if frame.isDestructive {
        return "refusing to send \(type): it can erase data or reset the ring. This is never overridden, including by --allow-unknown."
    }
    if frame.isMonitoringSetting {
        return "refusing to send \(type): it changes the ring's monitoring interval. Use `ringprobe monitor`, which validates the value and asks for confirmation."
    }
    if !Command.isKnownReadOnly(frame), !allowUnknown {
        return "refusing to send \(operand): not a known read-only command with its documented payload. Its effect is unknown; pass --allow-unknown to send it anyway."
    }
    return nil
}
