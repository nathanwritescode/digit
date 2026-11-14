import Foundation
import RingProtocol

/// Why the ring did not accept a monitoring interval.
public enum MonitoringSettingError: Error, Equatable, CustomStringConvertible {
    /// The ring answered `FC`.
    case unsupported
    /// The ring answered another error code (`FB`, `FD`, `FE` or `FF`).
    case rejected(UInt8)
    /// A reply that is neither the ack (exactly `00`) nor an error code: the code if the payload
    /// was one byte, otherwise nil.
    case unexpectedReply(UInt8?)

    public var description: String {
        switch self {
        case .unsupported: "the ring does not support the monitoring interval command"
        case .rejected(let code): String(format: "the ring rejected the monitoring interval (code %02X)", code)
        case .unexpectedReply(let code): "unexpected reply to the monitoring interval command" + (code.map { String(format: " (%02X)", $0) } ?? "")
        }
    }
}

extension RingSession {
    /// Sends the monitoring interval (`0x010C`) and maps the ack: exactly `00` is accepted, `FC` is
    /// `.unsupported`, the other error codes are `.rejected`, and anything else is `.unexpectedReply`.
    /// So a caller catches `MonitoringSettingError` for "the ring refused the setting" and only
    /// `RingError` (timeout, not connected, ...) for transport failures. On a timeout the ring may
    /// still have applied the setting. The ring cannot report this setting back, so the caller must
    /// remember it. Tested only against a scripted link; not yet sent to a ring through this method.
    public func setMonitoringInterval(_ interval: MonitoringInterval) async throws {
        let reply: Frame
        do {
            reply = try await request(Command.setMonitoringInterval(interval))
        } catch RingError.rejected(let frame) where frame.payload.count == 1 {
            let code = frame.payload[0]
            throw code == 0xFC ? MonitoringSettingError.unsupported : MonitoringSettingError.rejected(code)
        }
        guard reply.payload == [0x00] else {
            throw MonitoringSettingError.unexpectedReply(reply.payload.count == 1 ? reply.payload[0] : nil)
        }
    }
}
