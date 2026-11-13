import Foundation
import RingProtocol

/// Why the ring did not accept a monitoring interval.
public enum MonitoringSettingError: Error, Equatable, CustomStringConvertible {
    /// The ring answered `FC`.
    case unsupported
    /// Any other reply that is not `00`: the code if the payload was one byte, otherwise nil.
    case unexpectedReply(UInt8?)

    public var description: String {
        switch self {
        case .unsupported: "the ring does not support the monitoring interval command"
        case .unexpectedReply(let code): "unexpected reply to the monitoring interval command" + (code.map { String(format: " (%02X)", $0) } ?? "")
        }
    }
}

extension RingSession {
    /// Sends the monitoring interval (`0x010C`) and maps the ack: `00` is accepted, `FC` is
    /// unsupported, anything else is an error. The ring cannot report this setting back, so the
    /// caller must remember it. Tested only against a scripted link; not yet sent to a ring
    /// through this method.
    public func setMonitoringInterval(_ interval: MonitoringInterval) async throws {
        let reply: Frame
        do {
            reply = try await request(Command.setMonitoringInterval(interval))
        } catch RingError.rejected(let frame) where frame.payload == [0xFC] {
            throw MonitoringSettingError.unsupported
        }
        guard reply.payload == [0x00] else {
            throw MonitoringSettingError.unexpectedReply(reply.payload.count == 1 ? reply.payload[0] : nil)
        }
    }
}
