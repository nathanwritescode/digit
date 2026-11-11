import Foundation
import RingProtocol

/// Request/response conversations with a connected ring.
///
/// One conversation at a time: do not call these methods concurrently on the same connection.
public struct RingSession: Sendable {
    public let connection: RingConnection
    public var replyTimeout: TimeInterval = 5

    public init(connection: RingConnection) {
        self.connection = connection
    }

    /// Sends a command and returns the ring's reply to it. Ring-initiated events are acknowledged and skipped.
    public func request(_ command: Frame) async throws -> Frame {
        // Anything still queued belongs to an earlier conversation that failed or timed out.
        await connection.discardPendingFrames()
        try await connection.send(command)
        while let frame = await connection.nextFrame(timeout: replyTimeout) {
            if frame.type == command.type {
                if frame.isErrorReply { throw RingError.rejected(frame) }
                return frame
            }
            try await acknowledgeIfEvent(frame)
        }
        throw RingError.timeout(String(format: "reply to %04X", command.type))
    }

    public func deviceInfo() async throws -> DeviceInfo? {
        DeviceInfo(payload: try await request(Command.deviceInfo).payload)
    }

    public func currentActivity() async throws -> CurrentActivity? {
        CurrentActivity(payload: try await request(Command.currentActivity).payload)
    }

    public func setTime(_ date: Date = Date()) async throws {
        _ = try await request(Command.setTime(date))
    }

    /// Fetches the raw bytes of one kind of stored history. Returns an empty array when nothing is stored.
    ///
    /// The ring answers with a header frame, then data frames, then an end frame carrying a
    /// checksum of the concatenated data. `persist` then runs with the verified bytes, and only
    /// if it succeeds is the transfer acknowledged, so the data is durable before the ring is
    /// told it was received. No delete command is sent.
    ///
    /// Verified on an R99 with single-record transfers, where the ring kept its history after
    /// the acknowledgement. Transfers spanning several data frames have not been seen yet.
    public func fetchHistory(
        _ kind: HistoryKind,
        persist: @Sendable ([UInt8]) async throws -> Void
    ) async throws -> [UInt8] {
        let header = try await request(Command.history(kind))
        // A short header means there are no records of this kind.
        guard header.payload.count > 9 else { return [] }

        var data: [UInt8] = []
        while let frame = await connection.nextFrame(timeout: replyTimeout) {
            guard frame.group == Command.healthGroup else {
                try await acknowledgeIfEvent(frame)
                continue
            }
            if frame.key == Command.historyEndKey {
                let p = frame.payload
                let expected = p.count >= 6 ? UInt16(p[4]) | UInt16(p[5]) << 8 : nil
                guard let expected, CRC16.ccittFalse(data) == expected else {
                    // Not acknowledged and no resend requested: the caller starts over with a fresh request.
                    throw RingError.checksumMismatch
                }
                try await persist(data)
                try await connection.send(Command.historyTransferOK)
                return data
            }
            data += frame.payload
        }
        throw RingError.timeout("\(kind.name) history transfer")
    }

    /// Events the ring starts on its own (group `0x04`) must be echoed back with payload `[0x00]`.
    private func acknowledgeIfEvent(_ frame: Frame) async throws {
        guard frame.group == 0x04 else { return }
        try await connection.send(Frame(group: frame.group, key: frame.key, payload: [0x00]))
    }
}
