import Foundation
import RingProtocol

/// What `RingSession` needs from a connection to a ring. `RingConnection` is the Bluetooth
/// implementation; tests supply a scripted one.
public protocol RingLink: Sendable {
    func send(_ frame: Frame) async throws
    /// The next frame from the ring, in arrival order, or `nil` if none arrives within `timeout`.
    func nextFrame(timeout: TimeInterval) async -> Frame?
    /// Drops frames and partial frames left over from an earlier conversation.
    func discardPendingFrames() async
}

extension RingConnection: RingLink {}

/// Request/response conversations with a connected ring.
///
/// An actor that runs one conversation at a time: a call made while another is in progress
/// waits its turn, so conversations cannot steal each other's frames. Actor methods can
/// interleave at every `await`, so the actor alone is not enough; `exclusively` is the queue.
public actor RingSession {
    public nonisolated let link: any RingLink
    public nonisolated let replyTimeout: TimeInterval

    private var busy = false
    private var turnWaiters: [CheckedContinuation<Void, Never>] = []

    public init(link: any RingLink, replyTimeout: TimeInterval = 5) {
        self.link = link
        self.replyTimeout = replyTimeout
    }

    /// Runs `body` once every earlier conversation has finished. Turns are granted in call order.
    private func exclusively<T: Sendable>(_ body: () async throws -> T) async throws -> T {
        if busy {
            await withCheckedContinuation { turnWaiters.append($0) }
        } else {
            busy = true
        }
        defer {
            // Hand the turn straight to the next waiter so a newcomer cannot jump the queue.
            if turnWaiters.isEmpty { busy = false } else { turnWaiters.removeFirst().resume() }
        }
        return try await body()
    }

    /// Sends a command and returns the ring's reply to it. Ring-initiated events are acknowledged and skipped.
    public func request(_ command: Frame) async throws -> Frame {
        try await exclusively { try await converse(command) }
    }

    private func converse(_ command: Frame) async throws -> Frame {
        // Anything still queued belongs to an earlier conversation that failed or timed out.
        await link.discardPendingFrames()
        try await link.send(command)
        while let frame = await link.nextFrame(timeout: replyTimeout) {
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
        try await exclusively { try await transferHistory(kind, persist: persist) }
    }

    private func transferHistory(
        _ kind: HistoryKind,
        persist: @Sendable ([UInt8]) async throws -> Void
    ) async throws -> [UInt8] {
        let header = try await converse(Command.history(kind))
        // A short header means there are no records of this kind.
        guard header.payload.count > 9 else { return [] }

        var data: [UInt8] = []
        while let frame = await link.nextFrame(timeout: replyTimeout) {
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
                try await link.send(Command.historyTransferOK)
                return data
            }
            data += frame.payload
        }
        throw RingError.timeout("\(kind.name) history transfer")
    }

    /// Events the ring starts on its own (group `0x04`) must be echoed back with payload `[0x00]`.
    private func acknowledgeIfEvent(_ frame: Frame) async throws {
        guard frame.group == 0x04 else { return }
        try await link.send(Frame(group: frame.group, key: frame.key, payload: [0x00]))
    }
}
