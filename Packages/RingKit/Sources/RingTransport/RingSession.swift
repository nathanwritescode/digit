import Foundation
import RingProtocol

/// What `RingSession` needs from a connection to a ring. `RingConnection` is the Bluetooth
/// implementation; tests supply a scripted one.
public protocol RingLink: Sendable {
    /// Conformers must also refuse frames where `erasesRingData` is true, by throwing `RingError.refusedDestructive`.
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
    private nonisolated let link: any RingLink
    public nonisolated let replyTimeout: TimeInterval
    // Injected so tests can pin the time and zone. The zone is read on each use, not once, because
    // a long-lived session can outlast a change of time zone. The autoupdating zone is meant to
    // follow system changes; not verified on a device.
    private nonisolated let clock: @Sendable () -> Date
    private nonisolated let timeZone: @Sendable () -> TimeZone

    private var busy = false
    private var turnWaiters: [CheckedContinuation<Void, Never>] = []

    public init(
        link: any RingLink,
        replyTimeout: TimeInterval = 5,
        clock: @escaping @Sendable () -> Date = { Date() },
        timeZone: @escaping @Sendable () -> TimeZone = { TimeZone.autoupdatingCurrent }
    ) {
        self.link = link
        self.replyTimeout = replyTimeout
        self.clock = clock
        self.timeZone = timeZone
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
        try await transmit(command)
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

    /// Sets the ring's clock to `date` (default: now by the session's clock) in the session's current time zone.
    public func setTime(_ date: Date? = nil) async throws {
        _ = try await request(Command.setTime(date ?? clock(), timeZone: timeZone()))
    }

    /// Fetches one kind of stored history with the context needed to decode it later. When nothing
    /// is stored the transfer has empty data and `persist` is not called (nothing was received, so
    /// nothing is acknowledged).
    ///
    /// The ring answers with a header frame, then data frames, then an end frame carrying a
    /// checksum of the concatenated data. `persist` then runs with the verified transfer, and only
    /// if it succeeds is the transfer acknowledged, so the data is durable before the ring is
    /// told it was received. No delete command is sent.
    ///
    /// `persist` runs while this session's turn is held, so it must not call back into the
    /// session (any method), or the session deadlocks.
    ///
    /// Verified on an R99 with single-record transfers, where the ring kept its history after
    /// the acknowledgement. Transfers spanning several data frames have not been seen yet.
    public func fetchHistory(
        _ kind: HistoryKind,
        persist: @Sendable (HistoryTransfer) async throws -> Void
    ) async throws -> HistoryTransfer {
        try await exclusively { try await transferHistory(kind, persist: persist) }
    }

    private func transferHistory(
        _ kind: HistoryKind,
        persist: @Sendable (HistoryTransfer) async throws -> Void
    ) async throws -> HistoryTransfer {
        let header = try await converse(Command.history(kind))
        // A short header means there are no records of this kind.
        guard header.payload.count > 9 else { return makeTransfer(kind, header: header, data: []) }

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
                let transfer = makeTransfer(kind, header: header, data: data)
                try await persist(transfer)
                try await transmit(Command.historyTransferOK)
                return transfer
            }
            data += frame.payload
        }
        throw RingError.timeout("\(kind.name) history transfer")
    }

    private func makeTransfer(_ kind: HistoryKind, header: Frame, data: [UInt8]) -> HistoryTransfer {
        HistoryTransfer(kind: kind, header: header.payload, data: data,
                        fetchedAt: HistoryTransfer.truncatedToMilliseconds(clock()), timeZoneIdentifier: timeZone().identifier)
    }

    /// Every write goes through here so the delete-range refusal holds even for a link that forgets it.
    private func transmit(_ frame: Frame) async throws {
        guard !frame.erasesRingData else { throw RingError.refusedDestructive(frame) }
        try await link.send(frame)
    }

    /// Events the ring starts on its own (group `0x04`) must be echoed back with payload `[0x00]`.
    private func acknowledgeIfEvent(_ frame: Frame) async throws {
        guard frame.group == 0x04 else { return }
        try await transmit(Frame(group: frame.group, key: frame.key, payload: [0x00]))
    }
}
