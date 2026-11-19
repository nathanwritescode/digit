import Foundation
import RingProtocol
import RingTransport

/// The ring side of a sync: exactly the three things `RingSyncer` asks of a connected ring.
/// `RingSession` is the real implementation; tests script the ring's behaviour through this
/// protocol instead of building frames. Shaped by the YCBT family (history with a `persist` step,
/// a wall-clock clock-set, one monitoring interval); it will sit behind the planned ring
/// abstraction when a second family arrives.
///
/// Errors follow `RingSession`: `RingError` for transport failures and error replies, and
/// `MonitoringSettingError` when the ring refuses a monitoring interval.
public protocol SyncableRing: Sendable {
    /// Fetches one history kind. `persist` runs with the verified transfer before the ring is told
    /// it was received, and only if it succeeds is the transfer acknowledged. When the ring has
    /// nothing stored the returned transfer has empty data and `persist` is not called. `persist`
    /// runs while the ring's turn is held, so it must not call back into the ring. If `persist`
    /// throws, `fetchHistory` must rethrow that error unchanged (`RingSession` does): the sync
    /// recognises a store suspension on the raw error.
    func fetchHistory(
        _ kind: HistoryKind,
        persist: @Sendable (HistoryTransfer) async throws -> Void
    ) async throws -> HistoryTransfer

    /// Sets the ring's clock to `date` as wall-clock time in `timeZone`, exactly as given.
    func setTime(_ date: Date, timeZone: TimeZone) async throws

    /// Sends the monitoring interval. Throws `MonitoringSettingError` when the ring refuses it.
    func setMonitoringInterval(_ interval: MonitoringInterval) async throws
}

extension RingSession: SyncableRing {}
