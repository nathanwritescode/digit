import Foundation
import RingProtocol
import RingStore

/// The three store operations the sync uses. Internal: it exists so tests can make a store call
/// throw `RingStoreError.suspended` without announcing a suspension, which is process-wide and
/// must not be done from this target's tests. `RingStore` is the only production conformer.
protocol SyncStore: Sendable {
    func insert(_ transfer: HistoryTransfer, ringID: UUID) async throws -> InsertOutcome
    func recordClockSet(_ event: ClockSetEvent) async throws
    func ingest(ringID: UUID, kind: HistoryKind) async throws -> IngestionResult
}

extension RingStore: SyncStore {}
