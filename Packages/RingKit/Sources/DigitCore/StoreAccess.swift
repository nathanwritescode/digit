import Foundation
import RingProtocol
import RingStore
import RingSync

/// The store calls the pairing and sync models make themselves (not the sync's own, which go
/// through `RingSyncer`). `RingStore` is the production conformer; tests wrap one to make a call
/// throw `RingStoreError.suspended`, since announcing a real suspension is process-wide.
public protocol RingRegistry: Sendable {
    func rings() async throws -> [StoredRing]
    func ring(id: UUID) async throws -> StoredRing?
    func registerRing(protocolFamily: String, model: String?, firmware: String?, now: Date) async throws -> StoredRing
    func setPeripheralIdentifier(_ identifier: UUID?, forRing id: UUID) async throws
    func updateRing(id: UUID, model: String?, firmware: String?) async throws
}

extension RingStore: RingRegistry {}

/// The two phases of one sync, as `RingSyncer` runs them. The sync model runs the ring phase,
/// disconnects, and only then runs the derive phase.
public protocol SyncPhases: Sendable {
    func syncRing() async -> RingPhaseResult
    func ingestAll() async -> PerKind<IngestionOutcome>
}

extension RingSyncer: SyncPhases {}

/// What the models need from the outside world, injected so tests and previews can use fakes.
public struct DigitServices: Sendable {
    public var ringAccess: any RingAccess
    public var registry: any RingRegistry
    /// The reads behind the sleep and Days screens.
    public var history: any RingHistoryReader
    /// Builds the sync for one connected ring and one stored ring.
    public var makeSync: @Sendable (_ ring: any SyncableRing, _ ringID: UUID) -> any SyncPhases
    public var now: @Sendable () -> Date
    /// The device's zone, used only to say which days are recent (`RecentDays`), never to show a time.
    public var timeZone: @Sendable () -> TimeZone

    public init(
        ringAccess: any RingAccess,
        registry: any RingRegistry,
        history: any RingHistoryReader,
        makeSync: @escaping @Sendable (_ ring: any SyncableRing, _ ringID: UUID) -> any SyncPhases,
        now: @escaping @Sendable () -> Date = { Date() },
        timeZone: @escaping @Sendable () -> TimeZone = { TimeZone.autoupdatingCurrent }
    ) {
        self.ringAccess = ringAccess
        self.registry = registry
        self.history = history
        self.makeSync = makeSync
        self.now = now
        self.timeZone = timeZone
    }

    /// The production wiring: one store for everything, and `RingSyncer` with the default
    /// monitoring interval and the foreground budget.
    public init(ringAccess: any RingAccess, store: RingStore) {
        self.init(ringAccess: ringAccess, registry: store, history: store, makeSync: { ring, ringID in
            RingSyncer(
                ring: ring, store: store, ringID: ringID,
                monitoringInterval: SyncSettings.monitoringInterval, budget: SyncSettings.foregroundBudget)
        })
    }
}

/// Fixed settings of this slice.
public enum SyncSettings {
    /// Sent on every sync until the per-ring setting exists (#19). The default from AGENTS.md
    /// ("One monitoring interval").
    public static let monitoringInterval = MonitoringInterval.tenMinutes

    /// How long the ring steps of a foreground sync may start for. Chosen, not measured: generous
    /// because the owner is watching and a large backlog takes several conversations. The budget is
    /// checked between steps only (no cancellation yet, #52), so a sync can overrun it.
    public static let foregroundBudget: Duration = .seconds(120)

    /// How long pairing listens for rings.
    public static let discoveryDuration: Duration = .seconds(5)

    /// The protocol family and model recorded for a paired ring. Only the R99 is supported, and
    /// pairing lists only rings whose name starts with `YCBT.knownNamePrefixes`.
    public static let protocolFamily = "ycbt"
    public static let model = "R99"
}
