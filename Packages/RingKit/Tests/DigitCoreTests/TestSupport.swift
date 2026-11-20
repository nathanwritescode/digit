import DigitCore
import Foundation
import RingProtocol
import RingStore
import RingSync
import RingTransport
import Testing

// Fakes for the app's models. Every byte here is SYNTHETIC, built from the documented record
// layouts with made-up values; no ring was used. Names and identifiers are made up too.

// MARK: Time and synthetic records

/// 2027-01-15 08:00:00 UTC. In January London is on UTC+0, so ring-local time equals UTC.
let t0 = Date(timeIntervalSince1970: 1_800_000_000)
let london = TimeZone(identifier: "Europe/London")!

func le32(_ v: UInt32) -> [UInt8] { (0..<4).map { UInt8((v >> (8 * UInt32($0))) & 0xFF) } }
func le16(_ v: Int) -> [UInt8] { [UInt8(v & 0xFF), UInt8((v >> 8) & 0xFF)] }

/// Ring seconds for `seconds` after `t0`.
func ringTime(_ seconds: Int) -> UInt32 {
    UInt32(RingTime.ringSeconds(fromWallClock: t0.addingTimeInterval(TimeInterval(seconds))))
}

/// A short synthetic history: two activity buckets and two heart-rate readings; no sleep or
/// combined records.
let syntheticHistory: [HistoryKind: [UInt8]] = [
    .activity: le32(ringTime(-7200)) + le32(ringTime(-5400)) + le16(120) + le16(60) + le16(5)
        + le32(ringTime(-5400)) + le32(ringTime(-3600)) + le16(80) + le16(40) + le16(3),
    .heartRate: le32(ringTime(-3600)) + [0, 61] + le32(ringTime(-3000)) + [0, 63],
]

/// A made-up advertised name and identifier, for checking that no text carries them.
let syntheticName = "R99 ZQXJ"
let syntheticIdentifier = UUID(uuidString: "0F1E2D3C-4B5A-6978-8796-A5B4C3D2E1F0")!

// MARK: Event log

/// Everything the fakes did, in order, as one sequence. `@unchecked Sendable`: guarded by `lock`.
final class EventLog: @unchecked Sendable {
    private let lock = NSLock()
    private var entries: [String] = []
    func append(_ entry: String) { lock.withLock { entries.append(entry) } }
    var events: [String] { lock.withLock { entries } }
    func count(_ entry: String) -> Int { events.filter { $0 == entry }.count }
}

/// Holds a fake call until the test opens it. `@unchecked Sendable`: guarded by `lock`.
final class Gate: @unchecked Sendable {
    private let lock = NSLock()
    private var opened = false
    var isOpen: Bool { lock.withLock { opened } }
    func open() { lock.withLock { opened = true } }
    /// Waits, in short sleeps, until opened; gives up after `seconds` so no test can hang here.
    func wait(within seconds: Double = 10) async {
        let end = ContinuousClock.now + .seconds(seconds)
        while !isOpen, ContinuousClock.now < end {
            try? await Task.sleep(for: .milliseconds(1))
        }
    }
}

// MARK: Ring side

/// The ring as `RingSyncer` drives it, scripted. Logs `fetch <kind>`, `setTime`, `interval <n>`.
final class ScriptedRing: SyncableRing, @unchecked Sendable {
    enum Fetch {
        case data([UInt8])
        case empty
        case fail(any Error)
    }

    let log: EventLog
    let fetches: [HistoryKind: Fetch]
    let clockError: (any Error)?

    init(log: EventLog, fetches: [HistoryKind: Fetch]? = nil, clockError: (any Error)? = nil) {
        self.log = log
        self.fetches = fetches ?? syntheticHistory.mapValues { .data($0) }
        self.clockError = clockError
    }

    func fetchHistory(
        _ kind: HistoryKind, persist: @Sendable (HistoryTransfer) async throws -> Void
    ) async throws -> HistoryTransfer {
        log.append("fetch \(kind.name)")
        let data: [UInt8]
        switch fetches[kind] ?? .empty {
        case .fail(let error): throw error
        case .empty: data = []
        case .data(let bytes): data = bytes
        }
        let transfer = HistoryTransfer(
            kind: kind, header: Array(repeating: 1, count: data.isEmpty ? 9 : 12), data: data,
            fetchedAt: t0, timeZoneIdentifier: "Europe/London")
        if !data.isEmpty { try await persist(transfer) }
        return transfer
    }

    func setTime(_ date: Date, timeZone: TimeZone) async throws {
        log.append("setTime")
        if let clockError { throw clockError }
    }

    func setMonitoringInterval(_ interval: MonitoringInterval) async throws {
        log.append("interval \(interval.rawValue)")
    }
}

/// The ring-side protocol, scripted. Logs `discover`, `connect`, `deviceDetails`, `disconnect`.
/// `@unchecked Sendable`: every mutable field is guarded by `lock`.
final class FakeRingAccess: RingAccess, @unchecked Sendable {
    private struct Script {
        var candidates: [RingCandidate] = []
        var discoveryProblem: RingAccessProblem?
        var connectProblem: RingAccessProblem?
        var detailsProblem: RingAccessProblem?
        var firmware = "2.32"
        var ring: ScriptedRing
        var connectGate: Gate?
        var connectedIdentifier: UUID?
    }

    let log: EventLog
    private let lock = NSLock()
    private var script: Script

    private func get<T>(_ path: KeyPath<Script, T>) -> T { lock.withLock { script[keyPath: path] } }
    private func set<T>(_ path: WritableKeyPath<Script, T>, _ value: T) { lock.withLock { script[keyPath: path] = value } }

    var candidates: [RingCandidate] { get { get(\.candidates) } set { set(\.candidates, newValue) } }
    var discoveryProblem: RingAccessProblem? { get { get(\.discoveryProblem) } set { set(\.discoveryProblem, newValue) } }
    var connectProblem: RingAccessProblem? { get { get(\.connectProblem) } set { set(\.connectProblem, newValue) } }
    var detailsProblem: RingAccessProblem? { get { get(\.detailsProblem) } set { set(\.detailsProblem, newValue) } }
    var firmware: String { get { get(\.firmware) } set { set(\.firmware, newValue) } }
    var ring: ScriptedRing { get { get(\.ring) } set { set(\.ring, newValue) } }
    /// If set, `connect` waits for it before answering.
    var connectGate: Gate? { get { get(\.connectGate) } set { set(\.connectGate, newValue) } }
    /// The identifier `connect` was last given.
    private(set) var connectedIdentifier: UUID? { get { get(\.connectedIdentifier) } set { set(\.connectedIdentifier, newValue) } }

    init(log: EventLog) {
        self.log = log
        script = Script(ring: ScriptedRing(log: log))
    }

    func discoverRings(for duration: Duration) async throws(RingAccessProblem) -> [RingCandidate] {
        log.append("discover")
        if let discoveryProblem { throw discoveryProblem }
        return candidates
    }

    func connect(identifier: UUID) async throws(RingAccessProblem) -> any ConnectedRing {
        log.append("connect")
        connectedIdentifier = identifier
        if let connectGate { await connectGate.wait() }
        if let connectProblem { throw connectProblem }
        return FakeConnectedRing(access: self)
    }

    final class FakeConnectedRing: ConnectedRing, @unchecked Sendable {
        let access: FakeRingAccess
        init(access: FakeRingAccess) { self.access = access }
        var syncableRing: any SyncableRing { access.ring }
        func deviceDetails() async throws(RingAccessProblem) -> RingDeviceDetails {
            access.log.append("deviceDetails")
            if let problem = access.detailsProblem { throw problem }
            return RingDeviceDetails(firmware: access.firmware)
        }
        func disconnect() async { access.log.append("disconnect") }
    }
}

// MARK: Store side

/// The real store, with chosen calls made to throw `RingStoreError.suspended`, standing in for the
/// app announcing a suspension (process-wide, so never done in tests). Logs `store <call>`.
final class SuspendingRegistry: RingRegistry, @unchecked Sendable {
    let base: RingStore
    let log: EventLog
    var suspendedCalls: Set<String> = []

    init(_ base: RingStore, log: EventLog) {
        self.base = base
        self.log = log
    }

    private func call(_ name: String) throws {
        log.append("store \(name)")
        if suspendedCalls.contains(name) { throw RingStoreError.suspended }
    }

    func rings() async throws -> [StoredRing] { try call("rings"); return try await base.rings() }
    func ring(id: UUID) async throws -> StoredRing? { try call("ring"); return try await base.ring(id: id) }
    func registerRing(protocolFamily: String, model: String?, firmware: String?, now: Date) async throws -> StoredRing {
        try call("registerRing")
        return try await base.registerRing(protocolFamily: protocolFamily, model: model, firmware: firmware, now: now)
    }
    func setPeripheralIdentifier(_ identifier: UUID?, forRing id: UUID) async throws {
        try call("setPeripheralIdentifier")
        try await base.setPeripheralIdentifier(identifier, forRing: id)
    }
    func updateRing(id: UUID, model: String?, firmware: String?) async throws {
        try call("updateRing")
        try await base.updateRing(id: id, model: model, firmware: firmware)
    }
}

/// The real `RingSyncer`, logging `ingest` when the derive phase starts, so tests see its order
/// against `disconnect`.
struct LoggingPhases: SyncPhases {
    let syncer: RingSyncer
    let log: EventLog
    func syncRing() async -> RingPhaseResult { await syncer.syncRing() }
    func ingestAll() async -> PerKind<IngestionOutcome> {
        log.append("ingest")
        return await syncer.ingestAll()
    }
}

// MARK: Fixture

/// A lifecycle that ignores ring work, for tests that are not about background tasks.
@MainActor
final class NoOpLifecycle: RingWorkLifecycle {
    func ringWorkWillStart() {}
    func ringWorkDidEnd() {}
}

/// Everything one test needs: an in-memory store, the fakes and the models.
@MainActor
final class Fixture {
    let log = EventLog()
    let store: RingStore
    let registry: SuspendingRegistry
    let access: FakeRingAccess
    let pairedRing: PairedRing
    let pairing: PairingModel
    let sync: SyncModel
    let lifecycle: SuspensionLifecycle?

    /// `makeLifecycle`, if given, builds the lifecycle the models report to, from the shared ring.
    init(makeLifecycle: ((PairedRing) -> SuspensionLifecycle)? = nil) throws {
        store = try RingStore()
        registry = SuspendingRegistry(store, log: log)
        access = FakeRingAccess(log: log)
        let store = self.store
        let log = self.log
        let services = DigitServices(
            ringAccess: access, registry: registry, history: store,
            makeSync: { ring, ringID in
                LoggingPhases(
                    syncer: RingSyncer(
                        ring: ring, store: store, ringID: ringID, monitoringInterval: SyncSettings.monitoringInterval,
                        budget: .seconds(60), clock: { t0.addingTimeInterval(60) }, timeZone: { london }),
                    log: log)
            },
            now: { t0.addingTimeInterval(120) })
        pairedRing = PairedRing()
        lifecycle = makeLifecycle?(pairedRing)
        let reported: any RingWorkLifecycle = lifecycle ?? NoOpLifecycle()
        pairing = PairingModel(pairedRing: pairedRing, services: services, lifecycle: reported)
        sync = SyncModel(pairedRing: pairedRing, services: services, lifecycle: reported)
    }

    func load() async throws {
        try await pairedRing.load(from: store)
    }

    /// Stores a paired ring with the synthetic identifier, as a previous pairing would have.
    @discardableResult
    func storePairedRing(firmware: String = "2.32") async throws -> StoredRing {
        let ring = try await store.registerRing(protocolFamily: "ycbt", model: "R99", firmware: firmware)
        try await store.setPeripheralIdentifier(syntheticIdentifier, forRing: ring.id)
        try await load()
        return try #require(try await store.ring(id: ring.id))
    }

    /// Events from the fakes, without the registry's store calls.
    var ringEvents: [String] { log.events.filter { !$0.hasPrefix("store ") } }
}

/// Polls `condition` in short sleeps until it holds or `seconds` pass.
@MainActor
func eventually(within seconds: Double = 10, _ condition: () -> Bool) async -> Bool {
    let end = ContinuousClock.now + .seconds(seconds)
    while !condition() {
        if ContinuousClock.now > end { return false }
        try? await Task.sleep(for: .milliseconds(1))
    }
    return true
}
