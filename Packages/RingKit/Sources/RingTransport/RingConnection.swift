// @preconcurrency because the CBUUID statics in YCBT are not Sendable; all CoreBluetooth objects stay on the connection's queue.
@preconcurrency import CoreBluetooth
import Foundation
import RingProtocol

public enum RingError: Error, CustomStringConvertible {
    case bluetoothUnavailable(CBManagerState)
    /// No matching ring was reached before the deadline. For `connect(identifier:)` the system
    /// knew the identifier but the ring never connected: it is probably out of range, so retry later.
    case notFound
    /// `connect(identifier:)` only: this phone or Mac's Bluetooth system does not know the
    /// identifier and a scan did not hear it before the deadline. The identifier is probably stale
    /// (taken on another device, or before the system forgot the ring, for example after a
    /// network-settings reset), so the app should offer to pair the ring again.
    case unknownIdentifier
    case connectionFailed(String)
    case missingService
    case notConnected
    /// What was waited for. The text must be built from constants and command types only, never
    /// from ring or health data, because the sync keeps it in its results (`SyncError`).
    case timeout(String)
    case rejected(Frame)
    case checksumMismatch
    case refusedDestructive(Frame)
    /// A second connect or write was started while one was still in progress.
    case busy(String)
    /// `RingSession.stop()` was called; the link itself may still be up.
    case stopped
    /// More frames arrived for a conversation than it could hold while it was not reading.
    case tooManyFrames(Int)
    /// A request in the event (`0x04`) or live-value (`0x06`) group, whose reply the session's
    /// reader would deliver to subscribers instead of the request.
    case refusedUploadGroup(Frame)

    public var description: String {
        switch self {
        case .bluetoothUnavailable(let state): "Bluetooth is unavailable (state \(state.rawValue))"
        case .notFound: "no matching ring was found in range"
        case .unknownIdentifier: "this device's Bluetooth system does not know the ring and it was not heard in range; pair it again"
        case .connectionFailed(let why): "connection failed: \(why)"
        case .missingService: "the device does not expose the YCBT service"
        case .notConnected: "not connected"
        case .timeout(let what): "timed out waiting for \(what)"
        case .rejected(let frame): String(format: "ring rejected command %04X (code %02X)", frame.type, frame.payload.first ?? 0)
        case .checksumMismatch: "history transfer failed its checksum"
        case .busy(let what): "busy: \(what)"
        case .refusedDestructive(let frame): String(format: "refused to send %04X: it erases history from the ring", frame.type)
        case .stopped: "the session was stopped"
        case .tooManyFrames(let limit): "more than \(limit) frames arrived while the conversation was not reading"
        case .refusedUploadGroup(let frame): String(format: "refused to send %04X: replies in group %02X are treated as ring uploads", frame.type, frame.group)
        }
    }
}

public struct DiscoveredDevice: Sendable {
    /// The CoreBluetooth identifier, which `connect(identifier:)` takes. It is assigned by this
    /// phone or Mac's Bluetooth system, is not the Bluetooth address, differs on every phone or
    /// Mac, and can change (for example after a network-settings reset). On one device it is a
    /// stable pseudonym for the ring, so it identifies the owner's ring: never log or export it,
    /// and never write it into the repository, issues or pull requests.
    public let identifier: UUID
    public let name: String?
    public let rssi: Int
    public let serviceUUIDs: [String]

    /// Public so tools and tests can build devices without CoreBluetooth.
    public init(identifier: UUID, name: String?, rssi: Int, serviceUUIDs: [String]) {
        self.identifier = identifier
        self.name = name
        self.rssi = rssi
        self.serviceUUIDs = serviceUUIDs
    }

    /// Whether this device looks like a supported ring: it advertises the YCBT service, or its
    /// advertised name starts with one of `namePrefixes` (ignoring case). The rule
    /// `discoverRings` applies; public so tools can label a full scan the same way.
    public func isCandidateRing(namePrefixes: [String]) -> Bool {
        RingDiscovery.isCandidate(self, service: YCBT.service.uuidString, namePrefixes: namePrefixes)
    }
}

// A device's identifier and name identify the owner's ring and neighbours' devices, so printing or
// dumping one must not show them. The fields shown are listed by hand; a field added later stays
// hidden until someone adds it on purpose.
extension DiscoveredDevice: CustomStringConvertible, CustomDebugStringConvertible, CustomReflectable {
    public var description: String { "DiscoveredDevice(rssi: \(rssi), services: \(serviceUUIDs.count); identifier and name hidden)" }
    public var debugDescription: String { description }
    public var customMirror: Mirror {
        Mirror(self, children: ["rssi": rssi, "serviceCount": serviceUUIDs.count], displayStyle: .struct)
    }
}

public enum YCBT {
    public static let service = CBUUID(string: "BE940000-7333-BE46-B7AE-689E71722BD5")
    /// Advertised name prefixes of the rings in this family that digit knows, for callers to pass
    /// to `discoverRings`. Moves behind the ring abstraction when a second family arrives.
    public static let knownNamePrefixes = ["R99"]
    /// Commands in, replies out.
    public static let command = CBUUID(string: "BE940001-7333-BE46-B7AE-689E71722BD5")
    /// Ring-initiated uploads.
    public static let upload = CBUUID(string: "BE940003-7333-BE46-B7AE-689E71722BD5")
}

/// A Bluetooth link to one YCBT ring. Received frames queue up in order and are read with `nextFrame`.
///
/// Supports one conversation at a time; `RingSession` enforces that by queueing callers.
///
/// Every wait has a deadline and every waiter is resumed exactly once: each continuation is stored
/// in one property on `queue`, and whoever takes it out (the result callback, a timer, a disconnect
/// or a Bluetooth state change) sets it to `nil` first, so a later path finds nothing to resume.
/// The CoreBluetooth paths cannot be unit tested; they rely on that discipline.
/// `@unchecked Sendable` because all mutable state is confined to `queue`; the compiler cannot see that.
public final class RingConnection: NSObject, @unchecked Sendable {
    /// Called on the Bluetooth queue with every raw notification and write, for capture logs.
    private let onRawBytes: (@Sendable (_ outgoing: Bool, _ bytes: [UInt8]) -> Void)?

    private let queue = DispatchQueue(label: "digit.ring.bluetooth")

    // All mutable state below is only touched on `queue`.
    private var central: CBCentralManager!
    private var peripheral: CBPeripheral?
    private var commandCharacteristic: CBCharacteristic?
    private var stateWaiters: [Int: CheckedContinuation<Void, Error>] = [:]
    private var lastStateWaiterID = 0
    // What a scan has heard. Cleared when a listing scan returns and when a connect starts and
    // finishes, so names and identifiers of unrelated devices are not kept beyond the call.
    private var scanResults: [UUID: DiscoveredDevice] = [:]
    private var scanMatch: (@Sendable (DiscoveredDevice) -> Bool)?
    // The waiter of the running listing scan and which scan it belongs to. Resumed only by
    // `finishListing`, which takes it out first, so its timer and a Bluetooth state change cannot
    // both resume it.
    private var listingWaiter: CheckedContinuation<[DiscoveredDevice], Error>?
    private var listingGeneration = 0
    // Who started the running scan, if any. CoreBluetooth has one scan per central manager, so
    // only the owner may stop it; `RingDiscovery.scanConflict` refuses a second user. Released by
    // the listing scan's timer, or for a connect by `finishConnect` (every exit of an attempt
    // goes through it) or when the scan hears the matching device.
    private var scanOwner: RingDiscovery.ScanOwner?
    private var connectWaiter: CheckedContinuation<Void, Error>?
    private var connectAttempt = 0
    private var writeWaiter: CheckedContinuation<Void, Error>?
    private var writeAttempt = 0
    // Writes that timed out but whose confirmation may still arrive. Assumes with-response writes
    // are confirmed in order, and that a confirmation that never comes ends in an ATT timeout that
    // drops the link (which resets this counter). If a confirmation were lost on a link that
    // stayed up, every later one would be attributed to the previous write. Unverified on the ring.
    private var abandonedWrites = 0
    // Whether `didConnect` has arrived for the current attempt. CoreBluetooth hands back the same
    // CBPeripheral object for the same device, so once the ring is rediscovered the identity guard
    // cannot tell a late `didDisconnect` of an earlier, cancelled link from one for this attempt.
    // A disconnect can only be for this attempt after it connected (a failure before that arrives
    // as `didFailToConnect`), so disconnects before `didConnect` are ignored.
    private var didConnectCurrent = false
    // The peripheral whose pending or live link was most recently cancelled, until its
    // `didDisconnectPeripheral` or `didFailToConnect` arrives or `cancelGracePeriod` passes.
    // CoreBluetooth hands back the same CBPeripheral object for the same device, so a new attempt
    // that attaches it straight away would otherwise receive the old link's late callbacks as its
    // own, and its `connect` could be swallowed by the cancel. Only the latest cancelled link is
    // tracked. Unverified on hardware: how CoreBluetooth orders a cancel followed by a connect on
    // the same peripheral has not been observed.
    private var cancelledLink: CBPeripheral?
    private var cancelledLinkGeneration = 0
    // A peripheral the current attempt will connect to once `cancelledLink` (the same object) has
    // finished. `self.peripheral` stays nil meanwhile, so every `=== self.peripheral` guard drops
    // the old link's callbacks.
    private var waitingAttach: CBPeripheral?
    private var pendingNotifications = 0
    private var assemblers: [CBUUID: FrameAssembler] = [:]
    private let inbox = FrameInbox()
    public let writeTimeout: TimeInterval
    private let stateTimeout: TimeInterval

    /// How long a new attempt waits for a just-cancelled link to the same peripheral to report
    /// that it ended. Apple documents a `didDisconnectPeripheral` for cancelling a connected
    /// peripheral but not whether cancelling a pending connection produces any callback, so the
    /// wait must be bounded. It runs inside the attempt's own deadline. Chosen, not measured.
    static let cancelGracePeriod: TimeInterval = 1

    /// - Parameters:
    ///   - writeTimeout: how long a write may wait for the ring's confirmation.
    ///   - stateTimeout: how long to wait for Bluetooth to become ready (it may be waiting on a permission prompt).
    public init(
        writeTimeout: TimeInterval = 5,
        stateTimeout: TimeInterval = 15,
        onRawBytes: (@Sendable (_ outgoing: Bool, _ bytes: [UInt8]) -> Void)? = nil
    ) {
        self.writeTimeout = writeTimeout
        self.stateTimeout = stateTimeout
        self.onRawBytes = onRawBytes
        super.init()
        central = CBCentralManager(delegate: self, queue: queue)
    }

    // MARK: Scanning

    /// Lists every device heard advertising during `duration`, strongest signal first.
    ///
    /// Throws `RingError.busy` while a connect attempt is pending or another scan is running:
    /// they would share CoreBluetooth's one scan, and one could stop the other's. Throws
    /// `RingError.bluetoothUnavailable` if Bluetooth leaves powered-on before `duration` has
    /// passed, since what was heard is then incomplete. Unverified on hardware.
    public func scan(for duration: TimeInterval) async throws -> [DiscoveredDevice] {
        try await waitUntilPoweredOn()
        return try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<[DiscoveredDevice], Error>) in
            queue.async {
                if let conflict = RingDiscovery.scanConflict(owner: self.scanOwner, connectPending: self.connectWaiter != nil) {
                    continuation.resume(throwing: RingError.busy(conflict))
                    return
                }
                guard self.central.state == .poweredOn else {
                    continuation.resume(throwing: RingError.bluetoothUnavailable(self.central.state))
                    return
                }
                self.scanOwner = .listing
                self.scanResults = [:]
                self.scanMatch = nil
                self.listingWaiter = continuation
                self.listingGeneration += 1
                let generation = self.listingGeneration
                self.central.scanForPeripherals(withServices: nil)
                // Normally the exit of a listing scan; the other is Bluetooth leaving powered-on
                // (`centralManagerDidUpdateState`). Both go through `finishListing`, which resumes
                // the waiter once and releases ownership. Nothing can start another listing scan
                // meanwhile (connects and scans are refused while one runs), but the other exit
                // can end this one early, so the timer checks it still belongs to this scan.
                self.queue.asyncAfter(deadline: .now() + duration) {
                    guard self.listingWaiter != nil, self.listingGeneration == generation else { return }
                    self.finishListing(.success(RingDiscovery.ordered(Array(self.scanResults.values))))
                }
            }
        }
    }

    /// Lists the devices heard during `duration` that look like a supported ring, strongest first:
    /// those advertising the YCBT service, or whose advertised name starts with one of
    /// `namePrefixes` (ignoring case; pass `YCBT.knownNamePrefixes`).
    ///
    /// Each result carries the identifier for `connect(identifier:)`. It is assigned by this phone
    /// or Mac's Bluetooth system (it is not the Bluetooth address), differs on every device and can
    /// change, for example after a network-settings reset. It is a stable pseudonym for the ring
    /// on this device: never log or export it, or write it into the repository, issues or pull
    /// requests.
    ///
    /// The scan is unfiltered and the results are filtered afterwards. A scan filtered by service
    /// reports only peripherals advertising one of the listed services, and the R99 does not list
    /// the YCBT service (seen from a Mac: it lists `180D` and `FEE7`), so a filtered scan misses it.
    /// Throws `RingError.busy` like `scan(for:)`, and `RingError.bluetoothUnavailable` if Bluetooth
    /// leaves powered-on before the scan ends (a partial list is never returned).
    /// What an iPhone sees in the R99's advertisement has not been checked.
    public func discoverRings(for duration: TimeInterval, namePrefixes: [String]) async throws -> [DiscoveredDevice] {
        let devices = try await scan(for: duration)
        return RingDiscovery.candidates(in: devices, service: YCBT.service.uuidString, namePrefixes: namePrefixes)
    }

    // MARK: Connecting

    /// Scans for the first device accepted by `matching`, connects, and subscribes to the YCBT channels.
    ///
    /// Throws `RingError.notFound` if no device matched in time, `RingError.timeout` if one
    /// matched but setup did not finish in time, and `RingError.busy` while another connect
    /// attempt is pending or a scan is running.
    public func connect(timeout: TimeInterval = 20, matching: @escaping @Sendable (DiscoveredDevice) -> Bool) async throws {
        try await connect(to: .matching(matching), timeout: timeout)
    }

    /// Connects to the ring with this CoreBluetooth identifier and subscribes to the YCBT channels.
    ///
    /// The identifier is the one `discoverRings` returned on this phone or Mac. It is assigned by
    /// this device's Bluetooth system, is not the Bluetooth address, differs on every device and can
    /// change (for example after a network-settings reset); it is a stable pseudonym for the ring
    /// here, so never log or export it, or write it into the repository, issues or pull requests.
    ///
    /// If the system knows the identifier (`retrievePeripherals(withIdentifiers:)`), this connects
    /// without scanning; otherwise it falls back to a scan that accepts only a device with this
    /// identifier. That scan is unfiltered, and on iOS an unfiltered scan finds nothing while the
    /// app is in the background. A connection attempt never times out on its own (it stays pending
    /// while the ring is out of range), so the single deadline cancels it. Both make this a
    /// foreground mode: background reconnect (#54) needs a pending connection left open and a
    /// service-filtered scan instead.
    ///
    /// If the same peripheral's link was cancelled just before (a reconnect straight after a live
    /// link), the new `connect` waits for the old link to report that it ended, or for
    /// `cancelGracePeriod`, inside the same deadline.
    ///
    /// Throws, when the deadline passes:
    /// - `RingError.notFound` if the system knew the identifier but the ring never connected
    ///   (probably out of range: retry quietly);
    /// - `RingError.unknownIdentifier` if the system did not know it and the scan never heard it
    ///   (probably stale: offer to pair again);
    /// - `RingError.timeout` if the ring connected, or the scan heard it, but setup did not finish
    ///   (as `connect(matching:)` reports a device it heard).
    /// Throws `RingError.busy` while another connect attempt is pending or a scan is running.
    ///
    /// On hardware, connecting by identifier worked from a Mac with an R99, but which path ran
    /// (without a scan, or the fallback scan) was not observed. Cancelling a pending connection at
    /// the deadline and the wait for a cancelled link have not been tried with the R99.
    public func connect(identifier: UUID, timeout: TimeInterval = 20) async throws {
        try await connect(to: .identifier(identifier), timeout: timeout)
    }

    private enum ConnectTarget: Sendable {
        case matching(@Sendable (DiscoveredDevice) -> Bool)
        case identifier(UUID)
    }

    private func connect(to target: ConnectTarget, timeout: TimeInterval) async throws {
        try await waitUntilPoweredOn()
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            queue.async {
                if let conflict = RingDiscovery.scanConflict(owner: self.scanOwner, connectPending: self.connectWaiter != nil) {
                    continuation.resume(throwing: RingError.busy(conflict))
                    return
                }
                // Bluetooth may have gone away since `waitUntilPoweredOn` returned. That state change
                // was handled before this waiter was stored, so only the deadline would fail it.
                guard self.central.state == .poweredOn else {
                    continuation.resume(throwing: RingError.bluetoothUnavailable(self.central.state))
                    return
                }
                // Drop whatever an earlier attempt left behind so this one starts clean. This
                // cancels a live or pending link, which `attach` then waits out if it is the same
                // peripheral.
                self.resetLink(cancelling: true)
                self.scanResults = [:]
                self.connectWaiter = continuation
                self.connectAttempt += 1
                let attempt = self.connectAttempt
                let search: RingDiscovery.ConnectSearch
                switch target {
                case .matching(let matching):
                    search = .matching
                    self.startConnectScan(matching)
                case .identifier(let identifier):
                    if let known = self.central.retrievePeripherals(withIdentifiers: [identifier]).first {
                        search = .knownIdentifier
                        self.scanMatch = nil
                        self.attach(known)
                    } else {
                        search = .unknownIdentifier
                        self.startConnectScan { $0.identifier == identifier }
                    }
                }
                // One deadline covers scanning, waiting out a cancelled link, connecting, discovery
                // and subscribing.
                self.queue.asyncAfter(deadline: .now() + timeout) {
                    // A timer left over from an earlier attempt must not fail a later one.
                    guard self.connectWaiter != nil, self.connectAttempt == attempt else { return }
                    // A scan only sets a peripheral once it heard the device. A retrieved one is
                    // set (or waiting) whether or not the ring is in range; the rule ignores
                    // `heard` for it.
                    let heard = self.peripheral != nil || self.waitingAttach != nil
                    let error = RingDiscovery.connectTimeoutError(search, heard: heard, connected: self.didConnectCurrent)
                    // `finishConnect` stops the scan only if this attempt still owns it.
                    self.resetLink(cancelling: true)
                    self.finishConnect(.failure(error))
                }
            }
        }
    }

    public func disconnect() {
        queue.async {
            // A cancelled pending connection may get no delegate callback (Apple does not say),
            // so fail the waiters here.
            // A connect's scan is stopped by `finishConnect`; a listing scan is left to its timer.
            self.resetLink(cancelling: true)
            self.failPending(connect: RingError.notConnected, write: RingError.notConnected)
        }
    }

    // MARK: Frames

    public func send(_ frame: Frame) async throws {
        guard !frame.erasesRingData else { throw RingError.refusedDestructive(frame) }
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            queue.async {
                guard let peripheral = self.peripheral, peripheral.state == .connected,
                      let characteristic = self.commandCharacteristic else {
                    continuation.resume(throwing: RingError.notConnected)
                    return
                }
                guard self.writeWaiter == nil else {
                    continuation.resume(throwing: RingError.busy("another write is in progress"))
                    return
                }
                let bytes = frame.encoded()
                self.onRawBytes?(true, bytes)
                self.writeWaiter = continuation
                self.writeAttempt += 1
                let attempt = self.writeAttempt
                peripheral.writeValue(Data(bytes), for: characteristic, type: .withResponse)
                self.queue.asyncAfter(deadline: .now() + self.writeTimeout) {
                    // Only the write this timer was started for; a later write has its own timer.
                    guard let waiter = self.writeWaiter, self.writeAttempt == attempt else { return }
                    self.writeWaiter = nil
                    self.abandonedWrites += 1
                    waiter.resume(throwing: RingError.timeout("write confirmation"))
                }
            }
        }
    }

    /// Resets the assembler of the command characteristic, which carries the replies, and nothing
    /// else: queued complete frames stay, and so do partial frames on the upload characteristic,
    /// which carries no replies, so a stale partial there cannot be misread as one. The cost is
    /// that a misaligned upload stream is not recovered by this (only by reconnecting).
    public func discardPartialFrames() async {
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            queue.async {
                self.assemblers[YCBT.command]?.reset()
                continuation.resume()
            }
        }
    }

    /// The next frame from the ring, or `nil` if none arrives within `timeout`.
    public func nextFrame(timeout: TimeInterval) async -> Frame? {
        await inbox.next(timeout: timeout)
    }

    // MARK: Private

    private func waitUntilPoweredOn() async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            queue.async {
                switch self.central.state {
                case .poweredOn: continuation.resume()
                case .unknown, .resetting:
                    self.lastStateWaiterID += 1
                    let id = self.lastStateWaiterID
                    self.stateWaiters[id] = continuation
                    self.queue.asyncAfter(deadline: .now() + self.stateTimeout) {
                        guard let waiter = self.stateWaiters.removeValue(forKey: id) else { return }
                        waiter.resume(throwing: RingError.timeout("Bluetooth to become ready"))
                    }
                default: continuation.resume(throwing: RingError.bluetoothUnavailable(self.central.state))
                }
            }
        }
    }

    /// Starts the scan of a connect attempt. Only called with `connectWaiter` set and no scan
    /// running (checked by `scanConflict`).
    private func startConnectScan(_ matching: @escaping @Sendable (DiscoveredDevice) -> Bool) {
        scanMatch = matching
        scanOwner = .connect
        central.scanForPeripherals(withServices: nil)
    }

    /// The one way a listing scan ends: resumes its waiter (once; a later call finds nothing) and
    /// releases the scan and what it heard. Unverified on hardware: the early exit, a Bluetooth
    /// state change in the middle of a scan, has only been reasoned about, not run.
    private func finishListing(_ result: Result<[DiscoveredDevice], Error>) {
        guard let waiter = listingWaiter else { return }
        listingWaiter = nil
        if scanOwner == .listing {
            stopScanIfPoweredOn()
            scanOwner = nil
        }
        scanMatch = nil
        scanResults = [:]
        waiter.resume(with: result)
    }

    /// `stopScan` while Bluetooth is not powered on is API misuse (and the scan is gone anyway).
    private func stopScanIfPoweredOn() {
        if central.state == .poweredOn { central.stopScan() }
    }

    /// Makes `peripheral` the current link and starts connecting to it. The attempt never times
    /// out on its own; the connect deadline cancels it through `resetLink`. Holding it in
    /// `self.peripheral` (or `waitingAttach`) also keeps it alive, which a pending connection needs
    /// (CoreBluetooth cancels the attempt if the peripheral is deallocated).
    ///
    /// If this is the peripheral whose link was just cancelled, the connect is held back until
    /// that link has ended (`cancelledLinkEnded`), so its late callbacks cannot be taken for this
    /// attempt's and the cancel cannot swallow this connect.
    private func attach(_ peripheral: CBPeripheral) {
        if let cancelledLink, cancelledLink === peripheral {
            waitingAttach = peripheral
            return
        }
        self.peripheral = peripheral
        peripheral.delegate = self
        central.connect(peripheral)
    }

    /// Records that `peripheral`'s pending or live link was just cancelled, and bounds the wait
    /// for it to report that it ended.
    private func noteCancelled(_ peripheral: CBPeripheral) {
        cancelledLink = peripheral
        cancelledLinkGeneration += 1
        let generation = cancelledLinkGeneration
        queue.asyncAfter(deadline: .now() + Self.cancelGracePeriod) {
            // A later cancel has its own timer; one that already ended needs nothing.
            guard self.cancelledLinkGeneration == generation, self.cancelledLink != nil else { return }
            self.cancelledLinkEnded()
        }
    }

    /// The cancelled link reported that it ended, or the grace period passed: a connect that was
    /// waiting for it goes ahead. A waiting attach exists only while its attempt is pending, since
    /// every exit of an attempt goes through `resetLink` or `finishConnect`, which drop it.
    private func cancelledLinkEnded() {
        cancelledLink = nil
        guard let waiting = waitingAttach else { return }
        waitingAttach = nil
        guard connectWaiter != nil else { return }
        attach(waiting)
    }

    /// The one way a connect attempt ends: resumes its waiter (once; a later call finds nothing)
    /// and releases what the attempt held, whichever path got here.
    private func finishConnect(_ result: Result<Void, Error>) {
        guard let waiter = connectWaiter else { return }
        connectWaiter = nil
        if scanOwner == .connect {
            stopScanIfPoweredOn()
            scanOwner = nil
        }
        scanMatch = nil
        scanResults = [:]
        waitingAttach = nil
        waiter.resume(with: result)
    }

    /// Fails the connect and write waiters, if any. Safe to call when there are none.
    private func failPending(connect: Error, write: Error) {
        finishConnect(.failure(connect))
        if let waiter = writeWaiter {
            writeWaiter = nil
            waiter.resume(throwing: write)
        }
    }

    /// Fails the connect after a setup step went wrong and gives up on the link.
    private func failSetup(_ error: RingError) {
        resetLink(cancelling: true)
        finishConnect(.failure(error))
    }

    /// The one place the link's state is reset: used when dropping a peripheral, when it
    /// disconnects and when Bluetooth leaves powered-on, so the rules cannot drift apart.
    /// `cancelling` is false when the link is already gone (no point cancelling it).
    /// Clears the old peripheral's delegate so its later callbacks are not applied to a newer attempt,
    /// and records a cancelled link so a reconnect to the same peripheral waits it out.
    private func resetLink(cancelling: Bool) {
        if let peripheral {
            peripheral.delegate = nil
            if cancelling {
                // Every current peripheral has had `connect` called on it. One whose link has
                // already ended needs no wait (its callback, if still queued, is ignored by the
                // `didConnectCurrent` guard or finds a different current peripheral).
                let linked = peripheral.state != .disconnected
                central.cancelPeripheralConnection(peripheral)
                if linked { noteCancelled(peripheral) }
            }
        }
        // A waiting peripheral was never connected anew; the link it waits on is still tracked.
        waitingAttach = nil
        peripheral = nil
        didConnectCurrent = false
        commandCharacteristic = nil
        pendingNotifications = 0
        abandonedWrites = 0
        assemblers = [:]
        inbox.clear()
    }
}

extension RingConnection: CBCentralManagerDelegate {
    public func centralManagerDidUpdateState(_ central: CBCentralManager) {
        if central.state != .poweredOn {
            // Bluetooth went away (or is resetting) under a connection: no disconnect callback is
            // guaranteed to follow, so reset here and fail whatever is pending. Every link is gone,
            // including a cancelled one still being waited out, and the scan with them. A listing
            // scan is cut short, so what it heard is incomplete and it fails instead of returning
            // a partial or empty list.
            resetLink(cancelling: false)
            cancelledLink = nil
            let error = RingError.bluetoothUnavailable(central.state)
            failPending(connect: error, write: error)
            finishListing(.failure(error))
        }
        guard central.state != .unknown, central.state != .resetting else { return }
        let waiters = stateWaiters.values
        stateWaiters = [:]
        for waiter in waiters {
            if central.state == .poweredOn {
                waiter.resume()
            } else {
                waiter.resume(throwing: RingError.bluetoothUnavailable(central.state))
            }
        }
    }

    public func centralManager(_ central: CBCentralManager, didDiscover peripheral: CBPeripheral,
                               advertisementData: [String: Any], rssi RSSI: NSNumber) {
        // A report queued before the scan stopped must not refill the results.
        guard scanOwner != nil else { return }
        let services = (advertisementData[CBAdvertisementDataServiceUUIDsKey] as? [CBUUID]) ?? []
        // An advertised name that is empty or only whitespace counts as missing, like a missing one.
        let advertised = RingDiscovery.usableName(advertisementData[CBAdvertisementDataLocalNameKey] as? String)
        let sighting = DiscoveredDevice(
            identifier: peripheral.identifier,
            name: advertised ?? RingDiscovery.usableName(peripheral.name),
            rssi: RSSI.intValue,
            serviceUUIDs: services.map(\.uuidString))
        // A later report of the same device may leave out what an earlier one carried.
        let device = scanResults[sighting.identifier].map { $0.merging(sighting) } ?? sighting
        scanResults[device.identifier] = device

        guard scanOwner == .connect, connectWaiter != nil, self.peripheral == nil, waitingAttach == nil,
              scanMatch?(device) == true else { return }
        central.stopScan()
        scanOwner = nil
        scanMatch = nil
        attach(peripheral)
    }

    public func centralManager(_ central: CBCentralManager, didConnect peripheral: CBPeripheral) {
        // Callbacks for a peripheral an earlier attempt dropped must not touch the current one.
        guard peripheral === self.peripheral else { return }
        didConnectCurrent = true
        peripheral.discoverServices([YCBT.service])
    }

    /// Known limit: a `didFailToConnect` for a cancelled attempt that arrives after
    /// `cancelGracePeriod`, once a new attempt has connected the same CBPeripheral object, cannot
    /// be told apart from one for the new attempt, and fails it with `connectionFailed`. It fails
    /// safe (an error, not a hang). Unverified on hardware.
    public func centralManager(_ central: CBCentralManager, didFailToConnect peripheral: CBPeripheral, error: Error?) {
        // The end of a cancelled link: it belongs to that link, not to the current attempt.
        if let cancelledLink, cancelledLink === peripheral {
            cancelledLinkEnded()
            return
        }
        guard peripheral === self.peripheral else { return }
        resetLink(cancelling: false)
        finishConnect(.failure(RingError.connectionFailed(error?.localizedDescription ?? "unknown")))
    }

    public func centralManager(_ central: CBCentralManager, didDisconnectPeripheral peripheral: CBPeripheral, error: Error?) {
        // The end of a cancelled link (Apple documents this callback for cancelling a connected
        // peripheral): it belongs to that link, not to the current attempt.
        if let cancelledLink, cancelledLink === peripheral {
            cancelledLinkEnded()
            return
        }
        guard peripheral === self.peripheral, didConnectCurrent else { return }
        resetLink(cancelling: false)
        failPending(connect: RingError.connectionFailed(error?.localizedDescription ?? "disconnected"),
                    write: RingError.notConnected)
    }
}

extension RingConnection: CBPeripheralDelegate {
    public func peripheral(_ peripheral: CBPeripheral, didDiscoverServices error: Error?) {
        guard peripheral === self.peripheral else { return }
        if let error {
            failSetup(.connectionFailed(error.localizedDescription))
            return
        }
        guard let service = peripheral.services?.first(where: { $0.uuid == YCBT.service }) else {
            failSetup(.missingService)
            return
        }
        peripheral.discoverCharacteristics([YCBT.command, YCBT.upload], for: service)
    }

    public func peripheral(_ peripheral: CBPeripheral, didDiscoverCharacteristicsFor service: CBService, error: Error?) {
        guard peripheral === self.peripheral else { return }
        if let error {
            failSetup(.connectionFailed(error.localizedDescription))
            return
        }
        let characteristics = service.characteristics ?? []
        guard let command = characteristics.first(where: { $0.uuid == YCBT.command }) else {
            failSetup(.missingService)
            return
        }
        commandCharacteristic = command
        let subscribable = characteristics.filter { $0.uuid == YCBT.command || $0.uuid == YCBT.upload }
        pendingNotifications = subscribable.count
        for characteristic in subscribable {
            peripheral.setNotifyValue(true, for: characteristic)
        }
    }

    public func peripheral(_ peripheral: CBPeripheral, didUpdateNotificationStateFor characteristic: CBCharacteristic, error: Error?) {
        guard peripheral === self.peripheral else { return }
        if let error {
            failSetup(.connectionFailed(error.localizedDescription))
            return
        }
        pendingNotifications -= 1
        if pendingNotifications == 0 { finishConnect(.success(())) }
    }

    public func peripheral(_ peripheral: CBPeripheral, didWriteValueFor characteristic: CBCharacteristic, error: Error?) {
        guard peripheral === self.peripheral else { return }
        if abandonedWrites > 0 {
            abandonedWrites -= 1
            return
        }
        guard let waiter = writeWaiter else { return }
        writeWaiter = nil
        if let error { waiter.resume(throwing: error) } else { waiter.resume() }
    }

    public func peripheral(_ peripheral: CBPeripheral, didUpdateValueFor characteristic: CBCharacteristic, error: Error?) {
        guard peripheral === self.peripheral else { return }
        guard let value = characteristic.value, !value.isEmpty else { return }
        let bytes = [UInt8](value)
        onRawBytes?(false, bytes)
        // Each characteristic is its own byte stream, so each gets its own assembler.
        for frame in assemblers[characteristic.uuid, default: FrameAssembler()].append(bytes) {
            inbox.push(frame)
        }
    }
}

/// An ordered queue of received frames with a single waiting reader.
/// `@unchecked Sendable` because all state is guarded by `lock`.
private final class FrameInbox: @unchecked Sendable {
    private let lock = NSLock()
    private var frames: [Frame] = []
    private var waiter: (id: Int, continuation: CheckedContinuation<Frame?, Never>)?
    private var lastWaiterID = 0

    func push(_ frame: Frame) {
        lock.lock()
        if let current = waiter {
            waiter = nil
            lock.unlock()
            current.continuation.resume(returning: frame)
        } else {
            frames.append(frame)
            lock.unlock()
        }
    }

    /// Drops queued frames and releases a waiting reader, which sees `nil`.
    func clear() {
        lock.lock()
        frames.removeAll()
        let current = waiter
        waiter = nil
        lock.unlock()
        current?.continuation.resume(returning: nil)
    }

    func next(timeout: TimeInterval) async -> Frame? {
        await withCheckedContinuation { continuation in
            lock.lock()
            if !frames.isEmpty {
                let frame = frames.removeFirst()
                lock.unlock()
                continuation.resume(returning: frame)
                return
            }
            // Only one reader is supported; release a displaced one instead of leaving it hanging.
            let displaced = waiter
            lastWaiterID += 1
            let id = lastWaiterID
            waiter = (id, continuation)
            lock.unlock()
            displaced?.continuation.resume(returning: nil)
            DispatchQueue.global().asyncAfter(deadline: .now() + timeout) {
                self.lock.lock()
                if let current = self.waiter, current.id == id {
                    self.waiter = nil
                    self.lock.unlock()
                    current.continuation.resume(returning: nil)
                } else {
                    self.lock.unlock()
                }
            }
        }
    }
}
