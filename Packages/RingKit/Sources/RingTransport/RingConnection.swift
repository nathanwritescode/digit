@preconcurrency import CoreBluetooth
import Foundation
import RingProtocol

public enum RingError: Error, CustomStringConvertible {
    case bluetoothUnavailable(CBManagerState)
    case notFound
    case connectionFailed(String)
    case missingService
    case notConnected
    case timeout(String)
    case rejected(Frame)
    case checksumMismatch
    case refusedDestructive(Frame)

    public var description: String {
        switch self {
        case .bluetoothUnavailable(let state): "Bluetooth is unavailable (state \(state.rawValue))"
        case .notFound: "no matching ring found"
        case .connectionFailed(let why): "connection failed: \(why)"
        case .missingService: "the device does not expose the YCBT service"
        case .notConnected: "not connected"
        case .timeout(let what): "timed out waiting for \(what)"
        case .rejected(let frame): String(format: "ring rejected command %04X (code %02X)", frame.type, frame.payload.first ?? 0)
        case .checksumMismatch: "history transfer failed its checksum"
        case .refusedDestructive(let frame): String(format: "refused to send %04X: it erases history from the ring", frame.type)
        }
    }
}

public struct DiscoveredDevice: Sendable {
    public let identifier: UUID
    public let name: String?
    public let rssi: Int
    public let serviceUUIDs: [String]
}

public enum YCBT {
    public static let service = CBUUID(string: "BE940000-7333-BE46-B7AE-689E71722BD5")
    /// Commands in, replies out.
    public static let command = CBUUID(string: "BE940001-7333-BE46-B7AE-689E71722BD5")
    /// Ring-initiated uploads.
    public static let upload = CBUUID(string: "BE940003-7333-BE46-B7AE-689E71722BD5")
}

/// A Bluetooth link to one YCBT ring. Received frames queue up in order and are read with `nextFrame`.
///
/// Supports one conversation at a time; `RingSession` enforces that by queueing callers.
/// `@unchecked Sendable` because all mutable state is confined to `queue`; the compiler cannot see that.
public final class RingConnection: NSObject, @unchecked Sendable {
    /// Called on the Bluetooth queue with every raw notification and write, for capture logs.
    private let onRawBytes: (@Sendable (_ outgoing: Bool, _ bytes: [UInt8]) -> Void)?

    private let queue = DispatchQueue(label: "digit.ring.bluetooth")

    // All mutable state below is only touched on `queue`.
    private var central: CBCentralManager!
    private var peripheral: CBPeripheral?
    private var commandCharacteristic: CBCharacteristic?
    private var stateWaiters: [CheckedContinuation<Void, Error>] = []
    private var scanResults: [UUID: DiscoveredDevice] = [:]
    private var scanMatch: (@Sendable (DiscoveredDevice) -> Bool)?
    private var connectWaiter: CheckedContinuation<Void, Error>?
    private var connectAttempt = 0
    private var writeWaiter: CheckedContinuation<Void, Error>?
    private var pendingNotifications = 0
    private var assemblers: [CBUUID: FrameAssembler] = [:]
    private let inbox = FrameInbox()

    public init(onRawBytes: (@Sendable (_ outgoing: Bool, _ bytes: [UInt8]) -> Void)? = nil) {
        self.onRawBytes = onRawBytes
        super.init()
        central = CBCentralManager(delegate: self, queue: queue)
    }

    // MARK: Scanning

    /// Lists every device heard advertising during `duration`.
    public func scan(for duration: TimeInterval) async throws -> [DiscoveredDevice] {
        try await waitUntilPoweredOn()
        return await withCheckedContinuation { continuation in
            queue.async {
                self.scanResults = [:]
                self.scanMatch = nil
                self.central.scanForPeripherals(withServices: nil)
                self.queue.asyncAfter(deadline: .now() + duration) {
                    self.central.stopScan()
                    continuation.resume(returning: self.scanResults.values.sorted { $0.rssi > $1.rssi })
                }
            }
        }
    }

    // MARK: Connecting

    /// Scans for the first device accepted by `matching`, connects, and subscribes to the YCBT channels.
    public func connect(timeout: TimeInterval = 20, matching: @escaping @Sendable (DiscoveredDevice) -> Bool) async throws {
        try await waitUntilPoweredOn()
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            queue.async {
                self.connectWaiter = continuation
                self.connectAttempt += 1
                let attempt = self.connectAttempt
                self.scanMatch = matching
                self.central.scanForPeripherals(withServices: nil)
                self.queue.asyncAfter(deadline: .now() + timeout) {
                    // A timer left over from an earlier attempt must not fail a later one.
                    guard self.connectWaiter != nil, self.connectAttempt == attempt else { return }
                    self.central.stopScan()
                    let found = self.peripheral != nil
                    if let peripheral = self.peripheral { self.central.cancelPeripheralConnection(peripheral) }
                    self.finishConnect(.failure(found ? RingError.timeout("connection setup") : RingError.notFound))
                }
            }
        }
    }

    public func disconnect() {
        queue.async {
            if let peripheral = self.peripheral { self.central.cancelPeripheralConnection(peripheral) }
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
                let bytes = frame.encoded()
                self.onRawBytes?(true, bytes)
                self.writeWaiter = continuation
                peripheral.writeValue(Data(bytes), for: characteristic, type: .withResponse)
            }
        }
    }

    /// Drops frames and partial frames left over from an earlier conversation.
    public func discardPendingFrames() async {
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            queue.async {
                self.assemblers = [:]
                self.inbox.clear()
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
                case .unknown, .resetting: self.stateWaiters.append(continuation)
                default: continuation.resume(throwing: RingError.bluetoothUnavailable(self.central.state))
                }
            }
        }
    }

    private func finishConnect(_ result: Result<Void, Error>) {
        guard let waiter = connectWaiter else { return }
        connectWaiter = nil
        waiter.resume(with: result)
    }
}

extension RingConnection: CBCentralManagerDelegate {
    public func centralManagerDidUpdateState(_ central: CBCentralManager) {
        guard central.state != .unknown, central.state != .resetting else { return }
        let waiters = stateWaiters
        stateWaiters = []
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
        let services = (advertisementData[CBAdvertisementDataServiceUUIDsKey] as? [CBUUID]) ?? []
        let device = DiscoveredDevice(
            identifier: peripheral.identifier,
            name: (advertisementData[CBAdvertisementDataLocalNameKey] as? String) ?? peripheral.name,
            rssi: RSSI.intValue,
            serviceUUIDs: services.map(\.uuidString))
        scanResults[device.identifier] = device

        guard connectWaiter != nil, self.peripheral == nil, scanMatch?(device) == true else { return }
        central.stopScan()
        self.peripheral = peripheral
        peripheral.delegate = self
        central.connect(peripheral)
    }

    public func centralManager(_ central: CBCentralManager, didConnect peripheral: CBPeripheral) {
        peripheral.discoverServices([YCBT.service])
    }

    public func centralManager(_ central: CBCentralManager, didFailToConnect peripheral: CBPeripheral, error: Error?) {
        self.peripheral = nil
        finishConnect(.failure(RingError.connectionFailed(error?.localizedDescription ?? "unknown")))
    }

    public func centralManager(_ central: CBCentralManager, didDisconnectPeripheral peripheral: CBPeripheral, error: Error?) {
        self.peripheral = nil
        commandCharacteristic = nil
        assemblers = [:]
        inbox.clear()
        finishConnect(.failure(RingError.connectionFailed(error?.localizedDescription ?? "disconnected")))
        if let waiter = writeWaiter {
            writeWaiter = nil
            waiter.resume(throwing: RingError.notConnected)
        }
    }
}

extension RingConnection: CBPeripheralDelegate {
    public func peripheral(_ peripheral: CBPeripheral, didDiscoverServices error: Error?) {
        guard let service = peripheral.services?.first(where: { $0.uuid == YCBT.service }) else {
            central.cancelPeripheralConnection(peripheral)
            finishConnect(.failure(RingError.missingService))
            return
        }
        peripheral.discoverCharacteristics([YCBT.command, YCBT.upload], for: service)
    }

    public func peripheral(_ peripheral: CBPeripheral, didDiscoverCharacteristicsFor service: CBService, error: Error?) {
        let characteristics = service.characteristics ?? []
        guard let command = characteristics.first(where: { $0.uuid == YCBT.command }) else {
            central.cancelPeripheralConnection(peripheral)
            finishConnect(.failure(RingError.missingService))
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
        if let error {
            central.cancelPeripheralConnection(peripheral)
            finishConnect(.failure(RingError.connectionFailed(error.localizedDescription)))
            return
        }
        pendingNotifications -= 1
        if pendingNotifications == 0 { finishConnect(.success(())) }
    }

    public func peripheral(_ peripheral: CBPeripheral, didWriteValueFor characteristic: CBCharacteristic, error: Error?) {
        guard let waiter = writeWaiter else { return }
        writeWaiter = nil
        if let error { waiter.resume(throwing: error) } else { waiter.resume() }
    }

    public func peripheral(_ peripheral: CBPeripheral, didUpdateValueFor characteristic: CBCharacteristic, error: Error?) {
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
