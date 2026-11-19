import CoreBluetooth
import Foundation
import RingProtocol
import RingSync
import RingTransport

/// The real ring side: `RingConnection` for discovery and connecting, a new `RingSession` for
/// every connection, and the session as `RingSync`'s ring.
///
/// The `RingConnection` (and with it CoreBluetooth's central manager) is created on first use, so
/// the Bluetooth permission prompt appears with the first scan rather than at launch. One
/// connection object serves the whole process; it carries one link at a time.
///
/// Foreground only: connecting by identifier falls back to an unfiltered scan, which finds nothing
/// in the background on iOS (see "Known gaps in RingTransport" in AGENTS.md). Never run against a
/// ring by its tests; covered by the hardware check on #53.
///
/// `@unchecked Sendable`: the lazily created connection is guarded by `lock`.
public final class BluetoothRingAccess: RingAccess, @unchecked Sendable {
    /// How long a connect may take, scan through subscribe (the transport's default).
    public static let connectTimeout: TimeInterval = 20

    private let lock = NSLock()
    private var connection: RingConnection?

    public init() {}

    private func sharedConnection() -> RingConnection {
        lock.withLock {
            if let connection { return connection }
            let created = RingConnection()
            connection = created
            return created
        }
    }

    public func discoverRings(for duration: Duration) async throws(RingAccessProblem) -> [RingCandidate] {
        let seconds = Double(duration.components.seconds) + Double(duration.components.attoseconds) / 1e18
        let devices: [DiscoveredDevice]
        do {
            devices = try await sharedConnection().discoverRings(for: seconds, namePrefixes: YCBT.knownNamePrefixes)
        } catch {
            throw Self.problem(error)
        }
        // A candidate matched by service alone may have no name; it is listed under a placeholder.
        return devices.map { device in
            RingCandidate(identifier: device.identifier, name: device.name ?? "Unnamed ring", signal: SignalStrength(rssi: device.rssi))
        }
    }

    public func connect(identifier: UUID) async throws(RingAccessProblem) -> any ConnectedRing {
        let connection = sharedConnection()
        do {
            try await connection.connect(identifier: identifier, timeout: Self.connectTimeout)
        } catch {
            // A failed attempt may leave nothing to cancel; disconnecting is harmless either way.
            connection.disconnect()
            throw Self.problem(error)
        }
        let session = RingSession(link: connection)
        await session.start()
        return BluetoothConnectedRing(connection: connection, session: session)
    }

    /// Reduces a transport error to what the owner can act on. The only place CoreBluetooth's
    /// manager state is read. Never keeps the error itself, which can hold ring frames.
    static func problem(_ error: any Error) -> RingAccessProblem {
        guard let error = error as? RingError else { return .other }
        switch error {
        case .bluetoothUnavailable(let state):
            switch state {
            case .poweredOff: return .bluetoothOff
            case .unauthorized: return .bluetoothNotPermitted
            case .unsupported: return .bluetoothUnsupported
            case .unknown, .resetting, .poweredOn: return .bluetoothNotReady
            @unknown default: return .bluetoothNotReady
            }
        case .notFound: return .ringNotFound
        case .unknownIdentifier: return .unknownIdentifier
        case .connectionFailed, .busy: return .ringBusy
        case .missingService: return .notARing
        case .notConnected, .stopped: return .connectionLost
        case .timeout: return .timedOut
        case .rejected, .checksumMismatch, .tooManyFrames: return .unreadableReply
        case .refusedDestructive, .refusedUploadGroup: return .other
        }
    }
}

/// One connection and its session.
final class BluetoothConnectedRing: ConnectedRing {
    private let connection: RingConnection
    private let session: RingSession

    init(connection: RingConnection, session: RingSession) {
        self.connection = connection
        self.session = session
    }

    var syncableRing: any SyncableRing { session }

    func deviceDetails() async throws(RingAccessProblem) -> RingDeviceDetails {
        let info: DeviceInfo?
        do {
            info = try await session.deviceInfo()
        } catch {
            throw BluetoothRingAccess.problem(error)
        }
        guard let info else { throw .unreadableReply }
        return RingDeviceDetails(firmware: info.firmwareVersion)
    }

    func disconnect() async {
        // The session first: its reader would otherwise keep polling a dead link.
        await session.stop()
        connection.disconnect()
    }
}
