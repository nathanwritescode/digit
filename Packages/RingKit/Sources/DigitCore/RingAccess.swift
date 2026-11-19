import Foundation
import RingSync

/// The ring side of the app's flows: exactly what pairing and a foreground sync need from a ring.
///
/// `BluetoothRingAccess` is the real implementation over `RingConnection`, `RingSession` and
/// `RingSync`; tests and previews use a scripted fake. Errors are `RingAccessProblem`, never a
/// transport error, so the models and their tests stay free of CoreBluetooth types.
///
/// One ring connection at a time: the models never call `connect` while a connection they made
/// is still open, and never scan while connected.
public protocol RingAccess: Sendable {
    /// Lists the rings heard for `duration`, strongest first. The first call may ask for
    /// Bluetooth permission.
    func discoverRings(for duration: Duration) async throws(RingAccessProblem) -> [RingCandidate]

    /// Connects to the ring with this CoreBluetooth identifier and starts a session on the new
    /// connection (one session per connection, never reused).
    func connect(identifier: UUID) async throws(RingAccessProblem) -> any ConnectedRing
}

/// One open connection to a ring, with its own session.
public protocol ConnectedRing: Sendable {
    /// The ring's device information (only the firmware version is used).
    func deviceDetails() async throws(RingAccessProblem) -> RingDeviceDetails

    /// The ring as `RingSyncer` drives it: the session of this connection. Its errors are the
    /// transport's own, which `RingSync` reduces to `SyncError` in its result.
    var syncableRing: any SyncableRing { get }

    /// Stops the session, then disconnects. Called exactly once on every path, including
    /// failures; the connection is not used afterwards.
    func disconnect() async
}

/// What the app keeps from the ring's device information.
public struct RingDeviceDetails: Sendable, Equatable {
    /// As the ring reports it, for example `2.32`.
    public var firmware: String

    public init(firmware: String) {
        self.firmware = firmware
    }
}

/// A ring heard during pairing.
///
/// The advertised name may be shown on screen while pairing and is never stored or logged; the
/// identifier is a per-phone device identifier. Neither appears when a candidate is printed,
/// interpolated or dumped (`description`, `debugDescription` and `customMirror` hide both), so
/// keep them out of every log and error text.
public struct RingCandidate: Sendable, Equatable, Identifiable {
    /// The CoreBluetooth identifier this phone gave the ring; what pairing saves.
    public let identifier: UUID
    /// The advertised name, for the list on screen only.
    public let name: String
    public let signal: SignalStrength

    public var id: UUID { identifier }

    public init(identifier: UUID, name: String, signal: SignalStrength) {
        self.identifier = identifier
        self.name = name
        self.signal = signal
    }
}

extension RingCandidate: CustomStringConvertible, CustomDebugStringConvertible, CustomReflectable {
    public var description: String { "RingCandidate(signal: \(signal); identifier and name hidden)" }
    public var debugDescription: String { description }
    public var customMirror: Mirror { Mirror(self, children: ["signal": signal], displayStyle: .struct) }
}

/// A coarse indication of how close a ring is, for the pairing list.
public enum SignalStrength: Sendable, Equatable, Comparable {
    case unknown, weak, fair, strong

    /// CoreBluetooth reports 127 when the strength was not measured. The thresholds are chosen,
    /// not measured on the ring.
    public init(rssi: Int) {
        if rssi == 127 {
            self = .unknown
        } else if rssi >= -65 {
            self = .strong
        } else if rssi >= -80 {
            self = .fair
        } else {
            self = .weak
        }
    }

    public var label: String {
        switch self {
        case .unknown: "Signal unknown"
        case .weak: "Weak signal"
        case .fair: "Fair signal"
        case .strong: "Strong signal"
        }
    }
}

/// Why the ring could not be reached or read, in terms the owner can act on.
public enum RingAccessProblem: Error, Sendable, Equatable {
    case bluetoothOff
    /// Digit is not allowed to use Bluetooth (denied or restricted): the fix is in Settings.
    case bluetoothNotPermitted
    /// This device has no usable Bluetooth (the iOS simulator, for one).
    case bluetoothUnsupported
    /// Bluetooth is resetting or did not become ready in time.
    case bluetoothNotReady
    /// The ring was not heard or never connected.
    case ringNotFound
    /// The saved identifier is unknown to this phone and the ring was not heard: pair again.
    case unknownIdentifier
    /// The ring refused or dropped the connection, as it does while another phone or app holds it.
    case ringBusy
    case timedOut
    /// The device does not offer the ring's service.
    case notARing
    case connectionLost
    /// The ring's reply could not be read.
    case unreadableReply
    case other

    /// What to tell the owner. Holds no name, identifier or health value.
    public var message: String {
        switch self {
        case .bluetoothOff:
            "Bluetooth is off. Turn it on in Control Center or Settings, then try again."
        case .bluetoothNotPermitted:
            "Digit isn't allowed to use Bluetooth. Allow it in Settings, under Privacy & Security, Bluetooth, then try again."
        case .bluetoothUnsupported:
            "Bluetooth isn't available on this device."
        case .bluetoothNotReady:
            "Bluetooth isn't ready yet. Try again in a moment."
        case .ringNotFound:
            "Digit couldn't find the ring. Keep it close to this iPhone, make sure it's charged and not connected to another phone or app, then try again."
        case .unknownIdentifier:
            "Digit can no longer find this ring by its saved identity; pair it again."
        case .ringBusy:
            "The ring didn't accept the connection. If it's connected to another phone or app, disconnect it there, then try again."
        case .timedOut:
            "The ring stopped responding. Keep it close to this iPhone and try again."
        case .notARing:
            "That device doesn't look like a supported ring."
        case .connectionLost:
            "The connection to the ring was lost. Try again."
        case .unreadableReply:
            "The ring's reply couldn't be read. Try again."
        case .other:
            "Something went wrong while talking to the ring. Try again."
        }
    }

    /// Whether the fix is in the Settings app, so the screen can offer a way there.
    public var needsSettings: Bool { self == .bluetoothNotPermitted }
}
