import Foundation
import RingTransport

// How `ringprobe scan` shows what it heard. Advertised names identify devices (the ring's
// suffix, other people's gadgets) and identifiers identify the owner's ring, so neither is
// printed without `--show`, for any row.

enum ScanMatch: Equatable {
    /// Looks like a supported ring (`DiscoveredDevice.isCandidateRing`, the rule `discoverRings` uses).
    case candidate
    /// Not a candidate but its name contains the `--name` text.
    case nameMatch
    case other
}

func scanMatch(for device: DiscoveredDevice, nameOption: String?) -> ScanMatch {
    if device.isCandidateRing(namePrefixes: YCBT.knownNamePrefixes) { return .candidate }
    if let nameOption, (device.name ?? "").localizedCaseInsensitiveContains(nameOption) { return .nameMatch }
    return .other
}

let hiddenIdentifier = "(identifier hidden; --save-id or --show)"

/// One line of `scan` output. Without `show` it holds neither the identifier nor the name, only
/// the signal strength, why the row matters, and the advertised service UUIDs.
func scanRow(_ device: DiscoveredDevice, show: Bool, match: ScanMatch) -> String {
    let services = device.serviceUUIDs.isEmpty ? "" : "  services: \(device.serviceUUIDs.joined(separator: ","))"
    let label: String
    let identifier: String
    if show {
        label = device.name ?? "(unnamed)"
        identifier = device.identifier.uuidString.padding(toLength: hiddenIdentifier.count, withPad: " ", startingAt: 0)
    } else {
        switch match {
        case .candidate: label = "(candidate ring)"
        case .nameMatch: label = "(matches --name)"
        case .other: label = "(other device)"
        }
        identifier = hiddenIdentifier
    }
    return "\(String(format: "%4d", device.rssi)) dBm  \(identifier)  \(label)\(services)"
}
