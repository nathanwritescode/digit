import Foundation

/// Pure rules for turning what a scan heard into a list of candidate rings. Kept apart from
/// CoreBluetooth so they can be tested; `RingConnection` applies them to real advertisements.
enum RingDiscovery {
    /// CoreBluetooth reports this RSSI when the signal strength was not available.
    static let unavailableRSSI = 127

    /// Whether a device looks like a supported ring: it advertises `service`, or its advertised
    /// name starts with one of `namePrefixes` (ignoring case). Empty prefixes are ignored, so they
    /// cannot accept every device.
    ///
    /// Both tests are needed because it is not yet known whether the R99 puts the YCBT service in
    /// its advertisement at all (a 128-bit UUID takes 18 of the 31 advertising bytes).
    static func isCandidate(_ device: DiscoveredDevice, service: String, namePrefixes: [String]) -> Bool {
        if device.serviceUUIDs.contains(where: { $0.caseInsensitiveCompare(service) == .orderedSame }) {
            return true
        }
        guard let name = device.name else { return false }
        return namePrefixes.contains { prefix in
            !prefix.isEmpty && name.range(of: prefix, options: [.anchored, .caseInsensitive]) != nil
        }
    }

    /// Strongest signal first. A device whose RSSI was unavailable sorts after every measured one;
    /// ties are broken by identifier so the order is stable from one call to the next.
    static func ordered(_ devices: [DiscoveredDevice]) -> [DiscoveredDevice] {
        devices.sorted { a, b in
            let aKnown = a.rssi != unavailableRSSI, bKnown = b.rssi != unavailableRSSI
            if aKnown != bKnown { return aKnown }
            if aKnown, a.rssi != b.rssi { return a.rssi > b.rssi }
            return a.identifier.uuidString < b.identifier.uuidString
        }
    }

    /// The candidates among `devices`, strongest first.
    static func candidates(in devices: [DiscoveredDevice], service: String, namePrefixes: [String]) -> [DiscoveredDevice] {
        ordered(devices.filter { isCandidate($0, service: service, namePrefixes: namePrefixes) })
    }
}

extension DiscoveredDevice {
    /// Combines a later sighting of the same device with this one.
    ///
    /// CoreBluetooth may report a device more than once in a scan, for example once for its
    /// advertisement and again with its scan response, and a later report need not repeat what
    /// an earlier one carried. Keeping only the latest could lose the name or the service that
    /// makes it a candidate, so the name and services accumulate and the latest measured signal
    /// strength wins.
    func merging(_ later: DiscoveredDevice) -> DiscoveredDevice {
        var services = serviceUUIDs
        for uuid in later.serviceUUIDs where !services.contains(where: { $0.caseInsensitiveCompare(uuid) == .orderedSame }) {
            services.append(uuid)
        }
        return DiscoveredDevice(
            identifier: identifier,
            name: later.name ?? name,
            rssi: later.rssi == RingDiscovery.unavailableRSSI ? rssi : later.rssi,
            serviceUUIDs: services)
    }
}
