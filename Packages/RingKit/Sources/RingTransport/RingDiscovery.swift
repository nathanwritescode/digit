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
    /// Both tests are needed: the R99 does not list the YCBT service in what it advertises (seen
    /// from a Mac; a 128-bit UUID takes 18 of the 31 advertising bytes), so it is matched by name,
    /// and the service test is for other rings of the family that may.
    static func isCandidate(_ device: DiscoveredDevice, service: String, namePrefixes: [String]) -> Bool {
        if device.serviceUUIDs.contains(where: { $0.caseInsensitiveCompare(service) == .orderedSame }) {
            return true
        }
        guard let name = device.name else { return false }
        return namePrefixes.contains { prefix in
            !prefix.isEmpty && name.range(of: prefix, options: [.anchored, .caseInsensitive]) != nil
        }
    }

    /// The name if it holds anything but whitespace, otherwise nil: CoreBluetooth can report an
    /// empty name, which must count as no name. The name is returned unchanged, not trimmed.
    static func usableName(_ name: String?) -> String? {
        guard let name, !name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return nil }
        return name
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

    /// What is using the scanner. CoreBluetooth has one scan per central manager, so a second
    /// user would stop or redirect the first one's scan.
    enum ScanOwner: Equatable {
        /// `scan(for:)` or `discoverRings`, collecting everything heard until its timer ends.
        case listing
        /// A connect attempt looking for its device.
        case connect
    }

    /// Why a new listing scan or connect attempt must be refused, or `nil` if it may start. The
    /// rule is the same for both: nothing starts while a connect attempt is pending (scanning or
    /// not: one that retrieved its peripheral holds no scan, but a listing scan under it would
    /// share its scan results) or while a listing scan runs. `.connect` ownership implies
    /// `connectPending`.
    static func scanConflict(owner: ScanOwner?, connectPending: Bool) -> String? {
        if connectPending { return "another connection attempt is in progress" }
        if owner == .listing { return "a scan is in progress" }
        return nil
    }

    /// How a connect attempt was looking for its device, for choosing the error when its
    /// deadline passes.
    enum ConnectSearch {
        /// Scanning for a device accepted by a caller's match.
        case matching
        /// The system knew the identifier and handed back its peripheral; no scan.
        case knownIdentifier
        /// The system did not know the identifier; scanning for a device with it.
        case unknownIdentifier
    }

    /// The error for a connect attempt whose deadline passed.
    ///
    /// - `heard`: the scan heard the device (always false for `.knownIdentifier`, which does not scan).
    /// - `connected`: `didConnect` arrived for this attempt.
    ///
    /// Once a scan has heard the device it was in range, so a missed deadline is a setup timeout
    /// (`connect(matching:)` has always reported it that way, and the identifier fallback does the
    /// same). A retrieved peripheral exists whether or not the ring is in range, so for it only a
    /// completed connection shows the ring was reached. An identifier the system does not know and
    /// that no scan heard is most likely stale (taken on another device, or before a Bluetooth
    /// reset), which the caller should treat as a reason to pair again rather than retry.
    static func connectTimeoutError(_ search: ConnectSearch, heard: Bool, connected: Bool) -> RingError {
        let reached: Bool
        switch search {
        case .matching, .unknownIdentifier: reached = heard || connected
        case .knownIdentifier: reached = connected
        }
        if reached { return .timeout("connection setup") }
        return search == .unknownIdentifier ? .unknownIdentifier : .notFound
    }
}

extension DiscoveredDevice {
    /// Combines a later sighting of the same device with this one.
    ///
    /// CoreBluetooth may report a device more than once in a scan, for example once for its
    /// advertisement and again with its scan response, and a later report need not repeat what
    /// an earlier one carried. Keeping only the latest could lose the name or the service that
    /// makes it a candidate, so the name and services accumulate and the latest measured signal
    /// strength wins. An empty or whitespace-only name counts as no name, so it never replaces a real
    /// one, and two such names merge to nil.
    func merging(_ later: DiscoveredDevice) -> DiscoveredDevice {
        var services = serviceUUIDs
        for uuid in later.serviceUUIDs where !services.contains(where: { $0.caseInsensitiveCompare(uuid) == .orderedSame }) {
            services.append(uuid)
        }
        return DiscoveredDevice(
            identifier: identifier,
            name: RingDiscovery.usableName(later.name) ?? RingDiscovery.usableName(name),
            rssi: later.rssi == RingDiscovery.unavailableRSSI ? rssi : later.rssi,
            serviceUUIDs: services)
    }
}
