import Foundation
import Testing
@testable import RingTransport

// Synthetic devices only: made-up identifiers and names, no real advertisements.

private let ycbt = YCBT.service.uuidString

private func device(_ n: Int, name: String? = nil, rssi: Int = -60, services: [String] = []) -> DiscoveredDevice {
    DiscoveredDevice(
        identifier: UUID(uuidString: String(format: "00000000-0000-0000-0000-%012d", n))!,
        name: name, rssi: rssi, serviceUUIDs: services)
}

@Test func candidateByServiceWithoutName() {
    #expect(RingDiscovery.isCandidate(device(1, services: [ycbt]), service: ycbt, namePrefixes: ["R99"]))
}

@Test func candidateServiceMatchIgnoresCase() {
    #expect(RingDiscovery.isCandidate(device(1, services: [ycbt.lowercased()]), service: ycbt, namePrefixes: []))
}

@Test func candidateByNamePrefix() {
    #expect(RingDiscovery.isCandidate(device(1, name: "R99 0000"), service: ycbt, namePrefixes: ["R99"]))
    #expect(RingDiscovery.isCandidate(device(1, name: "r99 0000"), service: ycbt, namePrefixes: ["R99"]))
    #expect(RingDiscovery.isCandidate(device(1, name: "R99 0000"), service: ycbt, namePrefixes: ["X1", "R99"]))
}

@Test func nameMustStartWithPrefix() {
    #expect(!RingDiscovery.isCandidate(device(1, name: "Not R99"), service: ycbt, namePrefixes: ["R99"]))
    #expect(!RingDiscovery.isCandidate(device(1, name: "R9"), service: ycbt, namePrefixes: ["R99"]))
}

@Test func otherDevicesAreNotCandidates() {
    #expect(!RingDiscovery.isCandidate(device(1), service: ycbt, namePrefixes: ["R99"]))
    #expect(!RingDiscovery.isCandidate(device(1, name: "Headphones", services: ["180D"]), service: ycbt, namePrefixes: ["R99"]))
}

@Test func emptyPrefixAcceptsNothing() {
    #expect(!RingDiscovery.isCandidate(device(1, name: "Headphones"), service: ycbt, namePrefixes: [""]))
    #expect(!RingDiscovery.isCandidate(device(1, name: ""), service: ycbt, namePrefixes: [""]))
}

@Test func orderedStrongestFirst() {
    let ordered = RingDiscovery.ordered([device(1, rssi: -80), device(2, rssi: -40), device(3, rssi: -60)])
    #expect(ordered.map(\.rssi) == [-40, -60, -80])
}

@Test func unavailableSignalSortsLast() {
    // 127 is CoreBluetooth's "RSSI not available"; read naively it would be the strongest.
    let ordered = RingDiscovery.ordered([device(1, rssi: 127), device(2, rssi: -90), device(3, rssi: -50)])
    #expect(ordered.map(\.rssi) == [-50, -90, 127])
}

@Test func equalSignalOrderIsStable() {
    let a = device(1, rssi: -60), b = device(2, rssi: -60), c = device(3, rssi: 127), d = device(4, rssi: 127)
    let expected = [a, b, c, d].map(\.identifier)
    #expect(RingDiscovery.ordered([d, b, c, a]).map(\.identifier) == expected)
    #expect(RingDiscovery.ordered([c, a, d, b]).map(\.identifier) == expected)
}

@Test func candidatesFiltersAndOrders() {
    let devices = [
        device(1, name: "Headphones", rssi: -30),
        device(2, name: "R99 0000", rssi: -70),
        device(3, rssi: -50, services: [ycbt]),
        device(4, rssi: -40),
    ]
    let found = RingDiscovery.candidates(in: devices, service: ycbt, namePrefixes: ["R99"])
    #expect(found.map(\.identifier) == [devices[2].identifier, devices[1].identifier])
}

@Test func mergingKeepsNameAndServicesFromEarlierSightings() {
    let first = device(1, name: "R99 0000", rssi: -70, services: [ycbt])
    let later = device(1, rssi: -55, services: ["180D"])
    let merged = first.merging(later)
    #expect(merged.identifier == first.identifier)
    #expect(merged.name == "R99 0000")
    #expect(merged.rssi == -55)
    #expect(merged.serviceUUIDs == [ycbt, "180D"])
}

@Test func mergingPrefersLaterNameAndIgnoresUnavailableSignal() {
    let merged = device(1, name: "Old", rssi: -70).merging(device(1, name: "New", rssi: 127))
    #expect(merged.name == "New")
    #expect(merged.rssi == -70)
}

@Test func mergingDoesNotDuplicateServices() {
    let merged = device(1, services: [ycbt]).merging(device(1, services: [ycbt.lowercased()]))
    #expect(merged.serviceUUIDs == [ycbt])
}

@Test func mergedNameTable() {
    // Every pairing of nil, empty, whitespace-only and real names, earlier then later: the later
    // real name wins, else the earlier real name, else nil. Whitespace never counts as a name.
    let absent: [String?] = [nil, "", " ", "\t\n"]
    for earlier in absent + ["Old"] {
        for later in absent + ["New"] {
            let expected: String? = later == "New" ? "New" : earlier == "Old" ? "Old" : nil
            let merged = device(1, name: earlier).merging(device(1, name: later)).name
            #expect(merged == expected, "earlier \(String(describing: earlier)), later \(String(describing: later))")
        }
    }
}

@Test func usableNameTreatsWhitespaceAsMissing() {
    #expect(RingDiscovery.usableName(nil) == nil)
    #expect(RingDiscovery.usableName("") == nil)
    #expect(RingDiscovery.usableName("  \n") == nil)
    #expect(RingDiscovery.usableName(" R99 0000 ") == " R99 0000 ")
}

@Test func candidateRuleHasExplicitExpectations() {
    // Listed by hand so the test does not lean on the function under test: (device, is a candidate).
    let cases: [(DiscoveredDevice, Bool)] = [
        (device(1, name: "R99 0000"), true),
        (device(2, name: "r99 0000"), true),
        (device(3, services: [ycbt]), true),               // the YCBT service and no name
        (device(4, name: "Headphones", services: [ycbt]), true),
        (device(5, name: "Headphones", services: ["180D"]), false), // only an unrelated service
        (device(6, services: ["180D"]), false),
        (device(7, name: "Not R99"), false),
        (device(8, name: " "), false),
        (device(9), false),
    ]
    for (candidate, expected) in cases {
        #expect(candidate.isCandidateRing(namePrefixes: YCBT.knownNamePrefixes) == expected, "device \(candidate.identifier)")
    }
    let found = RingDiscovery.candidates(in: cases.map(\.0), service: ycbt, namePrefixes: YCBT.knownNamePrefixes)
    #expect(Set(found.map(\.identifier)) == Set(cases.filter(\.1).map(\.0.identifier)))
}

@Test func knownNamePrefixesAreExactlyTheR99() {
    #expect(YCBT.knownNamePrefixes == ["R99"])
}

// Scan ownership: one CoreBluetooth scan, so nothing starts while a connect is pending or a
// listing scan runs.

@Test func scanMayStartWhenIdle() {
    #expect(RingDiscovery.scanConflict(owner: nil, connectPending: false) == nil)
}

@Test func nothingStartsWhileListing() {
    #expect(RingDiscovery.scanConflict(owner: .listing, connectPending: false) != nil)
}

@Test func nothingStartsWhileConnectPending() {
    // Scanning, or connecting to a retrieved peripheral without a scan.
    #expect(RingDiscovery.scanConflict(owner: .connect, connectPending: true) != nil)
    #expect(RingDiscovery.scanConflict(owner: nil, connectPending: true) != nil)
}

// Which error a connect reports when its deadline passes.

private func isTimeout(_ error: RingError) -> Bool { if case .timeout = error { true } else { false } }
private func isNotFound(_ error: RingError) -> Bool { if case .notFound = error { true } else { false } }
private func isUnknownIdentifier(_ error: RingError) -> Bool { if case .unknownIdentifier = error { true } else { false } }

@Test func matchingTimeoutDependsOnWhetherTheDeviceWasHeard() {
    #expect(isNotFound(RingDiscovery.connectTimeoutError(.matching, heard: false, connected: false)))
    #expect(isTimeout(RingDiscovery.connectTimeoutError(.matching, heard: true, connected: false)))
    #expect(isTimeout(RingDiscovery.connectTimeoutError(.matching, heard: true, connected: true)))
}

@Test func knownIdentifierIsNotFoundUnlessItConnected() {
    // A retrieved peripheral exists whether or not the ring is in range, so `heard` says nothing.
    #expect(isNotFound(RingDiscovery.connectTimeoutError(.knownIdentifier, heard: true, connected: false)))
    #expect(isNotFound(RingDiscovery.connectTimeoutError(.knownIdentifier, heard: false, connected: false)))
    #expect(isTimeout(RingDiscovery.connectTimeoutError(.knownIdentifier, heard: true, connected: true)))
}

@Test func unknownIdentifierNeverHeardIsDistinct() {
    #expect(isUnknownIdentifier(RingDiscovery.connectTimeoutError(.unknownIdentifier, heard: false, connected: false)))
    // Heard by the fallback scan: in range, so a setup timeout, as for a name match.
    #expect(isTimeout(RingDiscovery.connectTimeoutError(.unknownIdentifier, heard: true, connected: false)))
}

@Test func errorDescriptionsCarryNoIdentifier() {
    // Synthetic identifier; the descriptions must not interpolate any identifier at all.
    for error in [RingError.notFound, .unknownIdentifier] {
        #expect(!error.description.contains("-"))
        #expect(!error.description.contains(device(1).identifier.uuidString))
    }
}

@Test func printingOrDumpingADeviceHidesItsIdentifierAndName() {
    let secret = device(7, name: "SECRETNAME 1234", rssi: -42, services: [ycbt])
    var dumped = ""
    dump(secret, to: &dumped)
    for text in ["\(secret)", String(reflecting: secret), String(describing: secret), dumped, "\([secret])"] {
        #expect(!text.contains(secret.identifier.uuidString))
        #expect(!text.contains("SECRETNAME"))
        #expect(!text.contains("000000000007"))
    }
    #expect("\(secret)".contains("-42"))
}
