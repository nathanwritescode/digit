import Foundation
import Testing
@testable import RingTransport

// Synthetic devices only: made-up identifiers and names, no real advertisements.

private let ycbt = "BE940000-7333-BE46-B7AE-689E71722BD5"

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
