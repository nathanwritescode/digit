import Foundation
import Testing
import RingTransport
@testable import ringprobe

// Synthetic devices only.
private let ringID = UUID(uuidString: "00000000-0000-0000-0000-000000000001")!
private let otherID = UUID(uuidString: "00000000-0000-0000-0000-000000000002")!
private let ring = DiscoveredDevice(identifier: ringID, name: "R99 TESTNAME", rssi: -50, serviceUUIDs: [])
private let gadget = DiscoveredDevice(identifier: otherID, name: "Gadget TESTNAME", rssi: -70, serviceUUIDs: ["180D"])

@Test func noRowShowsAnIdentifierOrNameWithoutShow() {
    for device in [ring, gadget] {
        for match in [ScanMatch.candidate, .nameMatch, .other] {
            let line = scanRow(device, show: false, match: match)
            #expect(!line.contains(device.identifier.uuidString), "\(match)")
            #expect(!line.localizedCaseInsensitiveContains("TESTNAME"), "\(match)")
            #expect(line.contains(hiddenIdentifier))
        }
    }
}

@Test func labelsSayWhyARowMattersWithoutNamingIt() {
    #expect(scanRow(ring, show: false, match: .candidate).contains("(candidate ring)"))
    #expect(scanRow(gadget, show: false, match: .nameMatch).contains("(matches --name)"))
    #expect(scanRow(gadget, show: false, match: .other).contains("(other device)"))
    #expect(scanRow(gadget, show: false, match: .other).contains("services: 180D"))
}

@Test func showPrintsIdentifiersAndNamesForEveryRow() {
    for (device, match) in [(ring, ScanMatch.candidate), (gadget, .other)] {
        let line = scanRow(device, show: true, match: match)
        #expect(line.contains(device.identifier.uuidString))
        #expect(line.contains(device.name!))
        #expect(!line.contains(hiddenIdentifier))
    }
    let unnamed = DiscoveredDevice(identifier: otherID, name: nil, rssi: -80, serviceUUIDs: [])
    #expect(scanRow(unnamed, show: true, match: .other).contains("(unnamed)"))
}

@Test func matchClassification() {
    #expect(scanMatch(for: ring, nameOption: nil) == .candidate)
    #expect(scanMatch(for: ring, nameOption: "zzz") == .candidate)
    #expect(scanMatch(for: gadget, nameOption: nil) == .other)
    #expect(scanMatch(for: gadget, nameOption: "gadget") == .nameMatch)
    #expect(scanMatch(for: gadget, nameOption: "nothing") == .other)
}
