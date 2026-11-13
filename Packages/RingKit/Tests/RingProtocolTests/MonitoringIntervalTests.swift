import Testing
@testable import RingProtocol

// Synthetic: payload layouts from the issue; off and 1, 5, 10 minutes were verified on an R99.
@Test(arguments: [(1, 1), (5, 5), (10, 10), (15, 15), (20, 20), (30, 30), (60, 60)])
func monitoringIntervalPayloadIsEnabledThenMinutes(minutes: Int, byte: UInt8) throws {
    let interval = try #require(MonitoringInterval(argument: String(minutes)))
    let frame = Command.setMonitoringInterval(interval)
    #expect(frame == Frame(group: 0x01, key: 0x0C, payload: [0x01, byte]))
    #expect(frame.type == 0x010C)
    #expect(interval.minutes == minutes)
}

@Test func monitoringOffPayloadIsDisabledWithTenMinutes() {
    #expect(Command.setMonitoringInterval(.off) == Frame(group: 0x01, key: 0x0C, payload: [0x00, 0x0A]))
    #expect(MonitoringInterval.off.minutes == nil)
}

@Test func monitoringIntervalExpressesExactlyTheAllowedValues() {
    #expect(MonitoringInterval.allCases.map(\.rawValue) == [0, 1, 5, 10, 15, 20, 30, 60])
}

@Test func monitoringArgumentParsingAcceptsOnlyAllowedValues() {
    #expect(MonitoringInterval(argument: "off") == .off)
    for bad in ["0", "7", "2", "61", "-5", "+5", "05", "5 ", " 5", "OFF", "off ", "", "1.0", "255", "256", "99999999999999999999", "٥"] {
        #expect(MonitoringInterval(argument: bad) == nil, "\(bad)")
    }
}

@Test func onlyAnExplicitYesConfirms() {
    #expect(MonitoringInterval.isConfirmation("yes"))
    #expect(MonitoringInterval.isConfirmation(" YES\n"))
    for no in [nil, "", "y", "no", "yes please", "ok", "1"] as [String?] {
        #expect(!MonitoringInterval.isConfirmation(no))
    }
}

@Test func monitoringIntervalIsNotOnTheReadOnlyList() {
    for interval in MonitoringInterval.allCases {
        #expect(!Command.isKnownReadOnly(Command.setMonitoringInterval(interval)))
    }
    #expect(!Command.knownReadOnlyTypes.contains(0x010C))
}

@Test func onlyValuesSentToTheR99AreMarkedVerified() {
    let verified = MonitoringInterval.allCases.filter(\.isVerifiedOnHardware).map(\.rawValue)
    #expect(verified == [0, 1, 5, 10])
}
