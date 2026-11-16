import Testing
import RingProtocol
@testable import ringprobe

@Test func argumentParsingAcceptsOnlyAllowedValues() {
    #expect(MonitoringInterval(argument: "off") == .off)
    for good in [1, 5, 10, 15, 20, 30, 60] {
        #expect(MonitoringInterval(argument: String(good))?.rawValue == good)
    }
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

@Test func onlyValuesSentToTheR99AreMarkedVerified() {
    #expect(MonitoringInterval.allCases.filter(\.isVerifiedOnHardware).map(\.rawValue) == [0, 1, 5, 10])
}

@Test func summaryNamesTheInterval() {
    #expect(MonitoringInterval.oneMinute.summary.contains("every 1 minute "))
    #expect(MonitoringInterval.thirtyMinutes.summary.contains("every 30 minutes"))
    #expect(MonitoringInterval.off.summary.contains("off"))
}
