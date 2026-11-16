import Testing
import RingProtocol

// Synthetic: payload layouts from the issue; off and 1, 5, 10 minutes were verified on an R99.
@Test(arguments: [(1, 0x01), (5, 0x05), (10, 0x0A), (15, 0x0F), (20, 0x14), (30, 0x1E), (60, 0x3C)] as [(Int, UInt8)])
func monitoringIntervalPayloadIsEnabledThenMinutes(minutes: Int, byte: UInt8) throws {
    let interval = try #require(MonitoringInterval(minutes: minutes))
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

@Test func monitoringMinutesInitialiserRejectsEverythingElse() {
    for bad in [-1, 0, 2, 7, 11, 59, 61, 255, 256, Int.max, Int.min] {
        #expect(MonitoringInterval(minutes: bad) == nil, "\(bad)")
    }
    for good in [1, 5, 10, 15, 20, 30, 60] {
        #expect(MonitoringInterval(minutes: good)?.rawValue == good)
    }
}

@Test func monitoringIntervalIsNotOnTheReadOnlyList() {
    for interval in MonitoringInterval.allCases {
        #expect(!Command.isKnownReadOnly(Command.setMonitoringInterval(interval)))
    }
    #expect(!Command.knownReadOnlyTypes.contains(0x010C))
}

@Test func monitoringSettingFrameIsRecognisedWhateverItsPayload() {
    for interval in MonitoringInterval.allCases {
        #expect(Command.setMonitoringInterval(interval).isMonitoringSetting)
    }
    #expect(Frame(group: 0x01, key: 0x0C, payload: [0x01, 0x07]).isMonitoringSetting)
    #expect(Frame(group: 0x01, key: 0x0C).isMonitoringSetting)
    #expect(!Frame(group: 0x01, key: 0x00).isMonitoringSetting)
    #expect(!Frame(group: 0x02, key: 0x0C).isMonitoringSetting)
    #expect(!Frame(group: 0x01, key: 0x26).isMonitoringSetting)
}
