import Testing
import RingProtocol
@testable import ringprobe

@Test func rawRefusesDestructiveCommandsWithAndWithoutAllowUnknown() {
    for type in [0x0540, 0x0552, 0x0568, 0x0575, 0x0578, 0x0730, 0x0731, 0x010E, 0x0151, 0x0157, 0x0316, 0x0809,
                 0x0700, 0x07FF, 0x0A00, 0x0AFF, 0x0C00, 0x0CFF, 0x0E00, 0x0EFF] as [UInt16] {
        let frame = Frame(group: UInt8(type >> 8), key: UInt8(type & 0xFF))
        for allow in [false, true] {
            let refusal = rawRefusal(frame, operand: "x", allowUnknown: allow)
            #expect(refusal?.contains("erase data or reset") == true, "\(type) \(allow)")
        }
    }
}

@Test func rawStillAllowsKnownReadCommandsAndOnlyUnknownOnesWithTheFlag() {
    #expect(rawRefusal(Command.deviceInfo, operand: "x", allowUnknown: false) == nil)
    #expect(rawRefusal(Frame(group: 0x02, key: 0x0C), operand: "x", allowUnknown: false) == nil)
    let unknown = Frame(group: 0x02, key: 0x7F)
    #expect(rawRefusal(unknown, operand: "x", allowUnknown: false) != nil)
    #expect(rawRefusal(unknown, operand: "x", allowUnknown: true) == nil)
}
