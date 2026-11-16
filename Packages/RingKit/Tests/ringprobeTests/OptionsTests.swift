import Testing
import RingProtocol
@testable import ringprobe

private func parsed(_ arguments: [String]) -> ProbeOptions? {
    if case .success(let options) = parseArguments(arguments) { options } else { nil }
}

private func message(_ arguments: [String]) -> String? {
    if case .failure(let error) = parseArguments(arguments) { error.message } else { nil }
}

@Test func commandOperandsAndFlagsParse() {
    let options = parsed(["--show", "raw", "0200:4743", "--trace", "020C", "--allow-unknown"])
    #expect(options?.command == "raw")
    #expect(options?.operands == ["0200:4743", "020C"])
    #expect(options?.show == true && options?.trace == true && options?.allowUnknown == true)
    #expect(parsed([])?.command == nil)
}

@Test func valueOptionsTakeTheirValue() {
    #expect(parsed(["scan", "--name", "X1", "5"])?.name == "X1")
    #expect(parsed(["info", "--id-file", "f"])?.idFile == "f")
    #expect(parsed(["scan", "--save-id", "f"])?.saveID == "f")
    #expect(parsed(["history", "all", "--save", "d"])?.saveDirectory == "d")
    #expect(parsed(["scan", "--name", "X1", "5"])?.operands == ["5"])
}

@Test func everyValueOptionRejectsAMissingValue() {
    for option in ["--name", "--id-file", "--save-id", "--save"] {
        // Given last.
        #expect(message(["scan", option]) == "\(option) needs a value", "\(option) last")
        // Followed by another option: it must not become the value.
        #expect(message(["scan", option, "--show"]) == "\(option) needs a value", "\(option) then --show")
        #expect(message(["scan", option, "--save-id", "file"]) == "\(option) needs a value", "\(option) then --save-id")
        // Empty.
        #expect(message(["scan", option, ""]) == "\(option) needs a value", "\(option) empty")
    }
}

@Test func optionValueThatLooksLikeAnOptionDoesNotDropTheSave() {
    // The reported case: `scan --name --save-id <file>` must fail, not save nothing and print.
    #expect(message(["scan", "--name", "--save-id", "file"]) == "--name needs a value")
}

@Test func repeatedValueOptionIsRefused() {
    #expect(message(["scan", "--name", "a", "--name", "b"]) == "--name was given more than once")
}

@Test func idIsRemovedAndPointsToIdFile() {
    let text = message(["info", "--id", "00000000-0000-0000-0000-000000000000"])
    #expect(text?.contains("unknown option --id") == true)
    #expect(text?.contains("--id-file") == true)
    // The refusal must not repeat the value.
    #expect(text?.contains("0000-0000") == false)
}

@Test func unknownOptionsAreRefused() {
    #expect(message(["info", "--bogus"]) == "unknown option --bogus")
    #expect(message(["--help"]) == "unknown option --help")
}

@Test func identifierOptionRules() {
    #expect(message(["scan", "--id-file", "f"]) == "--id-file chooses a ring to connect to; scan does not connect")
    #expect(message(["info", "--id-file", "f", "--name", "x"]) == "use only one of --id-file and --name")
    #expect(message(["info", "--save-id", "f"]) == "--save-id works only with scan")
    #expect(parsed(["scan", "--save-id", "f", "--name", "x"]) != nil)
    #expect(parsed(["info", "--name", "x"]) != nil)
}

@Test func monitorArgumentIsValidatedByTheParser() {
    // Validation happens before any connection exists; main asks for confirmation only after a
    // successful parse and after --id-file was read.
    #expect(parsed(["monitor", "10"])?.monitorInterval == .tenMinutes)
    #expect(parsed(["monitor", "off", "--id-file", "f"])?.monitorInterval == .off)
    #expect(parsed(["info"])?.monitorInterval == nil)
    for bad in [["monitor"], ["monitor", "7"], ["monitor", "10", "5"], ["monitor", "OFF"]] {
        #expect(message(bad) == "monitor needs one of: 1, 5, 10, 15, 20, 30, 60, off", "\(bad)")
    }
    // A malformed option is reported before the monitor argument is looked at.
    #expect(message(["monitor", "7", "--id"])?.contains("--id") == true)
}
