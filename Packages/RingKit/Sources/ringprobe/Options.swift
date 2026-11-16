import Foundation
import RingProtocol

// Option parsing for `ringprobe`, kept apart from `main.swift` so it is pure (no I/O, no
// Bluetooth) and unit tested through `@testable import ringprobe`.

struct OptionError: Error, Equatable {
    let message: String
}

struct ProbeOptions: Equatable {
    var trace = false
    var show = false
    var allowUnknown = false
    /// `--name`: match rings by advertised name instead of the default.
    var name: String?
    /// `--id-file`: a file holding the ring's identifier, written by `scan --save-id`.
    var idFile: String?
    /// `--save-id`: where `scan` saves the identifier of the one candidate ring.
    var saveID: String?
    /// `--save`: the directory `history` writes captures to.
    var saveDirectory: String?
    var command: String?
    var operands: [String] = []
    /// The validated argument of `monitor`; nil for every other command.
    var monitorInterval: MonitoringInterval?
}

/// Options that take a value, in usage order.
private let valueOptions = ["--name", "--id-file", "--save-id", "--save"]

/// Parses the command line (without the program name). Options may appear anywhere; everything
/// else is the command and its operands. An option's value may not start with `--` (so a missing
/// value cannot silently swallow the next option) and may not be empty. Never reads files.
func parseArguments(_ arguments: [String]) -> Result<ProbeOptions, OptionError> {
    var options = ProbeOptions()
    var positional: [String] = []
    var index = 0
    while index < arguments.count {
        let argument = arguments[index]
        index += 1
        // For messages only: the part before any `=`, so a value written as `--opt=value` is never
        // echoed (it could be an identifier).
        let key = String(argument.prefix { $0 != "=" })
        switch argument {
        case "--trace": options.trace = true
        case "--show": options.show = true
        case "--allow-unknown": options.allowUnknown = true
        case _ where key == "--id":
            // Removed: an identifier on the command line lands in shell history and transcripts.
            return .failure(OptionError(message: "unknown option --id: connect with --id-file <file>, which scan --save-id writes"))
        case _ where valueOptions.contains(argument):
            guard index < arguments.count, !arguments[index].isEmpty, !arguments[index].hasPrefix("--") else {
                return .failure(OptionError(message: "\(argument) needs a value"))
            }
            let value = arguments[index]
            index += 1
            let alreadyGiven: Bool
            switch argument {
            case "--name": alreadyGiven = options.name != nil; options.name = value
            case "--id-file": alreadyGiven = options.idFile != nil; options.idFile = value
            case "--save-id": alreadyGiven = options.saveID != nil; options.saveID = value
            default: alreadyGiven = options.saveDirectory != nil; options.saveDirectory = value
            }
            if alreadyGiven { return .failure(OptionError(message: "\(argument) was given more than once")) }
        case _ where argument.hasPrefix("--"):
            return .failure(OptionError(message: "unknown option \(key)"))
        default:
            positional.append(argument)
        }
    }
    options.command = positional.first
    options.operands = Array(positional.dropFirst())

    if options.command == "scan", options.idFile != nil {
        return .failure(OptionError(message: "--id-file chooses a ring to connect to; scan does not connect"))
    }
    if options.idFile != nil, options.name != nil {
        return .failure(OptionError(message: "use only one of --id-file and --name"))
    }
    if options.command != "scan", options.saveID != nil {
        return .failure(OptionError(message: "--save-id works only with scan"))
    }
    if options.command == "monitor" {
        guard options.operands.count == 1, let interval = MonitoringInterval(argument: options.operands[0]) else {
            return .failure(OptionError(message: "monitor needs one of: 1, 5, 10, 15, 20, 30, 60, off"))
        }
        options.monitorInterval = interval
    }
    return .success(options)
}
