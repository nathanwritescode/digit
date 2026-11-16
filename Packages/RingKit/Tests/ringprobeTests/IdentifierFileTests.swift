import Foundation
import Testing
@testable import ringprobe

// Synthetic identifiers only. Each test works in its own temporary directory.

private let sample = "00000000-0000-0000-0000-000000000042"

private func scratch() throws -> URL {
    let url = FileManager.default.temporaryDirectory.appendingPathComponent("ringprobe-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
    return url
}

private func failure(_ result: Result<UUID, IdentifierFileError>) -> String? {
    if case .failure(let error) = result { error.message } else { nil }
}

@Test func validIdentifierWithTrailingNewlineIsRead() throws {
    let dir = try scratch(); defer { try? FileManager.default.removeItem(at: dir) }
    let file = dir.appendingPathComponent("ring-id")
    try Data((sample + "\n").utf8).write(to: file)
    #expect(try readRingIdentifier(path: file.path).get() == UUID(uuidString: sample))
    try Data(sample.utf8).write(to: file)
    #expect(try readRingIdentifier(path: file.path).get() == UUID(uuidString: sample))
}

@Test func unreadableOrUnsuitableFilesAreRefusedWithoutEchoingContents() throws {
    let dir = try scratch(); defer { try? FileManager.default.removeItem(at: dir) }
    let secret = "SECRET-CONTENT"
    let cases: [(String, Data?, String)] = [
        ("missing", nil, "cannot read"),
        ("empty", Data(), "is empty"),
        ("oversized", Data(repeating: 0x41, count: 5_000_000), "too large"),
        ("barely-oversized", Data(repeating: 0x41, count: identifierFileLimit + 1), "too large"),
        ("non-utf8", Data([0xFF, 0xFE, 0xFD, 0x80]), "does not hold a ring identifier"),
        ("malformed", Data((secret + "\n").utf8), "does not hold a ring identifier"),
        ("padded-malformed", Data((sample + "x\n").utf8), "does not hold a ring identifier"),
    ]
    for (name, data, expected) in cases {
        let file = dir.appendingPathComponent(name)
        if let data { try data.write(to: file) }
        let text = failure(readRingIdentifier(path: file.path))
        #expect(text?.contains(expected) == true, "\(name): \(text ?? "no error")")
        #expect(text?.contains(secret) == false)
        #expect(text?.contains(sample) == false)
    }
}

@Test func aDirectoryIsNotARegularFile() throws {
    let dir = try scratch(); defer { try? FileManager.default.removeItem(at: dir) }
    #expect(failure(readRingIdentifier(path: dir.path))?.contains("not a regular file") == true)
}

@Test func aFifoIsRefusedWithoutHanging() throws {
    let dir = try scratch(); defer { try? FileManager.default.removeItem(at: dir) }
    let fifo = dir.appendingPathComponent("pipe").path
    #expect(mkfifo(fifo, 0o600) == 0)
    #expect(failure(readRingIdentifier(path: fifo))?.contains("not a regular file") == true)
}

@Test func aDeviceThatNeverEndsIsRefusedWithoutReading() {
    #expect(failure(readRingIdentifier(path: "/dev/zero"))?.contains("not a regular file") == true)
}
