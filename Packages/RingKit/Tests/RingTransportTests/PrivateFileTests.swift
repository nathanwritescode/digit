import Foundation
import Testing
@testable import RingTransport

// Synthetic bytes only.
@Test func privateFileIsCreatedOwnerOnlyAndNeverOverwritten() throws {
    let base = FileManager.default.temporaryDirectory.appendingPathComponent("privatefile-\(UUID().uuidString)")
    defer { try? FileManager.default.removeItem(at: base) }
    let directory = base.appendingPathComponent("nested").path
    try PrivateFile.createDirectory(directory)
    let dirMode = try FileManager.default.attributesOfItem(atPath: directory)[.posixPermissions] as? Int
    #expect(dirMode == 0o700)

    let path = directory + "/capture.json"
    let content = Data((0..<5000).map { UInt8($0 % 251) })
    try PrivateFile.write(content, to: path)
    let mode = try FileManager.default.attributesOfItem(atPath: path)[.posixPermissions] as? Int
    #expect(mode == 0o600)
    #expect(try Data(contentsOf: URL(fileURLWithPath: path)) == content)

    do {
        try PrivateFile.write(Data([1]), to: path)
        Issue.record("expected the second write to fail")
    } catch let failure as PrivateFile.Failure {
        #expect(failure.description.contains(path))
        #expect(failure.description.contains("exists"))
    }
    #expect(try Data(contentsOf: URL(fileURLWithPath: path)) == content)

    // An existing directory keeps its mode.
    try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: directory)
    try PrivateFile.createDirectory(directory)
    #expect(try FileManager.default.attributesOfItem(atPath: directory)[.posixPermissions] as? Int == 0o755)
}

@Test func failedWriteLeavesNoFileBehind() throws {
    // Writing into a missing directory fails at creation; nothing may be left at the path.
    let missing = NSTemporaryDirectory() + "ringkit-missing-\(UUID().uuidString)/capture.json"
    #expect(throws: PrivateFile.Failure.self) { try PrivateFile.write(Data([1, 2, 3]), to: missing) }
    #expect(!FileManager.default.fileExists(atPath: missing))
}

@Test func replaceOverwritesAndKeepsTheFileOwnerOnly() throws {
    let base = FileManager.default.temporaryDirectory.appendingPathComponent("privatefile-\(UUID().uuidString)")
    defer { try? FileManager.default.removeItem(at: base) }
    try PrivateFile.createDirectory(base.path)
    let path = base.appendingPathComponent("ring-id").path

    try PrivateFile.replace(Data("first\n".utf8), at: path)
    #expect(try Data(contentsOf: URL(fileURLWithPath: path)) == Data("first\n".utf8))

    // An existing file readable by others is replaced by an owner-only one.
    try FileManager.default.setAttributes([.posixPermissions: 0o644], ofItemAtPath: path)
    try PrivateFile.replace(Data("second\n".utf8), at: path)
    #expect(try Data(contentsOf: URL(fileURLWithPath: path)) == Data("second\n".utf8))
    #expect(try FileManager.default.attributesOfItem(atPath: path)[.posixPermissions] as? Int == 0o600)

    // No temporary file is left beside it.
    #expect(try FileManager.default.contentsOfDirectory(atPath: base.path) == ["ring-id"])
}

@Test func failedRenameLeavesNoTemporaryFileAndTheOldTargetIntact() throws {
    let base = FileManager.default.temporaryDirectory.appendingPathComponent("privatefile-\(UUID().uuidString)")
    defer { try? FileManager.default.removeItem(at: base) }
    try PrivateFile.createDirectory(base.path)
    // A non-empty directory cannot be replaced by a file, so the rename fails after the
    // temporary file was written.
    let target = base.appendingPathComponent("ring-id")
    try FileManager.default.createDirectory(at: target, withIntermediateDirectories: false)
    try Data("keep".utf8).write(to: target.appendingPathComponent("child"))

    #expect(throws: PrivateFile.Failure.self) { try PrivateFile.replace(Data("new".utf8), at: target.path) }

    #expect(try FileManager.default.contentsOfDirectory(atPath: base.path) == ["ring-id"])
    #expect(try Data(contentsOf: target.appendingPathComponent("child")) == Data("keep".utf8))
}

@Test func temporaryNameIsNotPredictable() {
    let path = "/tmp/dir/ring-id"
    let names = (0..<20).map { _ in PrivateFile.temporaryPath(for: path) }
    #expect(Set(names).count == names.count)
    #expect(!names.contains(path + ".tmp"))
    for name in names {
        #expect(name.hasPrefix(path + "."))
        #expect(name.hasSuffix(".tmp"))
        // A dot, a 36-character UUID and ".tmp".
        #expect(name.count == path.count + 41)
    }
}

@Test func targetIsNeverMissingOrPartialWhileItIsReplaced() throws {
    let base = FileManager.default.temporaryDirectory.appendingPathComponent("privatefile-\(UUID().uuidString)")
    defer { try? FileManager.default.removeItem(at: base) }
    try PrivateFile.createDirectory(base.path)
    let path = base.appendingPathComponent("ring-id").path
    let old = Data(repeating: 0x41, count: 4096), new = Data(repeating: 0x42, count: 4096)
    try PrivateFile.replace(old, at: path)

    final class Verdict: @unchecked Sendable {
        private let lock = NSLock()
        private var stop = false, bad = 0, reads = 0
        var stopped: Bool { lock.withLock { stop } }
        func finish() { lock.withLock { stop = true } }
        func record(good: Bool) { lock.withLock { reads += 1; if !good { bad += 1 } } }
        var counts: (reads: Int, bad: Int) { lock.withLock { (reads, bad) } }
    }
    let verdict = Verdict()
    let reader = Thread {
        while !verdict.stopped {
            let data = try? Data(contentsOf: URL(fileURLWithPath: path))
            verdict.record(good: data == old || data == new)
        }
    }
    reader.start()
    for i in 0..<300 { try PrivateFile.replace(i % 2 == 0 ? new : old, at: path) }
    verdict.finish()
    #expect(verdict.counts.reads > 0)
    #expect(verdict.counts.bad == 0)
}

@Test func tooLongFileNameFailsWithoutReplacingAnything() throws {
    let base = FileManager.default.temporaryDirectory.appendingPathComponent("privatefile-\(UUID().uuidString)")
    defer { try? FileManager.default.removeItem(at: base) }
    try PrivateFile.createDirectory(base.path)
    // 220 bytes: the target name itself is legal, but the temporary name beside it (41 bytes
    // longer) exceeds the 255-byte limit.
    let path = base.appendingPathComponent(String(repeating: "a", count: 220)).path
    #expect(throws: PrivateFile.Failure.self) { try PrivateFile.replace(Data([1]), at: path) }
    #expect(try FileManager.default.contentsOfDirectory(atPath: base.path).isEmpty)
}
