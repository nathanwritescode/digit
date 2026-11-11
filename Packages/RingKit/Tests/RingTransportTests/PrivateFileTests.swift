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
