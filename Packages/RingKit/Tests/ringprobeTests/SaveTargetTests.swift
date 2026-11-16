import Foundation
import Testing
@testable import ringprobe

// Each test makes its own temporary directories and repositories; none touches this repository.

private func scratch() throws -> URL {
    let url = FileManager.default.temporaryDirectory.appendingPathComponent("ringprobe-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
    return url
}

private func initRepository(at url: URL) throws {
    let result = runGit(directory: url.path, arguments: ["init", "-q"])
    try #require(result?.status == 0, "git init failed; these tests need git")
}

@Test func aTargetOutsideAnyWorkTreeIsAllowed() throws {
    let dir = try scratch(); defer { try? FileManager.default.removeItem(at: dir) }
    #expect(saveIDRefusal(path: dir.appendingPathComponent("ring-id").path) == nil)
    // Directories that do not exist yet do not matter.
    #expect(saveIDRefusal(path: dir.appendingPathComponent("new/deeper/ring-id").path) == nil)
}

@Test func aTargetInAnUnignoredWorkTreeIsRefused() throws {
    let repo = try scratch(); defer { try? FileManager.default.removeItem(at: repo) }
    try initRepository(at: repo)
    let refusal = saveIDRefusal(path: repo.appendingPathComponent("ring-id").path)
    #expect(refusal?.contains("could be committed") == true)
    #expect(refusal?.contains("captures/ring-id") == true)
    // A directory that does not exist yet inside the work tree is judged the same way.
    #expect(saveIDRefusal(path: repo.appendingPathComponent("sub/ring-id").path) != nil)
}

@Test func aTargetThatGitIgnoresIsAllowed() throws {
    let repo = try scratch(); defer { try? FileManager.default.removeItem(at: repo) }
    try initRepository(at: repo)
    try Data("captures/\n".utf8).write(to: repo.appendingPathComponent(".gitignore"))
    // The ignored directory exists, or not yet.
    #expect(saveIDRefusal(path: repo.appendingPathComponent("captures/ring-id").path) == nil)
    try FileManager.default.createDirectory(at: repo.appendingPathComponent("captures"), withIntermediateDirectories: false)
    #expect(saveIDRefusal(path: repo.appendingPathComponent("captures/ring-id").path) == nil)
    // A sibling that is not ignored is still refused.
    #expect(saveIDRefusal(path: repo.appendingPathComponent("ring-id").path) != nil)
}

@Test func relativePathsAreJudgedFromTheWorkingDirectory() throws {
    let repo = try scratch(); defer { try? FileManager.default.removeItem(at: repo) }
    try initRepository(at: repo)
    // Absolute and relative spellings of the same path must agree; check the absolute one resolved
    // through a symbolic link, as the temporary directory itself usually is on macOS.
    let link = try scratch(); defer { try? FileManager.default.removeItem(at: link) }
    let alias = link.appendingPathComponent("alias")
    try FileManager.default.createSymbolicLink(at: alias, withDestinationURL: repo)
    #expect(saveIDRefusal(path: alias.appendingPathComponent("ring-id").path) != nil)
}

@Test func ifGitCannotBeRunTheTargetIsRefused() throws {
    let dir = try scratch(); defer { try? FileManager.default.removeItem(at: dir) }
    let refusal = saveIDRefusal(path: dir.appendingPathComponent("ring-id").path, git: { _, _ in nil })
    #expect(refusal?.contains("git could not be run") == true)
}

@Test func anUnexpectedGitFailureRefuses() throws {
    let dir = try scratch(); defer { try? FileManager.default.removeItem(at: dir) }
    let path = dir.appendingPathComponent("ring-id").path
    #expect(saveIDRefusal(path: path, git: { _, _ in GitOutcome(status: 2, output: "") }) != nil)
    // Inside a work tree, an ignore check that errors (status 128) is not "ignored".
    let erroring: GitRunner = { _, arguments in
        arguments.first == "rev-parse" ? GitOutcome(status: 0, output: "true\n") : GitOutcome(status: 128, output: "")
    }
    #expect(saveIDRefusal(path: path, git: erroring) != nil)
    // Inside a .git directory.
    #expect(saveIDRefusal(path: path, git: { _, _ in GitOutcome(status: 0, output: "false\n") }) != nil)
}
