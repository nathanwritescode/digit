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

@Test func aSymlinkedPathIntoAnUnignoredRepositoryIsRefusedAndAnIgnoredOneAllowed() throws {
    let repo = try scratch(); defer { try? FileManager.default.removeItem(at: repo) }
    try initRepository(at: repo)
    try Data("captures/\n".utf8).write(to: repo.appendingPathComponent(".gitignore"))
    try FileManager.default.createDirectory(at: repo.appendingPathComponent("captures"), withIntermediateDirectories: false)
    let outside = try scratch(); defer { try? FileManager.default.removeItem(at: outside) }
    let alias = outside.appendingPathComponent("alias")
    try FileManager.default.createSymbolicLink(at: alias, withDestinationURL: repo)
    #expect(saveIDRefusal(path: alias.appendingPathComponent("ring-id").path) != nil)
    let ignoredAlias = outside.appendingPathComponent("ignored")
    try FileManager.default.createSymbolicLink(at: ignoredAlias, withDestinationURL: repo.appendingPathComponent("captures"))
    #expect(saveIDRefusal(path: ignoredAlias.appendingPathComponent("ring-id").path) == nil)
}

@Test func dotDotAfterASymlinkIsResolvedTheWayTheKernelDoes() throws {
    // <outside>/subalias/../ring-id, with subalias linking into a repository, writes inside the
    // repository: ".." goes up from the link's target, not from <outside>.
    let repo = try scratch(); defer { try? FileManager.default.removeItem(at: repo) }
    try initRepository(at: repo)
    try FileManager.default.createDirectory(at: repo.appendingPathComponent("sub"), withIntermediateDirectories: false)
    let outside = try scratch(); defer { try? FileManager.default.removeItem(at: outside) }
    try FileManager.default.createSymbolicLink(at: outside.appendingPathComponent("subalias"),
                                               withDestinationURL: repo.appendingPathComponent("sub"))
    let path = outside.path + "/subalias/../ring-id"
    #expect(saveIDRefusal(path: path) != nil)
    // The resolved path, which is what gets written, is the repository's, not <outside>'s.
    #expect(try resolveSaveTarget(path: path).get().hasSuffix("/" + repo.lastPathComponent + "/ring-id"))
    #expect(!FileManager.default.fileExists(atPath: repo.appendingPathComponent("ring-id").path))
}

@Test func aRelativePathIsJudgedFromTheWorkingDirectory() throws {
    let repo = try scratch(); defer { try? FileManager.default.removeItem(at: repo) }
    try initRepository(at: repo)
    try FileManager.default.createDirectory(at: repo.appendingPathComponent("sub"), withIntermediateDirectories: false)
    let before = FileManager.default.currentDirectoryPath
    defer { FileManager.default.changeCurrentDirectoryPath(before) }
    #expect(FileManager.default.changeCurrentDirectoryPath(repo.appendingPathComponent("sub").path))
    #expect(saveIDRefusal(path: "ring-id") != nil)
    #expect(saveIDRefusal(path: "../ring-id") != nil)
    #expect(saveIDRefusal(path: "new/ring-id") != nil)
}

@Test func nonexistentPartsMayNotGoUp() throws {
    let dir = try scratch(); defer { try? FileManager.default.removeItem(at: dir) }
    #expect(saveIDRefusal(path: dir.path + "/missing/../ring-id") != nil)
    #expect(saveIDRefusal(path: dir.path + "/..") != nil)
}

@Test func aRegularFileWhereADirectoryShouldBeIsRefused() throws {
    let dir = try scratch(); defer { try? FileManager.default.removeItem(at: dir) }
    try Data("x".utf8).write(to: dir.appendingPathComponent("file"))
    #expect(saveIDRefusal(path: dir.path + "/file/ring-id") != nil)
    #expect(saveIDRefusal(path: dir.path + "/file/deeper/ring-id") != nil)
}

@Test func theTemporaryFileMustBeIgnoredToo() throws {
    let repo = try scratch(); defer { try? FileManager.default.removeItem(at: repo) }
    try initRepository(at: repo)
    // Only the exact file name is ignored: a crash would leave an unignored temporary file.
    try Data("ring-id\n".utf8).write(to: repo.appendingPathComponent(".gitignore"))
    let refusal = saveIDRefusal(path: repo.appendingPathComponent("ring-id").path)
    #expect(refusal?.contains("containing directory") == true)
    // Ignoring the directory as a whole is fine.
    try Data("keep/\n".utf8).write(to: repo.appendingPathComponent(".gitignore"))
    #expect(saveIDRefusal(path: repo.appendingPathComponent("keep/ring-id").path) == nil)
}

@Test func aBrokenGitFileIsRefused() throws {
    let dir = try scratch(); defer { try? FileManager.default.removeItem(at: dir) }
    try Data("gitdir: /nonexistent\n".utf8).write(to: dir.appendingPathComponent(".git"))
    let refusal = saveIDRefusal(path: dir.appendingPathComponent("ring-id").path)
    #expect(refusal?.contains("could not judge") == true)
}

@Test func onlyThePlainNoRepositoryAnswerAllows() throws {
    let dir = try scratch(); defer { try? FileManager.default.removeItem(at: dir) }
    let path = dir.appendingPathComponent("ring-id").path
    let plain = "fatal: not a git repository (or any of the parent directories): .git\n"
    #expect(saveIDRefusal(path: path, git: { _, _ in GitOutcome(status: 128, output: "", errorOutput: plain) }) == nil)
    for stderr in ["fatal: detected dubious ownership in repository at '/x'\n", "fatal: not a git repository: /nonexistent\n", ""] {
        let refusal = saveIDRefusal(path: path, git: { _, _ in GitOutcome(status: 128, output: "", errorOutput: stderr) })
        #expect(refusal?.contains("could not judge") == true, "\(stderr)")
    }
    // git missing (the runner reports nil for env's 127), and a status other than 0 or 128.
    #expect(saveIDRefusal(path: path, git: { _, _ in nil }) != nil)
    #expect(saveIDRefusal(path: path, git: { _, _ in GitOutcome(status: 1, output: "", errorOutput: plain) }) != nil)
}

@Test func theChildGitDoesNotInheritRedirectingVariables() throws {
    // GIT_DIR pointing at an unrelated repository would make every directory look "inside" it;
    // the runner removes it. Checked by running a real git with the variable set in this process.
    let repo = try scratch(); defer { try? FileManager.default.removeItem(at: repo) }
    try initRepository(at: repo)
    let outside = try scratch(); defer { try? FileManager.default.removeItem(at: outside) }
    setenv("GIT_DIR", repo.appendingPathComponent(".git").path, 1)
    defer { unsetenv("GIT_DIR") }
    #expect(saveIDRefusal(path: outside.appendingPathComponent("ring-id").path) == nil)
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
