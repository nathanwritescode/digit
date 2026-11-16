import Foundation

// Reading and writing the ring's CoreBluetooth identifier. The identifier identifies the owner's
// ring on this Mac, so it never appears in terminal output or error messages.

struct IdentifierFileError: Error, Equatable {
    let message: String
}

/// The most bytes an identifier file may hold: a UUID is 36 characters plus a newline.
let identifierFileLimit = 64

/// Reads the identifier saved by `scan --save-id`. Refuses anything that is not a regular file
/// (a FIFO or `/dev/zero` would hang or never end) and reads at most `identifierFileLimit + 1`
/// bytes. Errors name the path (which the caller typed) and never echo the file's contents.
func readRingIdentifier(path: String) -> Result<UUID, IdentifierFileError> {
    // O_NONBLOCK so opening a FIFO with no writer returns at once; the type check follows.
    let fd = open(path, O_RDONLY | O_NONBLOCK)
    guard fd >= 0 else {
        return .failure(IdentifierFileError(message: "cannot read \(path): \(String(cString: strerror(errno)))"))
    }
    defer { close(fd) }
    var info = stat()
    guard fstat(fd, &info) == 0, (info.st_mode & S_IFMT) == S_IFREG else {
        return .failure(IdentifierFileError(message: "\(path) is not a regular file"))
    }
    var bytes = [UInt8](repeating: 0, count: identifierFileLimit + 1)
    var count = 0
    while count < bytes.count {
        let n = bytes.withUnsafeMutableBytes { read(fd, $0.baseAddress! + count, $0.count - count) }
        if n < 0 {
            if errno == EINTR { continue }
            return .failure(IdentifierFileError(message: "cannot read \(path): \(String(cString: strerror(errno)))"))
        }
        if n == 0 { break }
        count += n
    }
    if count == 0 {
        return .failure(IdentifierFileError(message: "\(path) is empty; write a ring identifier with scan --save-id"))
    }
    if count > identifierFileLimit {
        return .failure(IdentifierFileError(message: "\(path) is too large to hold a ring identifier; write one with scan --save-id"))
    }
    guard let text = String(bytes: bytes[0..<count], encoding: .utf8),
          let id = UUID(uuidString: text.trimmingCharacters(in: .whitespacesAndNewlines)) else {
        return .failure(IdentifierFileError(message: "\(path) does not hold a ring identifier; write one with scan --save-id"))
    }
    return .success(id)
}

/// What `git` said, or nil from a runner if it could not be run at all.
struct GitOutcome: Equatable {
    let status: Int32
    let output: String
}

typealias GitRunner = (_ directory: String, _ arguments: [String]) -> GitOutcome?

/// Runs `git -C <directory> <arguments>` through `env`. Returns nil if git could not be started
/// or was not found. Standard error is discarded.
func runGit(directory: String, arguments: [String]) -> GitOutcome? {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
    process.arguments = ["git", "-C", directory] + arguments
    let pipe = Pipe()
    process.standardOutput = pipe
    process.standardError = FileHandle.nullDevice
    process.standardInput = FileHandle.nullDevice
    do { try process.run() } catch { return nil }
    let data = pipe.fileHandleForReading.readDataToEndOfFile()
    process.waitUntilExit()
    // `env` exits 127 when it cannot find git.
    if process.terminationStatus == 127 { return nil }
    return GitOutcome(status: process.terminationStatus, output: String(decoding: data, as: UTF8.self))
}

/// Decides whether `scan --save-id` may write to `path`. The identifier must not be committed by
/// accident, so a target inside a git work tree is refused unless git ignores that path.
/// Returns nil if the write is allowed, or the reason it is refused. Nothing is written.
func saveIDRefusal(path: String, git: GitRunner = runGit) -> String? {
    let target = URL(fileURLWithPath: path).standardizedFileURL
    // The directory may not exist yet: ask git from the nearest one that does, and resolve
    // symbolic links there so git sees the same path as the work tree's own.
    var directory = target.deletingLastPathComponent()
    var tail = [target.lastPathComponent]
    var isDirectory: ObjCBool = false
    while directory.path != "/",
          !(FileManager.default.fileExists(atPath: directory.path, isDirectory: &isDirectory) && isDirectory.boolValue) {
        tail.insert(directory.lastPathComponent, at: 0)
        directory = directory.deletingLastPathComponent()
    }
    directory = directory.resolvingSymlinksInPath()
    let resolved = tail.reduce(directory) { $0.appendingPathComponent($1) }

    let suggestion = "choose a path outside the repository or one it ignores, for example ../../captures/ring-id (captures/ is git-ignored)"
    guard let inside = git(directory.path, ["rev-parse", "--is-inside-work-tree"]) else {
        return "cannot check whether \(path) could be committed (git could not be run); \(suggestion)"
    }
    if inside.status == 128 { return nil } // not inside a git repository
    guard inside.status == 0 else {
        return "cannot check whether \(path) could be committed (git failed); \(suggestion)"
    }
    // "false" means inside a .git directory: still part of a repository, so refuse.
    guard inside.output.trimmingCharacters(in: .whitespacesAndNewlines) == "true" else {
        return "\(path) is inside a git directory; \(suggestion)"
    }
    guard let ignored = git(directory.path, ["check-ignore", "-q", "--", resolved.path]) else {
        return "cannot check whether \(path) could be committed (git could not be run); \(suggestion)"
    }
    if ignored.status == 0 { return nil }
    return "\(path) is inside a git work tree and not ignored, so it could be committed; \(suggestion)"
}
