import Foundation
import RingTransport

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
    var errorOutput = ""
}

typealias GitRunner = (_ directory: String, _ arguments: [String]) -> GitOutcome?

/// Runs `git -C <directory> <arguments>` through `env`. Returns nil if git could not be started
/// or was not found. The child's environment has the variables that redirect git's discovery
/// removed, so the answer is about the directory and not about this process's environment, and
/// `LC_ALL=C` so the messages `saveIDRefusal` matches on are stable.
func runGit(directory: String, arguments: [String]) -> GitOutcome? {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
    process.arguments = ["git", "-C", directory] + arguments
    var environment = ProcessInfo.processInfo.environment
    for key in ["GIT_DIR", "GIT_WORK_TREE", "GIT_CEILING_DIRECTORIES", "GIT_DISCOVERY_ACROSS_FILESYSTEM"] {
        environment[key] = nil
    }
    environment["LC_ALL"] = "C"
    process.environment = environment
    let out = Pipe(), err = Pipe()
    process.standardOutput = out
    process.standardError = err
    process.standardInput = FileHandle.nullDevice
    do { try process.run() } catch { return nil }
    // Read both pipes concurrently so neither can fill and block git.
    final class Collected: @unchecked Sendable { var data = Data() }
    let collected = Collected()
    let done = DispatchSemaphore(value: 0)
    Thread { collected.data = err.fileHandleForReading.readDataToEndOfFile(); done.signal() }.start()
    let data = out.fileHandleForReading.readDataToEndOfFile()
    done.wait()
    process.waitUntilExit()
    // `env` exits 127 when it cannot find git.
    if process.terminationStatus == 127 { return nil }
    return GitOutcome(status: process.terminationStatus, output: String(decoding: data, as: UTF8.self),
                      errorOutput: String(decoding: collected.data, as: UTF8.self))
}

struct SaveTargetRefusal: Error, Equatable {
    let message: String
}

private let saveTargetAdvice = "choose a path outside the repository, or in a directory it ignores as a whole, for example ../../captures/ring-id (captures/ is git-ignored)"

/// Turns the path given to `--save-id` into ONE physical path, used for both the git check and
/// the write, so they cannot disagree. The nearest existing ancestor of the file's directory is
/// resolved with `realpath` (symbolic links followed, and `..` after a link resolved the way the
/// kernel does, not as text); what does not exist yet is appended, and may not contain `..`.
/// A component that exists but is not a directory is refused.
func resolveSaveTarget(path: String) -> Result<String, SaveTargetRefusal> {
    func refuse(_ why: String) -> Result<String, SaveTargetRefusal> {
        .failure(SaveTargetRefusal(message: "cannot use \(path): \(why)"))
    }
    let absolute = path.hasPrefix("/") ? path : FileManager.default.currentDirectoryPath + "/" + path
    let components = absolute.split(separator: "/", omittingEmptySubsequences: true).map(String.init)
    guard let name = components.last, name != "..", name != "." else { return refuse("it does not name a file") }
    let parent = components.dropLast()

    func isDirectory(_ path: String) -> Bool? {
        var info = stat()
        guard stat(path, &info) == 0 else { return nil }
        return (info.st_mode & S_IFMT) == S_IFDIR
    }
    // The longest leading part of the directory that exists (as the kernel sees it).
    var existing = parent.count
    while existing > 0, isDirectory("/" + parent.prefix(existing).joined(separator: "/")) == nil { existing -= 1 }
    let existingPath = "/" + parent.prefix(existing).joined(separator: "/")
    guard isDirectory(existingPath) == true else { return refuse("a part of its directory is not a directory") }
    let remaining = Array(parent.dropFirst(existing))
    guard !remaining.contains(where: { $0 == ".." }) else { return refuse("it goes up (..) from a directory that does not exist") }
    guard let real = realpath(existingPath, nil) else { return refuse("cannot resolve its directory") }
    defer { free(real) }
    let base = String(cString: real)
    return .success(([base == "/" ? "" : base] + remaining.filter { $0 != "." } + [name]).joined(separator: "/"))
}

/// Decides whether `scan --save-id` may write to `path`, and where. The identifier must not be
/// committed by accident, so a target inside a git work tree is refused unless git ignores both
/// the target and the temporary file `PrivateFile.replace` writes beside it (a process that dies
/// before the rename leaves that file behind, so the directory has to be ignored as a whole).
/// On success returns the one resolved path to write; nothing is written here.
func checkSaveTarget(path: String, git: GitRunner = runGit) -> Result<String, SaveTargetRefusal> {
    let resolved: String
    switch resolveSaveTarget(path: path) {
    case .success(let value): resolved = value
    case .failure(let refusal): return .failure(refusal)
    }
    func refuse(_ why: String) -> Result<String, SaveTargetRefusal> {
        .failure(SaveTargetRefusal(message: "\(why); \(saveTargetAdvice)"))
    }
    // git runs from the nearest existing directory of the resolved path.
    var directory = (resolved as NSString).deletingLastPathComponent
    var isDirectory: ObjCBool = false
    while directory != "/", !(FileManager.default.fileExists(atPath: directory, isDirectory: &isDirectory) && isDirectory.boolValue) {
        directory = (directory as NSString).deletingLastPathComponent
    }

    guard let inside = git(directory, ["rev-parse", "--is-inside-work-tree"]) else {
        return refuse("cannot check whether \(path) could be committed (git could not be run)")
    }
    // Only git's plain "no repository here" answer allows the write. Status 128 is also used for
    // a broken .git, dubious ownership and more, none of which say the path is safe.
    if inside.status == 128, inside.errorOutput.contains("not a git repository (or any of the parent directories)") {
        return .success(resolved)
    }
    guard inside.status == 0 else {
        return refuse("git could not judge whether \(path) could be committed")
    }
    // "false" means inside a .git directory: still part of a repository, so refuse.
    guard inside.output.trimmingCharacters(in: .whitespacesAndNewlines) == "true" else {
        return refuse("\(path) is inside a git directory")
    }
    let samples = [resolved, PrivateFile.temporaryPath(for: resolved)]
    for (index, sample) in samples.enumerated() {
        guard let ignored = git(directory, ["check-ignore", "-q", "--", sample]) else {
            return refuse("cannot check whether \(path) could be committed (git could not be run)")
        }
        if ignored.status == 0 { continue }
        if ignored.status != 1 { return refuse("git could not judge whether \(path) could be committed") }
        return refuse(index == 0
            ? "\(path) is inside a git work tree and not ignored, so it could be committed"
            : "\(path) is ignored, but the temporary file written beside it is not, and a crash could leave it there: ignore the containing directory as a whole")
    }
    return .success(resolved)
}

/// The refusal for `path`, or nil if `checkSaveTarget` allows it.
func saveIDRefusal(path: String, git: GitRunner = runGit) -> String? {
    if case .failure(let refusal) = checkSaveTarget(path: path, git: git) { refusal.message } else { nil }
}
