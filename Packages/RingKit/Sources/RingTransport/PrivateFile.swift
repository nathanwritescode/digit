import Foundation

/// Writes files that hold health data. Public only so `ringprobe` can use it.
public enum PrivateFile {
    public struct Failure: Error, CustomStringConvertible {
        public let description: String
    }

    private static func failure(_ action: String, _ path: String) -> Failure {
        Failure(description: "cannot \(action) \(path): \(String(cString: strerror(errno)))")
    }

    /// Creates `path` (and missing parents) with mode 0700 if it does not exist. An existing
    /// directory keeps its mode.
    public static func createDirectory(_ path: String) throws {
        do {
            try FileManager.default.createDirectory(
                atPath: path, withIntermediateDirectories: true,
                attributes: [.posixPermissions: 0o700])
        } catch {
            throw Failure(description: "cannot create \(path): \(error.localizedDescription)")
        }
    }

    /// Creates a new file at `path` with mode 0600 (open's mode is subject to the umask, which can
    /// only remove permissions), writes `data` and syncs it. Fails if the file already exists;
    /// an existing file is never overwritten. A file whose write fails part-way is removed.
    public static func write(_ data: Data, to path: String) throws {
        let fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0o600)
        guard fd >= 0 else { throw failure("create", path) }
        var failed: Failure?
        data.withUnsafeBytes { buffer in
            var offset = 0
            while offset < buffer.count {
                let n = Foundation.write(fd, buffer.baseAddress! + offset, buffer.count - offset)
                if n < 0 {
                    if errno == EINTR { continue }
                    failed = failure("write", path)
                    return
                }
                offset += n
            }
        }
        if failed == nil, fsync(fd) != 0 { failed = failure("sync", path) }
        if close(fd) != 0, failed == nil { failed = failure("close", path) }
        if let failed {
            // Do not leave a truncated file under a valid name for later readers to trip over.
            unlink(path)
            throw failed
        }
    }

    /// Writes `data` to `path` with mode 0600, replacing an existing file. The data goes to a new
    /// file beside it first (through `write`, so it is never readable by other accounts) and is
    /// then renamed over `path`, so readers see either the old file or the whole new one. A
    /// symbolic link at `path` is replaced, not followed. The containing directory is synced after
    /// the rename, on a best-effort basis, so the replacement survives a crash.
    ///
    /// The temporary name adds 41 bytes to the file's name (a dot, a UUID and `.tmp`), so a file
    /// name longer than 255 - 41 = 214 bytes (the usual per-name limit) fails to be created and
    /// nothing is replaced.
    public static func replace(_ data: Data, at path: String) throws {
        let temporary = temporaryPath(for: path)
        try write(data, to: temporary)
        guard rename(temporary, path) == 0 else {
            let error = failure("replace", path)
            unlink(temporary)
            throw error
        }
        syncDirectory(containing: path)
    }

    /// A new, unpredictable name beside `path`.
    public static func temporaryPath(for path: String) -> String {
        path + ".\(UUID().uuidString).tmp"
    }

    /// Best effort, and its failure is ignored on purpose: by now the file is in place, so
    /// reporting an error would turn a completed save into a failure. Some filesystems and
    /// unreadable directories cannot be synced.
    private static func syncDirectory(containing path: String) {
        let directory = (path as NSString).deletingLastPathComponent
        let fd = open(directory.isEmpty ? "." : directory, O_RDONLY)
        guard fd >= 0 else { return }
        _ = fsync(fd)
        close(fd)
    }
}
