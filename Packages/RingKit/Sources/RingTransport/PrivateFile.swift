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
    /// an existing file is never overwritten.
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
        if let failed { throw failed }
    }
}
