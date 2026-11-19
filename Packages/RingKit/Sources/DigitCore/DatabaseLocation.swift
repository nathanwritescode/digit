import Foundation
import RingStore
import RingSync

/// Where the app keeps its database, and opening it.
public enum DatabaseLocation {
    /// The App Group the app shares with its future widget (AGENTS.md, Decisions).
    public static let appGroupIdentifier = "group.com.nathantalley.digit"

    /// The database file inside the App Group container: in a subdirectory of its own, because
    /// the store protects and expects a directory dedicated to its database and its `-wal` and
    /// `-shm` files.
    public static func databaseURL(inAppGroupContainer container: URL) -> URL {
        container
            .appending(path: "Digit", directoryHint: .isDirectory)
            .appending(path: "Store", directoryHint: .isDirectory)
            .appending(path: "digit.sqlite", directoryHint: .notDirectory)
    }

    /// Creates the database's directory (owner-only) if needed and opens the store, all off the
    /// main actor (`RingStore.open(at:)` does its work on a background queue). Throws what
    /// `RingStore.open(at:)` throws, or the file system's error for the directory.
    public static func openStore(inAppGroupContainer container: URL) async throws -> RingStore {
        let url = databaseURL(inAppGroupContainer: container)
        try await createDirectory(url.deletingLastPathComponent())
        return try await RingStore.open(at: url)
    }

    // Nonisolated and async, so it runs on the global executor, not the caller's actor.
    private static func createDirectory(_ directory: URL) async throws {
        try FileManager.default.createDirectory(
            at: directory, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
    }

    /// Text for a store that could not be opened. Holds no path (a path names the container) and
    /// no raw error.
    public static func openFailureMessage(_ error: any Error) -> String {
        "Digit couldn't open its data on this iPhone (\(SyncError(error).description)). Try closing and reopening the app."
    }
}
