// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "RingKit",
    platforms: [.iOS("26.0"), .macOS(.v14)],
    products: [
        .library(name: "RingProtocol", targets: ["RingProtocol"]),
        .library(name: "RingTransport", targets: ["RingTransport"]),
        .library(name: "RingStore", targets: ["RingStore"]),
        .library(name: "RingSync", targets: ["RingSync"]),
        .library(name: "DigitCore", targets: ["DigitCore"]),
        .executable(name: "ringprobe", targets: ["ringprobe"]),
    ],
    dependencies: [
        // SQLite toolkit for the on-device store (MIT). Only RingStore depends on it.
        .package(url: "https://github.com/groue/GRDB.swift.git", from: "7.12.0"),
    ],
    targets: [
        // Pure Swift: frames, checksums and record decoding. No Bluetooth dependency.
        .target(name: "RingProtocol"),
        // CoreBluetooth link to the ring.
        .target(
            name: "RingTransport",
            dependencies: ["RingProtocol"]
        ),
        // On-device SQLite store for raw ring data. No Bluetooth, no UI, no network.
        .target(
            name: "RingStore",
            dependencies: [
                "RingProtocol",
                .product(name: "GRDB", package: "GRDB.swift"),
            ]
        ),
        // One sync against a connected ring: fetch and store history, set the clock, send the
        // monitoring interval, ingest. No Bluetooth import, no UI, no network.
        .target(
            name: "RingSync",
            dependencies: [
                "RingProtocol", "RingTransport", "RingStore",
                // Only to reduce GRDB's DatabaseError to its result codes (SyncError).
                .product(name: "GRDB", package: "GRDB.swift"),
            ]
        ),
        // The app's logic, testable outside the app target: pairing, foreground sync and the store's
        // suspension around background tasks. No SwiftUI, UIKit or networking imports.
        .target(
            name: "DigitCore",
            dependencies: ["RingProtocol", "RingTransport", "RingStore", "RingSync"]
        ),
        // macOS command-line tool for probing a ring and capturing raw data.
        .executableTarget(
            name: "ringprobe",
            dependencies: ["RingProtocol", "RingTransport"]
        ),
        .testTarget(name: "RingProtocolTests", dependencies: ["RingProtocol"]),
        // Tests the tool's own helpers through `@testable import ringprobe`.
        .testTarget(name: "ringprobeTests", dependencies: ["ringprobe", "RingProtocol"]),
        .testTarget(
            name: "RingStoreTests",
            dependencies: ["RingProtocol", "RingStore", .product(name: "GRDB", package: "GRDB.swift")]
        ),
        .testTarget(name: "RingTransportTests", dependencies: ["RingProtocol", "RingTransport"]),
        // GRDB only so tests can install SQL triggers that make one kind's store write fail.
        .testTarget(
            name: "RingSyncTests",
            dependencies: [
                "RingProtocol", "RingTransport", "RingStore", "RingSync",
                .product(name: "GRDB", package: "GRDB.swift"),
            ]
        ),
        .testTarget(
            name: "DigitCoreTests",
            dependencies: ["RingProtocol", "RingTransport", "RingStore", "RingSync", "DigitCore"]
        ),
    ]
)
