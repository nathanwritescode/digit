// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "RingKit",
    platforms: [.iOS("26.0"), .macOS(.v14)],
    products: [
        .library(name: "RingProtocol", targets: ["RingProtocol"]),
        .library(name: "RingTransport", targets: ["RingTransport"]),
        .library(name: "RingStore", targets: ["RingStore"]),
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
        // macOS command-line tool for probing a ring and capturing raw data.
        .executableTarget(
            name: "ringprobe",
            dependencies: ["RingProtocol", "RingTransport"]
        ),
        .testTarget(name: "RingProtocolTests", dependencies: ["RingProtocol"]),
        .testTarget(name: "RingStoreTests", dependencies: ["RingProtocol", "RingStore"]),
        .testTarget(name: "RingTransportTests", dependencies: ["RingProtocol", "RingTransport"]),
    ]
)
