// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "RingKit",
    platforms: [.iOS("26.0"), .macOS(.v14)],
    products: [
        .library(name: "RingProtocol", targets: ["RingProtocol"]),
        .library(name: "RingTransport", targets: ["RingTransport"]),
        .executable(name: "ringprobe", targets: ["ringprobe"]),
    ],
    targets: [
        // Pure Swift: frames, checksums and record decoding. No Bluetooth dependency.
        .target(name: "RingProtocol"),
        // CoreBluetooth link to the ring.
        .target(
            name: "RingTransport",
            dependencies: ["RingProtocol"]
        ),
        // macOS command-line tool for probing a ring and capturing raw data.
        .executableTarget(
            name: "ringprobe",
            dependencies: ["RingProtocol", "RingTransport"]
        ),
        .testTarget(name: "RingProtocolTests", dependencies: ["RingProtocol"]),
        .testTarget(name: "RingTransportTests", dependencies: ["RingProtocol", "RingTransport"]),
    ]
)
