# digit
Digit, the local first ring tracking app

## Layout

- `Packages/RingKit`: Swift package for talking to the ring.
  - `RingProtocol`: frame encoding, checksums and record decoding for the Yucheng "YCBT" protocol. No Bluetooth dependency.
  - `RingTransport`: CoreBluetooth connection and request/response sessions.
  - `ringprobe`: macOS command-line tool for probing a ring and capturing raw history.

- `App`: the iOS app (`Digit.xcodeproj`), SwiftUI, iPhone only, iOS 26.

## Building the app

```
xcodebuild -project App/Digit.xcodeproj -scheme Digit -destination 'generic/platform=iOS Simulator' CODE_SIGNING_ALLOWED=NO build
```

Or open `App/Digit.xcodeproj` in Xcode. Running on a device needs a signing team selected in Xcode.

## Probing a ring from a Mac

```
cd Packages/RingKit
swift test
swift run ringprobe info
swift run ringprobe history all --save ../../captures
```

Run `swift run ringprobe` with no arguments for the full command list. `history --save` writes one JSON file per kind (`<kind>-<timestamp>.json`) holding the raw bytes as hex plus the header payload, fetch time and time zone needed to decode them later. Health data is only printed with `--show`. Every known delete, reset or factory command (and the groups 0x07xx, 0x0Axx, 0x0Cxx, 0x0Exx) is always refused, even with `--allow-unknown`, and `raw` sends only known read-only commands unless `--allow-unknown` is given.
