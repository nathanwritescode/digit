# digit
Digit, the local first ring tracking app

## Layout

- `Packages/RingKit`: Swift package for talking to the ring.
  - `RingProtocol`: frame encoding, checksums and record decoding for the Yucheng "YCBT" protocol. No Bluetooth dependency.
  - `RingTransport`: CoreBluetooth connection and request/response sessions.
  - `ringprobe`: macOS command-line tool for probing a ring and capturing raw history.

## Probing a ring from a Mac

```
cd Packages/RingKit
swift test
swift run ringprobe info
swift run ringprobe history all --save ../../captures
```

Run `swift run ringprobe` with no arguments for the full command list. Health data is only printed with `--show`. The history-delete commands are always refused, and `raw` sends only known read-only commands unless `--allow-unknown` is given.
