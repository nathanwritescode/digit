# AGENTS.md

Guidance for coding agents working in this repo. Keep it current: when a decision, command, layout or protocol fact here changes, update this file in the same commit.

## What this is

Digit is a native iOS companion app for a Bluetooth smart ring. It fully replaces the vendor's app (SmartHealth): it pulls data off the ring, keeps its own on-device history, and writes to Apple Health.

Priorities for v1, in order: sleep tracking, then steps, then everything else the ring exposes. Live readings and a widget come after v1.

## Hard rules

1. **Health data never leaves the device.** No network code may touch health data. iCloud device backup and Apple Health's own iCloud sync are acceptable. Crash reporting or analytics are acceptable only if they carry no health data and stay isolated from it.
2. **Never delete data from the ring without the owner's explicit confirmation.** The `Health_Delete*` commands (`0x0540`–`0x0552`) are not implemented and `RingConnection.send` refuses them. Other commands that reset or erase the ring may exist and are not mapped, so `ringprobe raw` sends only known read-only commands; never pass `--allow-unknown` without the owner's go-ahead for that specific command.
3. **Keep health data out of agent transcripts.** Anything an agent prints or reads is sent to its model provider. `ringprobe` prints only counts by default; do not pass `--show` or `--trace`, and do not read files in `captures/`, without the owner's go-ahead.
4. **Never commit real health data or device identifiers.** Raw captures go in `captures/`, which is git-ignored. Test fixtures taken from real captures need the owner's approval first; otherwise build synthetic bytes with the same layout. Do not write the ring's advertised name suffix, Bluetooth address or serial number anywhere in the repo, issues or pull requests.
5. **Licensing.** Source is Apache 2.0; the "digit" name and branding are reserved (see `NOTICE`). Dependencies must have Apache-compatible licences. Third-party reverse-engineering projects are protocol references only: do not copy code from repos without a licence.
6. **Native only.** Swift and SwiftUI, iPhone first. No cross-platform frameworks.

## Layout

```
Packages/RingKit/
  Sources/RingProtocol/    pure Swift: frames, CRC, commands, record decoders (no Bluetooth)
  Sources/RingTransport/   CoreBluetooth connection and request/response sessions
  Sources/ringprobe/       macOS command-line tool for probing a ring
  Tests/RingProtocolTests/ Swift Testing tests
captures/                  raw ring captures (git-ignored, private)
```

Planned but not yet built: a ring abstraction with per-device capabilities, the on-device store (GRDB) with export, the Apple Health writer (write-only for now, behind an interface that can add reading later), and the SwiftUI app.

## Commands

Run from `Packages/RingKit`:

```
swift build
swift test
swift run ringprobe                 # usage
swift run ringprobe scan
swift run ringprobe info
swift run ringprobe history all --save ../../captures   # --save is required
swift run ringprobe raw 0200:4743   # send a known read-only frame
```

`ringprobe` needs a ring in range that is not connected to another device. It uses the Mac's Bluetooth, so the terminal running it needs Bluetooth permission. Bluetooth does not work in the iOS simulator; app-level Bluetooth testing needs a physical iPhone.

## The ring

- Advertises as `R99 xxxx`, where the suffix is specific to the device. Firmware 2.32. Vendor app: SmartHealth (Shenzhen Yucheng).
- Speaks the Yucheng "YCBT" protocol on service `BE940000-7333-BE46-B7AE-689E71722BD5`. `BE940001` takes commands and indicates replies; `BE940003` indicates ring-initiated uploads.
- Also exposes the standard Heart Rate service (`180D`), a pedometer service (`FEE7`), Nordic UART, and `AE00` (probably firmware update; unconfirmed).

### Protocol facts

- Frame: `[group][key][len u16 LE][payload][crc u16 LE]`. `len` counts the whole frame. CRC is CRC-16/CCITT-FALSE over everything before it.
- A one-byte payload `0xFB`–`0xFF` is an error reply (`0xFC` = unsupported).
- Ring-initiated events (group `0x04`) must be acknowledged with the same group and key and payload `[0x00]`.
- Timestamps are local wall-clock seconds since 2000-01-01, not UTC. The ring's clock must be set (`0x0100`) for history to be meaningful.
- History transfer: request `0x05xx`, header reply on the same key (payload of 9 bytes or fewer means empty), data frames, then `0x0580` with a CRC of the concatenated data, which the app acknowledges with `[0x00]` (or `[0x04]` to retry).

### Verified on this ring vs. not

| Area | Status |
|---|---|
| Frame encoding, CRC, device info (`0x0200`), current activity (`0x020C`), set time (`0x0100`) | Verified against the ring |
| History requests `0x0502`, `0x0504`, `0x0506`, `0x0509` | Ring answers. Heart-rate and combined history have returned one record each; activity and sleep only empty so far |
| History transfer sequence (header, data, end frame with checksum, acknowledgement) | Verified with single-record transfers; a transfer spanning several data frames has not been seen |
| Acknowledging a transfer | Does not discard it: a second fetch right after returned identical bytes |
| Activity, sleep and heart record decoders | Written from documentation, tested with synthetic data only. The one real heart-rate transfer is 6 bytes, matching the documented record size; its contents have not been checked |
| `0x0220` live snapshot, `0x0533` body data | Ring replies unsupported |
| `0x051A` SpO2 history, `0x051E` temperature history | No reply; support unknown |

Update this table as captures confirm or contradict it. Best external reference: `docs/PROTOCOL.md` in `PopShark57/SmartRingWatcher` (no licence: read, don't copy).

### Known gaps in RingTransport

Built for the command-line tool; these must be closed before the app relies on it. Each should have an issue.

- One conversation at a time, by convention only. Overlapping requests on one connection are not supported.
- Ring-initiated events are acknowledged only while a request is in flight; there is no continuous reader and no way to subscribe to uploads.
- History is returned as bare bytes, without the header frame, fetch time or time zone needed to re-decode it later.
- Connecting scans for all devices and matches by name. iOS needs scanning by service, reconnecting by stored identifier, and state restoration for background sync.
- Swift 5 language mode, no task cancellation support, and no automated tests.
- Time: the ring's clock does not follow daylight saving or travel, so it must be set on every sync; records from a ring whose clock was never set decode as dates in 2000 and must be rejected downstream.
- History is stored before it is acknowledged (`RingSession.fetchHistory` takes a `persist` step). The ring kept its history after acknowledgement in the one case tested; keep the ordering anyway, since that is one observation on one firmware.

## Conventions

- **Layering.** `RingProtocol` stays free of CoreBluetooth and of I/O so it is testable anywhere. Bluetooth code lives in `RingTransport`. UI code never parses bytes.
- **Tests.** Every decoder gets a test. Prefer bytes captured from the real ring over synthetic ones, and say in the test which they are. Run `swift test` before committing.
- **Unverified code.** If something is implemented from documentation and not confirmed on hardware, say so in a comment or in the table above. Do not present it as working.
- **Dependencies.** Keep them few. Ask the owner before adding one.
- **Comments.** Explain why, and record protocol quirks; don't restate the code.

## Workflow

Work is organised as GitHub issues, one pull request per issue.

| Role | Who | Responsibility |
|---|---|---|
| Planner | The lead session, on the most capable model | Writes issues, makes design decisions, does hardware work with the owner, runs reviews, reports to the owner. |
| Implementer | `implementer` subagent | Implements exactly one issue on its own branch and opens a pull request. Sonnet by default; Haiku for mechanical tasks; Opus where an issue calls for it. |
| Reviewers | `review-architecture`, `review-bugs`, `review-practices`, `review-privacy` subagents | Fresh, adversarial, read-only reviews of each pull request. |
| Owner | Nathan | Approves the plan, does what needs a person (wearing the ring, iPhone testing, accounts), merges. |

Agent definitions live in `.claude/agents/`.

**Issues** use the template in `.github/ISSUE_TEMPLATE/implementation.md`. Each must be self-contained: an implementer with no other context should be able to finish it from the issue and this file. Label issues that need the physical ring, a real iPhone or Apple Health permissions `needs-hardware`; those are not handed to an implementer alone.

**Review.** Every pull request gets the architecture, bugs and practices reviewers, each started fresh with no context from the implementation. Add the privacy reviewer when the change touches storage, export, Apple Health, logging, networking, dependencies, test fixtures or the commands sent to the ring. The planner checks each finding, sends blocking ones back to an implementer, and re-runs the affected reviewers on the fix. The owner merges once no blocking findings remain.

**Text on GitHub.** No "generated with" footers on issues or pull requests. An "Agent details" section in an issue is fine.

## Git

- Never commit to or push `main` directly. Check the current branch before committing. Each issue gets a branch named `issue-<number>-<short-slug>`, pushed for its pull request.
- Commits are signed with the owner's configured key (`commit.gpgsign` is on). Never bypass signing or override the author.
- No AI or assistant attribution anywhere in commits: no co-author trailers, no tool names in messages.
- Commit messages: imperative subject line, body explaining what and why, and what was and wasn't verified.

## Decisions

- **Database:** SQLite via GRDB (MIT). Raw ring records are the source of truth; anything derived can be rebuilt from them. Do not introduce SwiftData or Core Data.
- **Minimum iOS version:** iOS 26.
- **Bundle identifier:** `com.nathantalley.digit`.

## Open decisions

- **Apple Developer account:** free account for now; a paid one is needed for TestFlight and for builds that last longer than 7 days.
