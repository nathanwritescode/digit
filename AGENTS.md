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
  Tests/RingTransportTests/ session tests against a scripted fake RingLink (no Bluetooth)
App/
  Digit.xcodeproj          iOS app project (SwiftUI, iOS 26, Swift 6, iPhone only); depends on RingKit
  Digit/                   app sources (file-system-synchronised: new files are picked up automatically)
  Config/                  Info.plist (usage strings, background mode) and Digit.entitlements (HealthKit)
captures/                  raw ring captures (git-ignored, private)
```

Planned but not yet built: a ring abstraction with per-device capabilities, the on-device store (GRDB) with export, and the Apple Health writer (write-only for now, behind an interface that can add reading later). The SwiftUI app exists as an empty skeleton with a placeholder screen.

## Commands

Run from `Packages/RingKit`:

```
swift build
swift test
swift run ringprobe                 # usage
swift run ringprobe scan
swift run ringprobe info
swift run ringprobe history all --save ../../captures   # --save is required; one JSON file per kind (hex bytes, header, fetch time, time zone)
swift run ringprobe raw 0200:4743   # send a known read-only frame
```

Build the app for the simulator from the repository root (needs Xcode, no signing):

```
xcodebuild -project App/Digit.xcodeproj -scheme Digit -destination 'generic/platform=iOS Simulator' CODE_SIGNING_ALLOWED=NO build
```

Continuous integration (`.github/workflows/ci.yml`) runs `swift build` and `swift test` for RingKit, and the `xcodebuild` command above for the app, on every pull request and every push to `main`, using the newest released Xcode on GitHub's macOS runner. That toolchain can be older than a developer's local one, so a feature that builds locally may still fail there; the CI result is the one that counts.

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
- History transfer: request `0x05xx`, header reply on the same key (payload of 9 bytes or fewer means empty), data frames, then `0x0580` with a CRC of the concatenated data, which the app acknowledges with `[0x00]` (or `[0x04]` to retry). Data frames use a different key from the request (`0x0513` for sleep), and each arrived as one whole frame per notification, up to the MTU.

### Verified on this ring vs. not

| Area | Status |
|---|---|
| Frame encoding, CRC, device info (`0x0200`), current activity (`0x020C`), set time (`0x0100`) | Verified against the ring |
| History requests `0x0502`, `0x0504`, `0x0506`, `0x0509` | Verified: all four return data after a night of wear |
| History transfer sequence (header, data, end frame with checksum, acknowledgement) | Verified, including a transfer spanning three data frames |
| Acknowledging a transfer | Does not discard it: a second fetch right after returned identical bytes |
| Activity, sleep and heart record decoders | Verified against one real night: the Swift decoders matched an independent decode for all three, and the owner confirmed the sleep times matched their night (recorded on #6). Test fixtures remain synthetic |
| Combined record (`0x0509`, 20 bytes) | Layout matches documentation. `CombinedRecord` decodes it; the Swift decoder was run against a real R99 capture (36 records) and every exposed field matched an independent decode (test fixtures remain synthetic). Timestamp, steps, heart rate, SpO2, respiration, HRV and CVRR are plausible and agree with the other records |
| Monitoring interval (`0x010C`, payload `[enabled, minutes]`) | Verified at 1, 5 and 10 minutes and off: ack `00`; readings arrive at that spacing, aligned to the clock, and stop when off. Each scheduled reading produces a heart-rate record and a combined record, so this one setting drives heart rate, SpO2, respiration, HRV and CVRR. Before any setting was sent the ring measured hourly, presumably its default. Readings resuming after re-enabling was accepted by the ring but not yet confirmed from history |
| Battery percentage (device info byte 5) | Unverified: it has read 100% at every check so far (#25). Do not show it in the app until settled |
| `0x0220` live snapshot, `0x0533` body data | Ring replies unsupported |
| `0x051A` SpO2 history, `0x051E` temperature history | No reply at all, before or after a night of wear: treat as unsupported. SpO2 is available only in the combined record |
| SpO2 monitor (`0x0126`, payload `[enabled, minutes]`) | Acknowledged with `00` but no visible effect: with monitoring off, no SpO2 reading was logged at the next boundary. Not implemented |
| Read settings back (`0x0207`, payload `43 46`) | Rejected as unsupported. The app must remember settings and re-send them |
| On-demand measurement (`0x032F`, payload `[start 1/0, kind]`) | Verified for heart rate (kind 0) and SpO2 (kind 2): ack `00`, live values on `0x0601` or `0x0602`, then event `0x040E [kind, 01]` when done (about 27 s for heart rate, about 67 s for SpO2, with SpO2 values only arriving in the last few seconds). The result is also logged to history: heart rate as a heart-rate record with mode 0, SpO2 as a combined record with only SpO2 filled. No builder or tool command exists yet |
| Standard Heart Rate service (`180D`, characteristic `2A37`) | Verified: about one notification a second while subscribed, with no RR intervals, so it cannot supply HRV. Not used by the code yet |

Update this table as captures confirm or contradict it. Best external reference: `docs/PROTOCOL.md` in `PopShark57/SmartRingWatcher` (no licence: read, don't copy).

### Data quirks seen on this ring (R99, firmware 2.32)

Seen in one night of data unless stated otherwise. Treat them as properties of this device and firmware, not of the protocol, and write code that tolerates them without requiring them.

- **Sleep stages** are stored in time order, and each started one second after the previous one ended. Treat a gap of up to about a second as contiguous; do not require exactly one second.
- **Sleep header:** the 8 totals bytes at offset 12 of the 20-byte header (after the `AF FA` marker, length, start and end) used the newer layout: `FF FF`, then REM, deep and light totals as u16 seconds. They equalled the sums of the stage durations.
- **No awake stages** were reported. A brief wake-up appeared as a gap of a few minutes between two sessions.
- **Activity** records were 30-minute buckets; **heart-rate** records carried mode 0 for both scheduled and on-demand readings, so the mode byte does not tell them apart.
- **Combined record:** the blood-pressure fields are filled in, but a ring cannot measure blood pressure; they are vendor estimates. The temperature fields held the same value in every record and were not a body temperature; treat as absent. Body fat and glucose were always zero. The last two bytes, documented as reserved, varied between records and are not understood.
- **Setting validation is untested here.** On this ring only valid heart-rate intervals (1, 5 and 10 minutes) have been sent, and all were honoured. Another ring of the same family is reported to acknowledge any interval byte, including nonsense values, so the app must only ever send values from a known list.

### Known gaps in RingTransport

Built for the command-line tool; these must be closed before the app relies on it. Each should have an issue.

- Ring-initiated events are acknowledged only while a request is in flight; there is no continuous reader and no way to subscribe to uploads.
- Connecting scans for all devices and matches by name. iOS needs scanning by service, reconnecting by stored identifier, and state restoration for background sync.
- No task cancellation support. `RingSession` (an actor over the `RingLink` protocol) queues conversations, but `RingConnection` itself still assumes one conversation at a time, so the app must use it only through a session (`ringprobe raw` uses it directly, which is fine for a single-task command-line tool).
- When a continuous reader is added, `discardPendingFrames` must not drop ring-initiated events (group `0x04`), which still need acknowledging.
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

**Ring access.** The ring accepts one connection at a time, and subagents (implementers and reviewers) cannot obtain the owner's go-ahead, so access is limited by role:

- *Reviewers* never run `ringprobe`, with any arguments. They run in parallel and are asked to try to break things, so even argument-validation tests can end up sending frames.
- *Implementers* may run only `ringprobe info` and `ringprobe raw` with `0200:4743` (device info) or `020C` (today's activity), without `--show`, `--trace` or `--allow-unknown`, and only when their issue says a hardware check is in scope. The history requests (`0x05xx`) are on the read-only list but are not allowed here: `raw` starts a transfer it never saves or acknowledges. Output from these commands must not be copied into commits, issues or pull requests.
- *The planner, with the owner* does everything else: `scan` (it prints nearby device names, including the ring's name suffix), `settime`, fetching history (it writes health data to disk), anything using `--show`, `--trace` or `--allow-unknown`, and every command that changes a ring setting. Any new `ringprobe` subcommand is planner-only until this list says otherwise.

The planner starts at most one hardware-check implementer at a time, and never while the planner or the owner is using the ring. An issue with a hardware check in scope still carries `needs-hardware`; the planner decides whether the check is small enough to delegate under this rule.

**Review.** Every pull request gets the architecture, bugs and practices reviewers, each started fresh with no context from the implementation. Add the privacy reviewer when the change touches storage, export, Apple Health, logging, networking, dependencies, test fixtures or the commands sent to the ring. The planner checks each finding, sends blocking ones back to an implementer, and re-runs the affected reviewers on the fix. The owner merges once no blocking findings remain.

**Text on GitHub.** No "generated with" footers on issues or pull requests. An "Agent details" section in an issue is fine.

## Git

- Never commit to or push `main` directly. Check the current branch before committing. Each issue gets a branch named `issue-<number>-<short-slug>`, pushed for its pull request.
- Owner-requested changes with no issue (documentation, for example) use a branch named `docs-<short-slug>` or `chore-<short-slug>` and say in the pull request that there is no issue. They get the same review.
- Commits are signed with the owner's configured key (`commit.gpgsign` is on). Never bypass signing or override the author.
- No AI or assistant attribution anywhere in commits: no co-author trailers, no tool names in messages.
- Commit messages: imperative subject line, body explaining what and why, and what was and wasn't verified.

## Decisions

- **Database:** SQLite via GRDB (MIT). Raw ring records are the source of truth; anything derived can be rebuilt from them. Do not introduce SwiftData or Core Data.
- **Minimum iOS version:** iOS 26.
- **Concurrency defaults:** the app target defaults to main-actor isolation (`SWIFT_DEFAULT_ACTOR_ISOLATION = MainActor`); the RingKit package does not.
- **Bundle identifier:** `com.nathantalley.digit`.
- **Vendor estimates:** raw ring records are stored exactly as received, but values the ring cannot measure (blood pressure, and the temperature field on the R99) are never exposed as measurements in derived tables, the app, exports of typed data, or Apple Health.
- **One monitoring interval:** the app offers a single setting (off, 1, 5, 10, 15, 20, 30 or 60 minutes; default 10) that controls heart rate, SpO2, respiration and HRV together, because that is how the ring behaves. There is no separate SpO2 setting. The ring cannot report its settings, so the app stores the choice and re-sends it on every sync.

## Open decisions

- **Apple Developer account:** the owner enrolled in the paid Apple Developer Program on 2026-11-11; activation was pending. Until the paid team appears in Xcode, do not run the app on a device with the free Personal Team, so the bundle identifier is not registered to the wrong team. No signing team is committed to the project.
- **Repository visibility:** private since 2026-11-11 while GitHub Support removes a closed pull request; the intent is to make it public again. GitHub Actions minutes are metered while it is private.
