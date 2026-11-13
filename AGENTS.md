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
  Sources/RingStore/       GRDB-backed on-device store: rings, raw history transfers, transfer observations, clock-set log, typed activity, sleep and heart-rate tables with repeatable ingestion (no Bluetooth; the app does not link it yet)
  Sources/ringprobe/       macOS command-line tool for probing a ring
  Tests/RingProtocolTests/ Swift Testing tests
  Tests/RingStoreTests/    store tests on in-memory and temporary-file databases
  Tests/RingTransportTests/ session tests against a scripted fake RingLink (no Bluetooth)
App/
  Digit.xcodeproj          iOS app project (SwiftUI, iOS 26, Swift 6, iPhone only); depends on RingKit
  Digit/                   app sources (file-system-synchronised: new files are picked up automatically)
  Config/                  Info.plist (usage strings, background mode) and Digit.entitlements (HealthKit)
captures/                  raw ring captures (git-ignored, private)
```

Planned but not yet built: a ring abstraction with per-device capabilities (record decoding and time resolution in the store are specific to the YCBT family and move behind it when a second family arrives), export from the on-device store (the store has raw tables and typed activity, sleep and heart-rate tables; combined-record tables are planned), and the Apple Health writer (write-only for now, behind an interface that can add reading later). The SwiftUI app exists as an empty skeleton with a placeholder screen.

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
| Monitoring interval (`0x010C`, payload `[enabled, minutes]`) | Verified at 1, 5 and 10 minutes, off, and back on. 15, 20, 30 and 60 minutes have never been sent. Details below |
| Battery percentage (device info byte 5) | Unverified: it has read 100% at every check so far (#25). Do not show it in the app until settled |
| `0x0220` live snapshot, `0x0533` body data | Ring replies unsupported |
| `0x051A` SpO2 history, `0x051E` temperature history | No reply at all, before or after a night of wear: treat as unsupported. SpO2 is available only in the combined record |
| SpO2 monitor (`0x0126`; payload documented as `[enabled, minutes]`, only `[01 1E]` was sent) | Acknowledged with `00` but no visible effect over several hours. Not implemented. Details below |
| Read settings back (`0x0207`, payload `43 46`) | Rejected as unsupported. The app must remember settings and re-send them |
| On-demand measurement (`0x032F`, payload `[start 1/0, kind]`) | Verified for heart rate (kind 0) and SpO2 (kind 2). No builder or tool command yet. Details below |
| Standard Heart Rate service (`180D`, characteristic `2A37`) | Verified: about one notification a second while subscribed, with no RR intervals, so it cannot supply HRV. Not used by the code yet |

Update this table as captures confirm or contradict it. Best external reference: `docs/PROTOCOL.md` in `PopShark57/SmartRingWatcher` (no licence: read, don't copy).

### Monitoring and on-demand measurement on this ring (R99, firmware 2.32)

- **Scheduled readings.** `0x010C [01, minutes]` is acknowledged with `00`; readings then arrive at that spacing, aligned to the clock. `[00, 0A]` stops them, and re-enabling resumes them at the next boundary. Before any setting was sent the ring measured hourly, presumably its default.
- **One reading, two records.** Each scheduled reading produces a heart-rate record and a combined record, so this one setting drives heart rate, SpO2, respiration, HRV and CVRR. Heart rate therefore appears twice: typed heart-rate data and Apple Health use the heart-rate record only, and the combined record supplies SpO2, respiration, HRV and CVRR.
- **SpO2 monitor.** With scheduled readings off and `0x0126 [01 1E]` set, no SpO2 reading was logged at the next half-hour boundary, and none beyond the scheduled ones appeared over the following hours. The owner's ring was left with this setting enabled; the ring cannot report it and the app never sends it.
- **On-demand measurement.** `0x032F [01, kind]` is acknowledged with `00`. Heart rate (kind 0) sends live values on `0x0601` about once a second and finishes after about 27 s; SpO2 (kind 2) sends nothing for about a minute, then a short burst on `0x0602`, finishing at about 67 s. Completion is the event `0x040E [kind, 01]`, which must be acknowledged. Stop with `0x032F [00, kind]`.
- **On-demand results are logged to history.** Heart rate appears as a heart-rate record with mode 0, the same as a scheduled one. SpO2 appears as a combined record with SpO2 filled and every other field zero; treat those zeros as absent, never as readings. So history is the only path by which any reading reaches the store or Apple Health, live values are for display only, and nothing may assume readings fall on the interval grid.
- **Sync order.** Fetch and store every history kind first, then set the clock, then send the monitoring interval. The fetch log (see Decisions, Store) can only show which records predate a clock-set if each kind was stored just before it; a fetch that fails is never stored and leaves no log entry, so what to do about the clock-set when a kind fails is to be decided with the sync engine (#13). The clock goes before the interval because readings align to the ring's clock; the effect of that order is unverified.

### Data quirks seen on this ring (R99, firmware 2.32)

Seen in one night and the following day of data unless stated otherwise. Treat them as properties of this device and firmware, not of the protocol, and write code that tolerates them without requiring them.

- **Sleep stages** are stored in time order, and each started one second after the previous one ended. Treat a gap of up to about a second as contiguous; do not require exactly one second.
- **Sleep header:** the 8 totals bytes at offset 12 of the 20-byte header (after the `AF FA` marker, length, start and end) used the newer layout: `FF FF`, then REM, deep and light totals as u16 seconds. They equalled the sums of the stage durations.
- **No awake stages** were reported. A brief wake-up appeared as a gap of a few minutes between two sessions.
- **Activity** records were 30-minute buckets; **heart-rate** records carried mode 0 for both scheduled and on-demand readings, so the mode byte does not tell them apart.
- **Combined record:** the blood-pressure fields are filled in, but a ring cannot measure blood pressure; they are vendor estimates. The temperature fields held the same value in every record and were not a body temperature; treat as absent. Body fat and glucose were always zero. The last two bytes, documented as reserved, varied between records and are not understood.
- **Setting validation is untested here.** Only values believed valid have been sent to this ring: monitoring at 1, 5 and 10 minutes and off, and the SpO2 monitor at 30 minutes. Another ring of the same family is reported to acknowledge any interval byte, including nonsense values, so the app must only ever send values from a known list.

### Known gaps in RingTransport

Built for the command-line tool; these must be closed before the app relies on it. Each should have an issue.

- Deadlines are in place (connect covers scan through subscribe, writes, Bluetooth readiness, and each conversation as a whole), but the CoreBluetooth paths are covered by reasoning only, not tests, and have not been exercised at the edge of range. Reproducing the original hang on hardware is still open. The link owns the write deadline (`RingLink.send` contract); `RingSession` keeps a backstop that is always later than the link's `writeTimeout`.
- `persist` is deliberately outside the conversation deadline: abandoning it would make "durable before acknowledged" unknowable. It is therefore still an unbounded wait that holds the session's turn.
- The 60 s conversation cap has not been tested against a large backlog and is longer than an iOS background window; the sync engine needs its own budget.
- A conversation can overrun its deadline by up to about `writeTimeout + replyTimeout` for a final write (the write backstop, derived from the link's own `writeTimeout`).
- A late reply to a timed-out request can be accepted by a retry of the same kind of request; for history it fails safe through the checksum.
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
- **Store (`RingStore`):** raw transfers are de-duplicated by a SHA-256 of ring, kind and data (header, fetch time and zone excluded; the first row wins). Kinds are stored under frozen codes that never change, not `HistoryKind.name`. The caller supplies the database location, which must be a dedicated directory (the app will use an App Group container). File protection is `completeUntilFirstUserAuthentication`. It is a WAL-mode `DatabasePool`. Transfers are whole buffers, so inserting one deletes every earlier transfer for the same ring and kind whose data is a strict prefix of it, and skips one that is a prefix of a stored transfer, in one transaction. Observations (`transfer_observation`) are the fetch log: every `insert` writes one row per fetch per kind (ring, kind, data length, header, fetch time, zone, content hash), whatever the outcome and whether or not the transfer row survives, read with `observations`; the same data at the same instant is logged once. This is what tells ingestion which bytes existed before each sync, including that a buffer was unchanged at a sync. A transfer row's own fetch time and zone describe only the fetch that produced that row, never the earlier records inside its data, so record times are never decoded from them. **Typed tables** (migration v3: `heart_rate_reading`, `activity_interval`, `sleep_session`, `sleep_stage`) hold real instants in whole milliseconds, with the `utc_offset_seconds` used and a `time_source`; `sleep_stage.kind` and `time_source` are frozen text codes like history kinds. They have no reference to `history_transfer` and no provenance columns: pruning deletes transfer rows and a transfer's hash changes whenever its buffer grows (a plain foreign key would make the prune fail, a cascade would silently delete typed rows), so typed rows are always re-derived from the surviving transfers. `ingest(ringID:kind:)` (activity, sleep, heart rate; combined is rejected) decodes every stored transfer of that ring and kind and makes the table match the desired set in one transaction (insert, update, delete, leave unchanged rows alone so ids stay stable); running it twice changes nothing, and `rebuild(ringID:)` is delete-then-ingest. **Identity:** a record's identity is its bytes plus its occurrence number (how many identical records come before it in the same transfer), because after the clock goes back the ring repeats wall-clock times and a repeated reading with the same value is byte for byte the same. The same identity in several transfers is resolved once, from the occurrence with the earliest first sighting, so a record in two transfers neither of which extends the other is one row. Known limit (#32): if a rolled-over buffer dropped an earlier identical record, the numbering shifts and a record can be matched to the wrong twin; fixing it needs the transfers aligned. Sleep sessions with one ring-local start are one session the ring rewrote: plausibility and future checks run on every copy first (so a garbage rewrite cannot supersede a good copy), then only the copy latest in the buffer is kept and the others are counted in `superseded`, even when the copies resolve to different instants. Rejected and counted in `rejectedImplausible`: records whose ring-local time is before 2020-01-01 (the ring's clock was never set); an activity bucket that starts after it ends or is longer than 24 hours (zero length is accepted); a sleep session that starts after it ends, is longer than 48 hours, or has a stage that starts outside the session or ends more than 60 seconds after it (in real nights every last stage ended at the session end or one second before it; zero-length sessions are accepted); a heart-rate reading of 0 or 255 bpm. Separately in `rejectedFuture`: a record any of whose times (including stage ends) is more than 24 hours after the fetch that first showed it, which also catches bytes such as `FF FF FF FF`. **Collisions** (`IngestionResult.collisions`) mean a distinct record was dropped: two records that still resolve to one unique key (two readings in one ring second, an ambiguous repeated hour) keep the one later in the buffer and count the other. Each typed row stores its ring-local time (`ring_local_seconds`, raw seconds since 2000-01-01; the start for activity and sleep). Ring-local time is NOT unique (the hour repeated after a clock goes back, two readings in one ring second), and row ids and resolved instants are not stable (both change when resolution improves), so the exact key for export (#12) and the Apple Health writer (#14) is to be decided there, likely ring, kind, ring-local time, record content and occurrence; they keep their own sync state. **Time resolution** (`TimeResolver`; clock-sets ordered by instant, then the order recorded; tolerance 120 s): a record's bytes are first seen at the earliest observation (a prefix of its transfer) that covers it and last absent from the latest earlier observation that does not. Candidates are the latest clock-set at or before the last-absent fetch plus every clock-set between it and the first sighting (every clock-set before the first sighting when there is no last-absent fetch). For sleep the candidates are always every clock-set before the first sighting, never narrowed by the last-absent fetch: a session is stamped during the night and written later, so the clock in force at its end can be older than the latest clock-set at or before that fetch, and the period test narrows them. No candidates: `assumed`, the first-seeing fetch's zone at that fetch's instant (one offset for the whole sighting, since the ring's clock does not follow daylight saving). All candidates share one offset (every sync where it did not change, and a retried clock-set): `resolved`, provided the record's proposed instant under that offset is not earlier than the earliest candidate's instant minus 120 s and not later than the first sighting plus 120 s (a record older than every logged clock-set, such as history left by the vendor's app or after a reinstall, is not confirmed by it); otherwise the fit test below decides, which with one offset gives `resolved` or `assumed`. Otherwise each candidate is tested: its proposed instant (record time minus offset) must lie in its own period and not after the first sighting (both widened by 120 s), and, for heart rate and activity only, not before the last-absent fetch (widened), because a sleep session is written some time after it ends. Fitting candidates are grouped by offset: one distinct offset is `resolved`, several are `ambiguous` (the latest fitting is used), none are `assumed` (the latest candidate). `assumed` means "not confirmed by the logs", not "probably wrong". `ambiguous` records use the latest fitting offset; using buffer write order to settle them is tracked in #36. The repeat fetches in the log are what make this work, since the app fetches before it sets the clock. `resolved` assumes the clock-set log is complete and that this app is the only thing that sets the ring's clock; another app setting it, or a clock-set the ring applied but the app failed to log, breaks it. Logging every clock-set, including one whose outcome is unknown, is the sync engine's job (#13). Activity buckets are checked at their start as well as their end (the start is the row's instant and key). **Before the log:** a record whose window reaches back before the first logged clock-set (a sleep session, one with no last-absent fetch, or one whose last-absent fetch is earlier than the first clock-set) may have been stamped under the ring's unknown earlier clock (the vendor's app set it, or the app was reinstalled); if it would be `resolved` but its instant is earlier than the first clock-set plus 26 hours (the widest gap between two UTC offsets) plus 120 s, it is `ambiguous` with the same offset instead. **Spanning an offset change:** a sleep session is resolved from its end, and it is `ambiguous` if it spans a change of offset: for a clock-set `k` whose offset differs from the one before it, `start - offset(k-1) - 120 s < instant(k) < resolved end`, where `start` is the session's ring-local start (stamped under the clock before the change, so it is decoded with that clock's offset, not the end's, which would miss a change early in the session when the clock goes back). The rule assumes the ring stamps a session's end with the clock in force at that moment (unverified, #32). Known limit: when the offset changes during a session, a stage stamped under the other clock can fall outside the session's ring-local start and end, and the whole session is then rejected as implausible (counted, raw data kept) instead of being marked `ambiguous`; settling such sessions belongs with #36. Activity is not treated this way (a bucket is 30 minutes) and how the ring stamps the activity bucket that is open when a sync changes the offset is unknown. An `assumed` result is never upgraded or downgraded by these two rules, and a fallback zone identifier this system does not know silently becomes GMT. **Unverified assumptions:** the lower bound from the last-absent fetch assumes heart-rate and activity records are written within about two minutes of their reference time; how long after a bucket ends the ring writes it, and whether a scheduled reading is written only after its SpO2 part, are not measured. A clock-set in the middle of any other record is not handled. Decoding and time resolution here are specific to the YCBT family and move behind the ring abstraction when a second family arrives. Invariant: every observation's data is a prefix of at least one stored transfer for its ring and kind, which is how its bytes are recovered from a hash and a length; #32 must preserve this or migrate observations to an explicit transfer-and-range form. The fetch log is the authoritative record of fetch times, zones and headers; it is complete only from schema v2 on, since v1 kept no trace of repeat fetches of identical data. Known follow-ups:
  - The sleep queries (`SleepQueries.swift`) are async; the remaining synchronous functions (registration, insert, ingestion, the clock-set log) get async variants with the sync engine (#13).
  - `ingest` and `rebuild` do all decoding, hashing and resolution inside one write transaction, which holds the write lock for as long as that takes. Derive in a read and apply in a short write before the app links the store (#11, #13).
  - A fetch is timestamped when it finishes, not when it was requested, which can erode the 120-second bound at a sync that changes the offset (#13).
  - The sleep decoder silently drops stages with an unknown kind or zero duration, so they are neither stored nor counted; tracked in #35.
  - App Group sharing configuration (suspension notifications, a read-only opener for a widget) is needed before the app links the store (#13).
  - Once the ring's buffer is full and drops its oldest records, no transfer is a prefix of the next, so a full copy is stored on every sync; overlap-aware merging is needed once that is seen on hardware (#32).
  - Pruning assumes the ring never rewrites a record in place. Activity is now observed to list completed buckets only, and all four kinds have only ever grown at the end; an open sleep session is the remaining unverified case. It fails safe by keeping both rows, and ingestion then keeps the later copy and counts the earlier as superseded (#32). Unpruned rows also make the prefix scan on each insert grow.
- **Minimum iOS version:** iOS 26.
- **Concurrency defaults:** the app target defaults to main-actor isolation (`SWIFT_DEFAULT_ACTOR_ISOLATION = MainActor`); the RingKit package does not.
- **Bundle identifier:** `com.nathantalley.digit`.
- **Vendor estimates:** raw ring records are stored exactly as received, but values the ring cannot measure (blood pressure, and the temperature field on the R99) are never exposed as measurements in derived tables, the app, exports of typed data, or Apple Health.
- **One monitoring interval:** for YCBT rings like the R99 the app offers a single setting (off, 1, 5, 10, 15, 20, 30 or 60 minutes; default 10) that controls heart rate, SpO2, respiration and HRV together. This is a capability of the ring, not a rule for every device, and there is no separate SpO2 setting. The ring cannot report its settings, so the choice is stored per paired ring and re-sent to that ring on every sync. 15, 20, 30 and 60 minutes are unverified until each has been sent once.
- **Sleep nights (`SleepQueries.swift`, read-only, async):** a *night* is a maximal run of a ring's sessions, ordered by start, in which each starts no more than 60 minutes (inclusive; `sleepNightMaximumGapSeconds`) after the latest end seen so far in the night; overlapping, contained or touching sessions join. Grouping uses stored instants only and always covers all of the ring's sessions, so a range query never cuts a night. A night belongs to the *day* on which it ends on the ring's own clock: the night's end (the latest end among its sessions) plus the `utc_offset_seconds` of the session with that end (on a tie, the later in start order), read as a Gregorian date in GMT (`SleepDay`; never the device's time zone). Of a day's nights the one with the most time asleep (deep, light, rem, nap stages) is the *main sleep*, the later-ending one on a tie, and the rest are naps; this is unrelated to the `nap` stage kind. A night carries the least certain `time_source` of its sessions (resolved, ambiguous, assumed) and no session is left out because of it. Interruptions are the stretches inside the night covered by no session (not gaps between stages); overlapping or contained sessions add none.

## Open decisions

- **Apple Developer account:** the owner is moving to a paid developer team. Until that team is available in Xcode, do not run the app on a device signed with the free Personal Team, so the bundle identifier is not registered to the wrong team. No signing team is committed to the project.
