# Shortcomings

Known gaps and deliberate deviations from the TypeScript baileys-sqs (commit `6ef7d8b`). Add an entry whenever a change introduces a deviation, and remove it when it is resolved. Differences inside the library itself are listed in the root [SHORTCOMINGS.md](../SHORTCOMINGS.md).

## Not yet verified

- **Live run** is partial: run for an afternoon on two accounts beside the TypeScript connector, with matching output for the message kinds tested (see the root SHORTCOMINGS.md). Not yet exercised live: calls, view-once media, stickers, group administration and longer runs.
- **S3 uploads** are not exercised by any test (no S3 server in the test setup); the code follows the TypeScript connector's client configuration.
- **Docker Compose** has not been run. The image builds from the repository root (`docker build -f baileys-sqs/Dockerfile .`) and its binaries start.
- **Large messages need a raised queue limit.** A received video travels base64-encoded in one output message (16 MB of video is about 21 MB), so the queue server must accept it: ElasticMQ and RusticMQ (from `ee7053f`) with `pekko.http.server.parsing.max-content-length`, as mimi configures 500m; real SQS caps messages at 256 KB. A rejected message is logged and dropped; the TypeScript connector crashes on it (an unhandled EPIPE in the AWS SDK upload).
- **Listener and sender** are built but not tested.

## Output messages

- Protobuf values in event payloads follow the TypeScript connector's `JSON.stringify` (see baileys-rs's "Upstream JSON conversions" for the remaining gaps): enum names and base64 bytes inside decoded messages, a number for a received message's `messageTimestamp` and a decimal string for our own sends'.
- A connection that fails before a close code arrives is reported without `error` and `code`.
- Events arrive from baileys-rs one at a time, so the connector cannot see which events upstream's `ev.process` would have delivered together; each event name still produces one output message.

## Session and connection

- `SESSION_DIR` is read as in the TypeScript connector, but the session in it is one SQLite file, `auth.sqlite3` (with `-wal`/`-shm` beside it while running), not `creds.json` and key files; a TypeScript session found there is imported on first start. Unset, it does not fall back to `./auth` as in TypeScript: the session is then kept in memory only (also `SESSION_DIR=:memory:`), with a warning at start, so a deployment relying on that default must set `SESSION_DIR=./auth`. With it, the session and the disappearing-messages cache are two separate in-memory databases rather than tables of one file; neither outlives the process.

- The session is a SQLite file, not Baileys' multi-file JSON, and the disappearing-messages cache is a table in the same file rather than `ephemeral-cache.json`. A TypeScript session folder and its cache file are imported once; going back to the TypeScript connector with the same folder needs the original files, which are left in place but no longer updated.
- There is no separate `creds.update` save step or signal-key cache: the store writes through.
- `from`, `me_lid` and `/health`'s `connected` read the account from the store, as `sock.user` reads upstream's credentials.

## Commands

- A command without `to` fails with JavaScript's TypeError text (`Cannot read properties of undefined (reading 'includes')`), and an outgoing media fetch that exceeds ten seconds fails with Node's abort message, to keep `command_ack.error` the same.
- Content objects (`message`, `raw.body`) go through baileys-rs's `content_from_json`; dates in them are millisecond numbers, and see baileys-rs's shortcomings for the shapes it covers.
- `vote_poll` exists only here. It takes the poll's secret from the caller (`message_secret`) or from a table in the session file (`baileys_sqs_polls`). That table learns every poll in `messages.upsert`, received or sent, but not polls that arrive only in a history sync. The vote is encrypted with the creator's and our own JID in the chat's addressing mode, the same pair a phone uses; the ack reports both so a consumer can decrypt it. The key of our own sent poll carries no addressing mode, so for our own polls the mode comes from the chat: a group's metadata (one query per vote), or for a 1:1 chat whether the contact has a LID. A key the caller supplies without `addressingMode` takes the mode from the form of the author's JID. Verified live in a 1:1 LID chat and a LID group, on received and on own polls, with several options selected, withdrawn with an empty `selected_options`, and on polls known only from a history sync, given their `message_secret` and key (received and own). The poll's `selectableCount` is not checked (it is not cached), so a vote for more options than the poll allows is sent as given.
- `request_resend` exists only here: it calls the library's `request_placeholder_resend` (upstream's `requestPlaceholderResend`) for each key, which upstream calls only on its own, once, when a message arrives without content. The request matches upstream's stanza (`category="peer"`, `push_priority="high_force"`, `<meta appdata="default"/>`), but no live phone has answered it yet: tried 2026-10-22 on two seven-hour-old 1:1 LID messages, twice, with the phone in the foreground.
- Two TypeScript quirks are kept: a command that arrives before the first socket exists is acknowledged with `ok: true` and null ids, and a `messages.upsert` spanning several chats is grouped by the first message's chat.

## Tooling

- Logging uses `tracing` with snake_case field names instead of pino-pretty. Every message published to OUTPUT_QUEUE is also logged at debug level, with its event, size in bytes, FIFO group and deduplication ids, and SQS message id. The TypeScript connector logs only failures.
- The listener formats times as `M/D/YYYY, h:mm:ss AM` in local time (the en-US form of `toLocaleString`), and draws QR codes with the `qrcode` crate rather than qrcode-terminal.
- There is no hot-reload compose file (`docker-compose.dev.yml`, `Dockerfile.dev`); use `cargo run` or `cargo watch`.
- `elasticmq.conf` writes empty queue blocks as `{}`: RusticMQ's HOCON parser rejects `{ }` with a space inside, which ElasticMQ accepts.
