# Shortcomings

Known gaps and deliberate deviations from the TypeScript baileys-sqs (commit `8ad19d0`). Add an entry whenever a change introduces a deviation, and remove it when it is resolved. Differences inside baileys-rs itself are listed in its own `SHORTCOMINGS.md`.

## Not yet verified

- **Live run.** Nothing has run against a paired WhatsApp account; the tests fake the socket. A start with `--session-db :memory:` reached WhatsApp and emitted the pairing QR code, and nothing beyond that has been exercised.
- **S3 uploads** are not exercised by any test (no S3 server in the test setup); the code follows the TypeScript connector's client configuration.
- **Docker images** have not been built; the Dockerfile and compose file are untested.
- **Listener and sender** are built but not tested.

## Output messages

- Protobuf values in event payloads use baileys-rs's protobuf JSON. Bytes are base64 strings, as the TypeScript connector sends them (poll votes' `encPayload` and `encIv` among them), but enums are numbers and 64-bit integers are decimal strings, so `messageTimestamp` is `"1700000000"` (also in `meta.messages[].timestamp`). The TypeScript connector's `JSON.stringify` writes enum names for decoded protobuf messages and a number for `messageTimestamp`, and a received message's own envelope bytes (outside `message`) as `{"type":"Buffer","data":[...]}`.
- A disconnect's `error` is upstream's `DisconnectReason` name for the close code (`"restartRequired"`, `"loggedOut"`, …), not the Boom message text (`"Stream Errored (restart required)"`). A connection that fails before a close code arrives is reported without `error` and `code`.
- Events arrive from baileys-rs one at a time, so the connector cannot see which events upstream's `ev.process` would have delivered together; each event name still produces one output message.

## Session and connection

- `SESSION_DB` (a file path or `:memory:`, also `--session-db`) replaces `SESSION_DIR`. Deployments must rename the variable, pointing it at `auth.sqlite3` in the old folder; with only `SESSION_DIR` set the connector exits with that hint. With `:memory:`, the session and the disappearing-messages cache are two separate in-memory databases rather than tables of one file; neither outlives the process.

- The session is a SQLite file, not Baileys' multi-file JSON, and the disappearing-messages cache is a table in the same file rather than `ephemeral-cache.json`. A TypeScript session folder and its cache file are imported once; going back to the TypeScript connector with the same folder needs the original files, which are left in place but no longer updated.
- There is no separate `creds.update` save step or signal-key cache: the store writes through.
- `from`, `me_lid` and `/health`'s `connected` read the account from the store, as `sock.user` reads upstream's credentials.

## Commands

- A command without `to` fails with JavaScript's TypeError text (`Cannot read properties of undefined (reading 'includes')`), and an outgoing media fetch that exceeds ten seconds fails with Node's abort message, to keep `command_ack.error` the same.
- Content objects (`message`, `raw.body`) go through baileys-rs's `content_from_json`; dates in them are millisecond numbers, and see baileys-rs's shortcomings for the shapes it covers.
- Two TypeScript quirks are kept: a command that arrives before the first socket exists is acknowledged with `ok: true` and null ids, and a `messages.upsert` spanning several chats is grouped by the first message's chat.

## Tooling

- Logging uses `tracing` with snake_case field names instead of pino-pretty.
- The listener formats times as `M/D/YYYY, h:mm:ss AM` in local time (the en-US form of `toLocaleString`), and draws QR codes with the `qrcode` crate rather than qrcode-terminal.
- There is no hot-reload compose file (`docker-compose.dev.yml`, `Dockerfile.dev`); use `cargo run` or `cargo watch`.
- The images build from the parent directory, because `baileys-rs` is a path dependency.
- `elasticmq.conf` writes empty queue blocks as `{}`: RusticMQ's HOCON parser rejects `{ }` with a space inside, which ElasticMQ accepts.
