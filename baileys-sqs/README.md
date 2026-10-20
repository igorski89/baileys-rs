# baileys-sqs (Rust)

A Rust port of [baileys-sqs](https://github.com/igorski89/baileys-sqs) (commit `6ef7d8b`), the WhatsApp connector that uses AWS SQS for bidirectional message queuing. It lives in the [baileys-rs](https://github.com/igorski89/baileys-rs) repository, as the `baileys-sqs` workspace member, and WhatsApp runs on that library instead of Baileys; the environment variables, commands and output messages are the same, so existing producers and consumers keep working. Known differences are listed in [SHORTCOMINGS.md](SHORTCOMINGS.md).

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Your System    │────▶│  Input Queue    │────▶│   WhatsApp      │
│                 │     │  (SQS)          │     │   (baileys-rs)  │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
┌─────────────────┐     ┌─────────────────┐             │
│  Your System    │◀────│  Output Queue   │◀────────────┘
│                 │     │  (SQS)          │
└─────────────────┘     └─────────────────┘
```

## Build

From the repository root (the crate depends on the library by path, and on [RusticMQ](https://github.com/igorski89/rusticmq) at a pinned commit for its tests):

```bash
cargo build --release -p baileys-sqs   # target/release/{baileys-sqs,listener,sender}
cargo test -p baileys-sqs              # runs against an in-process RusticMQ
```

A plain `cargo test` at the root tests the library and the connector together.

## Binaries

| TypeScript | Rust | Purpose |
|---|---|---|
| `index.ts` | `baileys-sqs` | The connector |
| `listener.ts` | `listener` | Renders QR codes and pretty-prints OUTPUT_QUEUE |
| `sender.ts` | `sender` | Queues `send_text` commands from the command line |

## Configuration

Copy `.env.example` to `.env` and adjust. The variables are those of the TypeScript connector, except `SESSION_DB`, which replaces `SESSION_DIR`:

- `INPUT_QUEUE`, `OUTPUT_QUEUE` (required). A queue whose URL contains `fifo` is treated as FIFO.
- `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, with `SQS_*` and `S3_*` overrides for each service.
- `SESSION_DB` (default `./auth.sqlite3`), or `--session-db` on the command line, which takes precedence. The session is this one SQLite file, which also holds the connector's disappearing-messages durations (table `baileys_sqs_ephemeral`). While the connector runs, SQLite's write-ahead log keeps `auth.sqlite3-wal` and `auth.sqlite3-shm` beside it; they are part of the database, so back up all three files, or stop the connector first. `:memory:` keeps the session in memory only: nothing is written to disk and every start pairs a new device, which suits tests and CI. When the file is first created, a session the TypeScript connector left in the same folder (`creds.json`, key files and `ephemeral-cache.json`) is imported, so an existing pairing keeps working; the original files are left in place. To move from the TypeScript connector, replace `SESSION_DIR=/session` with `SESSION_DB=/session/auth.sqlite3`; a connector started with only `SESSION_DIR` set exits with that hint rather than pairing anew.
- `USE_PAIRING_CODE=true` with `PHONE_NUMBER` links with a pairing code instead of the QR code.
- `WHATSAPP_VERSION` (a JSON array) pins the WhatsApp Web version; otherwise the latest is fetched.
- `S3_BUCKET` and the other `S3_*` variables upload incoming media to S3 and put a presigned URL in `_media.url`; without them media is inlined as base64.
- `PORT`, `HTTP_HOST`, `AUTH_TOKEN` enable the HTTP endpoint.
- `LOG_LEVEL` (pino level names) and `SOCKET_URL`. As with pino, the level covers the connector and the WhatsApp library; other crates log warnings and errors only. `RUST_LOG`, when set, replaces the filter.

The bucket is not created automatically.

## Commands

Send the JSON below to `INPUT_QUEUE`, or POST it to `http://<host>:<PORT>/commands` with `Authorization: Bearer <AUTH_TOKEN>`. `GET /health` returns `{"ok":true,"connected":<bool>}` without authentication.

| `type` | Fields |
|---|---|
| `send_text` | `text`, or a full content object in `message`; `options` |
| `send_media` | `media: {type, data_base64 \| url, mimetype?, filename?}`; `message` (its `text` becomes the caption); `options` |
| `send_presence` | `presence`: `composing`, `recording` or `paused` |
| `send_reaction` | `reaction` (empty string removes it), `message_key` |
| `send_read_receipt` | `message_key` or `message_keys` |
| `send_edit` | `text`, `message_key` (`fromMe` defaults to true) |
| `send_delete` | `message_key` (`fromMe` defaults to true) |
| `send_location` | `location: {latitude, longitude, name?, address?}` |
| `send_contact` | `contact` or `contacts`, each with a `vcard` |
| `send_poll` | `poll: {name, values, selectableCount?}`; `options` |
| `raw` | `body`: any Baileys `sendMessage` content object; `options` |

`to` is a phone number or a JID. `options` follows Baileys' send options (`quoted`, `ephemeralExpiration`, `messageId`, …). A command with a `correlation_id` gets a `command_ack` on `OUTPUT_QUEUE`:

```json
{"type":"command_ack","payload":{"correlation_id":"...","command_type":"send_text","to":"1234567890@s.whatsapp.net","from":"18005551234:1@s.whatsapp.net","me_lid":"123456789012345@lid","ok":true,"message_id":"3EB0..."}}
```

On failure `ok` is false and `error` holds the message. `send_poll` acks carry `message_secret` (base64), needed to decrypt votes later.

Outgoing sends to a chat with disappearing messages carry the chat's duration, learned from `chats.update` and from incoming messages and kept in the session database, unless `options.ephemeralExpiration` is set.

## Output

Every WhatsApp event is published as

```json
{"id":"messages.upsert-1700000000000-0.123","type":"baileys_event","event":"messages.upsert","meta":{...},"payload":{...}}
```

with Baileys' event names and payload shapes (`meta` summarises `messages.upsert`, `messages.update` and `presence.update`; incoming media is attached to each message as `_media`). Connection changes are `{"type":"connection","payload":{"status":"connected"}}` or `{"status":"disconnected","error":...,"code":...}`, QR codes `{"type":"qr","payload":{"qr":...}}`, and pairing codes `{"type":"pairing_code",...}`. The connector reconnects five seconds after every close except a logout (401).

For FIFO queues the group id is the chat (or the event type when there is none) and the deduplication id is a readable prefix plus the SHA-256 of the content, so a redelivered event is dropped within SQS's five-minute window. The `sender` uses a random deduplication id.

## Local development

Docker Compose starts RusticMQ (a Rust port of ElasticMQ, built from its GitHub repository with the same `elasticmq.conf`), MinIO, the connector, the listener and the sender. The images are built from the repository root so the library is in the build context. Run Compose from `baileys-sqs/`:

```bash
docker compose up --build
docker compose logs -f listener   # QR code and messages
docker compose run --rm sender
```

Without Docker:

```bash
# 1. Queue server, from a RusticMQ checkout
cargo run --release -- --config path/to/baileys-rs/baileys-sqs/elasticmq.conf

# 2. Environment
export AWS_REGION=elasticmq AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local
export AWS_ENDPOINT_URL=http://localhost:9324
export INPUT_QUEUE=http://localhost:9324/queue/input-queue
export OUTPUT_QUEUE=http://localhost:9324/queue/output-queue
export SESSION_DB=./auth_info_baileys/auth.sqlite3

# 3. Connector, listener and sender, each in its own terminal
cargo run --release --bin baileys-sqs
cargo run --release --bin listener
cargo run --release --bin sender -- "+1234567890" "Hello, World!"
```

WhatsApp blocks connections from many data-centre IPs; pair from a local machine first.

## License

MIT. Igor Ievsiukov <igor.ievsiukov@gmail.com>
