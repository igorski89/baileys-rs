# Port status

## Objective and source

Port baileys-sqs (TypeScript, commit `8ad19d0`: `index.ts`, `listener.ts`, `sender.ts`) to Rust on baileys-rs, keeping its environment variables, input commands and output messages, and test it against RusticMQ.

## Implemented

The connector (`src/bin/baileys-sqs.rs`, library modules one per section of `index.ts`): environment with the `SQS_*`/`S3_*` over `AWS_*` precedence (`config`), output queue with FIFO group and content-based deduplication ids (`output`), the disappearing-messages cache persisted in a table of the session database, importing `ephemeral-cache.json` once (`ephemeral`), incoming media to S3 with presigned URLs or base64 (`media`), the eleven commands with `command_ack` (`commands`), the HTTP endpoint (`http`), the input poll loop (`poll`), and the WhatsApp connection with pairing codes, event forwarding and reconnects (`app`). WhatsApp events are converted with baileys-rs's `Event::upstream()`, and content objects with `content_from_json` and `SendOptions::from_json`; both were added to baileys-rs for this port (baileys-rs commits `168de41` and `8dd6cd3`, the second writing payload bytes as base64). The session is the SQLite file in `SESSION_DB` (or `--session-db`), or memory only with `:memory:`; a TypeScript session in the file's folder is imported when the file is first created (baileys-rs commit `eeab3c8` added opening a store by file path). The connector installs rustls' ring provider at start, since the AWS SDK links aws-lc-rs as well and rustls then has no default. The listener and sender CLIs are ported as binaries. Docker and compose files use RusticMQ in place of ElasticMQ.

Tests: `tests/helpers.rs` replays the TypeScript helpers' own output (`scripts/generate-helper-fixtures.mjs` runs `getFifoGroupId`, `getDedupPrefix`, `canonicalStringify`, `computeDedupId`, `normalizeJid` and `extensionFromMimetype` from `index.ts`), and the deduplication ids match byte for byte. `tests/connector.rs` runs an in-process RusticMQ: the poll loop runs commands in order, acknowledges them, applies the cached expiration and deletes failed and malformed messages; media, reaction, receipt, edit and raw commands become the expected library calls; a FIFO output queue drops a redelivered event; the HTTP endpoint's 200/400/401/404/503 answers; event forwarding with `meta`, `_media` and ephemeral learning; events are queued without limit before handling, so a slow media upload cannot make the broadcast channel drop any (as `ev.process` queues upstream); and RusticMQ loads the compose `elasticmq.conf`. Validation: 20 tests passed; clippy with warnings denied and formatting checks passed.

## Next step

Run the connector against a live account (with RusticMQ, `USE_PAIRING_CODE=true` and `PHONE_NUMBER`), watch it with the listener, send with the sender, and fix what it finds. See SHORTCOMINGS.md for what is unverified.
