# Shortcomings

Known gaps and deliberate deviations from upstream Baileys, kept here so they can be addressed later. `PORT_STATUS.md` is the chronological log; this file is the current list. Add an entry whenever an increment introduces a deviation, and remove it when it is resolved.

## Not yet ported

- **Live validation** is partial. Through the baileys-sqs connector (2026-10-20), on two real accounts beside the TypeScript connector: pairing, reconnects, history sync, text, media (including a 16 MB video), reactions, edits, deletes, polls, groups, disappearing messages and offline delivery work, and the event JSON matches upstream's apart from the deviations below. Not yet exercised live: calls, view-once and stickers, group administration, newsletters, business and community features, and anything run for longer than an afternoon.
- **Type-level audit.** `scripts/audit-socket-api.mjs` (150 socket methods) and `scripts/audit-utils-api.mjs` (157 utility exports covered, 38 without a counterpart on purpose, none missing) match by identifier only. Upstream's exported types (`src/Types`) were compared by hand for the socket configuration and the event map only.

Not gaps, but worth knowing: like upstream, the socket does not reconnect by itself (`examples/live.rs` shows the loop), and EXIF orientation is ignored when making thumbnails (sharp is used without `rotate()` upstream too).

## Deviations from upstream behaviour

### Link previews
- Opt-in through `Config::link_previews.enabled`. Upstream generates previews whenever its optional `link-preview-js` dependency is installed.
- Page parsing is a reimplementation of `link-preview-js` on the `scraper` crate (html5ever), pinned by 126 upstream cases. A new upstream release of that package would need the fixtures regenerated; the generator needs `link-preview-js` installed in `.cache/fixture-tools`.
- Relative image URLs are resolved with WHATWG rules, not Node's legacy `url.resolve`.
- Request headers and proxy settings come from the caller-supplied `Config::media_http` client, not from per-request fetch options.
- No test fetches a real page over TLS.

### Newsletters and MEX
- The reach-out timelock state is its own event (`ReachoutTimelockUpdated`), not part of the connection event.
- Newsletter metadata and untyped MEX results are returned as JSON values.
- `newsletter_update_picture` is tested against a mock server (request shape, 640×640 JPEG); it cannot be replayed byte for byte because the JPEG encoder differs from sharp's.
- Identifiers in MEX notification payloads are taken as strings.
- A plaintext newsletter message whose final string field is truncated is rejected; protobufjs accepts it.

### Business
- Product prices are unsigned integers, not arbitrary JavaScript numbers.
- A catalog add or edit result without a product is a typed error.

### Groups and communities
- V4 invite: the account embedded in the stub message is serialized in the native field order.
- V4 invite: an invite without an expiration is a typed error raised before the request.

### App state and chats
- Typed chat modifications cannot express an empty or unknown modification object, or NaN timestamps.

### Keys and sessions
- Privacy tokens are pruned once a day when coming online, as upstream does, but the index of tracked JIDs is written with each token rather than on a debounce timer.
- Signed-prekey rotation and the key-bundle digest check have mock-server tests only (upstream never calls rotation itself).
- Concurrent external credential writers during an active connection are not supported.
- Rotating the signed pre-key discards the previous one, as upstream does; there is no signed-prekey history.

### Relay
- A member label cut inside an astral character drops that character; upstream keeps half of it and sends it as invalid UTF-8, which a Rust string cannot hold.
- The asynchronous patch hook is awaited on the relay blocking worker (`Handle::block_on`); it must not wait on the relay it is patching.

### Poll votes (an addition)
- Upstream can decrypt votes but not send them. `Content::PollVote` builds an encrypted `pollUpdateMessage` (`encrypt_poll_vote`, the inverse of `decrypt_poll_vote`), and its stanza is copied from a phone's vote captured live: `decrypt-fail="hide"` on each `enc`, `<meta polltype="vote" content_type="add_on"/>`, and an empty `metadata` in the message. Without the meta node the server acked the stanza but did not deliver it. `relay_message` adds the meta node to every `pollUpdateMessage`, so a retry resend carries it too. A retry's `enc` node also carries `decrypt-fail` whenever the first send's does, which covers reactions and pins as well. Upstream omits it on retries; this follows the upstream PR WhiskeySockets/Baileys#2870. Both JIDs must be deviceless, and a JID with a device fails with a 400 error. The caller supplies the poll's secret and both JIDs (deviceless, in the chat's addressing mode: LIDs in a LID chat). Verified live in a 1:1 chat and in a LID-addressed group, voting on someone else's poll and on our own: the other side decrypts each vote, and both phones count it.

### History sync (additions)
- `socket::Config::full_history()` is the default configuration with a desktop browser (`Browser::windows("Desktop")`, sent as the WIN_HYBRID sub-platform), `sync_full_history` and a history policy that also processes the FULL (type 2) sync, which upstream's default skips. Full history arrives only on a newly linked device's first connection. Not macOS: on 2026-10-28 the server reset the connection right after the handshake, before any QR code, for `Browser::macos("Desktop")` with `sync_full_history` (the DARWIN sub-platform) in 6 of 7 tries with upstream and every try with this port; the registration payloads are byte-identical. Windows Desktop received a QR code every time in both. Not yet run live: no FULL sync has been seen, and nothing is known about its size or timing.
- `Socket::fetch_message_history_set` sends upstream's on-demand request and waits, with a required time limit, for the type-6 `MessagingHistorySet` answering it (`wait_for_history_set` does the waiting alone). Upstream emits the answer and leaves the matching to the caller. `history::Set::answers` matches a set whose `peer_data_request_session_id` is the request's message id, or one with messages from the requested chat. Neither rule has been checked live, and the phone has never answered a peer data request in our tests (see the placeholder resends in `~/Work/live-test/FINDINGS.md`).
- There is no completion signal for the FULL sync. As upstream, `MessagingHistoryStatus` reports INITIAL_BOOTSTRAP complete and RECENT complete (progress 100) or paused (120 s without a chunk).

### Media
- AVIF images and JPEG-compressed (YCbCr) TIFF images cannot be decoded for thumbnails or profile pictures; sharp handles both.
- Thumbnails and profile pictures are not byte-identical to sharp's: resampling and JPEG encoding differ. On the audit picture the mean channel difference is about 4 to 7 out of 255 (1 for greyscale).
- Waveforms are produced for AAC, M4A and non-16-bit WAV, where upstream's decoder yields none. Lossy formats may differ by one level per bin.
- The remote download limit counts received bytes exactly; upstream counts each chunk twice, so its effective limit is lower and depends on chunk sizes.

### USync and device cache
- A USync query assembled through its public fields rather than the builder methods lists built-in protocols before custom ones.
- The external device store holds the encoded JID with each device; upstream recomputes it on every read.

### Configuration
- Proxies are given as a URL in `Config::proxy` (HTTP CONNECT or SOCKS5, for the WebSocket) and through the caller's `media_http` client (for media and link previews); upstream takes agent objects. `socks5://` and `socks5h://` both let the proxy resolve the host name. An HTTPS proxy (TLS to the proxy itself) is not supported for the WebSocket, and no test crosses a real proxy.
- The caches a caller can supply are typed (`cache::Cache<u64>` for retry counters, `Cache<Option<Envelope>>` for placeholder resends, `Cache<Vec<u8>>` for prepared media, `devices::Store`, `calls::Cache`) where upstream has one untyped `CacheStore`. Their calls cannot fail, except the call-offer cache's.
- `Config::transaction` (upstream's `transactionOpts`) retries only the commits that end an app-state synchronization or patch upload; every other storage write is one SQLite transaction without retries.
- Cached group metadata is passed per send as a value; upstream takes one lookup callback in the configuration.

### Exported helpers
- `aggregate_event_responses` reads the decrypted responses the socket publishes. Upstream reads a runtime `eventResponse` property that its own events never set.
- The message update and aggregation helpers identify authors by the key's participant or chat; the alternate PN/LID key fields upstream prefers are not part of the protobuf key.
- `update_with_receipt` keeps stored device lists when the receipt's lists are empty.
- `media_extension` returns nothing where upstream throws (a message without a MIME type).
- `BinaryNode::dictionary` keeps insertion order and skips children without a name or value; JavaScript lists integer-like keys first.
- `Browser::appropriate` reads the kernel release from `uname -r` (or `ver` on Windows) and leaves it empty when that fails.
- The version lookups take a `reqwest::Client` instead of fetch options, and versions above the 32-bit range are treated as unparsable.

### Upstream JSON conversions
- `Event::upstream()` writes protobuf values as `JSON.stringify` writes upstream's, following which of them are protobufjs messages there: those use `toJSON` (base64 bytes, enum names, decimal-string longs), and the plain objects holding protobuf values (received envelopes, update and receipt entries, `chats.upsert`) use Buffer bytes, enum numbers and JavaScript-number longs. `EnvelopeMetadata::protobuf` marks the upserted messages upstream builds with `fromObject`. Checked against protobufjs for every schema field and against the TypeScript connector live. Known gaps: a required field holding its zero value is omitted (the native types cannot tell it from an absent one; protobufjs writes it), and a placeholder resend restored onto a cached message has its decoded `Long` timestamp as a string, not `{low, high, unsigned}`.
- A closed connection's `lastDisconnect.error` has upstream's Boom status and message (`ConnectionState::Closed::message`) but `data: null` where upstream keeps the stream-error stanza, and its `date` is the time of conversion. `creds.update` has an empty payload, and raw stanza events produce no entry.
- `content_from_json` takes dates (`event.startDate`, `timestamp`) as millisecond numbers, and accepts media as `{ url }` or a BufferJSON buffer; upstream cannot receive a buffer through `JSON.parse` at all, and streams have no JSON form.

### WAM
- The event tables are generated from one upstream revision (`scripts/generate-wam-constants.mjs`) and must be regenerated when upstream updates them.

## Performance and robustness (deferred by decision: parity first)

- The event buffer converts events to JSON and back while buffering.
- SQLite runs with write-ahead logging and full synchronous writes; `synchronous=NORMAL` would be faster but could lose the last commits (keys, sessions) on a power loss.
- Slow event subscribers lose events when the broadcast channel lags; the capacity is 256 by default.
- Receive and worker queues are unbounded.
- Storage calls hop through `spawn_blocking`.
- Several socket futures are tens of kilobytes (sending, receipts, notifications). The deepest are boxed after a debug-build stack overflow on a 2 MiB test thread, but there is little headroom; `-Zprint-type-sizes` shows the sizes.

## Test coverage gaps

- In the handler fixtures (`notification-handlers.json`) upstream's session refresh, pre-key upload and token issuance are stubs; the replay compares them with the requests the port puts on the wire, and always fails the key-bundle request, so a successful refresh is covered only by the session tests.
- Fixture generators depend on Node's experimental `stripTypeScriptTypes` and on slicing upstream sources by text markers, so they are tied to upstream commit `0af2386292`.
