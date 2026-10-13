# Shortcomings

Known gaps and deliberate deviations from upstream Baileys, kept here so they can be addressed later. `PORT_STATUS.md` is the chronological log; this file is the current list. Add an entry whenever an increment introduces a deviation, and remove it when it is resolved.

## Not yet ported

- **Live validation.** Nothing has run against a real account yet. `examples/live.rs` is ready (pairing-code login, event log, one outgoing text).
- **Type-level audit.** `scripts/audit-socket-api.mjs` (150 socket methods) and `scripts/audit-utils-api.mjs` (157 utility exports covered, 38 without a counterpart on purpose, none missing) match by identifier only. Upstream's exported types (`src/Types`) were compared by hand for the socket configuration and the event map only.
- **Pluggable caches and agents.** Upstream accepts caller-supplied stores for the retry counters, placeholder resends and uploaded media, and proxy agents for the WebSocket and for fetches. The port takes a caller-supplied HTTP client for media, a device-cache store and a call-offer cache; the rest is built in.

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

### Media
- AVIF images and JPEG-compressed (YCbCr) TIFF images cannot be decoded for thumbnails or profile pictures; sharp handles both.
- Thumbnails and profile pictures are not byte-identical to sharp's: resampling and JPEG encoding differ. On the audit picture the mean channel difference is about 4 to 7 out of 255 (1 for greyscale).
- Waveforms are produced for AAC, M4A and non-16-bit WAV, where upstream's decoder yields none. Lossy formats may differ by one level per bin.
- The remote download limit counts received bytes exactly; upstream counts each chunk twice, so its effective limit is lower and depends on chunk sizes.

### USync and device cache
- A USync query assembled through its public fields rather than the builder methods lists built-in protocols before custom ones.
- The external device store's calls cannot fail; upstream propagates an exception thrown by a cache. It stores the encoded JID with each device, which upstream recomputes on every read.

### Configuration
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

### WAM
- The event tables are generated from one upstream revision (`scripts/generate-wam-constants.mjs`) and must be regenerated when upstream updates them.

## Performance and robustness (deferred by decision: parity first)

- The event buffer converts events to JSON and back while buffering.
- SQLite runs with full synchronous writes and without WAL.
- Slow event subscribers lose events when the broadcast channel lags; the capacity is 256 by default.
- Receive and worker queues are unbounded.
- Storage calls hop through `spawn_blocking`.

## Test coverage gaps

- The encrypt, privacy-token and bad-ack notification handlers have mock-server tests but no upstream-generated fixtures.
- Fixture generators depend on Node's experimental `stripTypeScriptTypes` and on slicing upstream sources by text markers, so they are tied to upstream commit `0af2386292`.
