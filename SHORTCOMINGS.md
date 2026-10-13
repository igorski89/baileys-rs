# Shortcomings

Known gaps and deliberate deviations from upstream Baileys, kept here so they can be addressed later. `PORT_STATUS.md` is the chronological log; this file is the current list. Add an entry whenever an increment introduces a deviation, and remove it when it is resolved.

Entries marked *(unverified)* come from the earlier log and have not been re-checked against the code.

## Not yet ported

- **Live validation.** Nothing has run against a real account yet. `examples/live.rs` is ready (pairing-code login, event log, one outgoing text).
- **Custom USync protocol extensions and external device-cache adapters.** Only the built-in protocols and the in-memory device cache exist.
- **History media handling and the remote-size policy** for media downloads.
- **Image and audio format audit.** Decoder and encoder output has not been compared with upstream's sharp/jimp and audio-decode across formats.
- **Aggregate pre-key upload timeout.** Upstream bounds the whole upload at 30 seconds; here only the three backoff retries are bounded.
- **Privacy-token index scheduling** (tctoken prune scheduling and automatic AB-prop fetching). *(unverified)*
- **Automatic reconnection.** As upstream, the socket does not reconnect by itself; the example shows the loop.
- **Public API surface audit.** No source-to-port coverage audit of every upstream export, and no documented list of intentional API differences beyond this file.

## Deviations from upstream behaviour

### Link previews
- Opt-in through `Config::link_previews.enabled`. Upstream generates previews whenever its optional `link-preview-js` dependency is installed.
- Page parsing is a reimplementation of `link-preview-js` on the `scraper` crate (html5ever), pinned by 126 upstream cases. A new upstream release of that package would need the fixtures regenerated; the generator needs `link-preview-js` installed in `.cache/fixture-tools`.
- Relative image URLs are resolved with WHATWG rules, not Node's legacy `url.resolve`.
- Redirects follow the HTTP client's limit of ten, not fetch's twenty.
- Custom request headers and proxy settings are not passed to the page fetch.
- No test fetches a real page over TLS.

### Newsletters and MEX
- The reach-out timelock state is its own event (`ReachoutTimelockUpdated`), not part of the connection event.
- Newsletter metadata and untyped MEX results are returned as JSON values.
- `newsletter_update_picture` is not replayed against upstream fixtures.
- Identifiers in MEX notification payloads are taken as strings.
- A plaintext newsletter message whose final string field is truncated is rejected; protobufjs accepts it.

### Business
- Product prices are unsigned integers, not arbitrary JavaScript numbers.
- A catalog add or edit result without a product is a typed error.

### Groups and communities
- V4 invite: the account embedded in the stub message is serialized in the native field order.
- V4 invite: an invite without a group JID fails after the request has been sent, and the chatless stub message stays in the event buffer until the next flush. Upstream's outcome is not pinned.
- V4 invite: an invite without an expiration is a typed error raised before the request.

### App state and chats
- Typed chat modifications cannot express an empty or unknown modification object, or NaN timestamps.
- The app-state patch upload is not wrapped in a storage transaction; the new version is written after the server accepts it.

### Keys and sessions
- Signed-prekey rotation and the key-bundle digest check have mock-server tests only (upstream never calls rotation itself).
- Concurrent external credential writers during an active connection are not supported.
- Rotating the signed pre-key discards the previous one, as upstream does; there is no signed-prekey history.

### Relay
- The asynchronous patch hook is awaited on the relay blocking worker (`Handle::block_on`); it must not wait on the relay it is patching.
- Per-recipient patches suppress reporting tokens. *(unverified)*

### WAM
- The event tables are generated from one upstream revision (`scripts/generate-wam-constants.mjs`) and must be regenerated when upstream updates them.

### Events
- Device-less JIDs in the device cache are stored with device zero where upstream stores an undefined device.
- Cache expiry is purged on access, not by idle timers. *(unverified)*

## Performance and robustness (deferred by decision: parity first)

- The event buffer converts events to JSON and back while buffering.
- SQLite runs with full synchronous writes and without WAL.
- Slow event subscribers lose events when the broadcast channel lags; the capacity is 256 by default.
- Receive and worker queues are unbounded.
- Storage calls hop through `spawn_blocking`.

## Test coverage gaps

- The encrypt, privacy-token and bad-ack notification handlers have mock-server tests but no upstream-generated fixtures.
- Fixture generators depend on Node's experimental `stripTypeScriptTypes` and on slicing upstream sources by text markers, so they are tied to upstream commit `0af2386292`.
