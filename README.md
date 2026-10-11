# baileys-rs

A Rust port in progress of [WhiskeySockets/Baileys](https://github.com/WhiskeySockets/Baileys), based on upstream commit `0af2386292907f7d9742d8d41f830d8c48208fa1` (7.0.0-rc14).

Currently provides the full generated WhatsApp protobuf model, JID helpers, binary stanza encoding/decoding, crypto primitives, the Noise handshake and encrypted framing, persistent authentication/key storage, connection/pairing protocol helpers, and an async WebSocket connection. This is not yet a usable WhatsApp messaging client: Signal sessions, synchronization, messaging, and the high-level socket APIs remain to be ported. See [PORT_STATUS.md](PORT_STATUS.md) for scope and validation.

```rust
use baileys_rs::binary::{BinaryNode, encode, decode};

let request = BinaryNode::new("iq")
    .attr("to", "s.whatsapp.net")
    .attr("type", "get")
    .attr("id", "ABC123");
let wire = encode(&request)?;
assert_eq!(decode(&wire)?, request);
# Ok::<(), baileys_rs::binary::Error>(())
```

Build and check:

```sh
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

Protobuf bindings and token constants are generated at build time. The build includes a vendored protobuf compiler, so a separate `protoc` installation is unnecessary. The schema and token data are included locally; building does not require the upstream checkout or Node.

Regenerate the binary parity fixtures from the upstream checkout using Node with TypeScript stripping support (verified on Node 26):

```sh
node scripts/generate-binary-fixtures.mjs ../Baileys
```

Crypto and Noise fixtures run upstream code using its pinned runtime dependencies. Install those into the ignored fixture-tool directory, then regenerate:

```sh
npm install --prefix .cache/fixture-tools --ignore-scripts --no-audit --no-fund libsignal@6.0.0 protobufjs@7.5.6 whatsapp-rust-bridge@0.5.4 async-mutex@0.5.0
node scripts/generate-crypto-fixtures.mjs ../Baileys
node scripts/generate-noise-fixtures.mjs ../Baileys
node scripts/generate-auth-fixtures.mjs ../Baileys
node scripts/generate-connection-fixtures.mjs ../Baileys
cargo test --test auth upstream_adapter_resumes_and_updates_rust_export -- --ignored
```

These fixtures use fixed synthetic keys and a synthetic certificate authority. They verify compatibility with upstream code, not a live WhatsApp server. The Rust runtime has no JavaScript or WASM bridge dependency.

`crypto` exposes AES-256 GCM (12-byte nonces, appended authentication tags), CTR, PKCS#7 CBC, IV-prefixed CBC, SHA-256, MD5, HMAC-SHA-256/512, HKDF-SHA-256, and pairing-code PBKDF2. `KeyPair` supports X25519 agreement and the deterministic Curve25519 signature format used by upstream's pinned libsignal backend. Private keys have redacted debug output and are zeroized when dropped. The MIT notice for the signature algorithm source is included in `licenses/curve25519-js.txt`.

`NoiseHandler` verifies the server certificate chain, produces the client static-key ciphertext, authenticates the handshake payload, and transitions to encrypted transport. The default certificate authority and protocol header match upstream. Synthetic peers can provide an explicit alternate authority through `noise::Config`. Feed websocket fragments with `push_bytes`, then pull one frame at a time with `next_frame`; this allows processing a server hello before a coalesced first encrypted stanza. All state mutation requires exclusive Rust access. Frame size and pending-buffer limits are bounded.

The Rust verifier checks the underlying libsignal verification result rather than mirroring the TypeScript wrapper's assumption that an invalid signature always throws. It rejects noncanonical/weak signatures and non-contributory X25519 agreement. Noise authentication failures terminate the handler; counter exhaustion never wraps a nonce, and transport initialization requires a verified server handshake. Certificate verification follows upstream's chain signatures and issuer serial checks; certificate expiry and leaf-to-static-key binding are not additional checks in this port. These are material behavior choices to review when integrating the client.

`connection` builds login/registration protobuf payloads, browser/companion identifiers, QR data, and pairing replies. `configure_successful_pairing` verifies the ADV HMAC and account signature for standard/hosted identities, signs the device identity, and returns the reply plus credential updates. Phone pairing helpers wrap the companion ephemeral key, process the primary response, encrypt the companion key bundle, and derive the ADV secret. Request/finish helpers expose deterministic-material variants for fixture verification; production wrappers generate fresh random material. Browser payloads and both pairing exchanges are checked against upstream bytes. The socket integrates these helpers with persistent credentials and connection events.

`socket::Socket::connect(config, store)` returns a handle and event receiver after the WebSocket/Noise handshake. Account login follows asynchronously; `wait_for_open` waits for initialization, while QR and phone pairing use `Event::Qr` and `request_pairing_code`. A single Tokio task owns the WebSocket and Noise state, routes IQ replies, handles keepalives, and persists credential changes through `spawn_blocking`. `query` generates IDs, reports stanza errors/timeouts, and removes pending queries when its future is dropped. Pending requests and event history are bounded; broadcast subscribers must handle lag. Dropping the last socket handle closes its task, and `close`/`logout` explicitly close the connection. Reconnect by calling `connect` again with the same store; automatic reconnection is left to the caller, as in upstream.

Login checks server prekey counts, uploads and acknowledges persisted batches, activates the connection, and requests the key digest. Upload failures retry with exponential backoff; a missing digest triggers another upload. Pair-success, routing information, and phone-pairing ADV-secret updates persist before subsequent protocol steps. The local mock peer exercises registration/login, both pairing methods, reconnect routing, queries, cancellation, prekey retries, QR rotation, and shutdown. This integration has no live-account validation. Signed-prekey rotation, server-driven low-prekey notifications, PN/LID migration, and full initial synchronization still require implementation.

`auth::Credentials::generate` creates noise/pairing/identity keys, a signed prekey, the ADV secret, and upstream credential defaults. The current source's registration generator samples the first random byte; the port preserves that behavior while accepting valid imported 14-bit IDs. `me_id` returns a structured 401 error before authentication. Credential debug output omits keys and account data.

`auth::SqliteAuthStore::open(folder)` initializes or resumes `auth.sqlite3`. The `KeyStore` trait exposes all ten upstream key categories, atomic batch writes, deletion, and clearing keys while retaining credentials. `transaction` provides atomic credential/key updates, read-your-writes, a per-transaction read cache, and nested work using the same transaction. SQLite handles cross-process database locking and recovery; operations are synchronous and should run on a blocking executor in an async client. The database uses FULL synchronization and a 30-second lock timeout. New auth directories/files use Unix permissions 0700/0600. Persistence is plaintext, like upstream's multi-file adapter.

Prekey generation preserves the unuploaded range. `AuthTransaction::allocate_prekeys` persists new keys and allocation counters together; `mark_prekeys_uploaded` advances the upload cursor after acknowledgement. Transactional deletion of prekeys requires loading the key first, matching upstream's prekey manager. Invalid or corrupt persisted credentials are reported rather than replaced with a new identity.

The optional `CachedKeyStore` has configurable TTL/capacity, caches missing keys, and updates its cache only after successful backend writes. It assumes exclusive ownership of its backend; writers outside the wrapper require `invalidate`. Database transactions expose explicit `AuthTransaction` methods rather than JavaScript's implicit async-local transaction context and retry queues.

`import_baileys_folder` and `export_baileys_folder` bridge the upstream JSON folder format using BufferJSON-compatible bytes and lossless protobufjs Long values. Export requires a new destination directory and detects filename collisions before writing. Baileys replaces `/` with `__` and `:` with `-`, so ambiguous import filenames require an explicit filename-to-ID map; export returns that map. Session records retain the actual opaque JSON representation returned by libsignal-node, whose runtime shape differs from its TypeScript byte-array declaration. Unknown credential/contact fields survive import/export. The interoperability test runs the actual upstream multi-file adapter against synthetic Rust output; it is optional so normal Rust tests do not require Node.

The Rust API uses `Option` for absent protobuf fields and an enum for node content. Attributes preserve insertion order because stanza fixtures must match upstream bytes. Decoding raw string content returns bytes, matching upstream. The decoder adds byte, node-count, nesting, and decompression limits, and rejects trailing bytes. `Jid::parse` rejects invalid numeric device/agent fields rather than representing JavaScript `NaN`. The binary encoder preserves upstream's one-byte companion-device encoding.

Upstream's protobufjs schema contains nonzero-first proto3 enums. The build generates a temporary schema with synthetic zero variants where required by `protoc`. Existing field numbers and enum discriminants remain unchanged. Bindings preserve 64-bit integers as Rust integers, without JavaScript number conversion. Synthetic enum variants are code-generation accommodations, not newly discovered WhatsApp protocol values.

MIT licensed. The upstream copyright notice is preserved in [LICENSE](LICENSE).
