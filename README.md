# baileys-rs

A Rust port in progress of [WhiskeySockets/Baileys](https://github.com/WhiskeySockets/Baileys), based on upstream commit `0af2386292907f7d9742d8d41f830d8c48208fa1` (7.0.0-rc14).

Currently provides the full generated WhatsApp protobuf model, JID helpers, binary stanza encoding/decoding, crypto primitives, and the Noise handshake and encrypted framing. This is not yet a usable WhatsApp client: websocket connection, pairing, persistent auth, Signal sessions, and the high-level socket APIs remain to be ported. See [PORT_STATUS.md](PORT_STATUS.md) for scope and validation.

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
npm install --prefix .cache/fixture-tools --ignore-scripts --no-audit --no-fund libsignal@6.0.0 protobufjs@7.5.6 whatsapp-rust-bridge@0.5.4
node scripts/generate-crypto-fixtures.mjs ../Baileys
node scripts/generate-noise-fixtures.mjs ../Baileys
```

These fixtures use fixed synthetic keys and a synthetic certificate authority. They verify compatibility with upstream code, not a live WhatsApp server. The Rust runtime has no JavaScript or WASM bridge dependency.

`crypto` exposes AES-256 GCM (12-byte nonces, appended authentication tags), CTR, PKCS#7 CBC, IV-prefixed CBC, SHA-256, MD5, HMAC-SHA-256/512, HKDF-SHA-256, and pairing-code PBKDF2. `KeyPair` supports X25519 agreement and the deterministic Curve25519 signature format used by upstream's pinned libsignal backend. Private keys have redacted debug output and are zeroized when dropped. The MIT notice for the signature algorithm source is included in `licenses/curve25519-js.txt`.

`NoiseHandler` verifies the server certificate chain, produces the client static-key ciphertext, authenticates the handshake payload, and transitions to encrypted transport. The default certificate authority and protocol header match upstream. Synthetic peers can provide an explicit alternate authority through `noise::Config`. Feed websocket fragments with `push_bytes`, then pull one frame at a time with `next_frame`; this allows processing a server hello before a coalesced first encrypted stanza. All state mutation requires exclusive Rust access. Frame size and pending-buffer limits are bounded.

The Rust verifier checks the underlying libsignal verification result rather than mirroring the TypeScript wrapper's assumption that an invalid signature always throws. It rejects noncanonical/weak signatures and non-contributory X25519 agreement. Noise authentication failures terminate the handler; counter exhaustion never wraps a nonce, and transport initialization requires a verified server handshake. Certificate verification follows upstream's chain signatures and issuer serial checks; certificate expiry and leaf-to-static-key binding are not additional checks in this port. These are material behavior choices to review when integrating the client.

The Rust API uses `Option` for absent protobuf fields and an enum for node content. Attributes preserve insertion order because stanza fixtures must match upstream bytes. Decoding raw string content returns bytes, matching upstream. The decoder adds byte, node-count, nesting, and decompression limits, and rejects trailing bytes. `Jid::parse` rejects invalid numeric device/agent fields rather than representing JavaScript `NaN`. The binary encoder preserves upstream's one-byte companion-device encoding.

Upstream's protobufjs schema contains nonzero-first proto3 enums. The build generates a temporary schema with synthetic zero variants where required by `protoc`. Existing field numbers and enum discriminants remain unchanged. Bindings preserve 64-bit integers as Rust integers, without JavaScript number conversion. Synthetic enum variants are code-generation accommodations, not newly discovered WhatsApp protocol values.

MIT licensed. The upstream copyright notice is preserved in [LICENSE](LICENSE).
