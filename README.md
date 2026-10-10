# baileys-rs

A Rust port in progress of [WhiskeySockets/Baileys](https://github.com/WhiskeySockets/Baileys), based on upstream commit `0af2386292907f7d9742d8d41f830d8c48208fa1` (7.0.0-rc14).

Currently provides the full generated WhatsApp protobuf model, JID helpers, and binary stanza encoding/decoding. This is not yet a usable WhatsApp client: connection, pairing, Signal sessions, and the high-level socket APIs remain to be ported. See [PORT_STATUS.md](PORT_STATUS.md) for scope and validation.

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

The Rust API uses `Option` for absent protobuf fields and an enum for node content. Attributes preserve insertion order because stanza fixtures must match upstream bytes. Decoding raw string content returns bytes, matching upstream. The decoder adds byte, node-count, nesting, and decompression limits, and rejects trailing bytes. `Jid::parse` rejects invalid numeric device/agent fields rather than representing JavaScript `NaN`. The binary encoder preserves upstream's one-byte companion-device encoding.

Upstream's protobufjs schema contains nonzero-first proto3 enums. The build generates a temporary schema with synthetic zero variants where required by `protoc`. Existing field numbers and enum discriminants remain unchanged. Bindings preserve 64-bit integers as Rust integers, without JavaScript number conversion. Synthetic enum variants are code-generation accommodations, not newly discovered WhatsApp protocol values.

MIT licensed. The upstream copyright notice is preserved in [LICENSE](LICENSE).
