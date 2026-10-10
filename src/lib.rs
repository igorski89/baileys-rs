//! Rust implementation of the Baileys WhatsApp Web protocol.
//! See PORT_STATUS.md for implemented and remaining components.
pub mod binary;
pub mod jid;

/// Bindings generated from the complete upstream WAProto schema.
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}
