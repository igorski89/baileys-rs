//! Rust implementation of the Baileys WhatsApp Web protocol.
//! See PORT_STATUS.md for implemented and remaining components.
pub mod auth;
pub mod binary;
pub mod crypto;
pub mod defaults;
pub mod jid;
pub mod noise;
pub mod serialization;

/// Bindings generated from the complete upstream WAProto schema.
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}
