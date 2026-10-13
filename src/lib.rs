//! Rust implementation of the Baileys WhatsApp Web protocol.
//! See PORT_STATUS.md for implemented and remaining components.
pub mod app_state;
pub mod auth;
pub mod binary;
pub mod business;
pub mod calls;
pub mod chats;
pub mod communities;
pub mod connection;
pub mod crypto;
pub mod defaults;
pub mod devices;
pub mod event_buffer;
pub mod groups;
pub mod history;
pub mod jid;
pub mod media;
pub mod messages;
pub mod newsletters;
pub mod noise;
pub mod notifications;
pub mod offline;
pub mod privacy;
pub mod privacy_settings;
pub mod proto_json;
pub mod receipts;
pub mod relay;
pub mod reporting;
pub mod retry;
pub mod serialization;
pub mod signal;
pub mod socket;
pub mod usync;

/// Bindings generated from the complete upstream WAProto schema.
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}
