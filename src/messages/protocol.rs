//! Helpers for authenticated phone protocol messages.
use super::{Envelope, EnvelopeMetadata};
use crate::proto;
/// Apply a phone's recovered content to the source placeholder metadata subset.
/// Presence of a zero timestamp still overrides the cached timestamp (protobuf Long).
pub fn restore_placeholder(
    decoded: proto::WebMessageInfo,
    cached: Option<Envelope>,
) -> (proto::WebMessageInfo, Option<EnvelopeMetadata>) {
    let Some(cached) = cached else {
        return (decoded, None);
    };
    let metadata = EnvelopeMetadata {
        key_metadata: cached.key_metadata,
        category: None,
        retry_count: None,
        author: cached.author,
        sender: cached.sender,
        // Upstream sets the decoded message on the cached plain object.
        protobuf: cached.protobuf,
    };
    let original = cached.message;
    (
        proto::WebMessageInfo {
            key: original.key,
            message: decoded.message,
            message_timestamp: decoded.message_timestamp.or(original.message_timestamp),
            push_name: original.push_name,
            participant: original.participant,
            verified_biz_name: original.verified_biz_name,
            ..Default::default()
        },
        Some(metadata),
    )
}

/// protobufjs optional Long(0) is truthy, so present latestLid=0 wins.
pub fn lid_sync_mappings(
    payload: &proto::LidMigrationMappingSyncPayload,
) -> Vec<crate::signal::mapping::Mapping> {
    payload
        .pn_to_lid_mappings
        .iter()
        .map(|p| crate::signal::mapping::Mapping {
            pn: format!("{}@s.whatsapp.net", p.pn),
            lid: format!("{}@lid", p.latest_lid.unwrap_or(p.assigned_lid)),
        })
        .collect()
}
