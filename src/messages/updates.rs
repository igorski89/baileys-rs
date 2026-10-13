//! Caller-side helpers that fold receipts, reactions, poll votes and event responses into stored messages.
use super::{EventResponseUpdate, key_author};
use crate::{crypto, proto};
use indexmap::IndexMap;

/// The kind of client that generated a message ID, as upstream guesses it from the ID's shape.
pub fn device_kind(id: &str) -> &'static str {
    // JavaScript measures in UTF-16 units and `.` does not cross line terminators.
    let units = id.encode_utf16().count();
    let dots = |from: usize| {
        !id.chars()
            .skip(from)
            .any(|c| matches!(c, '\n' | '\r' | '\u{2028}' | '\u{2029}'))
    };
    if id.starts_with("3A") && units == 20 && dots(2) {
        "ios"
    } else if id.starts_with("3E") && units == 22 && dots(2) {
        "web"
    } else if (units == 21 || units == 32) && dots(0) {
        "android"
    } else if id.starts_with("3F") || (units == 18 && dots(0)) {
        "desktop"
    } else {
        "unknown"
    }
}
/// Upsert a receipt by user; fields the receipt leaves unset keep their stored value.
pub fn update_with_receipt(message: &mut proto::WebMessageInfo, receipt: proto::UserReceipt) {
    let Some(stored) = message
        .user_receipt
        .iter_mut()
        .find(|stored| stored.user_jid == receipt.user_jid)
    else {
        message.user_receipt.push(receipt);
        return;
    };
    if receipt.receipt_timestamp.is_some() {
        stored.receipt_timestamp = receipt.receipt_timestamp;
    }
    if receipt.read_timestamp.is_some() {
        stored.read_timestamp = receipt.read_timestamp;
    }
    if receipt.played_timestamp.is_some() {
        stored.played_timestamp = receipt.played_timestamp;
    }
    if !receipt.pending_device_jid.is_empty() {
        stored.pending_device_jid = receipt.pending_device_jid;
    }
    if !receipt.delivered_device_jid.is_empty() {
        stored.delivered_device_jid = receipt.delivered_device_jid;
    }
}
/// Replace the author's earlier reaction with this one; an absent text becomes empty.
pub fn update_with_reaction(message: &mut proto::WebMessageInfo, mut reaction: proto::Reaction) {
    let author = key_author(reaction.key.as_ref(), None, "me");
    message
        .reactions
        .retain(|stored| key_author(stored.key.as_ref(), None, "me") != author);
    reaction.text = Some(reaction.text.unwrap_or_default());
    message.reactions.push(reaction);
}
/// Replace the author's earlier vote; a vote without selected options only removes it.
pub fn update_with_poll_update(message: &mut proto::WebMessageInfo, update: proto::PollUpdate) {
    let author = key_author(update.poll_update_message_key.as_ref(), None, "me");
    message
        .poll_updates
        .retain(|stored| key_author(stored.poll_update_message_key.as_ref(), None, "me") != author);
    if update
        .vote
        .as_ref()
        .is_some_and(|vote| !vote.selected_options.is_empty())
    {
        message.poll_updates.push(update);
    }
}
/// Replace the author's earlier event response with this one.
pub fn update_with_event_response(
    message: &mut proto::WebMessageInfo,
    update: proto::EventResponse,
) {
    let author = key_author(update.event_response_message_key.as_ref(), None, "me");
    message.event_responses.retain(|stored| {
        key_author(stored.event_response_message_key.as_ref(), None, "me") != author
    });
    message.event_responses.push(update);
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct VoteAggregation {
    pub name: String,
    pub voters: Vec<String>,
}
/// Voters per option of a poll creation message; votes for unknown options are listed as "Unknown".
pub fn aggregate_votes(message: &proto::WebMessageInfo, me: Option<&str>) -> Vec<VoteAggregation> {
    let content = message.message.as_ref();
    // A decoded poll always has an option list, so an empty one does not fall through.
    let options = content
        .and_then(|m| m.poll_creation_message.as_ref())
        .map(|poll| &poll.options)
        .or(content
            .and_then(|m| m.poll_creation_message_v2.as_ref())
            .map(|poll| &poll.options))
        .or(content
            .and_then(|m| m.poll_creation_message_v3.as_ref())
            .map(|poll| &poll.options));
    // Upstream keys the table by the hash decoded as text.
    let text = |hash: &[u8]| String::from_utf8_lossy(hash).into_owned();
    let mut table = IndexMap::<String, VoteAggregation>::new();
    for option in options.into_iter().flatten() {
        let name = option.option_name.clone().unwrap_or_default();
        table.insert(
            text(&crypto::sha256(name.as_bytes())),
            VoteAggregation {
                name,
                voters: vec![],
            },
        );
    }
    for update in &message.poll_updates {
        let Some(vote) = &update.vote else { continue };
        for option in &vote.selected_options {
            table
                .entry(text(option))
                .or_insert_with(|| VoteAggregation {
                    name: "Unknown".into(),
                    voters: vec![],
                })
                .voters
                .push(key_author(
                    update.poll_update_message_key.as_ref(),
                    None,
                    me.unwrap_or("me"),
                ));
        }
    }
    table.into_values().collect()
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ResponseAggregation {
    pub response: &'static str,
    pub responders: Vec<String>,
}
/// Responders per answer of an event, from the decrypted responses the socket publishes.
pub fn aggregate_event_responses(
    responses: &[EventResponseUpdate],
    me: Option<&str>,
) -> Vec<ResponseAggregation> {
    use proto::message::event_response_message::EventResponseType as Type;
    let mut result: Vec<_> = [Type::Going, Type::NotGoing, Type::Maybe]
        .into_iter()
        .map(|kind| ResponseAggregation {
            response: kind.as_str_name(),
            responders: vec![],
        })
        .collect();
    for update in responses {
        let kind = update
            .response
            .response
            .and_then(|kind| Type::try_from(kind).ok());
        let Some(entry) = kind.and_then(|kind| {
            result
                .iter_mut()
                .find(|entry| entry.response == kind.as_str_name())
        }) else {
            continue;
        };
        entry.responders.push(key_author(
            Some(&update.event_response_message_key),
            None,
            me.unwrap_or("me"),
        ));
    }
    result
}
