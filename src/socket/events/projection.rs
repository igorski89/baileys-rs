use super::super::{Error, Event};
use crate::{
    auth, event_buffer, history,
    messages::{self, group_effects, own},
    proto,
    proto_json::{self, JsonField},
    receipts,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

pub(super) const META: &str = "__baileysReceiveMetadata";
pub(super) const GROUP: &str = "__baileysGroupEffect";
pub(super) enum Action {
    Native(Event),
    Payload(&'static str, Value),
    Chats(Vec<own::ChatUpdate>),
}
fn serialize<T: Serialize>(value: &T) -> Result<Value, Error> {
    serde_json::to_value(value).map_err(|e| Error::EventProjection(e.to_string()))
}
fn deserialize<T: DeserializeOwned>(value: &Value) -> Result<T, Error> {
    serde_json::from_value(value.clone()).map_err(|e| Error::EventProjection(e.to_string()))
}
fn array(value: &Value) -> Result<&Vec<Value>, Error> {
    value
        .as_array()
        .ok_or_else(|| Error::EventProjection("expected event array".into()))
}
fn number(value: &Value) -> Result<f64, Error> {
    Ok(f64::from_value(value)?)
}
fn message_value(
    message: &proto::WebMessageInfo,
    metadata: Option<&messages::EnvelopeMetadata>,
    runtime: Option<&serde_json::Map<String, Value>>,
) -> Result<Value, Error> {
    // A received envelope is a plain object holding a decoded protobufjs
    // message; other messages are protobufjs WebMessageInfo objects.
    let plain = metadata.is_some_and(|m| !m.protobuf);
    let mut value = if plain {
        let mut value = proto_json::plain(|| message.to_value());
        if let Some(content) = &message.message {
            value["message"] = proto_json::protobuf(|| content.to_value());
        }
        value
    } else {
        proto_json::protobuf(|| message.to_value())
    };
    if let Some(runtime) = runtime {
        value
            .as_object_mut()
            .expect("message object")
            .extend(runtime.clone());
    }
    if let Some(metadata) = metadata {
        value[META] = serialize(metadata)?;
        if let Some(key) = value.get_mut("key") {
            metadata.key_metadata.apply(key);
        }
    }
    Ok(value)
}
type MessageProjection = (proto::WebMessageInfo, serde_json::Map<String, Value>);
fn message_decode(value: &Value) -> Result<MessageProjection, Error> {
    let mut fields = crate::proto_json::object(value)?.clone();
    fields.remove(META);
    let mut protocol = fields.clone();
    // Runtime receipt timestamps may be NaN; a nullable message is also a
    // valid update. Retain fields that protobuf decoding cannot represent.
    protocol.retain(|name, value| proto::WebMessageInfo::from_value(&json!({name:value})).is_ok());
    let message = proto::WebMessageInfo::from_value(&Value::Object(protocol))?;
    let canonical = message.to_value();
    fields.retain(|name, value| canonical.get(name) != Some(value));
    Ok((message, fields))
}
type MessageProjections = (
    Vec<proto::WebMessageInfo>,
    Vec<serde_json::Map<String, Value>>,
);
fn messages_decode(value: &Value) -> Result<MessageProjections, Error> {
    let (messages, mut runtime): (Vec<_>, Vec<_>) = array(value)?
        .iter()
        .map(message_decode)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .unzip();
    if runtime.iter().all(serde_json::Map::is_empty) {
        runtime.clear();
    }
    Ok((messages, runtime))
}
fn metadata(value: &Value) -> Result<Option<messages::EnvelopeMetadata>, Error> {
    value.get(META).map(deserialize).transpose()
}
fn reaction_value(update: &own::ReactionUpdate) -> Result<Value, Error> {
    let mut reaction = update.reaction.to_value();
    // Upstream spreads the decoded reaction (`{ ...reactionMessage, key }`),
    // so its int64 timestamp stays a protobufjs Long, which JSON writes as
    // its two words.
    if let Some(ms) = update.reaction.sender_timestamp_ms {
        reaction["senderTimestampMs"] =
            json!({"low": ms as i32, "high": (ms >> 32) as i32, "unsigned": false});
    }
    if let Some(metadata) = &update.key_metadata
        && let Some(key) = reaction.get_mut("key")
    {
        metadata.apply(key);
    }
    Ok(reaction)
}
fn update(key: &proto::MessageKey, fields: Value) -> Value {
    json!({"key":key.to_value(), "update":fields})
}
/// An update whose key is the received message's key with the target id,
/// as upstream spreads `message.key`, runtime properties included.
fn keyed_update(
    key: &proto::MessageKey,
    metadata: &Option<messages::KeyMetadata>,
    fields: Value,
) -> Value {
    let mut value = update(key, fields);
    if let Some(metadata) = metadata {
        metadata.apply(&mut value["key"]);
    }
    value
}
/// Upstream's event values are plain objects, except the protobufjs messages
/// marked inside.
pub(super) fn project(event: Event) -> Result<Vec<Action>, Error> {
    proto_json::plain(|| project_plain(event))
}
fn project_plain(event: Event) -> Result<Vec<Action>, Error> {
    let (name, value) = match event {
        Event::ChatsUpdated(updates) => return Ok(vec![Action::Chats(updates)]),
        Event::ChatsUpserted(chats) => ("chats.upsert", json!(chats.iter().map(JsonField::to_value).collect::<Vec<_>>())),
        Event::ChatsDeleted(ids) => ("chats.delete", json!(ids)),
        Event::ContactsUpdated(contacts) => ("contacts.update", serialize(&contacts)?),
        Event::ContactsUpserted(contacts) => ("contacts.upsert", serialize(&contacts)?),
        Event::GroupsUpdated(groups) => ("groups.update", serialize(&groups)?),
        Event::GroupEffects(effects) => {
            let mut actions = vec![];
            for effect in effects {
                if let group_effects::GroupEvent::Changed { id, author, change } = &effect {
                    let (field, value) = match change {
                        group_effects::Change::Announce(v) => ("announce", json!(v)),
                        group_effects::Change::Restrict(v) => ("restrict", json!(v)),
                        group_effects::Change::Subject(v) => ("subject", json!(v)),
                        group_effects::Change::Description(v) => ("desc", json!(v)),
                        group_effects::Change::InviteCode(v) => ("inviteCode", json!(v)),
                        group_effects::Change::MemberAddMode(v) => ("memberAddMode", json!(v)),
                        group_effects::Change::JoinApprovalMode(v) => ("joinApprovalMode", json!(v)),
                    };
                    let mut value = json!({"id":id, field:value});
                    for (field, v) in [("author", &author.jid), ("authorPn", &author.pn), ("authorUsername", &author.username)] {
                        if let Some(v) = v { value[field] = json!(v); }
                    }
                    value[GROUP] = serialize(&effect)?;
                    actions.push(Action::Payload("groups.update", json!([value])));
                } else { actions.push(Action::Native(Event::GroupEffects(vec![effect]))); }
            }
            return Ok(actions);
        }
        Event::MessagesUpserted(upsert) => {
            let messages = upsert.messages.iter().enumerate().map(|(i, m)| message_value(m, upsert.metadata_for(i), upsert.runtime_fields.get(i))).collect::<Result<Vec<_>, _>>()?;
            let mut value = json!({"messages":messages, "type":match upsert.kind {own::UpsertKind::Append=>"append",own::UpsertKind::Notify=>"notify"}});
            if let Some(id) = upsert.request_id { value["requestId"] = json!(id); }
            ("messages.upsert", value)
        }
        Event::MessagesDeleted(keys) => ("messages.delete", json!({"keys":keys.iter().map(JsonField::to_value).collect::<Vec<_>>()})),
        Event::MessagesUpdated(updates) => ("messages.update", json!(updates.iter().map(|v| update(&v.key,json!({"status":v.status as i32,"messageTimestamp":v.message_timestamp.to_value()}))).collect::<Vec<_>>())),
        Event::EventResponsesUpdated(updates) => ("messages.update", json!(updates.iter().map(|v|update(&v.key,json!({"eventResponses":[{"eventResponseMessageKey":v.event_response_message_key.to_value(),"senderTimestampMs":v.sender_timestamp_ms.map(|t|t.to_value()),"response":proto_json::protobuf(||v.response.to_value())}]}))).collect::<Vec<_>>())),
        Event::MessageUpdates(updates) => ("messages.update", json!(updates.iter().map(|v|{
            let mut fields=v.update_value();
            // A message in an update is a decoded protobufjs message.
            if let Some(message)=&v.info.message && !v.runtime_fields.contains_key("message") { fields["message"]=proto_json::protobuf(||message.to_value()); }
            keyed_update(&v.key,&v.key_metadata,fields)
        }).collect::<Vec<_>>())),
        Event::MessagesStarred(updates) => ("messages.update", json!(updates.iter().map(|v| update(&v.key,json!({"starred":v.starred}))).collect::<Vec<_>>())),
        Event::MessageStubsUpdated(updates) => ("messages.update", json!(updates.iter().map(|v| update(&v.key,json!({"messageStubParameters":v.parameters}))).collect::<Vec<_>>())),
        Event::MessageContentsUpdated(updates) => ("messages.update", json!(updates.iter().map(|v| update(&v.key,json!({"message":v.message.as_ref().map(|m|proto_json::protobuf(||m.to_value()))}))).collect::<Vec<_>>())),
        Event::MessageMutations(updates) => ("messages.update", json!(updates.iter().map(|v| match v {
            own::Mutation::Revoke { key, original_key, key_metadata } => {
                let mut original = original_key.to_value();
                if let Some(metadata) = key_metadata { metadata.apply(&mut original); }
                keyed_update(key,key_metadata,json!({"message":null,"messageStubType":proto::web_message_info::StubType::Revoke as i32,"key":original}))
            }
            own::Mutation::Edit { key,message,timestamp,key_metadata } => keyed_update(key,key_metadata,json!({"message":proto_json::protobuf(||message.to_value()),"messageTimestamp":timestamp.to_value()})),
        }).collect::<Vec<_>>())),
        Event::MessageReactions(updates) => ("messages.reaction", json!(updates.iter().map(|v|Ok(json!({"key":v.key.as_ref().map(JsonField::to_value),"reaction":reaction_value(v)?}))).collect::<Result<Vec<_>, Error>>()?)),
        Event::MessageReceiptsUpdated(updates) => ("message-receipt.update", json!(updates.iter().map(|v|{
            let mut receipt=json!({"userJid":v.user_jid});
            if let Some(time)=v.receipt_timestamp { receipt["receiptTimestamp"]=time.to_value(); }
            if let Some(time)=v.read_timestamp { receipt["readTimestamp"]=time.to_value(); }
            json!({"key":v.key.to_value(),"receipt":receipt})
        }).collect::<Vec<_>>())),
        Event::MessagingHistorySet(set) => {
            // History chats are decoded protobufjs Conversations.
            let mut value=json!({"chats":proto_json::protobuf(||set.data.chats.iter().map(JsonField::to_value).collect::<Vec<_>>()),"contacts":serialize(&set.data.contacts)?,"messages":set.data.messages.iter().enumerate().map(|(i,m)|message_value(m,set.data.message_metadata.get(i).and_then(Option::as_ref),set.data.message_runtime_fields.get(i))).collect::<Result<Vec<_>,_>>()?});
            let past=json!(set.data.past_participants.iter().map(JsonField::to_value).collect::<Vec<_>>());
            if set.buffered {
                // The buffer's set: past participants only once some arrived.
                if !set.data.past_participants.is_empty() { value["pastParticipants"]=past; }
            } else {
                // processMessage spreads processHistoryMessage's result.
                value["lidPnMappings"]=serialize(&set.data.lid_pn_mappings)?;
                value["pastParticipants"]=past;
            }
            // The notification's unset fields are protobufjs' null defaults.
            value["syncType"]=json!(set.data.sync_type);
            value["progress"]=json!(set.data.progress);
            if let Some(v)=set.is_latest { value["isLatest"]=json!(v); }
            value["chunkOrder"]=json!(set.chunk_order);
            value["peerDataRequestSessionId"]=json!(set.peer_data_request_session_id);
            ("messaging-history.set",value)
        }
        event => return Ok(vec![Action::Native(event)]),
    };
    Ok(vec![Action::Payload(name, value)])
}
fn parse_list<T: JsonField>(value: &Value) -> Result<Vec<T>, Error> {
    array(value)?
        .iter()
        .map(|v| Ok(T::from_value(v)?))
        .collect()
}
pub(super) fn decode(batch: event_buffer::Batch) -> Result<Vec<Event>, Error> {
    let mut events = vec![];
    for (name, value) in batch.events {
        match name.as_str() {
            "chats.update" => events.push(Event::ChatsUpdated(parse_list(&value)?)),
            "chats.upsert" => events.push(Event::ChatsUpserted(parse_list(&value)?)),
            "chats.delete" => events.push(Event::ChatsDeleted(deserialize(&value)?)),
            "contacts.update" => events.push(Event::ContactsUpdated(deserialize::<
                Vec<auth::Contact>,
            >(&value)?)),
            "contacts.upsert" => events.push(Event::ContactsUpserted(deserialize::<
                Vec<auth::Contact>,
            >(&value)?)),
            "messages.delete" => events.push(Event::MessagesDeleted(parse_list(&value["keys"])?)),
            "messages.upsert" => {
                let (messages, runtime_fields) = messages_decode(&value["messages"])?;
                let mut metadata_values = vec![];
                let mut indices = vec![];
                for (i, v) in array(&value["messages"])?.iter().enumerate() {
                    if let Some(v) = metadata(v)? {
                        metadata_values.push(v);
                        indices.push(i);
                    }
                }
                let kind = match value["type"].as_str() {
                    Some("append") => own::UpsertKind::Append,
                    Some("notify") => own::UpsertKind::Notify,
                    _ => return Err(Error::EventProjection("invalid upsert type".into())),
                };
                events.push(Event::MessagesUpserted(own::Upsert {
                    messages,
                    kind,
                    request_id: value["requestId"].as_str().map(str::to_owned),
                    metadata: metadata_values,
                    metadata_indices: indices,
                    runtime_fields,
                }));
            }
            "messages.update" => {
                let mut updates = vec![];
                for entry in array(&value)? {
                    let key = proto::MessageKey::from_value(&entry["key"])?;
                    let (info, runtime_fields) = message_decode(&entry["update"])?;
                    updates.push(own::Update {
                        key,
                        key_metadata: messages::KeyMetadata::from_key(&entry["key"]),
                        info,
                        runtime_fields,
                    });
                }
                events.push(Event::MessageUpdates(updates));
            }
            "messages.reaction" => {
                let mut updates = vec![];
                for entry in array(&value)? {
                    updates.push(own::ReactionUpdate {
                        key: Some(proto::MessageKey::from_value(&entry["key"])?),
                        reaction: proto::Reaction::from_value(&entry["reaction"])?,
                        key_metadata: messages::KeyMetadata::from_key(&entry["reaction"]["key"]),
                    });
                }
                events.push(Event::MessageReactions(updates));
            }
            "message-receipt.update" => {
                let mut updates = vec![];
                for entry in array(&value)? {
                    let receipt = &entry["receipt"];
                    updates.push(receipts::UserUpdate {
                        key: proto::MessageKey::from_value(&entry["key"])?,
                        user_jid: receipt["userJid"]
                            .as_str()
                            .ok_or_else(|| Error::EventProjection("receipt user JID".into()))?
                            .into(),
                        receipt_timestamp: receipt
                            .get("receiptTimestamp")
                            .map(number)
                            .transpose()?,
                        read_timestamp: receipt.get("readTimestamp").map(number).transpose()?,
                    });
                }
                events.push(Event::MessageReceiptsUpdated(updates));
            }
            "groups.update" => {
                let mut groups = vec![];
                let mut effects = vec![];
                for group in array(&value)? {
                    if let Some(effect) = group.get(GROUP) {
                        effects.push(deserialize(effect)?);
                    } else {
                        groups.push(deserialize(group)?);
                    }
                }
                if !groups.is_empty() {
                    events.push(Event::GroupsUpdated(groups));
                }
                if !effects.is_empty() {
                    events.push(Event::GroupEffects(effects));
                }
            }
            "messaging-history.set" => {
                let (messages, message_runtime_fields) = messages_decode(&value["messages"])?;
                let message_metadata = array(&value["messages"])?
                    .iter()
                    .map(metadata)
                    .collect::<Result<Vec<_>, _>>()?;
                events.push(Event::MessagingHistorySet(history::Set {
                    data: history::Processed {
                        chats: parse_list(&value["chats"])?,
                        contacts: deserialize(&value["contacts"])?,
                        messages,
                        message_metadata,
                        message_runtime_fields,
                        lid_pn_mappings: vec![],
                        past_participants: value
                            .get("pastParticipants")
                            .map(parse_list)
                            .transpose()?
                            .unwrap_or_default(),
                        sync_type: value["syncType"].as_i64().unwrap_or(0) as i32,
                        progress: value["progress"].as_u64().map(|v| v as u32),
                    },
                    is_latest: value["isLatest"].as_bool(),
                    chunk_order: value["chunkOrder"].as_u64().map(|v| v as u32),
                    peer_data_request_session_id: value["peerDataRequestSessionId"]
                        .as_str()
                        .map(str::to_owned),
                    buffered: true,
                }));
            }
            _ => {
                return Err(Error::EventProjection(format!(
                    "unexpected buffered event {name}"
                )));
            }
        }
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_original_buffer_message_payload_survives_typed_projection() {
        let fixture: Value =
            serde_json::from_str(include_str!("../../../tests/fixtures/event_buffer.json"))
                .unwrap();
        let mut count = 0;
        for case in fixture["cases"].as_array().unwrap() {
            for step in case["steps"].as_array().unwrap() {
                for batch in step["emitted"].as_array().unwrap() {
                    for name in ["messages.upsert", "messaging-history.set"] {
                        if let Some(values) = batch[name]["messages"].as_array() {
                            for value in values {
                                let (message, runtime) = message_decode(value).unwrap();
                                assert_eq!(
                                    message_value(&message, None, Some(&runtime)).unwrap(),
                                    *value
                                );
                                count += 1;
                            }
                        }
                    }
                }
            }
        }
        assert!(count > 100);
    }
    #[test]
    fn runtime_nulls_nonfinite_receipts_and_unknown_fields_are_retained() {
        let value = json!({"key":{"id":"M","remoteJid":"200@s.whatsapp.net","participantAlt":"201@lid"},"message":null,"userReceipt":[{"userJid":"300@s.whatsapp.net","readTimestamp":"NaN"}],"futureRuntimeField":[true,null],"messageTimestamp":"18446744073709551615"});
        let (message, runtime) = message_decode(&value).unwrap();
        assert_eq!(message.message_timestamp, Some(u64::MAX));
        assert!(message.message.is_none());
        assert!(message.user_receipt.is_empty());
        assert_eq!(
            message_value(&message, None, Some(&runtime)).unwrap(),
            value
        );
    }
}
