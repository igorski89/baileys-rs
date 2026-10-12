//! Source-compatible event consolidation over extensible JSON payloads.
//! Callers supply JSON payloads and drive a monotonic clock.
use indexmap::{IndexMap, IndexSet};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
pub type EventMap = IndexMap<String, Value>;
const EVENTS: [&str; 12] = [
    "messaging-history.set",
    "chats.upsert",
    "chats.update",
    "chats.delete",
    "contacts.upsert",
    "contacts.update",
    "messages.upsert",
    "messages.update",
    "messages.delete",
    "messages.reaction",
    "message-receipt.update",
    "groups.update",
];
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid event-buffer {0}")]
    Input(&'static str),
    #[error("event-buffer condition failed: {0}")]
    Condition(String),
}
type Predicate = dyn Fn(&Data) -> Result<Option<bool>, Error> + Send + Sync;
#[derive(Clone)]
pub struct Condition(Arc<Predicate>);
impl std::fmt::Debug for Condition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Condition(..)")
    }
}
impl Condition {
    pub fn new<F: Fn(&Data) -> Option<bool> + Send + Sync + 'static>(f: F) -> Self {
        Self::try_new(move |data| Ok(f(data)))
    }
    pub fn try_new<F: Fn(&Data) -> Result<Option<bool>, Error> + Send + Sync + 'static>(
        f: F,
    ) -> Self {
        Self(Arc::new(f))
    }
    pub fn chat(condition: crate::app_state::ChatCondition) -> Self {
        Self::new(move |data| {
            let chat = data
                .history_chats
                .get(&condition.id)
                .or_else(|| data.chat_upserts.get(&condition.id))?;
            Some(condition.message_range.as_ref().is_none_or(|range| {
                range
                    .last_message_timestamp
                    .or(range.last_system_message_timestamp)
                    .unwrap_or(0) as f64
                    >= chat
                        .get("lastMessageRecvTimestamp")
                        .filter(|v| truth(v))
                        .map(number)
                        .unwrap_or(0.0)
            }))
        })
    }
    pub fn evaluate(&self, data: &Data) -> Result<Option<bool>, Error> {
        (self.0)(data)
    }
}
#[derive(Debug, Clone)]
pub struct ChatUpdate {
    pub data: Value,
    pub condition: Option<Condition>,
}
#[derive(Debug, Clone)]
pub struct Batch {
    pub events: EventMap,
    pub chat_conditions: Vec<Option<Condition>>,
}
#[derive(Debug, Clone, Default)]
pub struct Data {
    pub history_chats: IndexMap<String, Value>,
    pub history_contacts: IndexMap<String, Value>,
    pub history_messages: IndexMap<String, Value>,
    pub history_metadata: EventMap,
    pub history_present: bool,
    pub chat_upserts: IndexMap<String, Value>,
    pub chat_updates: IndexMap<String, ChatUpdate>,
    pub chat_deletes: IndexSet<String>,
    pub contact_upserts: IndexMap<String, Value>,
    pub contact_updates: IndexMap<String, Value>,
    pub message_upserts: IndexMap<String, Value>,
    pub message_updates: IndexMap<String, Value>,
    pub message_deletes: IndexMap<String, Value>,
    pub message_reactions: IndexMap<String, Value>,
    pub message_receipts: IndexMap<String, Value>,
    pub group_updates: IndexMap<String, Value>,
}
#[derive(Debug, Clone, Copy)]
enum TimerKind {
    Auto,
    Pending,
    Single,
}
#[derive(Debug, Clone, Copy)]
struct Timer {
    id: u64,
    at: Duration,
    kind: TimerKind,
}
#[derive(Debug, Default)]
pub struct Buffer {
    failed_emissions: Vec<Batch>,
    data: Data,
    history_cache: IndexSet<String>,
    active: bool,
    count: usize,
    timers: Vec<Timer>,
    next_timer: u64,
    auto: Option<u64>,
    pending: Option<u64>,
}
impl Buffer {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn data(&self) -> &Data {
        &self.data
    }
    /// Outputs already emitted by upstream before a subsequent append error.
    /// Drain these when emit returns an error; successful calls return outputs directly.
    pub fn take_failed_emissions(&mut self) -> Vec<Batch> {
        std::mem::take(&mut self.failed_emissions)
    }
    pub fn is_buffering(&self) -> bool {
        self.active
    }
    pub fn buffer_count(&self) -> usize {
        self.count
    }
    fn timer(&mut self, at: Duration, kind: TimerKind) -> u64 {
        let id = self.next_timer;
        self.next_timer += 1;
        self.timers.push(Timer { id, at, kind });
        id
    }
    pub fn buffer(&mut self, now: Duration) {
        if !self.active {
            self.active = true;
            self.count = 0;
            self.auto = Some(self.timer(now + Duration::from_secs(30), TimerKind::Auto));
        }
        self.count += 1;
    }
    /// Corresponds to createBufferedFunction's success and finally handlers.
    /// The caller must finish on both success and error.
    pub fn finish_function(&mut self, now: Duration, success: bool) {
        if success && self.count == 1 {
            self.timer(now + Duration::from_millis(100), TimerKind::Single);
        }
        self.count = self.count.saturating_sub(1);
        if self.count == 0 && self.pending.is_none() {
            self.pending = Some(self.timer(now + Duration::from_millis(100), TimerKind::Pending));
        }
    }
    pub fn next_deadline(&self) -> Option<Duration> {
        self.timers.iter().map(|t| t.at).min()
    }
    pub fn tick(&mut self, now: Duration) -> Vec<Batch> {
        let mut batches = vec![];
        while let Some((position, timer)) = self
            .timers
            .iter()
            .enumerate()
            .filter(|(_, t)| t.at <= now)
            .min_by_key(|(_, t)| (t.at, t.id))
            .map(|(i, t)| (i, *t))
        {
            self.timers.remove(position);
            let flush = match timer.kind {
                TimerKind::Auto | TimerKind::Pending => true,
                TimerKind::Single => self.active && self.count == 1,
            };
            if flush {
                let (_, out) = self.flush();
                batches.extend(out);
            }
        }
        batches
    }
    pub fn flush(&mut self) -> (bool, Vec<Batch>) {
        if !self.active {
            return (false, vec![]);
        }
        self.active = false;
        self.count = 0;
        let auto = self.auto.take();
        let pending = self.pending.take();
        self.timers
            .retain(|t| Some(t.id) != auto && Some(t.id) != pending);
        if self.history_cache.len() > 10000 {
            self.history_cache.clear();
        }
        let mut next = Data::default();
        let retained: Vec<_> = self
            .data
            .chat_updates
            .iter()
            .filter(|(_, u)| u.condition.is_some())
            .map(|(k, _)| k.clone())
            .collect();
        for id in retained {
            next.chat_updates.insert(
                id.clone(),
                self.data.chat_updates.shift_remove(&id).unwrap(),
            );
        }
        let batch = consolidate(&self.data);
        self.data = next;
        (
            true,
            if batch.is_empty() {
                vec![]
            } else {
                vec![Batch {
                    events: batch,
                    chat_conditions: vec![],
                }]
            },
        )
    }
    pub fn destroy(&mut self) {
        self.timers
            .retain(|t| !matches!(t.kind, TimerKind::Auto | TimerKind::Pending));
        self.auto = None;
        self.pending = None;
        self.history_cache.clear();
        self.failed_emissions.clear();
        self.data = Data::default();
        self.active = false;
        self.count = 0;
    }
    pub fn emit(&mut self, event: &str, payload: Value) -> Result<Vec<Batch>, Error> {
        let mut output = vec![];
        if event == "messages.upsert"
            && let Some(first) = self.data.message_upserts.first().map(|(_, v)| v)
        {
            let kind = first.get("type");
            if kind != payload.get("type") {
                let messages = values(&self.data.message_upserts)
                    .into_iter()
                    .map(|v| v["message"].clone())
                    .collect::<Vec<_>>();
                output.push(single(
                    "messages.upsert",
                    json!({"messages":messages,"type":kind}),
                ));
                self.data.message_upserts.clear();
            }
        }
        if self.active && EVENTS.contains(&event) {
            if let Err(error) = self.append(event, payload) {
                self.failed_emissions.extend(output);
                return Err(error);
            }
        } else {
            output.push(single(event, payload));
        }
        Ok(output)
    }
    pub fn emit_chat_updates(&mut self, updates: Vec<ChatUpdate>) -> Result<Vec<Batch>, Error> {
        if !self.active {
            return Ok(vec![Batch {
                events: IndexMap::from([(
                    "chats.update".into(),
                    Value::Array(updates.iter().map(|u| u.data.clone()).collect()),
                )]),
                chat_conditions: updates.into_iter().map(|u| u.condition).collect(),
            }]);
        }
        for update in updates {
            self.chat_update(update)?;
        }
        Ok(vec![])
    }
    /// Project native socket chat updates while keeping conditions callable.
    pub fn emit_native_chat_updates(
        &mut self,
        updates: Vec<crate::messages::own::ChatUpdate>,
    ) -> Result<Vec<Batch>, Error> {
        use crate::proto_json::JsonField;
        self.emit_chat_updates(
            updates
                .into_iter()
                .map(|update| ChatUpdate {
                    data: update.to_value(),
                    condition: update.conditional.map(Condition::chat),
                })
                .collect(),
        )
    }
    fn chat_update(&mut self, mut update: ChatUpdate) -> Result<(), Error> {
        object(&update.data)?;
        let id = property(update.data.get("id"));
        let matches = update
            .condition
            .as_ref()
            .map_or(Ok(Some(true)), |c| c.evaluate(&self.data))?;
        match matches {
            Some(true) => {
                update.condition = None;
                object_mut(&mut update.data)?.remove("conditional");
                if let Some(chat) = self
                    .data
                    .history_chats
                    .get_mut(&id)
                    .or_else(|| self.data.chat_upserts.get_mut(&id))
                {
                    *chat = concat(chat.clone(), update.data)?;
                } else {
                    let prior = self.data.chat_updates.get(&id).cloned();
                    update.condition = prior.as_ref().and_then(|u| u.condition.clone());
                    let prior = prior.map(|u| u.data).unwrap_or(json!({}));
                    update.data = concat(prior, update.data)?;
                    self.data.chat_updates.insert(id.clone(), update);
                }
            }
            None => {
                self.data.chat_updates.insert(id.clone(), update);
            }
            Some(false) => {}
        }
        self.data.chat_deletes.shift_remove(&id);
        Ok(())
    }
    fn absorb(&mut self, id: &str, history: bool) -> Result<(), Error> {
        let Some(update) = self.data.chat_updates.get(id) else {
            return Ok(());
        };
        let matches = update
            .condition
            .as_ref()
            .map_or(Ok(Some(true)), |c| c.evaluate(&self.data))?;
        if matches == Some(true) {
            let update = self.data.chat_updates.shift_remove(id).unwrap();
            let map = if history {
                &mut self.data.history_chats
            } else {
                &mut self.data.chat_upserts
            };
            if let Some(chat) = map.get_mut(id) {
                *chat = concat(update.data, chat.clone())?;
            }
        } else if matches == Some(false) {
            self.data.chat_updates.shift_remove(id);
        }
        Ok(())
    }
    fn append(&mut self, event: &str, payload: Value) -> Result<(), Error> {
        match event {
            "messaging-history.set" => {
                for chat in array(&payload["chats"])? {
                    object(chat)?;
                    let id = chat
                        .get("id")
                        .filter(|v| truth(v))
                        .map(|v| property(Some(v)))
                        .unwrap_or_default();
                    if let Some(existing) = self.data.history_chats.get_mut(&id) {
                        assign_field(
                            existing,
                            "endOfHistoryTransferType",
                            chat.get("endOfHistoryTransferType"),
                        )?;
                    } else if !self.history_cache.contains(&id) {
                        self.data.history_chats.insert(id.clone(), chat.clone());
                        self.history_cache.insert(id.clone());
                        self.absorb(&id, true)?;
                    }
                }
                for contact in array(&payload["contacts"])? {
                    let id = property(contact.get("id"));
                    if let Some(existing) = self.data.history_contacts.get_mut(&id) {
                        merge(existing, contact)?;
                    } else {
                        let cache = format!("c:{id}");
                        if !self.history_cache.contains(&cache)
                            || ["notify", "name", "verifiedName"]
                                .iter()
                                .any(|k| truth(&contact[*k]))
                        {
                            self.data.history_contacts.insert(id, contact.clone());
                            self.history_cache.insert(cache);
                        }
                    }
                }
                for message in array(&payload["messages"])? {
                    let id = message_key(&message["key"])?;
                    if !self.data.history_messages.contains_key(&id)
                        && !self.history_cache.contains(&id)
                    {
                        self.data
                            .history_messages
                            .insert(id.clone(), message.clone());
                        self.history_cache.insert(id);
                    }
                }
                self.data.history_present = true;
                if let Some(past) = payload
                    .get("pastParticipants")
                    .and_then(Value::as_array)
                    .filter(|a| !a.is_empty())
                {
                    let old = self
                        .data
                        .history_metadata
                        .get("pastParticipants")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    let mut groups = IndexMap::<String, Value>::new();
                    for entry in old.iter().chain(past) {
                        object(entry)?;
                        let id = entry
                            .get("groupJid")
                            .filter(|v| !v.is_null())
                            .map(|v| property(Some(v)))
                            .unwrap_or_else(|| entry.to_string());
                        if let Some(existing) = groups.get_mut(&id) {
                            let participants = existing
                                .get_mut("pastParticipants")
                                .and_then(Value::as_array_mut)
                                .ok_or(Error::Input("past participants"))?;
                            let mut seen: IndexSet<_> =
                                participants.iter().map(participant_sig).collect();
                            if let Some(new) = entry["pastParticipants"].as_array() {
                                for p in new {
                                    if seen.insert(participant_sig(p)) {
                                        participants.push(p.clone());
                                    }
                                }
                            }
                        } else {
                            let mut entry = entry.clone();
                            entry["pastParticipants"] = entry
                                .get("pastParticipants")
                                .filter(|v| truth(v))
                                .cloned()
                                .unwrap_or(json!([]));
                            groups.insert(id, entry);
                        }
                    }
                    self.data.history_metadata.insert(
                        "pastParticipants".into(),
                        Value::Array(groups.into_values().collect()),
                    );
                }
                for field in [
                    "syncType",
                    "progress",
                    "chunkOrder",
                    "peerDataRequestSessionId",
                ] {
                    if let Some(value) = payload.get(field) {
                        self.data
                            .history_metadata
                            .insert(field.into(), value.clone());
                    } else {
                        self.data.history_metadata.shift_remove(field);
                    }
                }
                let latest = payload
                    .get("isLatest")
                    .filter(|v| truth(v))
                    .cloned()
                    .or_else(|| self.data.history_metadata.get("isLatest").cloned())
                    .unwrap_or(Value::Bool(false));
                self.data.history_metadata.insert("isLatest".into(), latest);
            }
            "chats.upsert" => {
                for chat in array(&payload)? {
                    object(chat)?;
                    let id = chat
                        .get("id")
                        .filter(|v| truth(v))
                        .map(|v| property(Some(v)))
                        .unwrap_or_default();
                    let history = !id.is_empty()
                        && !self.data.chat_upserts.contains_key(&id)
                        && self.data.history_chats.contains_key(&id);
                    let map = if history {
                        &mut self.data.history_chats
                    } else {
                        &mut self.data.chat_upserts
                    };
                    if let Some(prior) = map.get_mut(&id) {
                        *prior = concat(prior.clone(), chat.clone())?;
                    } else {
                        map.insert(id.clone(), chat.clone());
                    }
                    self.absorb(&id, history)?;
                    self.data.chat_deletes.shift_remove(&id);
                }
            }
            "chats.update" => {
                for update in array(&payload)? {
                    self.chat_update(ChatUpdate {
                        data: update.clone(),
                        condition: None,
                    })?;
                }
            }
            "chats.delete" => {
                for id in array(&payload)? {
                    let id = property(Some(id));
                    self.data.chat_deletes.insert(id.clone());
                    self.data.chat_updates.shift_remove(&id);
                    self.data.chat_upserts.shift_remove(&id);
                    self.data.history_chats.shift_remove(&id);
                }
            }
            "contacts.upsert" => {
                for contact in array(&payload)? {
                    object(contact)?;
                    let id = property(contact.get("id"));
                    if let Some(prior) = self
                        .data
                        .contact_upserts
                        .get_mut(&id)
                        .or_else(|| self.data.history_contacts.get_mut(&id))
                    {
                        merge(prior, contact)?;
                    } else {
                        self.data
                            .contact_upserts
                            .insert(id.clone(), contact.clone());
                    } // Source drops the detached prior-update object after assigning the new contact.
                    self.data.contact_updates.shift_remove(&id);
                }
            }
            "contacts.update" => {
                for update in array(&payload)? {
                    let id = property(update.get("id"));
                    if let Some(prior) = self
                        .data
                        .history_contacts
                        .get_mut(&id)
                        .or_else(|| self.data.contact_upserts.get_mut(&id))
                    {
                        merge(prior, update)?;
                    } else {
                        merge(
                            self.data.contact_updates.entry(id).or_insert(json!({})),
                            update,
                        )?;
                    }
                }
            }
            "messages.upsert" => {
                let kind = payload.get("type").cloned().unwrap_or(Value::Null);
                for original in array(&payload["messages"])? {
                    let mut message = original.clone();
                    let id = message_key(&message["key"])?;
                    if let Some(existing) = self
                        .data
                        .message_upserts
                        .get(&id)
                        .map(|v| &v["message"])
                        .or_else(|| self.data.history_messages.get(&id))
                    {
                        assign_field(
                            &mut message,
                            "messageTimestamp",
                            existing.get("messageTimestamp"),
                        )?;
                    }
                    if let Some(update) = self.data.message_updates.shift_remove(&id) {
                        merge(&mut message, &update["update"])?;
                    }
                    if self.data.history_messages.contains_key(&id) {
                        self.data.history_messages.insert(id, message);
                    } else {
                        let kind = if kind == "notify"
                            || self
                                .data
                                .message_upserts
                                .get(&id)
                                .is_some_and(|v| v["type"] == "notify")
                        {
                            json!("notify")
                        } else {
                            kind.clone()
                        };
                        self.data
                            .message_upserts
                            .insert(id, json!({"message":message,"type":kind}));
                    }
                }
            }
            "messages.update" => {
                for update in array(&payload)? {
                    let key = &update["key"];
                    let id = message_key(key)?;
                    let existing = self.data.history_messages.get_mut(&id).or_else(|| {
                        self.data
                            .message_upserts
                            .get_mut(&id)
                            .and_then(|v| v.get_mut("message"))
                    });
                    if let Some(existing) = existing {
                        merge(existing, &update["update"])?;
                        let message = existing.clone();
                        if update["update"]["status"].as_f64() == Some(4.0)
                            && !truth(&key["fromMe"])
                        {
                            self.decrement(&message)?;
                        }
                    } else {
                        let prior = self
                            .data
                            .message_updates
                            .entry(id)
                            .or_insert_with(|| json!({"key":key,"update":{}}));
                        merge(&mut prior["update"], &update["update"])?;
                    }
                }
            }
            "messages.delete" => {
                if let Some(keys) = payload.get("keys") {
                    for key in array(keys)? {
                        let id = message_key(key)?;
                        self.data
                            .message_deletes
                            .entry(id.clone())
                            .or_insert(key.clone());
                        self.data.message_upserts.shift_remove(&id);
                        self.data.message_updates.shift_remove(&id);
                    }
                }
            }
            "messages.reaction" | "message-receipt.update" => {
                for entry in array(&payload)? {
                    let id = message_key(&entry["key"])?;
                    let reaction = event == "messages.reaction";
                    let field = if reaction { "reactions" } else { "userReceipt" };
                    let update = &entry[if reaction { "reaction" } else { "receipt" }];
                    let message = if let Some(message) = self.data.message_upserts.get_mut(&id) {
                        message
                            .get_mut("message")
                            .ok_or(Error::Input("upsert message"))?
                    } else {
                        let map = if reaction {
                            &mut self.data.message_reactions
                        } else {
                            &mut self.data.message_receipts
                        };
                        map.entry(id)
                            .or_insert_with(|| json!({"key":entry["key"],field:[]}))
                    };
                    update_list(message, update, reaction)?;
                }
            }
            "groups.update" => {
                for group in array(&payload)? {
                    object(group)?;
                    let id = property(group.get("id"));
                    self.data.group_updates.entry(id).or_insert(group.clone());
                }
            }
            _ => return Err(Error::Input("unsupported buffered event")),
        }
        Ok(())
    }
    fn decrement(&mut self, message: &Value) -> Result<(), Error> {
        let id = property(message["key"].get("remoteJid"));
        let chat = self
            .data
            .chat_updates
            .get_mut(&id)
            .map(|v| &mut v.data)
            .or_else(|| self.data.chat_upserts.get_mut(&id));
        if let Some(chat) = chat
            && real(message)
            && !truth(&message["key"]["fromMe"])
            && !truth(&message["messageStubType"])
            && let Some(count) = chat["unreadCount"].as_f64().filter(|v| *v > 0.0)
        {
            if count == 1.0 {
                object_mut(chat)?.remove("unreadCount");
            } else {
                chat["unreadCount"] = json!(count - 1.0);
            }
        }
        Ok(())
    }
}
fn single(event: &str, data: Value) -> Batch {
    Batch {
        events: IndexMap::from([(event.into(), data)]),
        chat_conditions: vec![],
    }
}
fn object(v: &Value) -> Result<&serde_json::Map<String, Value>, Error> {
    v.as_object().ok_or(Error::Input("object payload"))
}
fn object_mut(v: &mut Value) -> Result<&mut serde_json::Map<String, Value>, Error> {
    v.as_object_mut().ok_or(Error::Input("object payload"))
}
fn array(v: &Value) -> Result<&Vec<Value>, Error> {
    v.as_array().ok_or(Error::Input("array payload"))
}
fn merge(a: &mut Value, b: &Value) -> Result<(), Error> {
    let source = object(b)?.clone();
    object_mut(a)?.extend(source);
    Ok(())
}
fn assign_field(a: &mut Value, key: &str, value: Option<&Value>) -> Result<(), Error> {
    let target = object_mut(a)?;
    if let Some(v) = value {
        target.insert(key.into(), v.clone());
    } else {
        target.remove(key);
    }
    Ok(())
}
fn concat(mut a: Value, mut b: Value) -> Result<Value, Error> {
    if b.get("unreadCount") == Some(&Value::Null)
        && a["unreadCount"].as_f64().is_some_and(|v| v < 0.0)
    {
        object_mut(&mut a)?.remove("unreadCount");
        object_mut(&mut b)?.remove("unreadCount");
    }
    if let (Some(x), Some(y)) = (a["unreadCount"].as_f64(), b["unreadCount"].as_f64())
        && y >= 0.0
    {
        b["unreadCount"] = json!(y.max(0.0) + x.max(0.0));
    }
    merge(&mut a, &b)?;
    Ok(a)
}
fn truth(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(v) => v.as_f64().is_some_and(|v| v != 0.0),
        Value::String(v) => !v.is_empty(),
        _ => true,
    }
}
fn number(v: &Value) -> f64 {
    match v {
        Value::Null => 0.0,
        Value::Bool(v) => {
            if *v {
                1.0
            } else {
                0.0
            }
        }
        Value::Number(v) => v.as_f64().unwrap_or(f64::NAN),
        Value::String(v) => crate::privacy::number(v),
        _ => crate::privacy::number(&property(Some(v))),
    }
}
fn property(v: Option<&Value>) -> String {
    match v {
        None => "undefined".into(),
        Some(Value::Null) => "null".into(),
        Some(Value::String(v)) => v.clone(),
        Some(Value::Array(v)) => v
            .iter()
            .map(|v| {
                if v.is_null() {
                    String::new()
                } else {
                    property(Some(v))
                }
            })
            .collect::<Vec<_>>()
            .join(","),
        Some(Value::Object(_)) => "[object Object]".into(),
        Some(v) => v.to_string(),
    }
}
fn message_key(key: &Value) -> Result<String, Error> {
    object(key)?;
    Ok(format!(
        "{},{},{}",
        property(key.get("remoteJid")),
        property(key.get("id")),
        if truth(&key["fromMe"]) { "1" } else { "0" }
    ))
}
fn participant_sig(p: &Value) -> String {
    ["userJid", "leaveTs", "leaveReason"]
        .iter()
        .map(|k| {
            p.get(*k)
                .filter(|v| truth(v))
                .map(|v| property(Some(v)))
                .unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join(":")
}
fn values(map: &IndexMap<String, Value>) -> Vec<Value> {
    let mut indexed = map.iter().enumerate().collect::<Vec<_>>();
    indexed.sort_by_key(|(i, (key, _))| {
        key.parse::<u32>()
            .ok()
            .filter(|v| *v != u32::MAX && v.to_string() == **key)
            .map(|v| (0, u64::from(v)))
            .unwrap_or((1, *i as u64))
    });
    indexed.into_iter().map(|(_, (_, v))| v.clone()).collect()
}
fn real(message: &Value) -> bool {
    let absent = Value::Null;
    let mut content = &message["message"];
    for _ in 0..5 {
        let next = [
            "ephemeralMessage",
            "viewOnceMessage",
            "documentWithCaptionMessage",
            "viewOnceMessageV2",
            "viewOnceMessageV2Extension",
            "editedMessage",
            "associatedChildMessage",
            "groupStatusMessage",
            "groupStatusMessageV2",
        ]
        .iter()
        .find_map(|k| content.get(*k).filter(|v| truth(v)));
        if let Some(v) = next {
            content = v.get("message").unwrap_or(&absent)
        } else {
            break;
        }
    }
    content.as_object().is_some_and(|o| {
        o.keys().any(|k| {
            k == "conversation" || k.contains("Message") && k != "senderKeyDistributionMessage"
        }) && !truth(&content["protocolMessage"])
            && !truth(&content["reactionMessage"])
            && !truth(&content["pollUpdateMessage"])
    })
}
fn author(key: &Value) -> Result<String, Error> {
    let value = if truth(&key["fromMe"]) {
        return Ok("me".into());
    } else {
        key.get("participantAlt")
            .filter(|v| truth(v))
            .or_else(|| key.get("remoteJidAlt").filter(|v| truth(v)))
            .or_else(|| key.get("participant").filter(|v| truth(v)))
            .or_else(|| key.get("remoteJid"))
    };
    Ok(value
        .filter(|v| truth(v))
        .map(|v| property(Some(v)))
        .unwrap_or_default())
}
fn update_list(message: &mut Value, update: &Value, reaction: bool) -> Result<(), Error> {
    object(update)?;
    let field = if reaction { "reactions" } else { "userReceipt" };
    if !message.get(field).is_some_and(truth) {
        message[field] = json!([]);
    }
    if reaction {
        let id = author(&update["key"])?;
        let prior = array(&message[field])?
            .iter()
            .filter_map(|r| match author(&r["key"]) {
                Ok(author) if author != id => Some(Ok(r.clone())),
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let mut reaction = update.clone();
        if !truth(&reaction["text"]) {
            reaction["text"] = json!("");
        }
        let mut prior = prior;
        prior.push(reaction);
        message[field] = Value::Array(prior);
    } else {
        let list = message[field]
            .as_array_mut()
            .ok_or(Error::Input("receipt array"))?;
        if let Some(prior) = list
            .iter_mut()
            .find(|r| r.get("userJid") == update.get("userJid"))
        {
            merge(prior, update)?;
        } else {
            list.push(update.clone());
        }
    }
    Ok(())
}
fn consolidate(data: &Data) -> EventMap {
    let mut events = EventMap::new();
    if data.history_present {
        let mut history = json!({"chats":values(&data.history_chats),"messages":values(&data.history_messages),"contacts":values(&data.history_contacts)});
        object_mut(&mut history).unwrap().extend(
            data.history_metadata
                .iter()
                .map(|(k, v)| (k.clone(), v.clone())),
        );
        events.insert("messaging-history.set".into(), history);
    }
    if !data.chat_upserts.is_empty() {
        events.insert(
            "chats.upsert".into(),
            Value::Array(values(&data.chat_upserts)),
        );
    }
    if !data.chat_updates.is_empty() {
        let map = data
            .chat_updates
            .iter()
            .map(|(k, u)| (k.clone(), u.data.clone()))
            .collect();
        events.insert("chats.update".into(), Value::Array(values(&map)));
    }
    if !data.chat_deletes.is_empty() {
        events.insert("chats.delete".into(), json!(data.chat_deletes));
    }
    if !data.message_upserts.is_empty() {
        let list = values(&data.message_upserts);
        events.insert("messages.upsert".into(),json!({"messages":list.iter().map(|v|&v["message"]).collect::<Vec<_>>(),"type":list[0]["type"]}));
    }
    if !data.message_updates.is_empty() {
        events.insert(
            "messages.update".into(),
            Value::Array(values(&data.message_updates)),
        );
    }
    if !data.message_deletes.is_empty() {
        events.insert(
            "messages.delete".into(),
            json!({"keys":values(&data.message_deletes)}),
        );
    }
    for (name, map, field, item) in [
        (
            "messages.reaction",
            &data.message_reactions,
            "reactions",
            "reaction",
        ),
        (
            "message-receipt.update",
            &data.message_receipts,
            "userReceipt",
            "receipt",
        ),
    ] {
        let list = values(map)
            .iter()
            .flat_map(|v| {
                v[field]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|r| json!({"key":v["key"],item:r}))
            })
            .collect::<Vec<_>>();
        if !list.is_empty() {
            events.insert(name.into(), Value::Array(list));
        }
    }
    for (name, map) in [
        ("contacts.upsert", &data.contact_upserts),
        ("contacts.update", &data.contact_updates),
        ("groups.update", &data.group_updates),
    ] {
        if !map.is_empty() {
            events.insert(name.into(), Value::Array(values(map)));
        }
    }
    events
}
