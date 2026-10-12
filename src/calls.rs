//! Call-state events, offer metadata caching and generated call messages.
use crate::{binary::BinaryNode, proto, proto_json::JsonField};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("missing call info in call node")]
    MissingInfo,
    #[error("missing call id")]
    MissingId,
    #[error("call cache failed: {0}")]
    Cache(String),
    #[error(transparent)]
    Json(#[from] crate::proto_json::Error),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Offer,
    Ringing,
    PreAccept,
    Transport,
    RelayLatency,
    Timeout,
    Reject,
    Accept,
    Terminate,
}
pub fn status(node: &BinaryNode) -> Status {
    match node.tag.as_str() {
        "offer" | "offer_notice" => Status::Offer,
        "terminate" if node.attrs.get("reason").is_some_and(|r| r == "timeout") => Status::Timeout,
        "terminate" => Status::Terminate,
        "preaccept" => Status::PreAccept,
        "transport" => Status::Transport,
        "relaylatency" => Status::RelayLatency,
        "reject" => Status::Reject,
        "accept" => Status::Accept,
        _ => Status::Ringing,
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caller_pn: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_group: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_jid: Option<String>,
    pub id: String,
    /// JavaScript Date's clipped integral milliseconds; None represents an invalid date.
    pub date_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_video: Option<bool>,
    pub status: Status,
    pub offline: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<f64>,
}
pub trait OfferCache: Send + Sync {
    fn get<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<Option<Event>, Error>>;
    fn set<'a>(&'a self, id: &'a str, event: Event) -> BoxFuture<'a, Result<(), Error>>;
    fn delete<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<(), Error>>;
}
#[derive(Clone)]
pub struct Cache(Arc<dyn OfferCache>);
impl std::fmt::Debug for Cache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CallCache(<provider>)")
    }
}
impl Cache {
    pub fn new(provider: impl OfferCache + 'static) -> Self {
        Self(Arc::new(provider))
    }
    pub fn from_arc(provider: Arc<dyn OfferCache>) -> Self {
        Self(provider)
    }
    pub async fn get(&self, id: &str) -> Result<Option<Event>, Error> {
        self.0.get(id).await
    }
    pub async fn set(&self, id: &str, event: Event) -> Result<(), Error> {
        self.0.set(id, event).await
    }
    pub async fn delete(&self, id: &str) -> Result<(), Error> {
        self.0.delete(id).await
    }
}
impl Default for Cache {
    fn default() -> Self {
        Self::new(MemoryCache::default())
    }
}
/// Built-in five-minute expiry. Reads and writes prune expired entries without a timer task.
#[derive(Default)]
pub struct MemoryCache(Mutex<HashMap<String, (Event, Instant)>>);
impl OfferCache for MemoryCache {
    fn get<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<Option<Event>, Error>> {
        Box::pin(async move {
            let now = Instant::now();
            let mut values = self.0.lock().expect("call cache mutex");
            values.retain(|_, (_, expiry)| now <= *expiry);
            Ok(values.get(id).map(|(call, _)| call.clone()))
        })
    }
    fn set<'a>(&'a self, id: &'a str, event: Event) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            let now = Instant::now();
            let mut values = self.0.lock().expect("call cache mutex");
            values.retain(|_, (_, expiry)| now <= *expiry);
            values.insert(id.into(), (event, now + Duration::from_secs(300)));
            Ok(())
        })
    }
    fn delete<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.0.lock().expect("call cache mutex").remove(id);
            Ok(())
        })
    }
}
pub async fn process(node: &BinaryNode, cache: &Cache) -> Result<Event, Error> {
    let info = node.all_children().first().ok_or(Error::MissingInfo)?;
    let status = status(info);
    let id = info.attrs.get("call-id").cloned().ok_or(Error::MissingId)?;
    let timestamp = node
        .attrs
        .get("t")
        .map(|s| crate::privacy::number(s) * 1000.0)
        .unwrap_or(f64::NAN);
    let date_ms =
        (timestamp.is_finite() && timestamp.abs() <= 8.64e15).then(|| timestamp.trunc() as i64);
    let mut call = Event {
        chat_id: node.attrs.get("from").cloned(),
        from: info
            .attrs
            .get("from")
            .filter(|s| !s.is_empty())
            .or_else(|| info.attrs.get("call-creator"))
            .cloned(),
        caller_pn: info.attrs.get("caller_pn").cloned(),
        id,
        date_ms,
        status,
        offline: node.attrs.get("offline").is_some_and(|s| !s.is_empty()),
        is_group: None,
        group_jid: None,
        is_video: None,
        latency_ms: None,
    };
    if status == Status::RelayLatency {
        call.latency_ms = ["latency", "latency_ms", "latency-ms"]
            .into_iter()
            .find_map(|key| info.attrs.get(key).filter(|s| !s.is_empty()))
            .map(|s| crate::privacy::number(s))
            .filter(|n| n.is_finite());
    }
    if status == Status::Offer {
        call.is_video = Some(info.child("video").is_some());
        call.is_group = Some(
            info.attrs.get("type").is_some_and(|t| t == "group")
                || info.attrs.get("group-jid").is_some_and(|s| !s.is_empty()),
        );
        call.group_jid = info.attrs.get("group-jid").cloned();
        cache.set(&call.id, call.clone()).await?;
    }
    if let Some(existing) = cache.get(&call.id).await? {
        call.is_video = existing.is_video;
        call.is_group = existing.is_group;
        if call.caller_pn.as_ref().is_none_or(|s| s.is_empty()) {
            call.caller_pn = existing.caller_pn;
        }
    }
    if matches!(
        status,
        Status::Reject | Status::Accept | Status::Timeout | Status::Terminate
    ) {
        cache.delete(&call.id).await?;
    }
    Ok(call)
}
pub fn message(call: &Event) -> Result<Option<proto::WebMessageInfo>, Error> {
    let should_publish = call.status == Status::Timeout
        || (call.status == Status::Offer && call.is_group == Some(true));
    if !should_publish {
        return Ok(None);
    }
    let mut result = proto::WebMessageInfo {
        key: Some(proto::MessageKey {
            remote_jid: call.chat_id.clone(),
            id: Some(call.id.clone()),
            from_me: Some(false),
            ..Default::default()
        }),
        message_timestamp: Some(u64::from_value(&serde_json::json!(
            call.date_ms
                .map(|ms| (ms as f64 / 1000.0).floor())
                .unwrap_or(0.0)
        ))?),
        ..Default::default()
    };
    if call.status == Status::Timeout {
        use proto::web_message_info::StubType;
        result.message_stub_type = Some(match (
            call.is_group == Some(true),
            call.is_video == Some(true),
        ) {
            (true, true) => StubType::CallMissedGroupVideo,
            (true, false) => StubType::CallMissedGroupVoice,
            (false, true) => StubType::CallMissedVideo,
            (false, false) => StubType::CallMissedVoice,
        } as i32);
    } else {
        result.message = Some(proto::Message {
            call: Some(Box::new(proto::message::Call {
                call_key: Some(call.id.as_bytes().to_vec()),
                ..Default::default()
            })),
            ..Default::default()
        });
    }
    Ok(Some(result))
}
