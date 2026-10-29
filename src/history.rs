//! History download, inflation and chat/message/contact extraction.
mod status;
use crate::{auth::Contact, media, proto, relay, signal::mapping::Mapping};
use flate2::{Decompress, FlushDecompress, Status};
use futures_util::future::BoxFuture;
use prost::Message as _;
pub use status::{ProgressState, StatusTracker, StatusUpdate};
use std::{io::Read, sync::Arc};
use tokio_util::io::SyncIoBridge;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Media(#[from] media::Error),
    #[error(transparent)]
    Protobuf(#[from] prost::DecodeError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Inflate(#[from] flate2::DecompressError),
    #[error("truncated compressed history")]
    Truncated,
    #[error("invalid history {0}")]
    Input(&'static str),
    #[error("history worker failed")]
    Task,
}
#[derive(Debug, Clone, PartialEq)]
pub struct Chat {
    pub conversation: proto::Conversation,
    pub last_message_recv_timestamp: Option<f64>,
    /// Buffered runtime fields that cannot be represented by Conversation.
    /// These override the protocol projection, including explicit nulls.
    pub runtime_fields: serde_json::Map<String, serde_json::Value>,
}
impl crate::proto_json::JsonField for Chat {
    fn to_value(&self) -> serde_json::Value {
        let mut value = self.conversation.to_value();
        let object = value.as_object_mut().expect("Conversation JSON object");
        if let Some(timestamp) = self.last_message_recv_timestamp {
            object.insert("lastMessageRecvTimestamp".into(), timestamp.into());
        }
        object.extend(self.runtime_fields.clone());
        value
    }
    fn from_value(value: &serde_json::Value) -> Result<Self, crate::proto_json::Error> {
        use crate::proto_json::{Error, object};
        let mut fields = object(value)?.clone();
        if !fields.get("id").is_some_and(serde_json::Value::is_string) {
            return Err(Error::Input("chat id"));
        }
        let timestamp = fields.remove("lastMessageRecvTimestamp");
        let last_message_recv_timestamp = timestamp.as_ref().and_then(serde_json::Value::as_f64);
        let candidate =
            proto::Conversation::from_value(&serde_json::Value::Object(fields.clone()))?;
        let canonical = candidate.to_value();
        let mut runtime_fields = serde_json::Map::new();
        // Do not expose protobufjs' scalar coercion (e.g. a wrapped uint32 pin
        // timestamp) as protocol data. Keep the exact runtime value instead.
        fields.retain(|name, value| {
            if canonical.get(name) == Some(value) {
                true
            } else {
                runtime_fields.insert(name.clone(), value.clone());
                false
            }
        });
        if let Some(timestamp) = timestamp
            && last_message_recv_timestamp.is_none()
        {
            runtime_fields.insert("lastMessageRecvTimestamp".into(), timestamp);
        }
        Ok(Self {
            conversation: proto::Conversation::from_value(&serde_json::Value::Object(fields))?,
            last_message_recv_timestamp,
            runtime_fields,
        })
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Processed {
    pub chats: Vec<Chat>,
    pub contacts: Vec<Contact>,
    pub messages: Vec<proto::WebMessageInfo>,
    /// Receive metadata retained when buffered message upserts enter history.
    /// Empty for history downloaded from the protocol; otherwise message-aligned.
    pub message_metadata: Vec<Option<crate::messages::EnvelopeMetadata>>,
    pub message_runtime_fields: Vec<serde_json::Map<String, serde_json::Value>>,
    pub lid_pn_mappings: Vec<Mapping>,
    pub past_participants: Vec<proto::PastParticipants>,
    pub sync_type: i32,
    pub progress: Option<u32>,
}
impl Processed {
    /// Protocol JSON with buffered runtime overrides applied.
    pub fn message_value(&self, index: usize) -> Option<serde_json::Value> {
        use crate::proto_json::JsonField;
        let mut value = self.messages.get(index)?.to_value();
        if let Some(runtime) = self.message_runtime_fields.get(index) {
            value
                .as_object_mut()
                .expect("message object")
                .extend(runtime.clone());
        }
        Some(value)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Set {
    pub data: Processed,
    pub is_latest: Option<bool>,
    pub chunk_order: Option<u32>,
    pub peer_data_request_session_id: Option<String>,
    /// Consolidated by the event buffer, whose set has another shape than
    /// the one processMessage emits directly.
    pub buffered: bool,
}
impl Set {
    /// Whether this is the on-demand (type 6) answer to the request with the
    /// given id for `chat`: its session id is the request's, or, should the
    /// phone use another id, its messages are from that chat. Unverified live.
    pub fn answers(&self, request_id: &str, chat: &str) -> bool {
        self.data.sync_type == 6
            && (self.peer_data_request_session_id.as_deref() == Some(request_id)
                || self
                    .data
                    .messages
                    .iter()
                    .any(|m| m.key.as_ref().and_then(|k| k.remote_jid.as_deref()) == Some(chat)))
    }
}
fn nonempty(v: &Option<String>) -> Option<String> {
    v.as_ref().filter(|s| !s.is_empty()).cloned()
}
fn pn(id: &str) -> bool {
    id.ends_with("@s.whatsapp.net") || id.ends_with("@hosted")
}
fn lid(id: &str) -> bool {
    id.ends_with("@lid") || id.ends_with("@hosted.lid")
}
pub fn notification(
    message: Option<&proto::Message>,
) -> Option<proto::message::HistorySyncNotification> {
    relay::normalize(message)?
        .protocol_message
        .as_ref()?
        .history_sync_notification
        .clone()
}
pub fn process(item: &mut proto::HistorySync) -> Result<Processed, Error> {
    // Shape only, no content: which parts of the payload are present.
    tracing::debug!(
        sync_type = item.sync_type,
        chunk_order = ?item.chunk_order,
        progress = ?item.progress,
        conversations = item.conversations.len(),
        conversation_messages = item.conversations.iter().map(|c| c.messages.len()).sum::<usize>(),
        status_v3_messages = item.status_v3_messages.len(),
        pushnames = item.pushnames.len(),
        global_settings = item.global_settings.is_some(),
        recent_stickers = item.recent_stickers.len(),
        past_participants = item.past_participants.len(),
        call_log_records = item.call_log_records.len(),
        phone_number_to_lid_mappings = item.phone_number_to_lid_mappings.len(),
        accounts = item.accounts.len(),
        "history sync payload"
    );
    let mut result = Processed {
        chats: vec![],
        contacts: vec![],
        messages: vec![],
        message_metadata: vec![],
        message_runtime_fields: vec![],
        lid_pn_mappings: vec![],
        past_participants: item.past_participants.clone(),
        sync_type: item.sync_type,
        progress: item.progress,
    };
    for pair in &item.phone_number_to_lid_mappings {
        if let (Some(lid), Some(pn)) = (nonempty(&pair.lid_jid), nonempty(&pair.pn_jid)) {
            result.lid_pn_mappings.push(Mapping { lid, pn });
        }
    }
    match item.sync_type {
        0 | 2 | 3 | 6 => {
            for chat in &mut item.conversations {
                result.contacts.push(Contact {
                    id: chat.id.clone(),
                    name: nonempty(&chat.display_name)
                        .or_else(|| nonempty(&chat.name))
                        .or_else(|| nonempty(&chat.username)),
                    username: nonempty(&chat.username),
                    lid: nonempty(&chat.lid_jid).or_else(|| nonempty(&chat.account_lid)),
                    phone_number: nonempty(&chat.pn_jid),
                    ..Default::default()
                });
                let pair = if lid(&chat.id) {
                    nonempty(&chat.pn_jid)
                        .or_else(|| {
                            chat.messages
                                .iter()
                                .filter_map(|m| m.message.as_ref())
                                .filter(|m| m.key.as_ref().and_then(|k| k.from_me) == Some(true))
                                .filter_map(|m| m.user_receipt.first().map(|r| &r.user_jid))
                                .find(|jid| pn(jid))
                                .cloned()
                        })
                        .map(|pn| Mapping {
                            lid: chat.id.clone(),
                            pn,
                        })
                } else if pn(&chat.id) {
                    nonempty(&chat.lid_jid).map(|lid| Mapping {
                        lid,
                        pn: chat.id.clone(),
                    })
                } else {
                    None
                };
                if let Some(pair) = pair {
                    result.lid_pn_mappings.push(pair);
                }
                let messages = std::mem::take(&mut chat.messages);
                let mut received = None;
                for record in messages {
                    if chat.messages.is_empty() {
                        chat.messages.push(proto::HistorySyncMsg {
                            message: record.message.clone(),
                            msg_order_id: None,
                        });
                    }
                    let message = record.message.ok_or(Error::Input("message"))?;
                    let key = message.key.as_ref().ok_or(Error::Input("message key"))?;
                    if !key.from_me.unwrap_or(false) && received.is_none_or(|t| t == 0.0) {
                        received = Some(message.message_timestamp.unwrap_or(0) as f64);
                    }
                    use proto::web_message_info::StubType;
                    if matches!(
                        message
                            .message_stub_type
                            .and_then(|t| StubType::try_from(t).ok()),
                        Some(StubType::BizPrivacyModeToBsp | StubType::BizPrivacyModeToFb)
                    ) && let Some(name) = message
                        .message_stub_parameters
                        .first()
                        .filter(|s| !s.is_empty())
                    {
                        result.contacts.push(Contact {
                            id: key
                                .participant
                                .as_ref()
                                .filter(|s| !s.is_empty())
                                .or(key.remote_jid.as_ref())
                                .cloned()
                                .ok_or(Error::Input("business contact id"))?,
                            verified_name: Some(name.clone()),
                            ..Default::default()
                        });
                    }
                    result.messages.push(message);
                }
                result.chats.push(Chat {
                    conversation: chat.clone(),
                    last_message_recv_timestamp: received,
                    runtime_fields: Default::default(),
                });
            }
        }
        4 => {
            for c in &item.pushnames {
                result.contacts.push(Contact {
                    id: c.id.clone().ok_or(Error::Input("pushname id"))?,
                    notify: c.pushname.clone(),
                    ..Default::default()
                });
            }
        }
        _ => {}
    }
    Ok(result)
}
/// Strict zlib inflation with bounded compressed input and output scratch space.
/// Like Node inflate, trailing bytes after the first zlib stream are ignored.
pub fn inflate(mut input: impl Read) -> Result<Vec<u8>, Error> {
    let mut decoder = Decompress::new(true);
    let mut compressed = [0; 64 * 1024];
    let mut scratch = [0; 64 * 1024];
    let mut output = Vec::new();
    loop {
        let read = input.read(&mut compressed)?;
        if read == 0 {
            return Err(Error::Truncated);
        }
        let mut offset = 0;
        loop {
            let before_in = decoder.total_in();
            let before_out = decoder.total_out();
            let status = decoder.decompress(
                &compressed[offset..read],
                &mut scratch,
                FlushDecompress::None,
            )?;
            let consumed = (decoder.total_in() - before_in) as usize;
            let produced = (decoder.total_out() - before_out) as usize;
            output.extend_from_slice(&scratch[..produced]);
            offset += consumed;
            if status == Status::StreamEnd {
                std::io::copy(&mut input, &mut std::io::sink())?;
                return Ok(output);
            }
            if consumed == 0 && produced == 0 {
                break;
            }
            if offset == read && produced < scratch.len() {
                break;
            }
        }
    }
}
type Download = dyn Fn(
        proto::message::HistorySyncNotification,
    ) -> BoxFuture<'static, Result<proto::HistorySync, Error>>
    + Send
    + Sync;
type DownloadRaw = dyn Fn(proto::message::HistorySyncNotification) -> BoxFuture<'static, Result<Vec<u8>, Error>>
    + Send
    + Sync;
/// The inflated `HistorySync` protobuf of one history notification, exactly as
/// the phone sent it (unknown fields included). Not upstream.
#[derive(Debug, Clone, PartialEq)]
pub struct Payload {
    /// The id of the message that carried the notification.
    pub message_id: Option<String>,
    pub sync_type: Option<i32>,
    pub chunk_order: Option<u32>,
    pub peer_data_request_session_id: Option<String>,
    pub bytes: Arc<Vec<u8>>,
}
#[derive(Clone)]
pub struct Downloader(Arc<Download>, Option<Arc<DownloadRaw>>);
impl std::fmt::Debug for Downloader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoryDownloader").finish_non_exhaustive()
    }
}
impl Downloader {
    pub fn new<F, Fut>(download: F) -> Self
    where
        F: Fn(proto::message::HistorySyncNotification) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<proto::HistorySync, Error>> + Send + 'static,
    {
        Self(Arc::new(move |n| Box::pin(download(n))), None)
    }
    /// A downloader that yields the inflated protobuf bytes, so that
    /// [`download_and_process_raw`](Self::download_and_process_raw) can keep them.
    pub fn new_raw<F, Fut>(download: F) -> Self
    where
        F: Fn(proto::message::HistorySyncNotification) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Vec<u8>, Error>> + Send + 'static,
    {
        let raw: Arc<DownloadRaw> = Arc::new(move |n| Box::pin(download(n)));
        let decode = raw.clone();
        Self(
            Arc::new(move |n| {
                let raw = decode.clone();
                Box::pin(async move {
                    let bytes = raw(n).await?;
                    tokio::task::spawn_blocking(move || {
                        proto::HistorySync::decode(bytes.as_slice()).map_err(Error::from)
                    })
                    .await
                    .map_err(|_| Error::Task)?
                })
            }),
            Some(raw),
        )
    }
    pub fn from_http(client: media::HttpClient, options: media::DownloadOptions) -> Self {
        Self::new_raw(move |notification| {
            let client = client.clone();
            let options = options.clone();
            async move {
                let url = media::download_url(
                    notification.direct_path.as_deref(),
                    None,
                    options.host.as_deref(),
                )?;
                let keys = media::Keys::derive(
                    notification
                        .media_key
                        .as_deref()
                        .ok_or(media::Error::MissingMediaKey)?,
                    media::MediaType::History,
                )?;
                let (output, input) = tokio::io::duplex(64 * 1024);
                let download =
                    client.download_to(&url, keys, options.range, options.headers, output);
                let inflated = async move {
                    tokio::task::spawn_blocking(move || inflate(SyncIoBridge::new(input)))
                        .await
                        .map_err(|_| Error::Task)?
                };
                let (_, bytes) =
                    tokio::try_join!(async { download.await.map_err(Error::from) }, inflated)?;
                Ok(bytes)
            }
        })
    }
    pub async fn download(
        &self,
        notification: proto::message::HistorySyncNotification,
    ) -> Result<proto::HistorySync, Error> {
        (self.0)(notification).await
    }
    pub async fn download_and_process(
        &self,
        notification: proto::message::HistorySyncNotification,
    ) -> Result<Processed, Error> {
        Ok(self.download_and_process_raw(notification, false).await?.0)
    }
    /// As [`download_and_process`](Self::download_and_process), and with `keep`
    /// also the inflated bytes, when they are available: from an inline payload,
    /// or from a downloader made with [`new_raw`](Self::new_raw) or
    /// [`from_http`](Self::from_http).
    pub async fn download_and_process_raw(
        &self,
        notification: proto::message::HistorySyncNotification,
        keep: bool,
    ) -> Result<(Processed, Option<Arc<Vec<u8>>>), Error> {
        let inline = notification.initial_hist_bootstrap_inline_payload.clone();
        let (mut history, raw) = match (inline, &self.1) {
            (Some(bytes), _) => {
                let bytes = tokio::task::spawn_blocking(move || inflate(bytes.as_slice()))
                    .await
                    .map_err(|_| Error::Task)??;
                decode_kept(bytes, keep).await?
            }
            (None, Some(raw)) if keep => decode_kept(raw(notification).await?, true).await?,
            _ => (self.download(notification).await?, None),
        };
        let processed = tokio::task::spawn_blocking(move || process(&mut history))
            .await
            .map_err(|_| Error::Task)??;
        Ok((processed, raw))
    }
}
async fn decode_kept(
    bytes: Vec<u8>,
    keep: bool,
) -> Result<(proto::HistorySync, Option<Arc<Vec<u8>>>), Error> {
    let bytes = Arc::new(bytes);
    let decoded = bytes.clone();
    let history = tokio::task::spawn_blocking(move || {
        proto::HistorySync::decode(decoded.as_slice()).map_err(Error::from)
    })
    .await
    .map_err(|_| Error::Task)??;
    Ok((history, keep.then_some(bytes)))
}
type ShouldSync = dyn Fn(&proto::message::HistorySyncNotification) -> bool + Send + Sync;
#[derive(Clone)]
pub struct Policy(Arc<ShouldSync>);
impl Default for Policy {
    fn default() -> Self {
        Self::new(|n| n.sync_type != Some(2))
    }
}
impl std::fmt::Debug for Policy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoryPolicy").finish_non_exhaustive()
    }
}
impl Policy {
    pub fn new<F: Fn(&proto::message::HistorySyncNotification) -> bool + Send + Sync + 'static>(
        predicate: F,
    ) -> Self {
        Self(Arc::new(predicate))
    }
    pub fn should_process(&self, n: &proto::message::HistorySyncNotification) -> bool {
        (self.0)(n) && n.sync_type.is_some_and(|t| (0..=6).contains(&t))
    }
}

/// The peer request upstream's fetchMessageHistory sends to the phone for
/// messages older than the given one.
pub fn on_demand_request(
    count: i32,
    oldest: &proto::MessageKey,
    oldest_timestamp_ms: i64,
) -> proto::message::PeerDataOperationRequestMessage {
    use proto::message::{PeerDataOperationRequestType, peer_data_operation_request_message};
    proto::message::PeerDataOperationRequestMessage {
        peer_data_operation_request_type: Some(
            PeerDataOperationRequestType::HistorySyncOnDemand as i32,
        ),
        history_sync_on_demand_request: Some(
            peer_data_operation_request_message::HistorySyncOnDemandRequest {
                chat_jid: oldest.remote_jid.clone(),
                oldest_msg_from_me: oldest.from_me,
                oldest_msg_id: oldest.id.clone(),
                oldest_msg_timestamp_ms: Some(oldest_timestamp_ms),
                on_demand_msg_count: Some(count),
                ..Default::default()
            },
        ),
        ..Default::default()
    }
}
