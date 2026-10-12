//! History download, inflation and chat/message/contact extraction.
use crate::{auth::Contact, media, proto, relay, signal::mapping::Mapping};
use flate2::{Decompress, FlushDecompress, Status};
use futures_util::future::BoxFuture;
use prost::Message as _;
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
}
#[derive(Debug, Clone, PartialEq)]
pub struct Processed {
    pub chats: Vec<Chat>,
    pub contacts: Vec<Contact>,
    pub messages: Vec<proto::WebMessageInfo>,
    pub lid_pn_mappings: Vec<Mapping>,
    pub past_participants: Vec<proto::PastParticipants>,
    pub sync_type: i32,
    pub progress: Option<u32>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct Set {
    pub data: Processed,
    pub is_latest: Option<bool>,
    pub chunk_order: Option<u32>,
    pub peer_data_request_session_id: Option<String>,
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
    let mut result = Processed {
        chats: vec![],
        contacts: vec![],
        messages: vec![],
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
#[derive(Clone)]
pub struct Downloader(Arc<Download>);
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
        Self(Arc::new(move |n| Box::pin(download(n))))
    }
    pub fn from_http(client: media::HttpClient, options: media::DownloadOptions) -> Self {
        Self::new(move |notification| {
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
                let decode = async move {
                    tokio::task::spawn_blocking(move || {
                        proto::HistorySync::decode(inflate(SyncIoBridge::new(input))?.as_slice())
                            .map_err(Error::from)
                    })
                    .await
                    .map_err(|_| Error::Task)?
                };
                let (_, history) =
                    tokio::try_join!(async { download.await.map_err(Error::from) }, decode)?;
                Ok(history)
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
        let mut history = if let Some(bytes) = notification.initial_hist_bootstrap_inline_payload {
            tokio::task::spawn_blocking(move || {
                proto::HistorySync::decode(inflate(bytes.as_slice())?.as_slice())
                    .map_err(Error::from)
            })
            .await
            .map_err(|_| Error::Task)??
        } else {
            self.download(notification).await?
        };
        tokio::task::spawn_blocking(move || process(&mut history))
            .await
            .map_err(|_| Error::Task)?
    }
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
