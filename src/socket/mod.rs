//! Async WhatsApp WebSocket connection with exclusive Noise ownership and IQ routing.
pub mod acks;
mod actor;
mod app_state;
mod business;
mod calls;
mod chats;
mod communities;
pub use business::{ProductCreate, ProductUpdate};
pub mod dispatch;
mod event_responses;
mod events;
mod group_effects;
mod groups;
mod incoming_messages;
mod incoming_receipts;
pub mod initial_sync;
mod media;
mod newsletters;
mod notifications;
pub mod presence;
mod protocol_messages;
mod proxy;
mod receipts;
mod relay;
mod retries;
mod send;
pub mod startup;
use crate::{
    auth::{self, Credentials, SqliteAuthStore},
    binary::{self, BinaryNode},
    connection::{self, PayloadConfig},
    crypto, defaults,
    noise::{self, NoiseHandler},
    proto,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::{SinkExt, StreamExt};
pub use notifications::IdentityChange;
use prost::Message;
pub use relay::{RelayOptions, ServerProps};
pub use send::{SendOptions, send_metadata};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{broadcast, mpsc, oneshot, watch},
    time::timeout,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{self, client::IntoClientRequest},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Call(#[from] crate::calls::Error),
    #[error(transparent)]
    Notification(#[from] crate::notifications::Error),
    #[error(transparent)]
    Chat(#[from] crate::chats::Error),
    #[error(transparent)]
    Mex(#[from] crate::newsletters::Error),
    #[error(transparent)]
    Wam(#[from] crate::wam::Error),
    #[error(transparent)]
    EventBuffer(#[from] crate::event_buffer::Error),
    #[error(transparent)]
    EventJson(#[from] crate::proto_json::Error),
    #[error("socket event projection failed: {0}")]
    EventProjection(String),
    #[error(transparent)]
    History(#[from] crate::history::Error),
    #[error(transparent)]
    AppState(#[from] crate::app_state::Error),
    #[error(transparent)]
    Media(#[from] crate::media::Error),
    #[error("media connection query failed: {0}")]
    MediaConnection(Arc<Error>),
    #[error(transparent)]
    Auth(#[from] auth::Error),
    #[error(transparent)]
    Signal(#[from] crate::signal::Error),
    #[error(transparent)]
    Message(#[from] crate::messages::Error),
    #[error(transparent)]
    Generation(#[from] crate::messages::generation::Error),
    #[error(transparent)]
    Relay(#[from] crate::relay::Error),
    #[error(transparent)]
    Retry(#[from] crate::retry::Error),
    #[error(transparent)]
    Receipt(#[from] crate::receipts::Error),
    #[error(transparent)]
    USync(#[from] crate::usync::Error),
    #[error(transparent)]
    Group(#[from] crate::groups::Error),
    #[error(transparent)]
    Connection(#[from] connection::Error),
    #[error(transparent)]
    Noise(#[from] noise::Error),
    #[error(transparent)]
    Binary(#[from] binary::Error),
    #[error(transparent)]
    Crypto(#[from] crypto::Error),
    #[error(transparent)]
    Protobuf(#[from] prost::DecodeError),
    #[error("websocket operation failed: {0}")]
    WebSocket(#[from] tungstenite::Error),
    #[error("proxy connection failed: {0}")]
    Proxy(String),
    #[error("operation timed out")]
    Timeout,
    #[error("connection closed ({0})")]
    Closed(u16),
    #[error("invalid socket configuration")]
    Config,
    #[error("duplicate or excessive pending queries")]
    PendingLimit,
    #[error("event receiver lagged by {0} events")]
    EventLagged(u64),
    #[error("blocking auth task failed")]
    Task,
    #[error("pre-key upload failed")]
    PreKeyUpload,
    #[error("Pre-key upload timeout")]
    PreKeyUploadTimeout,
    #[error("encrypt/get digest returned no digest node")]
    DigestMissing,
    #[error("invite message has no expiration")]
    InviteExpiration,
    #[error("catalog result has no product")]
    MissingProduct,
    #[error("message tag counter exhausted")]
    TagExhausted,
}
impl Error {
    pub fn status_code(&self) -> Option<u16> {
        match self {
            Self::Timeout | Self::PreKeyUploadTimeout => Some(408),
            Self::History(crate::history::Error::Media(error)) => error.status_code(),
            Self::AppState(crate::app_state::Error::Media(error)) => error.status_code(),
            Self::AppState(crate::app_state::Error::Transport { status_code, .. }) => *status_code,
            Self::AppState(crate::app_state::Error::Modification(_)) => Some(400),
            Self::Media(error) => error.status_code(),
            Self::MediaConnection(error) => error.status_code(),
            Self::Receipt(error) => Some(error.status_code()),
            Self::Chat(error) => Some(error.status_code()),
            Self::Mex(error) => Some(error.status_code()),
            Self::Closed(code) => Some(*code),
            Self::Auth(e) => e.status_code(),
            Self::Signal(e) => e.status_code(),
            Self::Relay(e) => e.status_code(),
            Self::Generation(e) => e.status_code(),
            Self::USync(e) => e.status_code(),
            Self::Group(e) => Some(e.status_code()),
            Self::Binary(binary::Error::Stanza { code, .. }) => {
                code.and_then(|n| n.try_into().ok())
            }
            _ => None,
        }
    }
    fn disconnect_code(&self) -> u16 {
        self.status_code().unwrap_or(500)
    }
}
#[derive(Debug, Clone)]
pub struct Config {
    pub url: String,
    /// Reach the WebSocket through this proxy: `http://`, `socks5://` or `socks5h://`,
    /// optionally with `user:password@`. Media requests use `media_http` instead.
    pub proxy: Option<String>,
    pub origin: String,
    pub headers: BTreeMap<String, String>,
    pub payload: PayloadConfig,
    pub noise: noise::Config,
    pub connect_timeout: Duration,
    pub query_timeout: Duration,
    pub keep_alive_interval: Duration,
    pub qr_timeout: Option<Duration>,
    pub max_pending_queries: usize,
    pub event_capacity: usize,
    pub initialize_session: bool,
    /// Fetch server properties, the blocklist and privacy settings on connection open.
    /// Enabled by default, matching upstream.
    pub fire_init_queries: bool,
    /// Native opt-out for the connection-open presence announcement. Defaults to true.
    pub announce_presence_on_connect: bool,
    /// Announce available (true) or unavailable (false) when the connection opens.
    /// A saved self name is required for either announcement.
    pub mark_online_on_connect: bool,
    /// Sender filter for incoming stanzas; server stanzas bypass it.
    pub should_ignore_jid: presence::IgnoreJid,
    pub devices: crate::devices::Config,
    pub patch_message_before_sending: crate::relay::MessagePatch,
    pub enable_recent_message_cache: bool,
    pub max_msg_retry_count: u64,
    pub enable_auto_session_recreation: bool,
    pub process_receipts: bool,
    /// Automatically process incoming calls, including offline calls.
    pub process_calls: bool,
    /// Optional asynchronous offer cache. External providers remain owned by the caller
    /// and are never closed or cleared when the socket closes.
    pub call_offer_cache: Option<crate::calls::Cache>,
    /// Prepared media fetched from a URL is kept here so it is not uploaded again.
    pub media_cache: Option<crate::cache::Cache<Vec<u8>>>,
    /// Retry counters kept in a caller-supplied store instead of the built-in one.
    pub msg_retry_counter_cache: Option<crate::cache::Cache<u64>>,
    /// Pending placeholder resends, each with the message data it was requested for.
    pub placeholder_resend_cache: Option<crate::cache::Cache<Option<crate::messages::Envelope>>>,
    pub process_messages: bool,
    pub send_active_receipts: bool,
    pub retry_request_delay: Duration,
    /// Time allowed for a pre-key upload with its retries before whatever waits
    /// on it moves on; the upload itself is not cancelled.
    pub pre_key_upload_timeout: Duration,
    pub process_media_notifications: bool,
    pub process_app_state_notifications: bool,
    /// Automatically process group, picture, account-sync and device-list notifications,
    /// including offline ones. Disable to dispatch raw Node events with handle_notification.
    pub process_notifications: bool,
    pub get_message: crate::relay::MessageLookup,
    pub generation_hooks: crate::messages::generation::GenerationHooks,
    pub link_previews: LinkPreviewConfig,
    pub emit_own_events: bool,
    pub media_http: Option<crate::media::HttpClient>,
    /// Upload hosts tried before the ones the server lists.
    pub custom_upload_hosts: Vec<crate::media::UploadHost>,
    /// Verify the LTHash MACs of app-state patches and snapshots. Off by default.
    pub app_state_mac_verification: MacVerification,
    /// Retries of the storage commit that ends an app-state synchronization.
    pub transaction: TransactionOptions,
    pub media_preparation_hooks: crate::media::PreparationHooks,
    pub history_policy: crate::history::Policy,
    pub history_downloader: Option<crate::history::Downloader>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            url: defaults::DEFAULT_WEBSOCKET_URL.into(),
            proxy: None,
            origin: defaults::DEFAULT_ORIGIN.into(),
            headers: BTreeMap::new(),
            payload: PayloadConfig::default(),
            noise: noise::Config::default(),
            connect_timeout: Duration::from_secs(20),
            query_timeout: Duration::from_secs(60),
            keep_alive_interval: Duration::from_secs(30),
            qr_timeout: None,
            max_pending_queries: 128,
            event_capacity: 256,
            initialize_session: true,
            fire_init_queries: true,
            announce_presence_on_connect: true,
            mark_online_on_connect: true,
            should_ignore_jid: Default::default(),
            devices: Default::default(),
            patch_message_before_sending: Default::default(),
            enable_recent_message_cache: true,
            max_msg_retry_count: 5,
            enable_auto_session_recreation: true,
            process_receipts: true,
            process_calls: true,
            call_offer_cache: None,
            media_cache: None,
            msg_retry_counter_cache: None,
            placeholder_resend_cache: None,
            process_messages: true,
            send_active_receipts: false,
            retry_request_delay: Duration::from_millis(250),
            pre_key_upload_timeout: defaults::UPLOAD_TIMEOUT,
            process_media_notifications: true,
            process_app_state_notifications: true,
            process_notifications: true,
            get_message: Default::default(),
            generation_hooks: Default::default(),
            link_previews: Default::default(),
            emit_own_events: true,
            media_http: None,
            custom_upload_hosts: vec![],
            app_state_mac_verification: Default::default(),
            transaction: Default::default(),
            media_preparation_hooks: Default::default(),
            history_policy: Default::default(),
            history_downloader: None,
        }
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MacVerification {
    pub patch: bool,
    pub snapshot: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionOptions {
    pub max_commit_retries: usize,
    pub delay_between_tries: Duration,
}
impl Default for TransactionOptions {
    fn default() -> Self {
        Self {
            max_commit_retries: 10,
            delay_between_tries: Duration::from_secs(3),
        }
    }
}
/// Built-in link previews for text messages. Upstream generates them only when
/// its optional link-preview-js dependency is installed, so they are opt-in here.
#[derive(Debug, Clone)]
pub struct LinkPreviewConfig {
    /// Fetch the first URL of a text message and attach its title and thumbnail.
    pub enabled: bool,
    /// Width of the inline JPEG thumbnail.
    pub image_thumbnail_width: u32,
    /// Upload the page image as a link thumbnail instead of only inlining it.
    pub high_quality: bool,
    /// Time allowed for the page request.
    pub timeout: Duration,
}
impl Default for LinkPreviewConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            image_thumbnail_width: crate::link_preview::THUMBNAIL_WIDTH_PX,
            high_quality: false,
            timeout: Duration::from_secs(3),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    Connecting,
    Open,
    Closed { code: u16 },
}
#[derive(Clone)]
pub enum Event {
    Connection(ConnectionState),
    Online(bool),
    PresenceUpdated(presence::Update),
    Node(BinaryNode),
    Calls(Vec<crate::calls::Event>),
    Qr(String),
    PairingComplete,
    CredentialsUpdated,
    PendingNotifications,
    MediaUpdated(Vec<crate::media::RetryEvent>),
    ContactsUpdated(Vec<crate::auth::Contact>),
    ContactsUpserted(Vec<crate::auth::Contact>),
    MessagesDeleted(Vec<proto::MessageKey>),
    MessagesStarred(Vec<crate::app_state::StarUpdate>),
    ChatsDeleted(Vec<Option<String>>),
    LabelEdited(crate::app_state::LabelEdit),
    LabelAssociationUpdated(crate::app_state::LabelAssociation),
    SettingUpdated(crate::app_state::SettingUpdate),
    LidMappingUpdated {
        lid: Option<String>,
        pn: String,
    },
    ChatLocked(crate::app_state::ChatLock),
    MessageStubsUpdated(Vec<crate::messages::incoming::StubUpdate>),
    MessageContentsUpdated(Vec<crate::media::ContentUpdate>),
    GroupsUpdated(Vec<crate::groups::Metadata>),
    /// Groups created while connected, with the acting participant.
    GroupsUpserted(Vec<crate::notifications::GroupUpsert>),
    /// One account-sync blocklist change: added (blocked) or removed entries.
    BlocklistUpdated {
        blocklist: Vec<Option<String>>,
        add: bool,
    },
    /// One reaction to a newsletter message; the count is always one.
    NewsletterReaction {
        id: Option<String>,
        server_id: Option<String>,
        code: Option<String>,
    },
    /// View count of a newsletter message; NaN when the server text is not numeric.
    NewsletterView {
        id: Option<String>,
        server_id: Option<String>,
        count: f64,
    },
    NewsletterParticipantsUpdated {
        id: Option<String>,
        author: Option<String>,
        user: Option<String>,
        new_role: Option<String>,
        action: Option<String>,
    },
    NewsletterSettingsUpdated {
        id: Option<String>,
        update: serde_json::Value,
    },
    /// New-chat message quota state as sent by the server.
    MessageCappingUpdated(serde_json::Value),
    /// Outgoing-message restriction state; upstream reports it on connection.update.
    ReachoutTimelockUpdated(crate::newsletters::ReachoutTimelock),
    GroupEffects(Vec<crate::messages::group_effects::GroupEvent>),
    MessagesUpdated(Vec<crate::receipts::MessageUpdate>),
    /// Atomic messages.update batches produced by event consolidation.
    /// These can combine status, star, content, edit and revoke fields.
    MessageUpdates(Vec<crate::messages::own::Update>),
    MessageReceiptsUpdated(Vec<crate::receipts::UserUpdate>),
    MessagesUpserted(crate::messages::own::Upsert),
    MessageMutations(Vec<crate::messages::own::Mutation>),
    MessageReactions(Vec<crate::messages::own::ReactionUpdate>),
    ChatsUpdated(Vec<crate::messages::own::ChatUpdate>),
    ChatsUpserted(Vec<crate::history::Chat>),
    EventResponsesUpdated(Vec<crate::messages::EventResponseUpdate>),
    MessagingHistorySet(crate::history::Set),
    MessagingHistoryStatus(crate::history::StatusUpdate),
}
impl std::fmt::Debug for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MessagingHistoryStatus(status) => f
                .debug_tuple("MessagingHistoryStatus")
                .field(status)
                .finish(),
            Self::MessagingHistorySet(v) => f
                .debug_struct("MessagingHistorySet")
                .field("chats", &v.data.chats.len())
                .field("messages", &v.data.messages.len())
                .finish(),
            Self::ContactsUpserted(v) => f.debug_tuple("ContactsUpserted").field(&v.len()).finish(),
            Self::MessagesDeleted(v) => f.debug_tuple("MessagesDeleted").field(&v.len()).finish(),
            Self::MessagesStarred(v) => f.debug_tuple("MessagesStarred").field(&v.len()).finish(),
            Self::ChatsDeleted(v) => f.debug_tuple("ChatsDeleted").field(&v.len()).finish(),
            Self::LabelEdited(v) => f.debug_tuple("LabelEdited").field(v).finish(),
            Self::LabelAssociationUpdated(v) => {
                f.debug_tuple("LabelAssociationUpdated").field(v).finish()
            }
            Self::SettingUpdated(v) => f.debug_tuple("SettingUpdated").field(v).finish(),
            Self::LidMappingUpdated { lid, pn } => f
                .debug_struct("LidMappingUpdated")
                .field("lid", lid)
                .field("pn", pn)
                .finish(),
            Self::ChatLocked(v) => f.debug_tuple("ChatLocked").field(v).finish(),
            Self::EventResponsesUpdated(updates) => f
                .debug_tuple("EventResponsesUpdated")
                .field(&updates.len())
                .finish(),
            Self::GroupEffects(updates) => {
                f.debug_tuple("GroupEffects").field(&updates.len()).finish()
            }
            Self::ContactsUpdated(updates) => f
                .debug_tuple("ContactsUpdated")
                .field(&updates.len())
                .finish(),
            Self::MessageStubsUpdated(updates) => f
                .debug_tuple("MessageStubsUpdated")
                .field(&updates.len())
                .finish(),
            Self::Online(v) => f.debug_tuple("Online").field(v).finish(),
            Self::PresenceUpdated(v) => f.debug_tuple("PresenceUpdated").field(v).finish(),
            Self::Connection(s) => f.debug_tuple("Connection").field(s).finish(),
            Self::Calls(calls) => f.debug_tuple("Calls").field(calls).finish(),
            Self::Node(n) => f.debug_tuple("Node").field(&n.tag).finish(),
            Self::Qr(_) => f.write_str("Qr(<redacted>)"),
            Self::PairingComplete => f.write_str("PairingComplete"),
            Self::CredentialsUpdated => f.write_str("CredentialsUpdated"),
            Self::PendingNotifications => f.write_str("PendingNotifications"),
            Self::MediaUpdated(events) => {
                f.debug_tuple("MediaUpdated").field(&events.len()).finish()
            }
            Self::MessageContentsUpdated(updates) => f
                .debug_tuple("MessageContentsUpdated")
                .field(&updates.len())
                .finish(),
            Self::MessagesUpserted(upsert) => f
                .debug_tuple("MessagesUpserted")
                .field(&upsert.messages.len())
                .finish(),
            Self::MessageMutations(updates) => f
                .debug_tuple("MessageMutations")
                .field(&updates.len())
                .finish(),
            Self::MessageReactions(updates) => f
                .debug_tuple("MessageReactions")
                .field(&updates.len())
                .finish(),
            Self::ChatsUpdated(updates) => {
                f.debug_tuple("ChatsUpdated").field(&updates.len()).finish()
            }
            Self::ChatsUpserted(chats) => {
                f.debug_tuple("ChatsUpserted").field(&chats.len()).finish()
            }
            Self::MessagesUpdated(updates) => f
                .debug_tuple("MessagesUpdated")
                .field(&updates.len())
                .finish(),
            Self::MessageUpdates(updates) => f
                .debug_tuple("MessageUpdates")
                .field(&updates.len())
                .finish(),
            Self::MessageReceiptsUpdated(updates) => f
                .debug_tuple("MessageReceiptsUpdated")
                .field(&updates.len())
                .finish(),
            Self::GroupsUpserted(groups) => f
                .debug_tuple("GroupsUpserted")
                .field(&groups.len())
                .finish(),
            Self::BlocklistUpdated { blocklist, add } => f
                .debug_struct("BlocklistUpdated")
                .field("blocklist", &blocklist.len())
                .field("add", add)
                .finish(),
            Self::GroupsUpdated(groups) => {
                f.debug_tuple("GroupsUpdated").field(&groups.len()).finish()
            }
            Self::NewsletterReaction { id, server_id, .. } => f
                .debug_struct("NewsletterReaction")
                .field("id", id)
                .field("server_id", server_id)
                .finish(),
            Self::NewsletterView {
                id,
                server_id,
                count,
            } => f
                .debug_struct("NewsletterView")
                .field("id", id)
                .field("server_id", server_id)
                .field("count", count)
                .finish(),
            Self::NewsletterParticipantsUpdated { id, action, .. } => f
                .debug_struct("NewsletterParticipantsUpdated")
                .field("id", id)
                .field("action", action)
                .finish(),
            Self::NewsletterSettingsUpdated { id, .. } => f
                .debug_struct("NewsletterSettingsUpdated")
                .field("id", id)
                .finish(),
            Self::MessageCappingUpdated(_) => f.write_str("MessageCappingUpdated"),
            Self::ReachoutTimelockUpdated(state) => f
                .debug_tuple("ReachoutTimelockUpdated")
                .field(state)
                .finish(),
        }
    }
}
struct Inner {
    call_offer_cache: crate::calls::Cache,
    notification_lock: Arc<tokio::sync::Mutex<()>>,
    devices_lock: tokio::sync::Mutex<()>,
    prekey_low: std::sync::Mutex<std::collections::HashSet<String>>,
    restricted_recoveries: std::sync::Mutex<std::collections::HashSet<String>>,
    identity_debounce: std::sync::Mutex<std::collections::HashMap<String, tokio::time::Instant>>,
    message_lock: tokio::sync::Mutex<()>,
    active_receipts: std::sync::atomic::AtomicBool,
    retry_request_delay: Duration,
    media_cache: tokio::sync::Mutex<media::MediaCache>,
    media_store: Option<crate::cache::Cache<Vec<u8>>>,
    media_http: tokio::sync::OnceCell<crate::media::HttpClient>,
    app_state_sync: tokio::sync::OnceCell<crate::app_state::Synchronizer>,
    app_state_apply_lock: tokio::sync::Mutex<()>,
    app_state_patch_lock: tokio::sync::Mutex<()>,
    app_state_options: crate::app_state::SyncOptions,
    custom_upload_hosts: Vec<crate::media::UploadHost>,
    history_policy: crate::history::Policy,
    history_downloader: tokio::sync::OnceCell<crate::history::Downloader>,
    media_preparation_hooks: crate::media::PreparationHooks,
    own_messages: Option<mpsc::UnboundedSender<proto::WebMessageInfo>>,
    generation_hooks: crate::messages::generation::GenerationHooks,
    link_previews: LinkPreviewConfig,
    receipt_lock: tokio::sync::Mutex<()>,
    get_message: crate::relay::MessageLookup,
    retry_requests: Arc<retries::RetryRequests>,
    max_msg_retry_count: u64,
    enable_auto_session_recreation: bool,
    privacy_settings: std::sync::Mutex<Option<crate::privacy_settings::Settings>>,
    retry_manager: Option<crate::retry::Manager>,
    patch_message_before_sending: crate::relay::MessagePatch,
    relay_lock: tokio::sync::Mutex<()>,
    relay_state: std::sync::Mutex<relay::RelayState>,
    store: SqliteAuthStore,
    decryptor: Arc<crate::messages::SignalDecryptor>,
    devices: crate::devices::Cache,
    commands: mpsc::Sender<Command>,
    cancel: mpsc::UnboundedSender<u64>,
    events: events::Emitter,
    state: watch::Receiver<ConnectionState>,
    sequence: AtomicU64,
    prefix: String,
    query_timeout: Duration,
}
#[derive(Clone)]
pub struct Socket {
    inner: Arc<Inner>,
}
enum Command {
    AppStateKeys {
        keys: Vec<proto::message::AppStateSyncKey>,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    PushName {
        name: String,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    UnarchiveChats {
        value: bool,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    UploadPreKeys {
        count: u32,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    DisappearingMode {
        mode: auth::DisappearingMode,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    SignedPreKey {
        key: Box<crypto::SignedKeyPair>,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    RecordHistory {
        key: Option<proto::MessageKey>,
        timestamp: Option<u64>,
        reply: oneshot::Sender<Result<bool, Error>>,
    },
    IncrementAccountSync {
        reply: oneshot::Sender<Result<(), Error>>,
    },
    Presence {
        presence: presence::Presence,
        to: Option<String>,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    LastPropHash {
        hash: String,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    RetryReceipt {
        node: BinaryNode,
        count: u64,
        include_keys: bool,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    Query {
        node: BinaryNode,
        token: u64,
        timeout: Duration,
        reply: oneshot::Sender<Result<BinaryNode, Error>>,
    },
    Send {
        node: BinaryNode,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    PairingCode {
        phone: String,
        code: Option<String>,
        reply: oneshot::Sender<Result<String, Error>>,
    },
    Close {
        logout: bool,
        reply: oneshot::Sender<Result<(), Error>>,
    },
}
struct CancelGuard {
    sender: mpsc::UnboundedSender<u64>,
    token: u64,
}
impl Drop for CancelGuard {
    fn drop(&mut self) {
        let _ = self.sender.send(self.token);
    }
}
type WebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

impl Socket {
    /// The stored PN/LID mapping, upstream's `signalRepository.lidMapping`.
    /// Its lookups are synchronous and may query USync, so async callers run
    /// them on a blocking executor.
    pub fn lid_mapping(&self) -> crate::signal::mapping::Store {
        self.inner.decryptor.direct.lid_mapping().clone()
    }
    /// Return after the authenticated Noise exchange. Account login/pairing proceeds
    /// through events; an encrypted connection is not yet an authenticated account.
    pub async fn connect(
        config: Config,
        store: SqliteAuthStore,
    ) -> Result<(Self, broadcast::Receiver<Event>), Error> {
        if config.connect_timeout.is_zero()
            || config.query_timeout.is_zero()
            || config.keep_alive_interval.is_zero()
            || config.event_capacity == 0
            || config.max_pending_queries == 0
            || config.qr_timeout.is_some_and(|d| d.is_zero())
        {
            return Err(Error::Config);
        }
        let credentials = load_credentials(store.clone()).await?;
        let mut url = url::Url::parse(&config.url).map_err(|_| Error::Config)?;
        if !matches!(url.scheme(), "ws" | "wss") {
            return Err(Error::Config);
        }
        if url.scheme() == "wss"
            && let Some(routing) = &credentials.routing_info
        {
            url.query_pairs_mut()
                .append_pair("ED", &URL_SAFE_NO_PAD.encode(routing));
        }
        let mut request = url.as_str().into_client_request()?;
        request
            .headers_mut()
            .insert("origin", config.origin.parse().map_err(|_| Error::Config)?);
        for (name, value) in &config.headers {
            request.headers_mut().insert(
                tungstenite::http::HeaderName::from_bytes(name.as_bytes())
                    .map_err(|_| Error::Config)?,
                value.parse().map_err(|_| Error::Config)?,
            );
        }
        let ((mut ws, _), mut noise) = timeout(config.connect_timeout, async {
            let (mut ws, response) = match &config.proxy {
                Some(proxy) => {
                    let host = url.host_str().ok_or(Error::Config)?;
                    let port = url.port_or_known_default().ok_or(Error::Config)?;
                    let stream = proxy::connect(proxy, host, port).await?;
                    tokio_tungstenite::client_async_tls(request, stream).await?
                }
                None => tokio_tungstenite::connect_async(request).await?,
            };
            let ephemeral = crypto::KeyPair::generate()?;
            let public = ephemeral.public;
            let mut noise_config = config.noise.clone();
            noise_config.routing_info = credentials.routing_info.clone();
            let mut noise = NoiseHandler::new(ephemeral, noise_config)?;
            let hello = proto::HandshakeMessage {
                client_hello: Some(proto::handshake_message::ClientHello {
                    ephemeral: Some(public.to_vec()),
                    ..Default::default()
                }),
                ..Default::default()
            };
            ws.send(tungstenite::Message::Binary(
                noise.encode_frame(&hello.encode_to_vec())?.into(),
            ))
            .await?;
            let server_hello = loop {
                if let Some(noise::Frame::Handshake(frame)) = noise.next_frame()? {
                    break proto::HandshakeMessage::decode(frame.as_slice())?;
                }
                match ws.next().await {
                    Some(Ok(tungstenite::Message::Binary(bytes))) => noise.push_bytes(&bytes)?,
                    Some(Ok(tungstenite::Message::Ping(bytes))) => {
                        ws.send(tungstenite::Message::Pong(bytes)).await?
                    }
                    Some(Ok(tungstenite::Message::Pong(_))) => (),
                    Some(Ok(tungstenite::Message::Close(_))) | None => {
                        return Err(Error::Closed(428));
                    }
                    Some(Err(e)) => return Err(e.into()),
                    _ => return Err(Error::Config),
                }
            };
            let static_key = noise.process_handshake(&server_hello, &credentials.noise_key)?;
            let payload = if let Some(me) = &credentials.me {
                connection::login_payload(&me.id, &config.payload)?
            } else {
                connection::registration_payload(&credentials, &config.payload)?
            };
            let finish = proto::HandshakeMessage {
                client_finish: Some(proto::handshake_message::ClientFinish {
                    r#static: Some(static_key),
                    payload: Some(noise.encrypt(&payload.encode_to_vec())?),
                    ..Default::default()
                }),
                ..Default::default()
            };
            ws.send(tungstenite::Message::Binary(
                noise.encode_frame(&finish.encode_to_vec())?.into(),
            ))
            .await?;
            noise.finish_init()?;
            Ok::<_, Error>(((ws, response), noise))
        })
        .await
        .map_err(|_| Error::Timeout)??;
        let (events, receiver) = events::Emitter::new(config.event_capacity);
        let (state, state_rx) = watch::channel(ConnectionState::Connecting);
        let (commands, command_rx) = mpsc::channel(64);
        events.set_counter_command(commands.downgrade());
        let (cancel, cancel_rx) = mpsc::unbounded_channel();
        let mut random = [0; 4];
        getrandom::fill(&mut random).map_err(|_| crypto::Error::Random)?;
        let prefix = format!(
            "{}.{}-",
            u16::from_be_bytes([random[0], random[1]]),
            u16::from_be_bytes([random[2], random[3]])
        );
        let retry_manager = config
            .enable_recent_message_cache
            .then(|| crate::retry::Manager::new(config.max_msg_retry_count));
        let actor_retry_manager = retry_manager.clone();
        let retry_requests = Arc::new(retries::RetryRequests::new(
            config.msg_retry_counter_cache.clone(),
            config.placeholder_resend_cache.clone(),
        ));
        let actor_retry_requests = retry_requests.clone();
        let (receipt_sender, mut receipt_receiver) =
            mpsc::unbounded_channel::<dispatch::OnlineNode>();
        let receipt_sender = config.process_receipts.then_some(receipt_sender);
        let (message_sender, message_receiver) = mpsc::unbounded_channel();
        let message_sender = config.process_messages.then_some(message_sender);
        let (notification_sender, notification_receiver) = mpsc::unbounded_channel();
        let notification_sender = (config.process_app_state_notifications
            || config.process_notifications)
            .then_some(notification_sender);
        let (call_sender, call_receiver) = mpsc::unbounded_channel();
        let call_sender = config.process_calls.then_some(call_sender);
        let (offline_sender, offline_receiver) = mpsc::unbounded_channel();
        let (own_sender, own_receiver) = mpsc::unbounded_channel();
        let inner = Arc::new(Inner {
            call_offer_cache: config.call_offer_cache.clone().unwrap_or_default(),
            notification_lock: Arc::new(tokio::sync::Mutex::new(())),
            devices_lock: tokio::sync::Mutex::new(()),
            prekey_low: Default::default(),
            restricted_recoveries: Default::default(),
            identity_debounce: Default::default(),
            message_lock: tokio::sync::Mutex::new(()),
            active_receipts: std::sync::atomic::AtomicBool::new(config.send_active_receipts),
            retry_request_delay: config.retry_request_delay,
            media_cache: tokio::sync::Mutex::new(Default::default()),
            media_store: config.media_cache.clone(),
            media_http: tokio::sync::OnceCell::new_with(config.media_http.clone()),
            app_state_sync: tokio::sync::OnceCell::new(),
            app_state_apply_lock: tokio::sync::Mutex::new(()),
            app_state_patch_lock: tokio::sync::Mutex::new(()),
            app_state_options: crate::app_state::SyncOptions {
                verify_snapshot: config.app_state_mac_verification.snapshot,
                verify_patch: config.app_state_mac_verification.patch,
                max_commit_attempts: config.transaction.max_commit_retries,
                commit_retry_delay: config.transaction.delay_between_tries,
            },
            custom_upload_hosts: config.custom_upload_hosts.clone(),
            history_policy: config.history_policy.clone(),
            history_downloader: tokio::sync::OnceCell::new_with(config.history_downloader.clone()),
            media_preparation_hooks: config.media_preparation_hooks.clone(),
            own_messages: config.emit_own_events.then_some(own_sender),
            generation_hooks: config.generation_hooks.clone(),
            link_previews: config.link_previews.clone(),
            receipt_lock: tokio::sync::Mutex::new(()),
            get_message: config.get_message.clone(),
            retry_requests,
            max_msg_retry_count: config.max_msg_retry_count,
            enable_auto_session_recreation: config.enable_auto_session_recreation,
            privacy_settings: std::sync::Mutex::new(None),
            retry_manager,
            patch_message_before_sending: config.patch_message_before_sending.clone(),
            relay_lock: tokio::sync::Mutex::new(()),
            relay_state: std::sync::Mutex::new(Default::default()),
            store: store.clone(),
            decryptor: Arc::new(crate::messages::SignalDecryptor::new(store.clone())),
            devices: crate::devices::Cache::new(config.devices.clone()),
            commands,
            cancel,
            events: events.clone(),
            state: state_rx,
            sequence: AtomicU64::new(0),
            prefix: prefix.clone(),
            query_timeout: config.query_timeout,
        });
        send::spawn_own_worker(&inner, own_receiver);
        startup::spawn_worker(&inner, config.fire_init_queries);
        dispatch::spawn_worker(&inner, offline_receiver);
        calls::spawn_worker(&inner, call_receiver);
        incoming_messages::spawn_worker(&inner, message_receiver);
        notifications::spawn_worker(&inner, notification_receiver);
        let receipt_inner = Arc::downgrade(&inner);
        tokio::spawn(async move {
            while let Some(online) = receipt_receiver.recv().await {
                let dispatch::OnlineNode { node, scope } = online;
                let _scope = scope;
                let Some(inner) = receipt_inner.upgrade() else {
                    break;
                };
                if let Err(error) = (Socket { inner }).handle_receipt(&node).await {
                    tracing::debug!(?error, "receipt handling failed");
                }
            }
        });
        let actor_inner = Arc::downgrade(&inner);
        tokio::spawn(async move {
            actor::run(
                &mut ws,
                &mut noise,
                config,
                store,
                credentials,
                prefix,
                command_rx,
                cancel_rx,
                events,
                state,
                actor_retry_manager,
                actor_retry_requests,
                receipt_sender,
                message_sender,
                notification_sender,
                offline_sender,
                call_sender,
                actor_inner,
            )
            .await;
        });
        Ok((Self { inner }, receiver))
    }
    pub fn message_retry_manager(&self) -> Option<crate::retry::Manager> {
        self.inner.retry_manager.clone()
    }
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.inner.events.subscribe()
    }
    /// Begin collecting the twelve upstream bufferable event families.
    pub fn buffer_events(&self) {
        self.inner.events.buffer();
    }
    pub fn is_buffering_events(&self) -> bool {
        self.inner.events.is_buffering()
    }
    pub fn initial_sync_phase(&self) -> initial_sync::Phase {
        self.inner.events.initial_sync_phase()
    }
    pub fn flush_events(&self) -> Result<bool, Error> {
        self.inner.events.flush()
    }
    /// Complete a manually started buffered operation; also call on failure.
    pub fn finish_buffered_events(&self, success: bool) {
        self.inner.events.finish(success);
    }
    /// Run an operation with source-compatible completion/debounce timing.
    /// Dropping the future completes the scope through its failure path.
    pub async fn with_buffered_events<T>(
        &self,
        operation: impl std::future::Future<Output = Result<T, Error>>,
    ) -> Result<T, Error> {
        self.inner.events.buffer();
        let scope = events::Scope(Some(self.inner.events.clone()));
        let result = operation.await;
        scope.finish(result.is_ok());
        result
    }
    /// Buffer an explicitly dispatched online stanza and flush when it ends.
    /// This releases the shared buffer even if other operations are still running,
    /// matching processNodeWithBuffer. Dropping the future also releases it.
    pub async fn with_node_events<T>(
        &self,
        operation: impl std::future::Future<Output = Result<T, Error>>,
    ) -> Result<T, Error> {
        let scope = self.inner.events.node_scope();
        let result = operation.await;
        scope.finish()?;
        result
    }
    pub fn state(&self) -> ConnectionState {
        self.inner.state.borrow().clone()
    }
    pub async fn wait_for_open(&self, limit: Duration) -> Result<(), Error> {
        let mut state = self.inner.state.clone();
        timeout(limit, async {
            loop {
                let current = state.borrow().clone();
                match current {
                    ConnectionState::Open => return Ok(()),
                    ConnectionState::Closed { code } => return Err(Error::Closed(code)),
                    ConnectionState::Connecting => (),
                }
                state.changed().await.map_err(|_| Error::Closed(428))?;
            }
        })
        .await
        .map_err(|_| Error::Timeout)?
    }
    pub async fn query(
        &self,
        mut node: BinaryNode,
        limit: Option<Duration>,
    ) -> Result<BinaryNode, Error> {
        let token = self
            .inner
            .sequence
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| Error::TagExhausted)?;
        if node.attrs.get("id").is_none_or(|s| s.is_empty()) {
            node.attrs
                .insert("id".into(), format!("{}{token}", self.inner.prefix));
        }
        let (reply, receiver) = oneshot::channel();
        self.inner
            .commands
            .send(Command::Query {
                node,
                token,
                timeout: limit.unwrap_or(self.inner.query_timeout),
                reply,
            })
            .await
            .map_err(|_| self.closed_error())?;
        let _guard = CancelGuard {
            sender: self.inner.cancel.clone(),
            token,
        };
        receiver.await.map_err(|_| self.closed_error())?
    }
    pub async fn issue_privacy_tokens(
        &self,
        jids: &[String],
        timestamp: Option<u64>,
    ) -> Result<BinaryNode, Error> {
        let timestamp = match timestamp {
            Some(timestamp) => timestamp,
            None => std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| Error::Config)?
                .as_secs(),
        };
        self.query(crate::privacy::issuance_query(jids, timestamp), None)
            .await
    }
    pub async fn execute_usync(
        &self,
        query: &crate::usync::Query,
    ) -> Result<Option<crate::usync::ResultList>, Error> {
        let sequence = self
            .inner
            .sequence
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| Error::TagExhausted)?;
        let stanza = query.stanza(&format!("{}{sequence}", self.inner.prefix))?;
        Ok(query.parse(&self.query(stanza, None).await?)?)
    }
    pub async fn on_whatsapp(&self, inputs: &[String]) -> Result<Vec<(String, bool)>, Error> {
        let query = crate::usync::contact_query(inputs);
        if query.users.is_empty() {
            return Ok(vec![]);
        }
        Ok(self
            .execute_usync(&query)
            .await?
            .map(|results| {
                results
                    .list
                    .into_iter()
                    .filter_map(|user| {
                        if matches!(
                            user.protocols.get("contact"),
                            Some(crate::usync::Value::Contact(true))
                        ) {
                            Some((user.id, true))
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default())
    }
    pub async fn fetch_lid_mappings(
        &self,
        jids: &[String],
    ) -> Result<Vec<crate::signal::mapping::Mapping>, Error> {
        let mut query = crate::usync::Query::default().with_protocol(crate::usync::Protocol::Lid);
        query.context = "background".into();
        query.users = jids
            .iter()
            .filter(|jid| !crate::jid::Jid::parse(jid).is_some_and(|j| j.is_lid_user()))
            .map(crate::usync::User::with_id)
            .collect();
        if query.users.is_empty() {
            return Ok(vec![]);
        }
        Ok(self
            .execute_usync(&query)
            .await?
            .map(|results| {
                results
                    .list
                    .into_iter()
                    .filter_map(|user| match user.protocols.get("lid") {
                        Some(crate::usync::Value::Text(lid)) if !lid.is_empty() => {
                            Some(crate::signal::mapping::Mapping {
                                pn: user.id,
                                lid: lid.clone(),
                            })
                        }
                        _ => None,
                    })
                    .collect()
            })
            .unwrap_or_default())
    }
    /// Resolve stored PN aliases, fetching missing users through background LID USync.
    pub async fn resolve_lid_mappings(
        &self,
        pns: &[String],
    ) -> Result<Vec<crate::signal::mapping::Mapping>, Error> {
        let mapping = self.inner.decryptor.direct.lid_mapping().clone();
        let inputs = pns.to_vec();
        let stored = tokio::task::spawn_blocking(move || mapping.get_lids_for_pns(&inputs))
            .await
            .map_err(|_| Error::Task)??;
        let known: std::collections::HashSet<_> = stored
            .iter()
            .filter_map(|p| crate::jid::Jid::parse(&p.pn).map(|j| j.user))
            .collect();
        let missing: indexmap::IndexSet<_> = pns
            .iter()
            .filter_map(|pn| crate::jid::Jid::parse(pn))
            .filter(|j| j.is_pn_user() || j.is_hosted_pn_user())
            .filter(|j| !known.contains(&j.user))
            .map(|j| crate::jid::encode(&j.user, "s.whatsapp.net", None, None))
            .collect();
        if missing.is_empty() {
            return Ok(stored);
        }
        let fetched = self
            .fetch_lid_mappings(&missing.into_iter().collect::<Vec<_>>())
            .await?;
        let mapping = self.inner.decryptor.direct.lid_mapping().clone();
        let inputs = pns.to_vec();
        Ok(tokio::task::spawn_blocking(move || {
            mapping.store_mappings(&fetched)?;
            mapping.get_lids_for_pns(&inputs)
        })
        .await
        .map_err(|_| Error::Task)??)
    }
    /// Check unique sessions, resolve PN aliases and fetch missing/forced bundles.
    /// As upstream does, unmapped PNs are omitted from the encrypt IQ.
    pub async fn assert_sessions(&self, jids: &[String], force: bool) -> Result<bool, Error> {
        let unique = jids.iter().cloned().collect::<indexmap::IndexSet<_>>();
        let mut required = vec![];
        for jid in unique {
            if !force {
                let resolved = if crate::jid::Jid::parse(&jid)
                    .is_some_and(|j| j.is_pn_user() || j.is_hosted_pn_user())
                {
                    self.resolve_lid_mappings(std::slice::from_ref(&jid))
                        .await
                        .is_ok()
                } else {
                    true
                };
                let repository = self.inner.decryptor.direct.clone();
                let check = jid.clone();
                if resolved
                    && tokio::task::spawn_blocking(move || {
                        repository.validate_session(&check).unwrap_or(false)
                    })
                    .await
                    .map_err(|_| Error::Task)?
                {
                    continue;
                }
            }
            required.push(jid);
        }
        if required.is_empty() {
            return Ok(false);
        }
        let mut wire = required
            .iter()
            .filter(|jid| {
                crate::jid::Jid::parse(jid)
                    .is_some_and(|j| j.is_lid_user() || j.is_hosted_lid_user())
            })
            .cloned()
            .collect::<Vec<_>>();
        let pns = required
            .into_iter()
            .filter(|jid| {
                crate::jid::Jid::parse(jid).is_some_and(|j| j.is_pn_user() || j.is_hosted_pn_user())
            })
            .collect::<Vec<_>>();
        wire.extend(
            self.resolve_lid_mappings(&pns)
                .await?
                .into_iter()
                .map(|p| p.lid),
        );
        let result = self
            .query(
                crate::signal::wire::session_query_with_reason(&wire, force)?,
                None,
            )
            .await?;
        let repository = self.inner.decryptor.direct.clone();
        tokio::task::spawn_blocking(move || {
            crate::signal::wire::inject_session_bundles(&result, &repository)
        })
        .await
        .map_err(|_| Error::Task)??;
        Ok(true)
    }
    /// Discover relay devices, refresh returned LID sessions, and persist device lists.
    pub async fn get_usync_devices(
        &self,
        jids: &[String],
        use_cache: bool,
        ignore_zero: bool,
    ) -> Result<Vec<crate::devices::Device>, Error> {
        let mut plan = self.inner.devices.plan(jids, use_cache).await?;
        if plan.to_fetch.is_empty() {
            return Ok(plan.devices);
        }
        let mut query = crate::usync::Query::default()
            .with_protocol(crate::usync::Protocol::Devices)
            .with_protocol(crate::usync::Protocol::Lid);
        query.context = "message".into();
        query.users = plan
            .to_fetch
            .iter()
            .map(crate::usync::User::with_id)
            .collect();
        let Some(result) = self.execute_usync(&query).await? else {
            return Ok(plan.devices);
        };
        let pairs = result
            .list
            .iter()
            .filter_map(|u| match u.protocols.get("lid") {
                Some(crate::usync::Value::Text(lid)) if !lid.is_empty() => {
                    Some(crate::signal::mapping::Mapping {
                        pn: u.id.clone(),
                        lid: lid.clone(),
                    })
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if !pairs.is_empty() {
            let mapping = self.inner.decryptor.direct.lid_mapping().clone();
            let updates = pairs.clone();
            tokio::task::spawn_blocking(move || mapping.store_mappings(&updates))
                .await
                .map_err(|_| Error::Task)??;
            if let Err(error) = self
                .assert_sessions(&pairs.into_iter().map(|p| p.lid).collect::<Vec<_>>(), true)
                .await
            {
                tracing::warn!(?error, "failed to refresh sessions for discovered LIDs");
            }
        }
        let credentials = load_credentials(self.inner.store.clone()).await?;
        let extracted = crate::usync::extract_device_jids(
            &result.list,
            credentials.me_id()?,
            credentials
                .me
                .as_ref()
                .and_then(|me| me.lid.as_deref())
                .unwrap_or_default(),
            ignore_zero,
        )?;
        // Cache writes are serialized with device-list notifications.
        let groups = {
            let _serial = self.inner.devices_lock.lock().await;
            self.inner.devices.update(extracted).await?
        };
        let updates = groups
            .iter()
            .map(|(user, devices)| {
                crate::auth::KeyMutation::put(
                    user,
                    crate::auth::KeyValue::DeviceList(
                        devices
                            .iter()
                            .map(|d| d.device.unwrap_or(0).to_string())
                            .collect(),
                    ),
                )
            })
            .collect::<Vec<_>>();
        plan.devices.extend(groups.into_values().flatten());
        if !updates.is_empty() {
            let store = self.inner.store.clone();
            let outcome = tokio::task::spawn_blocking(move || {
                use crate::auth::KeyStore;
                store.set(&updates)
            })
            .await
            .map_err(|_| Error::Task)?;
            if let Err(error) = outcome {
                tracing::warn!(?error, "failed to persist discovered device lists");
            }
        }
        Ok(plan.devices)
    }
    pub async fn invalidate_user_devices(&self, jids: &[String]) -> Result<(), Error> {
        let users = jids
            .iter()
            .map(|j| {
                crate::jid::Jid::parse(j)
                    .map(|j| j.user)
                    .ok_or(crate::signal::Error::Address)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(self.inner.devices.invalidate_users(&users).await?)
    }
    /// Fetch bundles and establish sessions on a blocking executor.
    pub async fn establish_sessions(
        &self,
        jids: &[String],
        repository: crate::signal::direct::Repository,
    ) -> Result<usize, Error> {
        if jids.is_empty() {
            return Ok(0);
        }
        let result = self
            .query(crate::signal::wire::session_query(jids)?, None)
            .await?;
        Ok(tokio::task::spawn_blocking(move || {
            crate::signal::wire::inject_session_bundles(&result, &repository)
        })
        .await
        .map_err(|_| Error::Task)??)
    }
    /// Decode/decrypt a received Node event using this socket's persistent keys.
    /// Consumes Signal counters; callers must process each stanza once, in arrival order.
    /// Disable automatic processing before using this on raw Node events.
    /// handle_message adds receipt/acknowledgement and retry orchestration.
    pub async fn decrypt_message(
        &self,
        node: BinaryNode,
    ) -> Result<crate::messages::Decrypted, Error> {
        let store = self.inner.store.clone();
        let decryptor = self.inner.decryptor.clone();
        tokio::task::spawn_blocking(move || {
            let credentials = store.load_credentials()?;
            let me = credentials.me_id()?;
            let lid = credentials
                .me
                .as_ref()
                .and_then(|me| me.lid.as_deref())
                .unwrap_or_default();
            Ok(crate::messages::decrypt_node(
                &node,
                me,
                lid,
                decryptor.as_ref(),
            )?)
        })
        .await
        .map_err(|_| Error::Task)?
    }
    /// Encrypt a fanout using persistent sessions. Session discovery and stanza relay
    /// are separate steps; this call consumes counters for successful recipients.
    pub async fn create_participant_nodes(
        &self,
        recipients: Vec<String>,
        message: proto::Message,
        extra_attrs: indexmap::IndexMap<String, String>,
        device_sent_message: Option<proto::Message>,
    ) -> Result<crate::relay::ParticipantNodes, Error> {
        if recipients.is_empty() {
            return Ok(Default::default());
        }
        let repository = crate::relay::Repository::with_mappings(
            self.inner.store.clone(),
            self.inner.decryptor.direct.lid_mapping().clone(),
        );
        let patch = self.inner.patch_message_before_sending.clone();
        tokio::task::spawn_blocking(move || {
            Ok(repository.transaction(&recipients, |transaction| {
                let credentials = transaction.credentials()?;
                let me = credentials.me_id()?;
                let lid = credentials.me.as_ref().and_then(|me| me.lid.as_deref());
                crate::relay::create_participant_nodes_patched(
                    crate::relay::Fanout {
                        recipients: &recipients,
                        message: &message,
                        me,
                        me_lid: lid,
                        extra_attrs: &extra_attrs,
                        device_sent_message: device_sent_message.as_ref(),
                    },
                    transaction,
                    |message, recipients| patch.apply(message, Some(recipients)),
                )
            })?)
        })
        .await
        .map_err(|_| Error::Task)?
    }
    pub async fn relay_newsletter(
        &self,
        destination: &str,
        message: &proto::Message,
        message_id: Option<&str>,
        options: &crate::relay::StanzaOptions,
    ) -> Result<String, Error> {
        if !crate::jid::Jid::parse(destination).is_some_and(|jid| jid.is_newsletter()) {
            return Err(crate::relay::Error::Recipient.into());
        }
        self.relay_message(
            destination,
            message.clone(),
            RelayOptions {
                message_id: message_id.map(str::to_owned),
                stanza: options.clone(),
                ..Default::default()
            },
        )
        .await
    }
    pub async fn send_node(&self, node: BinaryNode) -> Result<(), Error> {
        let (reply, receiver) = oneshot::channel();
        self.inner
            .commands
            .send(Command::Send { node, reply })
            .await
            .map_err(|_| self.closed_error())?;
        receiver.await.map_err(|_| self.closed_error())?
    }
    pub async fn request_pairing_code(
        &self,
        phone: impl Into<String>,
        code: Option<String>,
    ) -> Result<String, Error> {
        let (reply, receiver) = oneshot::channel();
        self.inner
            .commands
            .send(Command::PairingCode {
                phone: phone.into(),
                code,
                reply,
            })
            .await
            .map_err(|_| self.closed_error())?;
        receiver.await.map_err(|_| self.closed_error())?
    }
    pub async fn close(&self) -> Result<(), Error> {
        self.close_inner(false).await
    }
    pub async fn logout(&self) -> Result<(), Error> {
        self.close_inner(true).await
    }
    async fn close_inner(&self, logout: bool) -> Result<(), Error> {
        let (reply, receiver) = oneshot::channel();
        self.inner
            .commands
            .send(Command::Close { logout, reply })
            .await
            .map_err(|_| self.closed_error())?;
        receiver.await.map_err(|_| self.closed_error())?
    }
    fn closed_error(&self) -> Error {
        match self.state() {
            ConnectionState::Closed { code } => Error::Closed(code),
            _ => Error::Closed(428),
        }
    }
}
async fn load_credentials(store: SqliteAuthStore) -> Result<Credentials, Error> {
    tokio::task::spawn_blocking(move || store.load_credentials())
        .await
        .map_err(|_| Error::Task)?
        .map_err(Error::from)
}
async fn save_credentials(store: SqliteAuthStore, credentials: Credentials) -> Result<(), Error> {
    tokio::task::spawn_blocking(move || store.save_credentials(&credentials))
        .await
        .map_err(|_| Error::Task)?
        .map_err(Error::from)
}
/// Callback names emitted by Baileys' stanza router, for equivalent Rust filters.
pub fn callback_names(node: &BinaryNode) -> Vec<String> {
    let child = node
        .all_children()
        .first()
        .map(|c| c.tag.as_str())
        .unwrap_or("");
    let mut names = Vec::new();
    for (key, value) in &node.attrs {
        names.push(format!("CB:{},{key}:{value},{child}", node.tag));
        names.push(format!("CB:{},{key}:{value}", node.tag));
        names.push(format!("CB:{},{key}", node.tag));
    }
    names.push(format!("CB:{},,{child}", node.tag));
    names.push(format!("CB:{}", node.tag));
    names
}
