//! Async WhatsApp WebSocket connection with exclusive Noise ownership and IQ routing.
mod actor;
mod groups;
mod receipts;
mod relay;
mod retries;
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
use prost::Message;
pub use relay::{RelayOptions, ServerProps};
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
    Auth(#[from] auth::Error),
    #[error(transparent)]
    Signal(#[from] crate::signal::Error),
    #[error(transparent)]
    Message(#[from] crate::messages::Error),
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
    #[error("operation timed out")]
    Timeout,
    #[error("connection closed ({0})")]
    Closed(u16),
    #[error("invalid socket configuration")]
    Config,
    #[error("duplicate or excessive pending queries")]
    PendingLimit,
    #[error("blocking auth task failed")]
    Task,
    #[error("message tag counter exhausted")]
    TagExhausted,
}
impl Error {
    pub fn status_code(&self) -> Option<u16> {
        match self {
            Self::Timeout => Some(408),
            Self::Receipt(error) => Some(error.status_code()),
            Self::Closed(code) => Some(*code),
            Self::Auth(e) => e.status_code(),
            Self::Signal(e) => e.status_code(),
            Self::Relay(e) => e.status_code(),
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
    pub devices: crate::devices::Config,
    pub patch_message_before_sending: crate::relay::MessagePatch,
    pub enable_recent_message_cache: bool,
    pub max_msg_retry_count: u64,
    pub enable_auto_session_recreation: bool,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            url: defaults::DEFAULT_WEBSOCKET_URL.into(),
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
            devices: Default::default(),
            patch_message_before_sending: Default::default(),
            enable_recent_message_cache: true,
            max_msg_retry_count: 5,
            enable_auto_session_recreation: true,
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
    Node(BinaryNode),
    Qr(String),
    PairingComplete,
    CredentialsUpdated,
    PendingNotifications,
    GroupsUpdated(Vec<crate::groups::Metadata>),
}
impl std::fmt::Debug for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connection(s) => f.debug_tuple("Connection").field(s).finish(),
            Self::Node(n) => f.debug_tuple("Node").field(&n.tag).finish(),
            Self::Qr(_) => f.write_str("Qr(<redacted>)"),
            Self::PairingComplete => f.write_str("PairingComplete"),
            Self::CredentialsUpdated => f.write_str("CredentialsUpdated"),
            Self::PendingNotifications => f.write_str("PendingNotifications"),
            Self::GroupsUpdated(groups) => {
                f.debug_tuple("GroupsUpdated").field(&groups.len()).finish()
            }
        }
    }
}
struct Inner {
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
    events: broadcast::Sender<Event>,
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
            let (mut ws, response) = tokio_tungstenite::connect_async(request).await?;
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
        let (events, receiver) = broadcast::channel(config.event_capacity);
        let (state, state_rx) = watch::channel(ConnectionState::Connecting);
        let (commands, command_rx) = mpsc::channel(64);
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
        let retry_requests = Arc::new(retries::RetryRequests::default());
        let actor_retry_requests = retry_requests.clone();
        let inner = Arc::new(Inner {
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
        let mut plan = self.inner.devices.plan(jids, use_cache)?;
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
        let groups = self.inner.devices.update(extracted)?;
        let updates = groups
            .iter()
            .map(|(user, devices)| {
                crate::auth::KeyMutation::put(
                    user,
                    crate::auth::KeyValue::DeviceList(
                        devices.iter().map(|d| d.device.to_string()).collect(),
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
    pub fn invalidate_user_devices(&self, jids: &[String]) -> Result<(), Error> {
        let users = jids
            .iter()
            .map(|j| {
                crate::jid::Jid::parse(j)
                    .map(|j| j.user)
                    .ok_or(crate::signal::Error::Address)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(self.inner.devices.invalidate_users(&users)?)
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
    /// Receipt/acknowledgement and retry orchestration are separate operations.
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
