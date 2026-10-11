//! Async WhatsApp WebSocket connection with exclusive Noise ownership and IQ routing.
mod actor;
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
            Self::Closed(code) => Some(*code),
            Self::Auth(e) => e.status_code(),
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
        }
    }
}
struct Inner {
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
        let inner = Arc::new(Inner {
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
            )
            .await;
        });
        Ok((Self { inner }, receiver))
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
