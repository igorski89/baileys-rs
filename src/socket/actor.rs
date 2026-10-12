use super::*;
use crate::{
    auth::{KeyCategory, KeyMutation, KeyStore, KeyValue},
    binary::NodeContent,
};
use std::collections::{HashMap, VecDeque};
use tokio::time::{Instant, MissedTickBehavior};

enum PendingKind {
    Query {
        token: u64,
        reply: oneshot::Sender<Result<BinaryNode, Error>>,
    },
    Heartbeat,
    InitCount,
    InitUpload {
        last: u32,
        count: u32,
        attempt: u8,
        open: bool,
    },
    InitActive,
    InitDigest,
    CodeFinish,
}
struct Pending {
    kind: PendingKind,
    deadline: Instant,
}
struct Actor<'a> {
    receipts: Option<mpsc::UnboundedSender<BinaryNode>>,
    messages: Option<mpsc::UnboundedSender<BinaryNode>>,
    ws: &'a mut WebSocket,
    noise: &'a mut NoiseHandler,
    config: Config,
    store: SqliteAuthStore,
    credentials: Credentials,
    events: broadcast::Sender<Event>,
    state: watch::Sender<ConnectionState>,
    pending: HashMap<String, Pending>,
    prefix: String,
    epoch: u64,
    last_recv: Instant,
    qr_refs: VecDeque<String>,
    qr_deadline: Option<Instant>,
    qr_first: bool,
    close_reply: Option<oneshot::Sender<Result<(), Error>>>,
    upload_retry: Option<(Instant, u32, u8, bool)>,
    initializing: bool,
    server_time_offset_ms: i128,
}
#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    ws: &mut WebSocket,
    noise: &mut NoiseHandler,
    config: Config,
    store: SqliteAuthStore,
    credentials: Credentials,
    prefix: String,
    mut commands: mpsc::Receiver<Command>,
    mut cancel: mpsc::UnboundedReceiver<u64>,
    events: broadcast::Sender<Event>,
    state: watch::Sender<ConnectionState>,
    retry_manager: Option<crate::retry::Manager>,
    retry_requests: Arc<super::retries::RetryRequests>,
    receipts: Option<mpsc::UnboundedSender<BinaryNode>>,
    messages: Option<mpsc::UnboundedSender<BinaryNode>>,
) {
    let mut actor = Actor {
        receipts,
        messages,
        ws,
        noise,
        config,
        store,
        credentials,
        prefix,
        events,
        state,
        pending: HashMap::new(),
        epoch: 0,
        last_recv: Instant::now(),
        qr_refs: VecDeque::new(),
        qr_deadline: None,
        qr_first: true,
        close_reply: None,
        upload_retry: None,
        initializing: false,
        server_time_offset_ms: 0,
    };
    actor.emit(Event::Connection(ConnectionState::Connecting));
    let mut maintenance = tokio::time::interval(Duration::from_millis(25));
    maintenance.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut heartbeat = tokio::time::interval_at(
        Instant::now() + actor.config.keep_alive_interval,
        actor.config.keep_alive_interval,
    );
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let result: Result<(), Error> = async {
        loop {
            while let Some(frame) = actor.noise.next_frame()? {
                match frame {
                    noise::Frame::Node(node) => {
                        actor.last_recv = Instant::now();
                        actor.node(node).await?;
                    }
                    _ => return Err(Error::Closed(500)),
                }
            }
            tokio::select! {
                message = actor.ws.next() => match message {
                    Some(Ok(tungstenite::Message::Binary(bytes))) => actor.noise.push_bytes(&bytes)?,
                    Some(Ok(tungstenite::Message::Ping(bytes))) => {
                        timeout(actor.config.connect_timeout, actor.ws.send(tungstenite::Message::Pong(bytes)))
                            .await.map_err(|_| Error::Timeout)??;
                    }
                    Some(Ok(tungstenite::Message::Pong(_))) => (),
                    Some(Ok(tungstenite::Message::Close(_))) | None => return Err(Error::Closed(428)),
                    Some(Err(e)) => return Err(e.into()),
                    _ => return Err(Error::Closed(500)),
                },
                command = commands.recv() => match command {
                    Some(command) => actor.command(command).await?,
                    None => return Err(Error::Closed(428)),
                },
                Some(token) = cancel.recv() => {
                    actor.pending.retain(|_, p| !matches!(&p.kind, PendingKind::Query { token: t, .. } if *t == token));
                },
                _ = maintenance.tick() => actor.maintain().await?,
                _ = heartbeat.tick() => {
                    if actor.last_recv.elapsed() > actor.config.keep_alive_interval + Duration::from_secs(5) {
                        return Err(Error::Closed(408));
                    }
                    if !actor.pending.values().any(|p| matches!(p.kind, PendingKind::Heartbeat)) {
                        actor.internal_query(
                            BinaryNode::new("iq").attr("to", "@s.whatsapp.net").attr("type", "get").attr("xmlns", "w:p")
                                .with_content(NodeContent::Children(vec![BinaryNode::new("ping")])),
                            PendingKind::Heartbeat,
                        ).await?;
                    }
                },
            }
        }
    }.await;
    let code = result
        .as_ref()
        .err()
        .map(Error::disconnect_code)
        .unwrap_or(428);
    if let Some(manager) = retry_manager
        && let Err(error) = manager.clear()
    {
        tracing::debug!(?error, "retry state cleanup failed");
    }
    retry_requests.clear();
    actor.state.send_replace(ConnectionState::Closed { code });
    actor.emit(Event::Connection(ConnectionState::Closed { code }));
    for (_, pending) in actor.pending.drain() {
        if let PendingKind::Query { reply, .. } = pending.kind {
            let _ = reply.send(Err(Error::Closed(code)));
        }
    }
    tracing::debug!(status_code = code, "WhatsApp connection closed");
    let _ = timeout(actor.config.connect_timeout, actor.ws.close(None)).await;
    if let Some(reply) = actor.close_reply.take() {
        let _ = reply.send(Ok(()));
    }
}
impl Actor<'_> {
    fn emit(&self, event: Event) {
        let _ = self.events.send(event);
    }
    fn tag(&mut self) -> Result<String, Error> {
        let n = self.epoch;
        self.epoch = self.epoch.checked_add(1).ok_or(Error::TagExhausted)?;
        Ok(format!("{}internal-{n}", self.prefix))
    }
    async fn send(&mut self, node: &BinaryNode) -> Result<(), Error> {
        let wire = self.noise.encode_frame(&binary::encode(node)?)?;
        timeout(
            self.config.connect_timeout,
            self.ws.send(tungstenite::Message::Binary(wire.into())),
        )
        .await
        .map_err(|_| Error::Timeout)??;
        Ok(())
    }
    async fn persist(&mut self) -> Result<(), Error> {
        save_credentials(self.store.clone(), self.credentials.clone()).await?;
        self.emit(Event::CredentialsUpdated);
        Ok(())
    }
    async fn internal_query(
        &mut self,
        mut node: BinaryNode,
        kind: PendingKind,
    ) -> Result<(), Error> {
        let id = self.tag()?;
        node.attrs.insert("id".into(), id.clone());
        self.pending.insert(
            id,
            Pending {
                kind,
                deadline: Instant::now() + self.config.query_timeout,
            },
        );
        self.send(&node).await
    }
    async fn command(&mut self, command: Command) -> Result<(), Error> {
        match command {
            Command::AppStateKeys { keys, reply } => {
                let store = self.store.clone();
                let mut credentials = self.credentials.clone();
                let prepared = tokio::task::spawn_blocking(move || {
                    store.transaction_with_error(|tx| {
                        use base64::{Engine, engine::general_purpose::STANDARD};
                        for key in keys {
                            let id = key.key_id.and_then(|k| k.key_id).ok_or(
                                crate::messages::Error::Envelope("missing app-state key id"),
                            )?;
                            let id = STANDARD.encode(id);
                            let mutation = if let Some(data) = key.key_data {
                                KeyMutation::put(id.clone(), KeyValue::AppStateSyncKey(data))
                            } else {
                                KeyMutation::delete(KeyCategory::AppStateSyncKey, id.clone())
                            };
                            tx.set(&[mutation])?;
                            credentials.my_app_state_key_id = Some(id);
                        }
                        tx.save_credentials(&credentials)?;
                        Ok::<_, Error>(credentials)
                    })
                })
                .await
                .map_err(|_| Error::Task);
                let result = match prepared {
                    Ok(Ok(credentials)) => {
                        self.credentials = credentials;
                        self.emit(Event::CredentialsUpdated);
                        Ok(())
                    }
                    Ok(Err(error)) | Err(error) => Err(error),
                };
                let _ = reply.send(result);
            }
            Command::PushName { name, reply } => {
                let result = if let Some(me) = self.credentials.me.as_mut() {
                    if me.name.as_ref() == Some(&name) {
                        Ok(())
                    } else {
                        me.name = Some(name);
                        self.persist().await
                    }
                } else {
                    Err(Error::Config)
                };
                let _ = reply.send(result);
            }
            Command::UnarchiveChats { value, reply } => {
                self.credentials.account_settings = auth::AccountSettings {
                    unarchive_chats: value,
                    ..Default::default()
                };
                let result = self.persist().await;
                let _ = reply.send(result);
            }
            Command::RetryReceipt {
                node,
                count,
                include_keys,
                reply,
            } => {
                let store = self.store.clone();
                let prepared = tokio::task::spawn_blocking(move || {
                    store.transaction_with_error(|tx| {
                        let credentials = tx.load_credentials()?;
                        // Device identity is required even on the no-key first-attempt path.
                        credentials
                            .account
                            .as_ref()
                            .ok_or(crate::relay::Error::Identity)?;
                        let prekey = if include_keys {
                            let batch = tx.allocate_prekeys(1)?;
                            let id = batch.first_pre_key_id;
                            let values = tx.get(KeyCategory::PreKey, &[id.to_string()])?;
                            let pair = match values.get(&id.to_string()) {
                                Some(KeyValue::PreKey(pair)) => pair.clone(),
                                _ => {
                                    return Err(auth::Error::InvalidCredentials(
                                        "missing retry prekey",
                                    )
                                    .into());
                                }
                            };
                            tx.mark_prekeys_uploaded(batch.last_pre_key_id)?;
                            Some((id, pair))
                        } else {
                            None
                        };
                        let receipt = crate::retry::request::receipt(
                            &node,
                            node.attrs.get("id").ok_or(Error::Config)?,
                            count,
                            &credentials,
                            prekey.as_ref().map(|(id, pair)| (*id, pair)),
                        )?;
                        binary::encode(&receipt)?;
                        Ok::<_, Error>((receipt, tx.load_credentials()?))
                    })
                })
                .await
                .map_err(|_| Error::Task);
                match prepared {
                    Ok(Ok((receipt, credentials))) => {
                        self.credentials = credentials;
                        if include_keys {
                            self.emit(Event::CredentialsUpdated);
                        }
                        let result = self.send(&receipt).await;
                        if result.is_err() {
                            let _ = reply.send(Err(Error::Closed(428)));
                            result?;
                        } else {
                            let _ = reply.send(Ok(()));
                        }
                    }
                    Ok(Err(error)) | Err(error) => {
                        let _ = reply.send(Err(error));
                    }
                }
            }
            Command::Query {
                node,
                token,
                timeout: limit,
                reply,
            } => {
                if reply.is_closed() {
                    return Ok(());
                }
                let id = node.attrs.get("id").ok_or(Error::Config)?.clone();
                if self.pending.contains_key(&id)
                    || self.pending.len() >= self.config.max_pending_queries
                {
                    let _ = reply.send(Err(Error::PendingLimit));
                    return Ok(());
                }
                if limit.is_zero() {
                    let _ = reply.send(Err(Error::Timeout));
                    return Ok(());
                }
                self.pending.insert(
                    id,
                    Pending {
                        kind: PendingKind::Query { token, reply },
                        deadline: Instant::now() + limit,
                    },
                );
                self.send(&node).await?;
            }
            Command::Send { node, reply } => {
                let result = self.send(&node).await;
                if result.is_err() {
                    let _ = reply.send(Err(Error::Closed(428)));
                    result?;
                } else {
                    let _ = reply.send(Ok(()));
                }
            }
            Command::PairingCode { phone, code, reply } => {
                if reply.is_closed() {
                    return Ok(());
                }
                if matches!(*self.state.borrow(), ConnectionState::Open) {
                    let _ = reply.send(Err(Error::Config));
                    return Ok(());
                }
                let mut credentials = self.credentials.clone();
                let browser = self.config.payload.browser.clone();
                let id = self.tag()?;
                let result = tokio::task::spawn_blocking(move || {
                    let (code, node) = connection::pairing_code_request(
                        &mut credentials,
                        &phone,
                        code.as_deref(),
                        &browser,
                        &id,
                    )?;
                    Ok::<_, connection::Error>((credentials, code, node))
                })
                .await
                .map_err(|_| Error::Task)?;
                match result {
                    Ok((credentials, code, node)) => {
                        self.credentials = credentials;
                        self.persist().await?;
                        self.send(&node).await?;
                        let _ = reply.send(Ok(code));
                    }
                    Err(e) => {
                        let _ = reply.send(Err(e.into()));
                    }
                }
            }
            Command::Close { logout, reply } => {
                self.close_reply = Some(reply);
                if logout && let Some(me) = &self.credentials.me {
                    let jid = me.id.clone();
                    let id = self.tag()?;
                    self.send(
                        &BinaryNode::new("iq")
                            .attr("to", "@s.whatsapp.net")
                            .attr("type", "set")
                            .attr("id", id)
                            .attr("xmlns", "md")
                            .with_content(NodeContent::Children(vec![
                                BinaryNode::new("remove-companion-device")
                                    .attr("jid", jid)
                                    .attr("reason", "user_initiated"),
                            ])),
                    )
                    .await?;
                }
                return Err(Error::Closed(if logout { 401 } else { 428 }));
            }
        }
        Ok(())
    }
    async fn node(&mut self, node: BinaryNode) -> Result<(), Error> {
        if let Some(id) = node.attrs.get("id")
            && let Some(pending) = self.pending.remove(id)
        {
            self.response(pending.kind, node.clone()).await?;
        }
        if node.tag == "receipt"
            && let Some(sender) = &self.receipts
        {
            let _ = sender.send(node.clone());
        }
        if node.tag == "message"
            && let Some(sender) = &self.messages
        {
            let _ = sender.send(node.clone());
        }
        match node.tag.as_str() {
            "stream:error" => {
                let code = node
                    .attrs
                    .get("code")
                    .and_then(|n| n.parse().ok())
                    .unwrap_or_else(|| {
                        if node
                            .all_children()
                            .first()
                            .is_some_and(|n| n.tag == "conflict")
                        {
                            440
                        } else {
                            500
                        }
                    });
                return Err(Error::Closed(code));
            }
            "failure" => {
                return Err(Error::Closed(
                    node.attrs
                        .get("reason")
                        .and_then(|n| n.parse().ok())
                        .unwrap_or(500),
                ));
            }
            "xmlstreamend" => return Err(Error::Closed(428)),
            "success" => {
                self.update_server_time(&node);
                if self.initializing || matches!(*self.state.borrow(), ConnectionState::Open) {
                    return Ok(());
                }
                self.initializing = true;
                self.qr_deadline = None;
                self.qr_refs.clear();
                let me = self.credentials.me.as_mut().ok_or(Error::Closed(500))?;
                me.lid = node.attrs.get("lid").cloned();
                self.persist().await?;
                if let Some(me) = &self.credentials.me
                    && let Some(lid) = &me.lid
                {
                    let mapping = crate::signal::mapping::Mapping {
                        pn: me.id.clone(),
                        lid: lid.clone(),
                    };
                    let decoded = crate::jid::Jid::parse(&me.id).ok_or(Error::Config)?;
                    let store = self.store.clone();
                    let outcome = tokio::task::spawn_blocking(move || {
                        let repository = crate::signal::direct::Repository::new(store.clone());
                        repository
                            .lid_mapping()
                            .store_mappings(std::slice::from_ref(&mapping))?;
                        store.set(&[KeyMutation::put(
                            decoded.user,
                            KeyValue::DeviceList(vec![decoded.device.unwrap_or(0).to_string()]),
                        )])?;
                        repository.migrate_session(&mapping.pn, &mapping.lid)?;
                        Ok::<_, crate::signal::Error>(())
                    })
                    .await
                    .map_err(|_| Error::Task)
                    .and_then(|result| result.map_err(Error::Signal));
                    if let Err(error) = outcome {
                        tracing::warn!(?error, "failed to initialize own LID session");
                    }
                }
                if self.config.initialize_session {
                    self.internal_query(
                        BinaryNode::new("iq")
                            .attr("to", "@s.whatsapp.net")
                            .attr("type", "get")
                            .attr("xmlns", "encrypt")
                            .with_content(NodeContent::Children(vec![BinaryNode::new("count")])),
                        PendingKind::InitCount,
                    )
                    .await?;
                } else {
                    self.open().await?;
                }
            }
            "iq" if node.child("pair-device").is_some()
                && node.attrs.get("type").is_some_and(|v| v == "set") =>
            {
                let id = node.attrs.get("id").ok_or(Error::Closed(500))?;
                self.send(
                    &BinaryNode::new("iq")
                        .attr("to", "@s.whatsapp.net")
                        .attr("type", "result")
                        .attr("id", id),
                )
                .await?;
                self.qr_refs = node
                    .child("pair-device")
                    .ok_or(Error::Closed(500))?
                    .children("ref")
                    .filter_map(|n| match &n.content {
                        Some(NodeContent::Bytes(b)) => {
                            Some(String::from_utf8_lossy(b).into_owned())
                        }
                        Some(NodeContent::Text(s)) => Some(s.clone()),
                        _ => None,
                    })
                    .collect();
                self.qr_first = true;
                self.next_qr()?;
            }
            "iq" if node.child("pair-success").is_some() => {
                self.update_server_time(&node);
                let pairing = connection::configure_successful_pairing(&node, &self.credentials)?;
                pairing.apply(&mut self.credentials);
                self.persist().await?;
                self.qr_refs.clear();
                self.qr_deadline = None;
                self.emit(Event::PairingComplete);
                self.send(&pairing.reply).await?;
                self.unified_session().await?;
            }
            "notification"
                if node.attrs.get("type").is_some_and(|s| s == "mediaretry")
                    && self.config.process_media_notifications =>
            {
                match crate::media::decode_retry(&node) {
                    Ok(event) => self.emit(Event::MediaUpdated(vec![event])),
                    Err(error) => tracing::debug!(?error, "media notification decode failed"),
                }
                let ack = crate::receipts::acknowledgement(&node, None, None);
                self.send(&ack).await?;
            }
            "notification" if node.child("link_code_companion_reg").is_some() => {
                let credentials = self.credentials.clone();
                let notification = node.clone();
                let id = self.tag()?;
                let finish = tokio::task::spawn_blocking(move || {
                    connection::pairing_code_finish(&credentials, &notification, &id)
                })
                .await
                .map_err(|_| Error::Task)??;
                self.credentials.adv_secret_key = finish.adv_secret_key;
                self.persist().await?;
                self.internal_query(finish.node, PendingKind::CodeFinish)
                    .await?;
                if let (Some(id), Some(from)) = (node.attrs.get("id"), node.attrs.get("from")) {
                    let mut ack = BinaryNode::new("ack")
                        .attr("id", id)
                        .attr("to", from)
                        .attr("class", "notification");
                    for key in ["participant", "recipient", "type"] {
                        if let Some(value) = node.attrs.get(key).filter(|s| !s.is_empty()) {
                            ack.attrs.insert(key.into(), value.clone());
                        }
                    }
                    self.send(&ack).await?;
                }
            }
            "ib" => {
                if node.child("downgrade_webclient").is_some() {
                    return Err(Error::Closed(411));
                }
                if node.child("offline_preview").is_some() {
                    self.send(
                        &BinaryNode::new("ib").with_content(NodeContent::Children(vec![
                            BinaryNode::new("offline_batch").attr("count", "100"),
                        ])),
                    )
                    .await?;
                }
                if let Some(bytes) = node
                    .child("edge_routing")
                    .and_then(|n| n.child_buffer("routing_info"))
                {
                    self.credentials.routing_info = Some(bytes.to_vec());
                    self.persist().await?;
                }
                if node.child("offline").is_some() {
                    self.emit(Event::PendingNotifications);
                }
            }
            _ => (),
        }
        self.emit(Event::Node(node));
        Ok(())
    }
    async fn response(&mut self, kind: PendingKind, node: BinaryNode) -> Result<(), Error> {
        let valid = node.assert_error_free().is_ok();
        match kind {
            PendingKind::Query { reply, .. } => {
                let result = node.assert_error_free().map(|()| node).map_err(Error::from);
                let _ = reply.send(result);
            }
            PendingKind::Heartbeat => (),
            PendingKind::CodeFinish => {
                node.assert_error_free()?;
                self.credentials.registered = true;
                self.persist().await?;
            }
            PendingKind::InitCount => {
                let count = node
                    .child("count")
                    .and_then(|n| n.attrs.get("value"))
                    .and_then(|n| n.parse::<u32>().ok());
                if valid && let Some(count) = count {
                    let last = self.credentials.next_pre_key_id.saturating_sub(1);
                    let store = self.store.clone();
                    let missing = tokio::task::spawn_blocking(move || {
                        store
                            .get(KeyCategory::PreKey, &[last.to_string()])
                            .map(|keys| last > 0 && !keys.contains_key(&last.to_string()))
                    })
                    .await
                    .map_err(|_| Error::Task)??;
                    let threshold = if count == 0 { 812 } else { 5 };
                    if count <= threshold || missing {
                        self.upload(threshold, 0, false).await?;
                        return Ok(());
                    }
                }
                self.active().await?;
            }
            PendingKind::InitUpload {
                last,
                count,
                attempt,
                open,
            } => {
                if valid {
                    let store = self.store.clone();
                    tokio::task::spawn_blocking(move || {
                        store.transaction(|tx| tx.mark_prekeys_uploaded(last))
                    })
                    .await
                    .map_err(|_| Error::Task)??;
                    self.credentials = load_credentials(self.store.clone()).await?;
                    self.emit(Event::CredentialsUpdated);
                }
                if !valid {
                    self.retry_upload(count, attempt, open).await?;
                } else if open {
                    self.open().await?;
                } else {
                    self.active().await?;
                }
            }
            PendingKind::InitActive => {
                self.internal_query(
                    BinaryNode::new("iq")
                        .attr("to", "@s.whatsapp.net")
                        .attr("type", "get")
                        .attr("xmlns", "encrypt")
                        .with_content(NodeContent::Children(vec![BinaryNode::new("digest")])),
                    PendingKind::InitDigest,
                )
                .await?;
            }
            PendingKind::InitDigest => {
                if valid && node.child("digest").is_none() {
                    self.upload(5, 0, true).await?;
                } else {
                    self.open().await?;
                }
            }
        }
        Ok(())
    }
    async fn upload(&mut self, count: u32, attempt: u8, open: bool) -> Result<(), Error> {
        let store = self.store.clone();
        let (batch, keys, credentials) = tokio::task::spawn_blocking(move || {
            store.transaction(|tx| {
                let batch = tx.allocate_prekeys(count)?;
                let ids: Vec<_> = (batch.first_pre_key_id..=batch.last_pre_key_id)
                    .map(|n| n.to_string())
                    .collect();
                let keys = tx.get(KeyCategory::PreKey, &ids)?;
                Ok((batch, keys, tx.load_credentials()?))
            })
        })
        .await
        .map_err(|_| Error::Task)??;
        self.credentials = credentials;
        self.emit(Event::CredentialsUpdated);
        let mut list = Vec::new();
        for id in batch.first_pre_key_id..=batch.last_pre_key_id {
            let Some(KeyValue::PreKey(pair)) = keys.get(&id.to_string()) else {
                return Err(Error::Closed(500));
            };
            list.push(
                BinaryNode::new("key").with_content(NodeContent::Children(vec![
                    bytes_node("id", id.to_be_bytes()[1..].to_vec()),
                    bytes_node("value", pair.public.to_vec()),
                ])),
            );
        }
        let signed = &self.credentials.signed_pre_key;
        let node = BinaryNode::new("iq")
            .attr("to", "@s.whatsapp.net")
            .attr("type", "set")
            .attr("xmlns", "encrypt")
            .with_content(NodeContent::Children(vec![
                bytes_node(
                    "registration",
                    u32::from(self.credentials.registration_id)
                        .to_be_bytes()
                        .to_vec(),
                ),
                bytes_node("type", vec![5]),
                bytes_node(
                    "identity",
                    self.credentials.signed_identity_key.public.to_vec(),
                ),
                BinaryNode::new("list").with_content(NodeContent::Children(list)),
                BinaryNode::new("skey").with_content(NodeContent::Children(vec![
                    bytes_node("id", signed.key_id.to_be_bytes()[1..].to_vec()),
                    bytes_node("value", signed.key_pair.public.to_vec()),
                    bytes_node("signature", signed.signature.clone()),
                ])),
            ]));
        self.internal_query(
            node,
            PendingKind::InitUpload {
                last: batch.last_pre_key_id,
                count,
                attempt,
                open,
            },
        )
        .await
    }
    async fn retry_upload(&mut self, count: u32, attempt: u8, open: bool) -> Result<(), Error> {
        if attempt < 3 {
            self.upload_retry = Some((
                Instant::now() + Duration::from_secs(1 << attempt),
                count,
                attempt + 1,
                open,
            ));
        } else if open {
            self.open().await?;
        } else {
            self.active().await?;
        }
        Ok(())
    }
    fn update_server_time(&mut self, node: &BinaryNode) {
        if let Some(seconds) = node
            .attrs
            .get("t")
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|t| *t > 0)
            && let Ok(now) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        {
            self.server_time_offset_ms = i128::from(seconds) * 1000 - now.as_millis() as i128;
        }
    }
    async fn active(&mut self) -> Result<(), Error> {
        self.internal_query(
            BinaryNode::new("iq")
                .attr("to", "@s.whatsapp.net")
                .attr("type", "set")
                .attr("xmlns", "passive")
                .with_content(NodeContent::Children(vec![BinaryNode::new("active")])),
            PendingKind::InitActive,
        )
        .await
    }
    async fn open(&mut self) -> Result<(), Error> {
        self.state.send_replace(ConnectionState::Open);
        self.emit(Event::Connection(ConnectionState::Open));
        self.unified_session().await
    }
    async fn unified_session(&mut self) -> Result<(), Error> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::Config)?
            .as_millis();
        let id = (now as i128 + self.server_time_offset_ms + 3 * 24 * 60 * 60 * 1000)
            .rem_euclid(7 * 24 * 60 * 60 * 1000);
        self.send(
            &BinaryNode::new("ib").with_content(NodeContent::Children(vec![
                BinaryNode::new("unified_session").attr("id", id.to_string()),
            ])),
        )
        .await
    }
    fn next_qr(&mut self) -> Result<(), Error> {
        let reference = self.qr_refs.pop_front().ok_or(Error::Timeout)?;
        self.emit(Event::Qr(connection::pairing_qr(
            &reference,
            &self.credentials,
            &self.config.payload.browser,
        )));
        let duration = self
            .config
            .qr_timeout
            .unwrap_or_else(|| Duration::from_secs(if self.qr_first { 60 } else { 20 }));
        self.qr_first = false;
        self.qr_deadline = Some(Instant::now() + duration);
        Ok(())
    }
    async fn maintain(&mut self) -> Result<(), Error> {
        if self
            .upload_retry
            .as_ref()
            .is_some_and(|(deadline, ..)| *deadline <= Instant::now())
        {
            let (_, count, attempt, open) = self.upload_retry.take().expect("checked above");
            self.upload(count, attempt, open).await?;
        }
        if self.qr_deadline.is_some_and(|t| t <= Instant::now()) {
            self.next_qr()?;
        }
        let expired: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, p)| {
                p.deadline <= Instant::now()
                    || matches!(&p.kind,PendingKind::Query{reply,..} if reply.is_closed())
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            if let Some(p) = self.pending.remove(&id) {
                match p.kind {
                    PendingKind::Query { reply, .. } => {
                        let _ = reply.send(Err(Error::Timeout));
                    }
                    PendingKind::InitCount => self.active().await?,
                    PendingKind::InitUpload {
                        count,
                        attempt,
                        open,
                        ..
                    } => self.retry_upload(count, attempt, open).await?,
                    PendingKind::InitActive | PendingKind::InitDigest => self.open().await?,
                    PendingKind::CodeFinish => return Err(Error::Timeout),
                    PendingKind::Heartbeat => (),
                }
            }
        }
        Ok(())
    }
}
fn bytes_node(tag: &str, bytes: Vec<u8>) -> BinaryNode {
    BinaryNode::new(tag).with_content(NodeContent::Bytes(bytes))
}
