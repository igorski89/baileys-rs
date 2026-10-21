use super::{Error, Socket, load_credentials};
use crate::{
    binary,
    devices::Device,
    groups::AddressingMode,
    jid::{self, Jid},
    messages, privacy, proto, relay,
};
use indexmap::IndexMap;
use std::{
    collections::HashSet,
    time::{SystemTime, UNIX_EPOCH},
};

/// Relay-related AB properties. Defaults match upstream before properties are fetched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerProps {
    pub privacy_token_on_1_to_1: bool,
    pub profile_pic_privacy_token: bool,
    pub lid_trusted_token_issue_to_lid: bool,
}
impl Default for ServerProps {
    fn default() -> Self {
        Self {
            privacy_token_on_1_to_1: true,
            profile_pic_privacy_token: true,
            lid_trusted_token_issue_to_lid: false,
        }
    }
}
impl ServerProps {
    /// Numeric AB IDs take precedence over named aliases; absent values keep their state.
    pub fn apply(&mut self, props: &IndexMap<String, String>) {
        for (id, name, target) in [
            (
                "10518",
                "privacy_token_sending_on_all_1_on_1_messages",
                &mut self.privacy_token_on_1_to_1,
            ),
            (
                "9666",
                "profile_scraping_privacy_token_in_photo_iq",
                &mut self.profile_pic_privacy_token,
            ),
            (
                "14303",
                "lid_trusted_token_issue_to_lid",
                &mut self.lid_trusted_token_issue_to_lid,
            ),
        ] {
            if let Some(value) = props.get(id).or_else(|| props.get(name)) {
                *target = value == "true" || value == "1";
            }
        }
    }
}
/// Options for explicit relay; automatic resend caching remains pending.
#[derive(Debug, Clone)]
pub struct RelayOptions {
    pub message_id: Option<String>,
    pub stanza: relay::StanzaOptions,
    /// Controls group/status discovery. Direct sends use the cache, matching upstream.
    pub use_user_devices_cache: bool,
    pub use_cached_group_metadata: bool,
    pub cached_group_metadata: Option<crate::groups::Metadata>,
    pub status_jid_list: Vec<String>,
}
impl Default for RelayOptions {
    fn default() -> Self {
        Self {
            message_id: None,
            stanza: Default::default(),
            use_user_devices_cache: true,
            use_cached_group_metadata: true,
            cached_group_metadata: None,
            status_jid_list: vec![],
        }
    }
}
#[derive(Default)]
pub(super) struct RelayState {
    pub props: ServerProps,
    pub in_flight: HashSet<String>,
}
struct Issuing {
    socket: Socket,
    jid: String,
}
impl Drop for Issuing {
    fn drop(&mut self) {
        if let Ok(mut state) = self.socket.inner.relay_state.lock() {
            state.in_flight.remove(&self.jid);
        }
    }
}
fn now() -> Result<u64, Error> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Config)?
        .as_secs())
}
impl Socket {
    pub fn server_props(&self) -> Result<ServerProps, Error> {
        Ok(self
            .inner
            .relay_state
            .lock()
            .map_err(|_| Error::Task)?
            .props)
    }
    pub fn apply_server_props(
        &self,
        props: &IndexMap<String, String>,
    ) -> Result<ServerProps, Error> {
        let mut state = self.inner.relay_state.lock().map_err(|_| Error::Task)?;
        state.props.apply(props);
        Ok(state.props)
    }
    /// Prepare all cipher state atomically, then send. A transport failure retains
    /// committed counters because bytes may already have reached the peer.
    /// Set the account's member label in a group.
    pub async fn update_member_label(&self, jid: &str, label: &str) -> Result<String, Error> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::Config)?
            .as_secs() as i64;
        let mut options = RelayOptions::default();
        options
            .stanza
            .additional_nodes
            .push(relay::member_label_meta());
        self.relay_message(jid, relay::member_label_message(label, now), options)
            .await
    }
    pub async fn relay_message(
        &self,
        destination: &str,
        message: proto::Message,
        mut options: RelayOptions,
    ) -> Result<String, Error> {
        // A phone's vote carries this node; without it the server acks a vote but does not deliver it.
        // Added here, not in send_message, so retry resends carry it too (upstream sends no votes).
        if relay::normalize(Some(&message)).is_some_and(|m| m.poll_update_message.is_some())
            && !options
                .stanza
                .additional_nodes
                .iter()
                .any(|n| n.tag == "meta")
        {
            options.stanza.additional_nodes.push(
                binary::BinaryNode::new("meta")
                    .attr("polltype", "vote")
                    .attr("content_type", "add_on"),
            );
        }
        let _serial = self.inner.relay_lock.lock().await;
        let credentials = load_credentials(self.inner.store.clone()).await?;
        let me = credentials.me_id()?.to_owned();
        let me_lid = credentials
            .me
            .as_ref()
            .and_then(|me| me.lid.clone())
            .filter(|s| !s.is_empty());
        let target = Jid::parse(destination).ok_or(relay::Error::Recipient)?;
        let id = options
            .message_id
            .clone()
            .filter(|s| !s.is_empty())
            .map(Ok)
            .unwrap_or_else(|| messages::generate_id_v2(Some(&me)))?;
        let patch = self.inner.patch_message_before_sending.clone();
        if target.is_newsletter() {
            let original = message.clone();
            let patched = tokio::task::spawn_blocking(move || patch.apply(&message, Some(&[])))
                .await
                .map_err(|_| Error::Task)??;
            // The source encodes an array patch as an object with no protobuf fields.
            let patched = match patched {
                relay::Patch::Shared(message) => *message,
                relay::Patch::PerRecipient(_) => Default::default(),
            };
            let node =
                relay::newsletter_stanza(destination, &id, &original, &patched, &options.stanza)?;
            self.send_node(node).await?;
            return Ok(id);
        }
        let group = target.is_group() || destination == jid::STORIES_JID;
        let retry = options.stanza.retry.is_some();
        let mut reporting_message = if !group && !retry {
            let patch = patch.clone();
            let original = message.clone();
            let destination = destination.to_owned();
            relay::reporting_patch_message(
                tokio::task::spawn_blocking(move || patch.apply(&original, Some(&[destination])))
                    .await
                    .map_err(|_| Error::Task)??,
            )
        } else {
            None
        };
        let mut sender = me.clone();
        let devices = if let Some(participant) = &options.stanza.retry {
            vec![Device::from(
                Jid::parse(&participant.jid).ok_or(relay::Error::Recipient)?,
            )]
        } else if group {
            let metadata = if target.is_group() {
                match options
                    .cached_group_metadata
                    .take()
                    .filter(|_| options.use_cached_group_metadata)
                {
                    Some(metadata) => Some(metadata),
                    None => Some(self.group_metadata(destination).await?),
                }
            } else {
                None
            };
            let mut users = metadata
                .as_ref()
                .map(|m| {
                    m.participants
                        .iter()
                        .filter_map(|p| p.id.clone())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if let Some(metadata) = metadata {
                if let Some(expiration) = metadata
                    .ephemeral_duration
                    .and_then(|n| n.0)
                    .filter(|n| *n > 0.0)
                {
                    options
                        .stanza
                        .additional_attributes
                        .insert("expiration".into(), expiration.to_string());
                }
                options.stanza.additional_attributes.insert(
                    "addressing_mode".into(),
                    match metadata.addressing_mode {
                        AddressingMode::Pn => "pn",
                        AddressingMode::Lid => "lid",
                    }
                    .into(),
                );
            }
            if destination == jid::STORIES_JID {
                users.extend(options.status_jid_list.clone());
            }
            if options
                .stanza
                .additional_attributes
                .get("addressing_mode")
                .is_none_or(|s| s.is_empty() || s == "lid")
            {
                sender = me_lid.clone().unwrap_or_else(|| me.clone());
            }
            self.get_usync_devices(&users, options.use_user_devices_cache, false)
                .await?
        } else {
            let own = if target.is_lid_user() {
                me_lid.as_deref().unwrap_or(&me)
            } else {
                &me
            };
            let own = Jid::parse(own).ok_or(relay::Error::Recipient)?;
            let server = if target.is_lid_user() {
                "lid"
            } else {
                "s.whatsapp.net"
            };
            if options
                .stanza
                .additional_attributes
                .get("category")
                .is_some_and(|s| s == "peer")
            {
                let mut devices = vec![Device::from(
                    Jid::parse(&jid::encode(&target.user, server, None, None)).unwrap(),
                )];
                if target.user != own.user {
                    devices.push(Device::from(
                        Jid::parse(&jid::encode(&own.user, server, None, None)).unwrap(),
                    ));
                }
                devices
            } else {
                let sender_server = if target.is_lid_user() && me_lid.is_some() {
                    "lid"
                } else {
                    "s.whatsapp.net"
                };
                self.get_usync_devices(
                    &[
                        jid::encode(&own.user, sender_server, None, None),
                        destination.into(),
                    ],
                    true,
                    false,
                )
                .await?
            }
        };
        let recipients = relay::direct_recipients(&devices, &me, me_lid.as_deref())?;
        let required = if group && !retry {
            let store = self.inner.store.clone();
            let destination = destination.to_owned();
            let devices = devices.clone();
            tokio::task::spawn_blocking(move || {
                let repo = relay::Repository::new(store);
                repo.transaction(&[], |tx| {
                    let mut memory = if destination == jid::STORIES_JID {
                        Default::default()
                    } else {
                        tx.sender_key_memory(&destination)?
                    };
                    Ok(relay::sender_key_recipients(&devices, &mut memory, false))
                })
            })
            .await
            .map_err(|_| Error::Task)??
        } else {
            recipients.all.clone()
        };
        self.assert_sessions(&required, false).await?;
        let one_to_one = relay::is_one_to_one(destination, &options.stanza)?;
        if one_to_one && target.is_pn_user() {
            self.resolve_lid_mappings(&[destination.into()]).await?;
        }
        let privacy_repo = privacy::Repository::with_mappings(
            self.inner.store.clone(),
            self.inner.decryptor.direct.lid_mapping().clone(),
        );
        let storage = if one_to_one {
            let repo = privacy_repo.clone();
            let dest = destination.to_owned();
            tokio::task::spawn_blocking(move || repo.storage_jid(&dest))
                .await
                .map_err(|_| Error::Task)??
        } else {
            destination.into()
        };
        let props = self.server_props()?;
        let timestamp = now()?;
        let mut scope = required;
        scope.extend(recipients.all.iter().cloned());
        if let Some(participant) = &options.stanza.retry {
            scope.push(participant.jid.clone());
        }
        let repository = relay::Repository::with_mappings(
            self.inner.store.clone(),
            self.inner.decryptor.direct.lid_mapping().clone(),
        );
        let destination_owned = destination.to_owned();
        let id_owned = id.clone();
        let storage_owned = storage.clone();
        let recent_message = if !retry && self.inner.retry_manager.is_some() {
            Some(message.clone())
        } else {
            None
        };
        let protocol =
            relay::normalize(Some(&message)).is_some_and(|m| m.protocol_message.is_some());
        let (node, sender_timestamp) = tokio::task::spawn_blocking(move || {
            repository.transaction(&scope, |tx| {
                let attrs = relay::encrypted_attributes(&message);
                let dsm = relay::device_sent_message(&destination_owned, &message);
                let mut content = vec![];
                let mut participants = vec![];
                let mut include_identity = retry;
                if group && !retry {
                    let patched = match patch.apply(&message, None)? {
                        relay::Patch::Shared(message) => *message,
                        relay::Patch::PerRecipient(_) => {
                            return Err(relay::Error::Patch(
                                "Per-jid patching is not supported in groups".into(),
                            ));
                        }
                    };
                    reporting_message = Some(patched.clone());
                    let nodes = tx.group_fanout_patched(
                        relay::GroupFanout {
                            destination: &destination_owned,
                            sender: &sender,
                            message: &patched,
                            devices: &devices,
                            me: &me,
                            me_lid: me_lid.as_deref(),
                            extra_attrs: &attrs,
                        },
                        |message, recipients| patch.apply(message, Some(recipients)),
                    )?;
                    content.push(nodes.encrypted);
                    include_identity |= nodes.participants.should_include_device_identity;
                    participants.extend(nodes.participants.nodes);
                    for failure in nodes.participants.failures {
                        tracing::debug!(?failure.error, "recipient encryption failed");
                    }
                } else {
                    for (targets, payload, override_dsm) in [
                        (&recipients.own, &dsm, None),
                        (&recipients.other, &message, Some(&dsm)),
                    ] {
                        let nodes = relay::create_participant_nodes_patched(
                            relay::Fanout {
                                recipients: targets,
                                message: payload,
                                me: &me,
                                me_lid: me_lid.as_deref(),
                                extra_attrs: &attrs,
                                device_sent_message: override_dsm,
                            },
                            tx,
                            |message, recipients| patch.apply(message, Some(recipients)),
                        )?;
                        include_identity |= nodes.should_include_device_identity;
                        participants.extend(nodes.nodes);
                        for failure in nodes.failures {
                            tracing::debug!(?failure.error, "recipient encryption failed");
                        }
                    }
                }
                if let Some(participant) = &options.stanza.retry {
                    let distribution =
                        relay::retry_distribution(tx, &destination_owned, &me, me_lid.as_deref())?;
                    if let Some(error) = distribution.warning {
                        tracing::debug!(?error, "retry distribution unavailable");
                    }
                    let payload = relay::retry_payload(
                        &destination_owned,
                        &message,
                        &participant.jid,
                        &me,
                        me_lid.as_deref(),
                        distribution.bytes.as_deref(),
                    )?;
                    let mut node = relay::encrypt_retry(participant, &payload, tx)?;
                    // As on the first send; upstream omits it on retries (see SHORTCOMINGS).
                    if let Some(hide) = attrs.get("decrypt-fail") {
                        node.attrs.insert("decrypt-fail".into(), hide.clone());
                    }
                    content.push(node);
                }
                let (token, sender_timestamp) = if one_to_one {
                    tx.privacy_token(&storage_owned, timestamp)?
                } else {
                    (None, None)
                };
                let report_bytes = if !retry
                    && reporting_message.as_ref().is_some_and(|message| {
                        message
                            .message_context_info
                            .as_ref()
                            .is_some_and(|context| context.message_secret.is_some())
                            && crate::reporting::should_include_token(message)
                    }) {
                    match messages::encode(reporting_message.as_ref().unwrap()) {
                        Ok(bytes) => Some(bytes),
                        Err(error) => {
                            tracing::debug!(?error, "report token encoding failed");
                            None
                        }
                    }
                } else {
                    None
                };
                let assembled = relay::assemble(relay::Stanza {
                    destination: &destination_owned,
                    id: &id_owned,
                    message: &message,
                    me: &me,
                    options: &options.stanza,
                    content,
                    participants,
                    include_device_identity: include_identity,
                    account: credentials.account.as_ref(),
                    reporting: report_bytes
                        .as_deref()
                        .map(|b| (reporting_message.as_ref().unwrap(), b)),
                    privacy_token: token.as_deref(),
                    privacy_token_on_1_to_1: props.privacy_token_on_1_to_1,
                })?;
                if let Some(error) = assembled.reporting_error {
                    tracing::debug!(?error, "report token unavailable");
                }
                binary::encode(&assembled.node)?;
                Ok((assembled.node, sender_timestamp))
            })
        })
        .await
        .map_err(|_| Error::Task)??;
        self.send_node(node).await?;
        if one_to_one
            && !protocol
            && destination != jid::PSA_WID
            && !target.is_bot()
            && !target.is_meta_ai()
            && privacy::should_issue(sender_timestamp, timestamp)
        {
            let launch = self
                .inner
                .relay_state
                .lock()
                .map_err(|_| Error::Task)?
                .in_flight
                .insert(storage.clone());
            if launch {
                let socket = self.clone();
                let destination = destination.to_owned();
                let guard = Issuing {
                    socket: socket.clone(),
                    jid: storage.clone(),
                };
                tokio::spawn(async move {
                    let _guard = guard;
                    let result = async {
                        let repo = privacy_repo.clone();
                        let issuance = tokio::task::spawn_blocking(move || {
                            repo.issuance_jid(&destination, props.lid_trusted_token_issue_to_lid)
                        })
                        .await
                        .map_err(|_| Error::Task)??;
                        let response = socket
                            .issue_privacy_tokens(&[issuance], Some(timestamp))
                            .await?;
                        tokio::task::spawn_blocking(move || {
                            privacy_repo.store_from_iq(&response, &storage)?;
                            privacy_repo.mark_issued(&storage, timestamp)
                        })
                        .await
                        .map_err(|_| Error::Task)??;
                        Ok::<_, Error>(())
                    }
                    .await;
                    if let Err(error) = result {
                        tracing::debug!(?error, "privacy token issuance failed");
                    }
                });
            }
        }
        if let (Some(manager), Some(message)) = (&self.inner.retry_manager, recent_message) {
            manager.add_recent_message(destination, &id, message)?;
        }
        Ok(id)
    }
}
