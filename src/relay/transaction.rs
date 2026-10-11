use super::{Encryptor, Error, Fanout, ParticipantNodes};
use crate::{
    auth::{AuthTransaction, Credentials, KeyCategory, KeyMutation, KeyValue, SqliteAuthStore},
    binary::{BinaryNode, NodeContent},
    devices::Device,
    jid::{self, Jid},
    messages, proto,
    signal::{self, SenderKeyName, direct, group},
};
use indexmap::IndexMap;
use std::{cell::RefCell, collections::BTreeMap, sync::Arc};

#[derive(Clone)]
pub struct Repository {
    store: SqliteAuthStore,
    direct: direct::Repository,
}
impl Repository {
    pub fn new(store: SqliteAuthStore) -> Self {
        Self {
            direct: direct::Repository::new(store.clone()),
            store,
        }
    }
    pub fn with_mappings(store: SqliteAuthStore, mappings: signal::mapping::Store) -> Self {
        Self {
            direct: direct::Repository::with_mappings(store.clone(), mappings),
            store,
        }
    }
    /// Resolve every potential recipient before locking SQLite. Call only transaction
    /// methods inside `work`; using another repository for this store would deadlock.
    /// All returned ciphertext/state commits together; errors discard the whole batch.
    pub fn transaction<T>(
        &self,
        recipients: &[String],
        work: impl FnOnce(&Transaction<'_, '_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut addresses = IndexMap::new();
        for jid in recipients {
            if !addresses.contains_key(jid) {
                addresses.insert(jid.clone(), self.direct.address(jid).map_err(Arc::new));
            }
        }
        self.store.transaction_with_error(|tx| {
            work(&Transaction {
                tx: RefCell::new(tx),
                addresses,
            })
        })
    }
}
pub struct Transaction<'a, 'db> {
    tx: RefCell<&'a mut AuthTransaction<'db>>,
    addresses: IndexMap<String, Result<String, Arc<signal::Error>>>,
}
impl Encryptor for Transaction<'_, '_> {
    fn encrypt(&self, jid: &str, data: &[u8]) -> Result<direct::Encrypted, Error> {
        let resolved;
        let id = if let Some(id) = self.addresses.get(jid) {
            id.as_ref()
                .map_err(|error| signal::Error::Shared(error.clone()))?
        } else {
            // Per-recipient patches may introduce targets outside the discovery list.
            // Resolve persisted aliases through this transaction, without re-locking SQLite.
            let mut address = signal::Address::from_jid(jid)?;
            let decoded = Jid::parse(jid).ok_or(Error::Recipient)?;
            if !matches!(decoded.domain_type, 1 | 129)
                && let Some(KeyValue::LidMapping(lid)) = self
                    .tx
                    .borrow_mut()
                    .get(KeyCategory::LidMapping, std::slice::from_ref(&decoded.user))?
                    .remove(&decoded.user)
            {
                address = signal::Address::from_jid(&jid::encode(
                    &lid,
                    if address.device == 99 {
                        "hosted.lid"
                    } else {
                        "lid"
                    },
                    Some(address.device),
                    None,
                ))?;
            }
            resolved = address.to_string();
            &resolved
        };
        Ok(direct::Repository::encrypt_in_transaction(
            &mut self.tx.borrow_mut(),
            id,
            data,
        )?)
    }
}
impl super::SenderKeys for Transaction<'_, '_> {
    fn has_sender_key(&self, name: &SenderKeyName) -> Result<bool, signal::Error> {
        let id = name.to_string();
        Ok(self
            .tx
            .borrow_mut()
            .get(KeyCategory::SenderKey, std::slice::from_ref(&id))?
            .contains_key(&id))
    }
    fn distribution(&self, name: &SenderKeyName) -> Result<Vec<u8>, signal::Error> {
        group::update_in_transaction(
            &mut self.tx.borrow_mut(),
            name,
            group::Record::create_distribution,
        )
    }
}
impl Transaction<'_, '_> {
    pub fn credentials(&self) -> Result<Credentials, Error> {
        Ok(self.tx.borrow().load_credentials()?)
    }
    pub fn privacy_token(
        &self,
        storage: &str,
        now: u64,
    ) -> Result<(Option<Vec<u8>>, Option<u64>), Error> {
        let mut tx = self.tx.borrow_mut();
        let Some(KeyValue::Tctoken(entry)) = tx
            .get(KeyCategory::Tctoken, &[storage.into()])?
            .remove(storage)
        else {
            return Ok((None, None));
        };
        let sender = entry.sender_timestamp;
        if entry.token.is_empty() {
            return Ok((None, sender));
        }
        if crate::privacy::is_expired(entry.timestamp.as_ref(), now) {
            let mutation = match sender {
                Some(sender) => KeyMutation::put(
                    storage,
                    KeyValue::Tctoken(crate::auth::TcToken {
                        sender_timestamp: Some(sender),
                        ..Default::default()
                    }),
                ),
                None => KeyMutation::delete(KeyCategory::Tctoken, storage),
            };
            let _ = tx.set(&[mutation]);
            Ok((None, sender))
        } else {
            Ok((Some(entry.token), sender))
        }
    }
    pub fn encrypt_group(
        &self,
        name: &SenderKeyName,
        data: &[u8],
    ) -> Result<group::EncryptedGroupMessage, Error> {
        Ok(group::encrypt_in_transaction(
            &mut self.tx.borrow_mut(),
            name,
            data,
        )?)
    }
    pub fn sender_key_memory(&self, jid: &str) -> Result<BTreeMap<String, bool>, Error> {
        let mut keys = self
            .tx
            .borrow_mut()
            .get(KeyCategory::SenderKeyMemory, &[jid.into()])?;
        match keys.remove(jid) {
            Some(KeyValue::SenderKeyMemory(memory)) => Ok(memory),
            None => Ok(BTreeMap::new()),
            _ => Err(signal::Error::Invalid.into()),
        }
    }
    pub fn save_sender_key_memory(
        &self,
        jid: &str,
        memory: BTreeMap<String, bool>,
    ) -> Result<(), Error> {
        Ok(self
            .tx
            .borrow_mut()
            .set(&[KeyMutation::put(jid, KeyValue::SenderKeyMemory(memory))])?)
    }
    pub fn group_fanout(&self, input: GroupFanout<'_>) -> Result<GroupNodes, Error> {
        self.group_fanout_patched(input, |message, _| {
            Ok(super::Patch::Shared(Box::new(message.clone())))
        })
    }
    /// Patches the sender-key distribution; the group payload must already be patched.
    pub fn group_fanout_patched(
        &self,
        input: GroupFanout<'_>,
        patch: impl FnOnce(&proto::Message, &[String]) -> Result<super::Patch, Error>,
    ) -> Result<GroupNodes, Error> {
        if !Jid::parse(input.destination).is_some_and(|j| j.is_group())
            && input.destination != jid::STORIES_JID
        {
            return Err(Error::Recipient);
        }
        let mut memory = if input.destination == jid::STORIES_JID {
            BTreeMap::new()
        } else {
            self.sender_key_memory(input.destination)?
        };
        let bytes = messages::encode(input.message)?;
        let group = self.encrypt_group(
            &SenderKeyName::new(input.destination, input.sender)?,
            &bytes,
        )?;
        let recipients = sender_key_recipients(input.devices, &mut memory, false);
        let distribution = proto::Message {
            sender_key_distribution_message: Some(proto::message::SenderKeyDistributionMessage {
                group_id: Some(input.destination.into()),
                axolotl_sender_key_distribution_message: Some(group.distribution),
            }),
            ..Default::default()
        };
        let participants = super::create_participant_nodes_patched(
            Fanout {
                recipients: &recipients,
                message: &distribution,
                me: input.me,
                me_lid: input.me_lid,
                extra_attrs: input.extra_attrs,
                device_sent_message: None,
            },
            self,
            patch,
        )?;
        let mut node = BinaryNode::new("enc").attr("v", "2").attr("type", "skmsg");
        node.attrs.extend(input.extra_attrs.clone());
        self.save_sender_key_memory(input.destination, memory)?;
        Ok(GroupNodes {
            encrypted: node.with_content(NodeContent::Bytes(group.ciphertext)),
            participants,
        })
    }
}
pub struct GroupFanout<'a> {
    pub destination: &'a str,
    pub sender: &'a str,
    pub message: &'a proto::Message,
    pub devices: &'a [Device],
    pub me: &'a str,
    pub me_lid: Option<&'a str>,
    pub extra_attrs: &'a IndexMap<String, String>,
}
#[derive(Debug)]
pub struct GroupNodes {
    pub encrypted: BinaryNode,
    pub participants: ParticipantNodes,
}
/// Source marks requested recipients before fanout, including partial failures.
pub fn sender_key_recipients(
    devices: &[Device],
    memory: &mut BTreeMap<String, bool>,
    force: bool,
) -> Vec<String> {
    let mut recipients = vec![];
    for device in devices {
        let hosted = Jid::parse(&device.jid)
            .is_some_and(|j| j.is_hosted_lid_user() || j.is_hosted_pn_user());
        if (!memory.get(&device.jid).copied().unwrap_or(false) || force)
            && !hosted
            && device.device != 99
        {
            recipients.push(device.jid.clone());
            memory.insert(device.jid.clone(), true);
        }
    }
    recipients
}
