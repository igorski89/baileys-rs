use super::{
    Error, device_sent_message, encode_device_identity, encode_newsletter_message, media_type,
    message_type, normalize,
};
use crate::{
    binary::{BinaryNode, NodeContent},
    devices::Device,
    jid::{self, Jid},
    proto, reporting,
};
use indexmap::IndexMap;
pub struct RetryDistribution {
    pub bytes: Option<Vec<u8>>,
    pub warning: Option<crate::signal::Error>,
}
/// Prefer an existing LID sender key. A distribution failure sends the retry
/// without SKDM, rather than falling back to a different sender identity.
pub fn retry_distribution(
    repository: &crate::signal::group::Repository,
    destination: &str,
    me: &str,
    me_lid: Option<&str>,
) -> Result<RetryDistribution, Error> {
    let destination_jid = Jid::parse(destination).ok_or(Error::Recipient)?;
    if destination_jid.is_group() || destination == jid::STORIES_JID {
        for sender in me_lid
            .filter(|s| !s.is_empty())
            .into_iter()
            .chain(std::iter::once(me))
        {
            let name = crate::signal::SenderKeyName::new(destination, sender)?;
            if repository.has_sender_key(&name)? {
                return Ok(match repository.distribution(&name) {
                    Ok(bytes) => RetryDistribution {
                        bytes: Some(bytes),
                        warning: None,
                    },
                    Err(error) => RetryDistribution {
                        bytes: None,
                        warning: Some(error),
                    },
                });
            }
        }
    }
    Ok(RetryDistribution {
        bytes: None,
        warning: None,
    })
}

#[derive(Debug, Clone)]
pub struct Retry {
    pub jid: String,
    pub count: u32,
}
pub fn encrypt_retry(
    recipient: &Retry,
    payload: &proto::Message,
    repository: &impl super::Encryptor,
) -> Result<BinaryNode, Error> {
    retry_node(recipient, &crate::messages::encode(payload)?, repository)
}
pub fn encrypt_retry_with_byte(
    recipient: &Retry,
    payload: &proto::Message,
    repository: &impl super::Encryptor,
    random_byte: u8,
) -> Result<BinaryNode, Error> {
    use prost::Message as _;
    retry_node(
        recipient,
        &crate::messages::pad_with_byte(&payload.encode_to_vec(), random_byte),
        repository,
    )
}
fn retry_node(
    recipient: &Retry,
    bytes: &[u8],
    repository: &impl super::Encryptor,
) -> Result<BinaryNode, Error> {
    let encrypted = repository.encrypt(&recipient.jid, bytes)?;
    Ok(BinaryNode::new("enc")
        .attr("v", "2")
        .attr(
            "type",
            if encrypted.kind == crate::signal::direct::MessageType::PreKey {
                "pkmsg"
            } else {
                "msg"
            },
        )
        .attr("count", recipient.count.to_string())
        .with_content(NodeContent::Bytes(encrypted.ciphertext)))
}
#[derive(Debug, Clone, Default)]
pub struct StanzaOptions {
    pub retry: Option<Retry>,
    pub additional_attributes: IndexMap<String, String>,
    pub additional_nodes: Vec<BinaryNode>,
}
#[derive(Debug, Default, PartialEq, Eq)]
pub struct DirectRecipients {
    pub all: Vec<String>,
    pub own: Vec<String>,
    pub other: Vec<String>,
}
/// Preserve device ordering and duplicates; skip only the exact sender device.
pub fn direct_recipients(
    devices: &[Device],
    me: &str,
    me_lid: Option<&str>,
) -> Result<DirectRecipients, Error> {
    let own = Jid::parse(me).ok_or(Error::Recipient)?;
    let own_lid = me_lid.and_then(Jid::parse);
    let mut result = DirectRecipients::default();
    for device in devices {
        if device.jid == me || me_lid == Some(device.jid.as_str()) {
            continue;
        }
        if device.user == own.user || own_lid.as_ref().is_some_and(|me| me.user == device.user) {
            result.own.push(device.jid.clone());
        } else {
            result.other.push(device.jid.clone());
        }
        result.all.push(device.jid.clone());
    }
    Ok(result)
}
pub fn encrypted_attributes(message: &proto::Message) -> IndexMap<String, String> {
    let mut attributes = IndexMap::new();
    let media = media_type(message);
    if !media.is_empty() {
        attributes.insert("mediatype".into(), media.into());
    }
    if normalize(Some(message))
        .is_some_and(|m| m.pin_in_chat_message.is_some() || m.reaction_message.is_some())
    {
        attributes.insert("decrypt-fail".into(), "hide".into());
    }
    attributes
}
pub fn is_one_to_one(destination: &str, options: &StanzaOptions) -> Result<bool, Error> {
    let jid = Jid::parse(destination).ok_or(Error::Recipient)?;
    Ok(!jid.is_group()
        && destination != jid::STORIES_JID
        && !jid.is_newsletter()
        && options.retry.is_none()
        && options
            .additional_attributes
            .get("category")
            .is_none_or(|s| s != "peer"))
}
/// Group retries include a current SKDM when available. Own-device retries wrap the
/// payload in DSM without copying root context, matching the source retry path.
pub fn retry_payload(
    destination: &str,
    message: &proto::Message,
    recipient: &str,
    me: &str,
    me_lid: Option<&str>,
    distribution: Option<&[u8]>,
) -> Result<proto::Message, Error> {
    let target = Jid::parse(recipient).ok_or(Error::Recipient)?;
    let mut payload = message.clone();
    let destination_jid = Jid::parse(destination).ok_or(Error::Recipient)?;
    if (destination_jid.is_group() || destination == jid::STORIES_JID)
        && let Some(distribution) = distribution
    {
        payload.sender_key_distribution_message =
            Some(proto::message::SenderKeyDistributionMessage {
                group_id: Some(destination.into()),
                axolotl_sender_key_distribution_message: Some(distribution.to_vec()),
            });
    }
    let own = if target.is_lid_user() {
        me_lid
    } else {
        Some(me)
    };
    if own.is_some_and(|me| jid::are_same_user(recipient, me)) {
        payload = device_sent_message(destination, &payload);
        payload.message_context_info = None;
    }
    Ok(payload)
}
pub struct Stanza<'a> {
    pub destination: &'a str,
    pub id: &'a str,
    pub message: &'a proto::Message,
    pub me: &'a str,
    pub options: &'a StanzaOptions,
    pub content: Vec<BinaryNode>,
    pub participants: Vec<BinaryNode>,
    pub include_device_identity: bool,
    pub account: Option<&'a proto::AdvSignedDeviceIdentity>,
    /// Message after patching and the independently padded bytes used for reporting.
    pub reporting: Option<(&'a proto::Message, &'a [u8])>,
    /// Already resolved and expiry-checked peer token; issuance is a separate step.
    pub privacy_token: Option<&'a [u8]>,
    pub privacy_token_on_1_to_1: bool,
}
#[derive(Debug)]
pub struct Assembled {
    pub node: BinaryNode,
    pub reporting_error: Option<reporting::Error>,
}
/// Assemble the encrypted relay tail. Fanout and cipher work must run before this call.
pub fn assemble(mut input: Stanza<'_>) -> Result<Assembled, Error> {
    let destination = Jid::parse(input.destination).ok_or(Error::Recipient)?;
    if destination.is_newsletter() {
        return Err(Error::Recipient);
    }
    let mut attrs = input.options.additional_attributes.clone();
    if input.options.retry.is_some()
        && !destination.is_group()
        && input.destination != jid::STORIES_JID
    {
        attrs.insert("device_fanout".into(), "false".into());
    }
    if !input.participants.is_empty() {
        if attrs.get("category").is_some_and(|s| s == "peer") {
            if let Some(peer) = input.participants[0].all_children().first() {
                input.content.push(peer.clone());
            }
        } else {
            input.content.push(
                BinaryNode::new("participants")
                    .with_content(NodeContent::Children(input.participants)),
            );
        }
    }
    let mut node = BinaryNode::new("message")
        .attr("id", input.id)
        .attr("to", input.destination)
        .attr("type", message_type(Some(input.message)));
    node.attrs.extend(attrs);
    if let Some(retry) = &input.options.retry {
        Jid::parse(&retry.jid).ok_or(Error::Recipient)?;
        if destination.is_group() {
            node.attrs.insert("to".into(), input.destination.into());
            node.attrs.insert("participant".into(), retry.jid.clone());
        } else {
            node.attrs.insert("to".into(), retry.jid.clone());
            if jid::are_same_user(&retry.jid, input.me) {
                node.attrs
                    .insert("recipient".into(), input.destination.into());
            }
        }
    } else {
        node.attrs.insert("to".into(), input.destination.into());
    }
    if input.include_device_identity || input.options.retry.is_some() {
        input.content.push(
            BinaryNode::new("device-identity").with_content(NodeContent::Bytes(
                encode_device_identity(input.account.ok_or(Error::Identity)?, true),
            )),
        );
    }
    let mut reporting_error = None;
    if !destination.is_newsletter()
        && input.options.retry.is_none()
        && let Some((message, bytes)) = input.reporting
        && reporting::should_include_token(message)
    {
        let key = proto::MessageKey {
            remote_jid: Some(input.destination.into()),
            from_me: Some(true),
            id: Some(input.id.into()),
            participant: None,
        };
        match reporting::message_token(bytes, message, &key) {
            Ok(Some(report)) => input.content.push(report),
            Ok(None) => (),
            Err(error) => reporting_error = Some(error),
        }
    }
    if input.privacy_token_on_1_to_1
        && is_one_to_one(input.destination, input.options)?
        && let Some(token) = input.privacy_token.filter(|t| !t.is_empty())
    {
        input
            .content
            .push(BinaryNode::new("tctoken").with_content(NodeContent::Bytes(token.to_vec())));
    }
    input.content.extend(input.options.additional_nodes.clone());
    node.content = Some(NodeContent::Children(input.content));
    Ok(Assembled {
        node,
        reporting_error,
    })
}
/// Newsletter relay returns early upstream: plaintext is unpadded; supplemental
/// nodes, reporting and identity nodes are omitted, and `to` overrides are retained.
pub fn newsletter_stanza(
    destination: &str,
    id: &str,
    original: &proto::Message,
    patched: &proto::Message,
    options: &StanzaOptions,
) -> Result<BinaryNode, Error> {
    if !Jid::parse(destination).is_some_and(|j| j.is_newsletter()) {
        return Err(Error::Recipient);
    }
    if let Some(retry) = &options.retry {
        Jid::parse(&retry.jid).ok_or(Error::Recipient)?;
    }
    let mut node = BinaryNode::new("message")
        .attr("to", destination)
        .attr("id", id)
        .attr("type", message_type(Some(original)));
    node.attrs.extend(options.additional_attributes.clone());
    if options.retry.is_some() {
        node.attrs.insert("device_fanout".into(), "false".into());
    }
    Ok(node.with_content(NodeContent::Children(vec![
        BinaryNode::new("plaintext")
            .with_content(NodeContent::Bytes(encode_newsletter_message(patched))),
    ])))
}
