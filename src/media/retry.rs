use super::{Error, encode_component};
use crate::{
    binary::{BinaryNode, NodeContent},
    crypto, jid, proto,
};
use indexmap::IndexMap;
use prost::Message as _;
use zeroize::Zeroizing;
fn retry_key(media_key: &[u8]) -> Result<Zeroizing<Vec<u8>>, Error> {
    Ok(Zeroizing::new(crypto::hkdf(
        media_key,
        32,
        None,
        b"WhatsApp Media Retry Notification",
    )?))
}
pub fn retry_request(
    key: &proto::MessageKey,
    media_key: &[u8],
    me: &str,
) -> Result<BinaryNode, Error> {
    let mut iv = [0u8; 12];
    getrandom::fill(&mut iv).map_err(|_| crypto::Error::Random)?;
    retry_request_with_iv(key, media_key, me, &iv)
}
pub fn retry_request_with_iv(
    key: &proto::MessageKey,
    media_key: &[u8],
    me: &str,
    iv: &[u8; 12],
) -> Result<BinaryNode, Error> {
    let id = key.id.as_deref().ok_or(Error::Node("missing message ID"))?;
    let plaintext = proto::ServerErrorReceipt {
        stanza_id: Some(id.into()),
    }
    .encode_to_vec();
    let ciphertext =
        crypto::aes_encrypt_gcm(&plaintext, &retry_key(media_key)?, iv, id.as_bytes())?;
    let mut rmr = BinaryNode::new("rmr");
    if let Some(jid) = &key.remote_jid {
        rmr = rmr.attr("jid", jid);
    }
    rmr = rmr.attr("from_me", (key.from_me == Some(true)).to_string());
    if let Some(participant) = key.participant.as_deref().filter(|s| !s.is_empty()) {
        rmr = rmr.attr("participant", participant);
    }
    Ok(BinaryNode::new("receipt")
        .attr("id", id)
        .attr("to", jid::normalized_user(me))
        .attr("type", "server-error")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("encrypt").with_content(NodeContent::Children(vec![
                BinaryNode::new("enc_p").with_content(NodeContent::Bytes(ciphertext)),
                BinaryNode::new("enc_iv").with_content(NodeContent::Bytes(iv.to_vec())),
            ])),
            rmr,
        ])))
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryCiphertext {
    pub ciphertext: Vec<u8>,
    pub iv: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct RetryFailure {
    pub code: Option<f64>,
    pub status: u16,
    pub attrs: IndexMap<String, String>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct RetryEvent {
    pub key: proto::MessageKey,
    pub media: Option<RetryCiphertext>,
    pub error: Option<RetryFailure>,
}
pub fn retry_status(code: f64) -> Option<u16> {
    use proto::media_retry_notification::ResultType;
    if !code.is_finite() || code.fract() != 0.0 {
        return None;
    }
    match code as i64 {
        n if n == ResultType::Success as i64 => Some(200),
        n if n == ResultType::DecryptionError as i64 => Some(412),
        n if n == ResultType::NotFound as i64 => Some(404),
        n if n == ResultType::GeneralError as i64 => Some(418),
        _ => None,
    }
}
pub fn decode_retry(node: &BinaryNode) -> Result<RetryEvent, Error> {
    let rmr = node.child("rmr").ok_or(Error::Node("missing rmr"))?;
    let mut event = RetryEvent {
        key: proto::MessageKey {
            id: node.attrs.get("id").cloned(),
            remote_jid: rmr.attrs.get("jid").cloned(),
            from_me: Some(rmr.attrs.get("from_me").is_some_and(|s| s == "true")),
            participant: rmr.attrs.get("participant").cloned(),
        },
        media: None,
        error: None,
    };
    if let Some(error) = node.child("error") {
        let code = error
            .attrs
            .get("code")
            .map(|s| crate::privacy::number(s))
            .unwrap_or(f64::NAN);
        let status = retry_status(code).unwrap_or(500);
        if status < 400 {
            return Err(Error::Node("success code in media retry error"));
        }
        event.error = Some(RetryFailure {
            code: Some(code),
            status,
            attrs: error.attrs.clone(),
        });
    } else if let Some(encrypt) = node.child("encrypt")
        && let (Some(ciphertext), Some(iv)) = (
            encrypt.child_buffer("enc_p"),
            encrypt.child_buffer("enc_iv"),
        )
    {
        event.media = Some(RetryCiphertext {
            ciphertext: ciphertext.to_vec(),
            iv: iv.to_vec(),
        });
    } else {
        event.error = Some(RetryFailure {
            code: None,
            status: 404,
            attrs: Default::default(),
        });
    }
    Ok(event)
}
pub fn decrypt_retry(
    data: &RetryCiphertext,
    media_key: &[u8],
    message_id: &str,
) -> Result<proto::MediaRetryNotification, Error> {
    let plaintext = crypto::aes_decrypt_gcm(
        &data.ciphertext,
        &retry_key(media_key)?,
        &data.iv,
        message_id.as_bytes(),
    )?;
    Ok(proto::MediaRetryNotification::decode(plaintext.as_slice())?)
}
/// Construct upload URLs independently of the HTTP client.
pub fn upload_url(host: &str, kind: super::MediaType, base64_hash: &str, auth: &str) -> String {
    let token = super::upload_token(base64_hash);
    format!(
        "https://{host}{}/{token}?auth={}&token={token}",
        kind.upload_path().unwrap_or("undefined"),
        encode_component(auth)
    )
}
