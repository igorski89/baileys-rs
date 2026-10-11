//! Reporting-content selection and authenticated reporting tokens.
use crate::{
    binary::{BinaryNode, NodeContent},
    crypto, proto,
};
use serde::Deserialize;
use std::{borrow::Cow, sync::OnceLock};
use zeroize::Zeroizing;
#[derive(Deserialize)]
struct Field {
    f: u32,
    #[serde(default)]
    m: bool,
    s: Option<Vec<Field>>,
}
fn fields() -> &'static [Field] {
    static FIELDS: OnceLock<Vec<Field>> = OnceLock::new();
    FIELDS.get_or_init(|| {
        serde_json::from_str(include_str!("../proto/reporting-fields.json"))
            .expect("vendored reporting fields")
    })
}
pub fn should_include_token(message: &proto::Message) -> bool {
    message.reaction_message.is_none()
        && message.enc_reaction_message.is_none()
        && message.enc_event_response_message.is_none()
        && message.poll_update_message.is_none()
}
fn read_varint(data: &[u8], offset: &mut usize) -> Option<i32> {
    let mut value = 0i32;
    for shift in (0..=35).step_by(7) {
        let byte = *data.get(*offset)?;
        *offset += 1;
        // Preserve upstream's JavaScript bitwise 32-bit wrapping for six-byte varints.
        value |= i32::from(byte & 127).wrapping_shl(shift);
        if byte & 128 == 0 {
            return Some(value);
        }
    }
    None
}
fn write_varint(mut value: u32, output: &mut Vec<u8>) {
    while value > 127 {
        output.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    output.push(value as u8);
}
/// Source-compatible selection and stable field sorting. Malformed/oversized input is absent.
/// Negative wrapped lengths are rejected rather than following unsafe JavaScript offsets.
pub fn extract_content(data: &[u8]) -> Option<Vec<u8>> {
    if data.len() > 16 * 1024 * 1024 {
        return None;
    }
    extract(data, fields(), 0)
}
fn extract<'a>(data: &'a [u8], config: &[Field], depth: usize) -> Option<Vec<u8>> {
    if depth > 8 {
        return None;
    }
    let mut output = Vec::<(u32, Cow<'a, [u8]>)>::new();
    let mut offset = 0;
    while offset < data.len() {
        let start = offset;
        let tag = read_varint(data, &mut offset)?;
        let number = (tag >> 3) as u32;
        let field = config.iter().find(|f| f.f == number);
        match tag & 7 {
            0 => {
                read_varint(data, &mut offset)?;
            }
            1 => {
                offset = offset.checked_add(8)?;
                data.get(..offset)?;
            }
            5 => {
                offset = offset.checked_add(4)?;
                data.get(..offset)?;
            }
            2 => {
                let length = usize::try_from(read_varint(data, &mut offset)?).ok()?;
                let end = offset.checked_add(length)?;
                let value = data.get(offset..end)?;
                if let Some(field) = field
                    && (field.m || field.s.is_some())
                {
                    let filtered =
                        extract(value, field.s.as_deref().unwrap_or_default(), depth + 1)?;
                    if !filtered.is_empty() {
                        let mut bytes = Vec::new();
                        write_varint(tag as u32, &mut bytes);
                        write_varint(filtered.len().try_into().ok()?, &mut bytes);
                        bytes.extend(filtered);
                        output.push((number, Cow::Owned(bytes)));
                    }
                    offset = end;
                    continue;
                }
                offset = end;
            }
            _ => return None,
        }
        if field.is_some() {
            output.push((number, Cow::Borrowed(data.get(start..offset)?)));
        }
    }
    output.sort_by_key(|(number, _)| *number);
    let length = output.iter().map(|(_, b)| b.len()).sum();
    let mut bytes = Vec::with_capacity(length);
    for (_, field) in output {
        bytes.extend_from_slice(&field);
    }
    Some(bytes)
}
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("missing reporting key address")]
    Address,
    #[error(transparent)]
    Crypto(#[from] crypto::Error),
}
pub fn message_token(
    bytes: &[u8],
    message: &proto::Message,
    key: &proto::MessageKey,
) -> Result<Option<BinaryNode>, Error> {
    let Some(secret) = message
        .message_context_info
        .as_ref()
        .and_then(|c| c.message_secret.as_deref())
    else {
        return Ok(None);
    };
    let Some(id) = key.id.as_deref().filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    let remote = key.remote_jid.as_deref();
    let participant = key
        .participant
        .as_deref()
        .filter(|s| !s.is_empty())
        .or(remote);
    let (from, to) = if key.from_me == Some(true) {
        (remote, participant)
    } else {
        (participant, remote)
    };
    let text = format!(
        "{id}{}{}Report Token",
        from.ok_or(Error::Address)?,
        to.ok_or(Error::Address)?
    );
    // Source converts concatenated UTF-8 bytes to latin1, then HKDF encodes that string as UTF-8.
    let info = text
        .as_bytes()
        .iter()
        .map(|b| char::from(*b))
        .collect::<String>();
    let key = Zeroizing::new(crypto::hkdf(secret, 32, None, info.as_bytes())?);
    let Some(content) = extract_content(bytes).filter(|b| !b.is_empty()) else {
        return Ok(None);
    };
    let token = crypto::hmac_sha256(&content, &key)[..16].to_vec();
    Ok(Some(BinaryNode::new("reporting").with_content(
        NodeContent::Children(vec![
            BinaryNode::new("reporting_token")
                .attr("v", "2")
                .with_content(NodeContent::Bytes(token)),
        ]),
    )))
}
