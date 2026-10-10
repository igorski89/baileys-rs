use super::{BinaryNode, Error, NodeContent};
use crate::jid::{self, Jid};
use flate2::read::ZlibDecoder;
use std::{collections::HashMap, io::Read, sync::LazyLock};

mod tokens {
    include!(concat!(env!("OUT_DIR"), "/tokens.rs"));
}
const DICTIONARY_0: u8 = 236;
const INTEROP_JID: u8 = 245;
const FB_JID: u8 = 246;
const AD_JID: u8 = 247;
const LIST_8: u8 = 248;
const LIST_16: u8 = 249;
const JID_PAIR: u8 = 250;
const HEX_8: u8 = 251;
const BINARY_8: u8 = 252;
const BINARY_20: u8 = 253;
const BINARY_32: u8 = 254;
const NIBBLE_8: u8 = 255;
static TOKEN_MAP: LazyLock<HashMap<&'static str, (Option<u8>, u8)>> = LazyLock::new(|| {
    let mut map = HashMap::new();
    for (index, token) in tokens::SINGLE.iter().enumerate() {
        map.insert(*token, (None, index as u8));
    }
    for (dict, tokens) in tokens::DOUBLE.iter().enumerate() {
        for (index, token) in tokens.iter().enumerate() {
            map.insert(*token, (Some(dict as u8), index as u8));
        }
    }
    map
});

#[derive(Debug, Clone, Copy)]
pub struct DecodeLimits {
    pub max_bytes: usize,
    pub max_depth: usize,
    pub max_nodes: usize,
}
impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_bytes: 16 * 1024 * 1024,
            max_depth: 128,
            max_nodes: 100_000,
        }
    }
}

pub fn encode(node: &BinaryNode) -> Result<Vec<u8>, Error> {
    let mut out = vec![0];
    write_node(node, &mut out, 0)?;
    Ok(out)
}
fn write_node(node: &BinaryNode, out: &mut Vec<u8>, depth: usize) -> Result<(), Error> {
    if depth > 128 {
        return Err(Error::Limit("depth"));
    }
    if node.tag.is_empty() {
        return Err(Error::Invalid("empty tag"));
    }
    write_list(
        1 + 2 * node.attrs.len() + usize::from(node.content.is_some()),
        out,
    )?;
    write_string(&node.tag, out)?;
    for (key, value) in &node.attrs {
        write_string(key, out)?;
        write_string(value, out)?;
    }
    match &node.content {
        None => (),
        Some(NodeContent::Text(text)) => write_string(text, out)?,
        Some(NodeContent::Bytes(bytes)) => write_bytes(bytes, out)?,
        Some(NodeContent::Children(nodes)) => {
            write_list(nodes.len(), out)?;
            for node in nodes {
                write_node(node, out, depth + 1)?;
            }
        }
    }
    Ok(())
}
fn write_list(size: usize, out: &mut Vec<u8>) -> Result<(), Error> {
    match size {
        0 => out.push(0),
        1..=255 => out.extend_from_slice(&[LIST_8, size as u8]),
        256..=65535 => {
            out.push(LIST_16);
            out.extend_from_slice(&(size as u16).to_be_bytes());
        }
        _ => return Err(Error::Limit("list size")),
    }
    Ok(())
}
fn write_bytes(bytes: &[u8], out: &mut Vec<u8>) -> Result<(), Error> {
    let n = bytes.len();
    if n < 256 {
        out.extend_from_slice(&[BINARY_8, n as u8]);
    } else if n < 1 << 20 {
        out.extend_from_slice(&[BINARY_20, ((n >> 16) & 15) as u8, (n >> 8) as u8, n as u8]);
    } else {
        let n = u32::try_from(n).map_err(|_| Error::Limit("byte length"))?;
        out.push(BINARY_32);
        out.extend_from_slice(&n.to_be_bytes());
    }
    out.extend_from_slice(bytes);
    Ok(())
}
fn write_string(value: &str, out: &mut Vec<u8>) -> Result<(), Error> {
    if value.is_empty() {
        return write_bytes(&[], out);
    }
    if let Some((dict, index)) = TOKEN_MAP.get(value) {
        if let Some(dict) = dict {
            out.push(DICTIONARY_0 + dict);
        }
        out.push(*index);
    } else if value.len() <= 127
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || b == b'-' || b == b'.')
    {
        write_packed(value, NIBBLE_8, out);
    } else if value.len() <= 127
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'A'..=b'F').contains(&b))
    {
        write_packed(value, HEX_8, out);
    } else if let Some(jid) = Jid::parse(value) {
        if let Some(device) = jid.device {
            out.extend_from_slice(&[AD_JID, jid.domain_type, device as u8]);
            write_string(&jid.user, out)?;
        } else {
            out.push(JID_PAIR);
            if jid.user.is_empty() {
                out.push(0);
            } else {
                write_string(&jid.user, out)?;
            }
            write_string(&jid.server, out)?;
        }
    } else {
        write_bytes(value.as_bytes(), out)?;
    }
    Ok(())
}
fn write_packed(value: &str, tag: u8, out: &mut Vec<u8>) {
    out.extend_from_slice(&[
        tag,
        value.len().div_ceil(2) as u8 | if value.len() % 2 == 1 { 128 } else { 0 },
    ]);
    let nibble = |b| match b {
        b'0'..=b'9' => b - b'0',
        b'A'..=b'F' => b - b'A' + 10,
        b'-' => 10,
        b'.' => 11,
        _ => 15,
    };
    for pair in value.as_bytes().chunks(2) {
        out.push(nibble(pair[0]) << 4 | pair.get(1).map_or(15, |b| nibble(*b)));
    }
}

pub fn decode(frame: &[u8]) -> Result<BinaryNode, Error> {
    decode_with_limits(frame, DecodeLimits::default())
}
pub fn decode_with_limits(frame: &[u8], limits: DecodeLimits) -> Result<BinaryNode, Error> {
    let (&flags, payload) = frame.split_first().ok_or(Error::EndOfStream)?;
    if payload.len() > limits.max_bytes {
        return Err(Error::Limit("bytes"));
    }
    let inflated;
    let bytes = if flags & 2 != 0 {
        let mut result = Vec::new();
        ZlibDecoder::new(payload)
            .take(limits.max_bytes as u64 + 1)
            .read_to_end(&mut result)?;
        if result.len() > limits.max_bytes {
            return Err(Error::Limit("inflated bytes"));
        }
        inflated = result;
        &inflated
    } else {
        payload
    };
    let mut reader = Reader {
        bytes,
        pos: 0,
        nodes: 0,
        limits,
    };
    let node = reader.node(0)?;
    if reader.pos != bytes.len() {
        return Err(Error::Invalid("trailing bytes"));
    }
    Ok(node)
}
struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
    nodes: usize,
    limits: DecodeLimits,
}
impl<'a> Reader<'a> {
    fn bytes(&mut self, length: usize) -> Result<&'a [u8], Error> {
        let end = self.pos.checked_add(length).ok_or(Error::EndOfStream)?;
        let bytes = self.bytes.get(self.pos..end).ok_or(Error::EndOfStream)?;
        self.pos = end;
        Ok(bytes)
    }
    fn byte(&mut self) -> Result<u8, Error> {
        Ok(self.bytes(1)?[0])
    }
    fn int(&mut self, n: usize) -> Result<usize, Error> {
        Ok(self.bytes(n)?.iter().fold(0, |n, b| (n << 8) | *b as usize))
    }
    fn list_size(&mut self, tag: u8) -> Result<usize, Error> {
        match tag {
            0 => Ok(0),
            LIST_8 => self.int(1),
            LIST_16 => self.int(2),
            _ => Err(Error::Invalid("list tag")),
        }
    }
    fn raw(&mut self, tag: u8) -> Result<&'a [u8], Error> {
        let n = match tag {
            BINARY_8 => self.int(1)?,
            BINARY_20 => self.int(3)? & 0xfffff,
            BINARY_32 => self.int(4)?,
            _ => return Err(Error::Invalid("byte tag")),
        };
        self.bytes(n)
    }
    fn next_string(&mut self, depth: usize) -> Result<String, Error> {
        let tag = self.byte()?;
        self.string(tag, depth)
    }
    fn string(&mut self, tag: u8, depth: usize) -> Result<String, Error> {
        if depth > self.limits.max_depth {
            return Err(Error::Limit("string depth"));
        }
        if tag >= 1 && (tag as usize) < tokens::SINGLE.len() {
            return Ok(tokens::SINGLE[tag as usize].into());
        }
        match tag {
            0 => Ok(String::new()),
            236..=239 => {
                let index = self.byte()? as usize;
                tokens::DOUBLE
                    .get((tag - DICTIONARY_0) as usize)
                    .and_then(|dict| dict.get(index))
                    .map(|s| (*s).into())
                    .ok_or(Error::Invalid("dictionary token"))
            }
            BINARY_8 | BINARY_20 | BINARY_32 => {
                Ok(String::from_utf8_lossy(self.raw(tag)?).into_owned())
            }
            HEX_8 | NIBBLE_8 => {
                let size = self.byte()?;
                let mut value = String::new();
                for byte in self.bytes((size & 127) as usize)? {
                    for n in [byte >> 4, byte & 15] {
                        let c = match (tag, n) {
                            (_, 0..=9) => b'0' + n,
                            (HEX_8, _) => b'A' + n - 10,
                            (NIBBLE_8, 10) => b'-',
                            (NIBBLE_8, 11) => b'.',
                            (NIBBLE_8, 15) => 0,
                            _ => return Err(Error::Invalid("packed nibble")),
                        };
                        value.push(c as char);
                    }
                }
                if size & 128 != 0 {
                    value.pop();
                }
                Ok(value)
            }
            JID_PAIR => {
                let user = self.next_string(depth + 1)?;
                let server = self.next_string(depth + 1)?;
                if server.is_empty() {
                    return Err(Error::Invalid("jid server"));
                }
                Ok(format!("{user}@{server}"))
            }
            AD_JID => {
                let domain = self.byte()?;
                let device = self.byte()? as u16;
                let user = self.next_string(depth + 1)?;
                let server = match domain {
                    1 => "lid",
                    128 => "hosted",
                    129 => "hosted.lid",
                    _ => "s.whatsapp.net",
                };
                Ok(jid::encode(&user, server, Some(device), None))
            }
            FB_JID => {
                let user = self.next_string(depth + 1)?;
                let device = self.int(2)?;
                let server = self.next_string(depth + 1)?;
                Ok(format!("{user}:{device}@{server}"))
            }
            INTEROP_JID => {
                let user = self.next_string(depth + 1)?;
                let device = self.int(2)?;
                let integrator = self.int(2)?;
                let before = self.pos;
                let server = self.next_string(depth + 1).unwrap_or_else(|_| {
                    self.pos = before;
                    "interop".into()
                });
                Ok(format!("{integrator}-{user}:{device}@{server}"))
            }
            _ => Err(Error::Invalid("string tag")),
        }
    }
    fn node(&mut self, depth: usize) -> Result<BinaryNode, Error> {
        if depth > self.limits.max_depth {
            return Err(Error::Limit("depth"));
        }
        self.nodes += 1;
        if self.nodes > self.limits.max_nodes {
            return Err(Error::Limit("nodes"));
        }
        let list_tag = self.byte()?;
        let size = self.list_size(list_tag)?;
        if size == 0 {
            return Err(Error::Invalid("empty node"));
        }
        let tag = self.next_string(0)?;
        if tag.is_empty() {
            return Err(Error::Invalid("empty tag"));
        }
        let mut node = BinaryNode::new(tag);
        for _ in 0..((size - 1) / 2) {
            let key = self.next_string(0)?;
            let value = self.next_string(0)?;
            node.attrs.insert(key, value);
        }
        if size % 2 == 0 {
            let tag = self.byte()?;
            node.content = Some(match tag {
                0 | LIST_8 | LIST_16 => {
                    let size = self.list_size(tag)?;
                    let mut nodes = Vec::new();
                    for _ in 0..size {
                        nodes.push(self.node(depth + 1)?);
                    }
                    NodeContent::Children(nodes)
                }
                BINARY_8 | BINARY_20 | BINARY_32 => NodeContent::Bytes(self.raw(tag)?.to_vec()),
                _ => NodeContent::Text(self.string(tag, 0)?),
            });
        }
        Ok(node)
    }
}
