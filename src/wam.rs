//! WAM telemetry buffers: the event and global tables and their binary encoding.
mod constants;
pub use constants::*;

/// Value type of a WAM field; enumerations list their named codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Integer,
    Boolean,
    Text,
    Number,
    Timer,
    Enum(&'static [(&'static str, i64)]),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Prop {
    pub name: &'static str,
    pub id: u32,
    pub kind: Kind,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Event {
    pub name: &'static str,
    pub id: u32,
    pub weight: i64,
    pub wam_channel: &'static str,
    pub private_stats_id_int: Option<i64>,
    pub props: &'static [Prop],
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Global {
    pub name: &'static str,
    pub id: u32,
    pub kind: Kind,
    pub validator: Option<&'static str>,
    pub channels: &'static [&'static str],
}
/// A field value. Booleans are sent as the integers one and zero.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Number(f64),
    Text(String),
}
/// One event with its fields and the globals written before it, in order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EventInput {
    pub name: String,
    pub props: Vec<(String, Value)>,
    pub globals: Vec<(String, Value)>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct BinaryInfo {
    pub protocol_version: u8,
    pub sequence: u16,
    pub events: Vec<EventInput>,
}
impl Default for BinaryInfo {
    fn default() -> Self {
        Self {
            protocol_version: 5,
            sequence: 0,
            events: vec![],
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("unknown WAM event {0}")]
    UnknownEvent(String),
    #[error("unknown WAM global {0}")]
    UnknownGlobal(String),
    /// A null event field, which the format cannot express.
    #[error("missing")]
    Missing,
    #[error("WAM key {0} is out of range")]
    Key(u32),
}

// A field the event does not define has no key; upstream writes it in the wide form as zero.
fn header(buffer: &mut Vec<u8>, key: Option<u32>, flag: u8) -> Result<(), Error> {
    match key {
        Some(key) if key < 256 => buffer.extend([flag, key as u8]),
        key => {
            let key = key.unwrap_or(0);
            let key = u16::try_from(key).map_err(|_| Error::Key(key))?;
            buffer.push(flag | FLAG_BYTE);
            buffer.extend(key.to_le_bytes());
        }
    }
    Ok(())
}
fn serialize(buffer: &mut Vec<u8>, key: Option<u32>, value: &Value, flag: u8) -> Result<(), Error> {
    let number = match value {
        Value::Null if flag == FLAG_GLOBAL => return header(buffer, key, flag),
        Value::Null => return Err(Error::Missing),
        Value::Text(text) => {
            let length = text.len();
            if length < 256 {
                header(buffer, key, flag | (8 << 4))?;
                buffer.push(length as u8);
            } else if length < 65536 {
                header(buffer, key, flag | (9 << 4))?;
                buffer.extend((length as u16).to_le_bytes());
            } else {
                header(buffer, key, flag | (10 << 4))?;
                buffer.extend((length as u32).to_le_bytes());
            }
            buffer.extend(text.as_bytes());
            return Ok(());
        }
        Value::Bool(value) => f64::from(u8::from(*value)),
        Value::Number(value) => *value,
    };
    if number.fract() != 0.0 || !number.is_finite() {
        header(buffer, key, flag | (7 << 4))?;
        buffer.extend(number.to_le_bytes());
    } else if number == 0.0 || number == 1.0 {
        header(buffer, key, flag | ((number as u8 + 1) << 4))?;
    } else if (-128.0..128.0).contains(&number) {
        header(buffer, key, flag | (3 << 4))?;
        buffer.extend((number as i8).to_le_bytes());
    } else if (-32768.0..32768.0).contains(&number) {
        header(buffer, key, flag | (4 << 4))?;
        buffer.extend((number as i16).to_le_bytes());
    } else if (-2147483648.0..2147483648.0).contains(&number) {
        header(buffer, key, flag | (5 << 4))?;
        buffer.extend((number as i32).to_le_bytes());
    } else {
        header(buffer, key, flag | (7 << 4))?;
        buffer.extend(number.to_le_bytes());
    }
    Ok(())
}
/// Encode a WAM buffer: the header, then each event's globals, marker and fields.
pub fn encode(info: &BinaryInfo) -> Result<Vec<u8>, Error> {
    let mut buffer = b"WAM".to_vec();
    buffer.extend([info.protocol_version, 1]);
    buffer.extend(info.sequence.to_be_bytes());
    buffer.push(0);
    for input in &info.events {
        for (name, value) in &input.globals {
            let global = WEB_GLOBALS
                .iter()
                .find(|global| global.name == name)
                .ok_or_else(|| Error::UnknownGlobal(name.clone()))?;
            serialize(&mut buffer, Some(global.id), value, FLAG_GLOBAL)?;
        }
        let event = WEB_EVENTS
            .iter()
            .find(|event| event.name == input.name)
            .ok_or_else(|| Error::UnknownEvent(input.name.clone()))?;
        let extended = input.props.iter().any(|(_, value)| *value != Value::Null);
        let flag = if extended {
            FLAG_EVENT
        } else {
            FLAG_EVENT | FLAG_EXTENDED
        };
        serialize(
            &mut buffer,
            Some(event.id),
            &Value::Number(-(event.weight as f64)),
            flag,
        )?;
        for (index, (name, value)) in input.props.iter().enumerate() {
            let key = event
                .props
                .iter()
                .find(|prop| prop.name == name)
                .map(|prop| prop.id);
            // Upstream marks every field but the last with the event flag.
            let flag = if index + 1 < input.props.len() {
                FLAG_EVENT
            } else {
                FLAG_FIELD | FLAG_EXTENDED
            };
            serialize(&mut buffer, key, value, flag)?;
        }
    }
    Ok(buffer)
}
