//! Baileys BufferJSON and protobufjs Long compatibility for persisted auth state.
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};
use serde_json::Value;

pub(crate) fn decode_bytes(value: Value) -> Result<Vec<u8>, &'static str> {
    match value {
        Value::String(value) => STANDARD.decode(value).map_err(|_| "invalid base64 buffer"),
        Value::Array(values) => values
            .into_iter()
            .map(|v| {
                v.as_u64()
                    .and_then(|n| u8::try_from(n).ok())
                    .ok_or("invalid buffer byte")
            })
            .collect(),
        Value::Object(mut value) if value.get("type").and_then(Value::as_str) == Some("Buffer") => {
            decode_bytes(value.remove("data").ok_or("missing buffer data")?)
        }
        Value::Object(value) if !value.is_empty() => {
            let mut entries: Vec<_> = value
                .into_iter()
                .map(|(k, v)| {
                    Ok((
                        k.parse::<usize>()
                            .map_err(|_| "invalid numeric buffer key")?,
                        v.as_u64()
                            .and_then(|n| u8::try_from(n).ok())
                            .ok_or("invalid buffer byte")?,
                    ))
                })
                .collect::<Result<_, &'static str>>()?;
            entries.sort_by_key(|(k, _)| *k);
            Ok(entries.into_iter().map(|(_, v)| v).collect())
        }
        _ => Err("invalid buffer representation"),
    }
}
pub mod bytes {
    use super::*;
    pub fn serialize<S: Serializer>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serde_json::json!({"type":"Buffer","data":STANDARD.encode(value)}).serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        decode_bytes(Value::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}
pub mod bytes32 {
    use super::*;
    pub fn serialize<S: Serializer>(value: &[u8; 32], serializer: S) -> Result<S::Ok, S::Error> {
        bytes::serialize(value, serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[u8; 32], D::Error> {
        bytes::deserialize(deserializer)?
            .try_into()
            .map_err(|_| D::Error::custom("key must contain 32 bytes"))
    }
}
pub mod optional_bytes {
    use super::*;
    pub fn serialize<S: Serializer>(
        value: &Option<Vec<u8>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(value) => bytes::serialize(value, serializer),
            None => serializer.serialize_none(),
        }
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<u8>>, D::Error> {
        let value = Value::deserialize(deserializer)?;
        if value.is_null() {
            Ok(None)
        } else {
            decode_bytes(value).map(Some).map_err(D::Error::custom)
        }
    }
}
pub(crate) fn integer(value: Value) -> Result<u64, &'static str> {
    match value {
        Value::Number(n) => n
            .as_u64()
            .or_else(|| n.as_i64().map(|n| n as u64))
            .ok_or("invalid integer"),
        Value::String(s) => s
            .parse::<u64>()
            .or_else(|_| s.parse::<i64>().map(|n| n as u64))
            .map_err(|_| "invalid integer string"),
        Value::Object(n) => {
            let word = |name| {
                n.get(name)
                    .and_then(|n| n.as_i64())
                    .and_then(|n| {
                        if (-2147483648..=4294967295).contains(&n) {
                            Some(n as u32 as u64)
                        } else {
                            None
                        }
                    })
                    .ok_or("invalid Long word")
            };
            Ok(word("low")? | (word("high")? << 32))
        }
        _ => Err("invalid integer representation"),
    }
}
pub mod optional_i64 {
    use super::*;
    pub fn serialize<S: Serializer>(value: &Option<i64>, serializer: S) -> Result<S::Ok, S::Error> {
        match value {
            Some(n) => serializer.serialize_str(&n.to_string()),
            None => serializer.serialize_none(),
        }
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<i64>, D::Error> {
        let value = Value::deserialize(deserializer)?;
        if value.is_null() {
            Ok(None)
        } else {
            integer(value)
                .map(|n| Some(n as i64))
                .map_err(D::Error::custom)
        }
    }
}
pub mod optional_u64 {
    use super::*;
    pub fn serialize<S: Serializer>(value: &Option<u64>, serializer: S) -> Result<S::Ok, S::Error> {
        match value {
            Some(n) => serializer.serialize_str(&n.to_string()),
            None => serializer.serialize_none(),
        }
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<u64>, D::Error> {
        let value = Value::deserialize(deserializer)?;
        if value.is_null() {
            Ok(None)
        } else {
            integer(value).map(Some).map_err(D::Error::custom)
        }
    }
}
