//! Complete schema-generated JSON conversion for native protobuf messages.
//! Bytes use BufferJSON, 64-bit integers use decimal strings, enum fields use
//! numeric values and oneof choices are flattened under their schema names.
//!
//! Inside `with_upstream_json`, values are written as `JSON.stringify` writes
//! upstream's: protobufjs messages through `toJSON` (base64 bytes, enum names,
//! decimal-string longs), and plain objects holding protobuf values (`plain`)
//! with Buffer bytes, enum numbers and JavaScript-number longs.
use serde_json::{Value, json};
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Decode(#[from] prost::DecodeError),
    #[error("unknown protobuf JSON message {0}")]
    UnknownMessage(String),
    #[error("invalid protobuf JSON {0}")]
    Input(&'static str),
    #[error("invalid protobuf JSON field {field}: {source}")]
    Field {
        field: &'static str,
        source: Box<Error>,
    },
}
impl Error {
    pub fn field(self, field: &'static str) -> Self {
        Self::Field {
            field,
            source: Box::new(self),
        }
    }
}
pub trait JsonField: Sized {
    fn to_value(&self) -> Value;
    fn from_value(value: &Value) -> Result<Self, Error>;
    fn present(&self) -> bool {
        true
    }
}
pub trait JsonMessage: JsonField + prost::Message + Default {}
pub fn parse<T: JsonField>(value: &Value) -> Result<T, Error> {
    T::from_value(value)
}
// Prost may box recursive oneof variants; keep codegen independent of that decision.
fn into_oneof<T, U: From<T>>(value: T) -> U {
    value.into()
}
pub fn object(value: &Value) -> Result<&serde_json::Map<String, Value>, Error> {
    value.as_object().ok_or(Error::Input("message object"))
}
#[derive(Clone, Copy, Default)]
struct Mode {
    /// Inside `with_upstream_json`.
    upstream: bool,
    /// Writing a protobufjs message (`toJSON`) rather than a plain object.
    protobuf: bool,
}
thread_local! {
    static MODE: std::cell::Cell<Mode> = const { std::cell::Cell::new(Mode { upstream: false, protobuf: false }) };
}
struct Restore(Mode);
impl Drop for Restore {
    fn drop(&mut self) {
        MODE.set(self.0);
    }
}
fn scoped<R>(mode: Mode, f: impl FnOnce() -> R) -> R {
    let _restore = Restore(MODE.replace(mode));
    f()
}
fn protobuf_json() -> bool {
    let mode = MODE.get();
    mode.upstream && mode.protobuf
}
fn plain_json() -> bool {
    let mode = MODE.get();
    mode.upstream && !mode.protobuf
}
/// Run `f` writing upstream's JSON; values are protobufjs messages unless
/// marked `plain`.
pub fn with_upstream_json<R>(f: impl FnOnce() -> R) -> R {
    scoped(
        Mode {
            upstream: true,
            protobuf: true,
        },
        f,
    )
}
/// Within `with_upstream_json`, write values as a plain JavaScript object
/// holding them (e.g. a received message's envelope); elsewhere no change.
pub fn plain<R>(f: impl FnOnce() -> R) -> R {
    let mode = MODE.get();
    scoped(
        Mode {
            protobuf: !mode.upstream && mode.protobuf,
            ..mode
        },
        f,
    )
}
/// Within `with_upstream_json`, write values as a protobufjs message's
/// `toJSON` (e.g. a decoded message inside a plain envelope).
pub fn protobuf<R>(f: impl FnOnce() -> R) -> R {
    let mode = MODE.get();
    scoped(
        Mode {
            protobuf: true,
            ..mode
        },
        f,
    )
}
/// An enum value: its name in protobufjs JSON when known, else its number.
pub fn enum_to_value(value: i32, name: Option<&'static str>) -> Value {
    match name {
        Some(name) if protobuf_json() => json!(name),
        _ => json!(value),
    }
}
pub fn bytes_to_value(bytes: &[u8]) -> Value {
    if protobuf_json() {
        use base64::Engine as _;
        json!(base64::engine::general_purpose::STANDARD.encode(bytes))
    } else {
        buffer_value(bytes)
    }
}
/// A Node `Buffer` in `JSON.stringify` form, whatever the bytes mode.
pub fn buffer_value(bytes: &[u8]) -> Value {
    json!({"type":"Buffer","data":bytes})
}
// protobufjs' fixed64 reader/formatter uses a signed Long for this schema.
pub fn fixed64_value(value: u64) -> Value {
    long_value(value as i64)
}
/// A JavaScript number in plain objects, a decimal string otherwise.
fn long_value(value: impl Into<i128> + std::fmt::Display + Copy) -> Value {
    if plain_json() {
        let value: i128 = value.into();
        // Integers beyond 2^53 lose precision as JavaScript numbers.
        if value.unsigned_abs() <= 1 << 53 {
            json!(value as i64)
        } else {
            json!(value as f64)
        }
    } else {
        json!(value.to_string())
    }
}
pub fn bytes_from_value(value: &Value) -> Result<Vec<u8>, Error> {
    crate::serialization::decode_bytes(value.clone()).map_err(Error::Input)
}
pub fn enum_value(value: &Value, name: impl Fn(&str) -> Option<i32>) -> Option<i32> {
    match value {
        Value::String(s) => name(s),
        Value::Number(_) => Some(int32(value)),
        _ => None,
    }
}
fn number(value: &Value) -> f64 {
    match value {
        Value::Number(v) => v.as_f64().unwrap_or(f64::NAN),
        Value::Null => 0.0,
        Value::Bool(v) => {
            if *v {
                1.0
            } else {
                0.0
            }
        }
        Value::String(v) => crate::privacy::number(v),
        _ => crate::privacy::number(&string(value)),
    }
}
fn int32(value: &Value) -> i32 {
    let n = number(value);
    if !n.is_finite() || n == 0.0 {
        0
    } else {
        n.trunc().rem_euclid(4294967296.0) as u32 as i32
    }
}
fn string(value: &Value) -> String {
    match value {
        Value::String(v) => v.clone(),
        Value::Array(v) => v
            .iter()
            .map(|v| {
                if v.is_null() {
                    String::new()
                } else {
                    string(v)
                }
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".into(),
        Value::Number(value) => {
            let n = value.as_f64().unwrap();
            if n == 0.0 {
                "0".into()
            } else if n.abs() >= 1e21 || n.abs() < 1e-6 {
                let text = format!("{n:e}");
                let (digits, exponent) = text.split_once('e').unwrap();
                let exponent: i32 = exponent.parse().unwrap();
                format!("{digits}e{exponent:+}")
            } else {
                n.to_string()
            }
        }
        _ => value.to_string(),
    }
}
fn truth(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(v) => v.as_f64().is_some_and(|v| v != 0.0),
        Value::String(v) => !v.is_empty(),
        _ => true,
    }
}
impl JsonField for String {
    fn to_value(&self) -> Value {
        json!(self)
    }
    fn from_value(v: &Value) -> Result<Self, Error> {
        Ok(string(v))
    }
    fn present(&self) -> bool {
        !self.is_empty()
    }
}
impl JsonField for bool {
    fn to_value(&self) -> Value {
        json!(self)
    }
    fn from_value(v: &Value) -> Result<Self, Error> {
        Ok(truth(v))
    }
    fn present(&self) -> bool {
        *self
    }
}
impl JsonField for i32 {
    fn to_value(&self) -> Value {
        json!(self)
    }
    fn from_value(v: &Value) -> Result<Self, Error> {
        Ok(int32(v))
    }
    fn present(&self) -> bool {
        *self != 0
    }
}
impl JsonField for u32 {
    fn to_value(&self) -> Value {
        json!(self)
    }
    fn from_value(v: &Value) -> Result<Self, Error> {
        Ok(int32(v) as u32)
    }
    fn present(&self) -> bool {
        *self != 0
    }
}
impl JsonField for u8 {
    fn to_value(&self) -> Value {
        json!(self)
    }
    fn from_value(v: &Value) -> Result<Self, Error> {
        u8::try_from(int32(v)).map_err(|_| Error::Input("byte"))
    }
    fn present(&self) -> bool {
        *self != 0
    }
}
impl JsonField for i64 {
    fn to_value(&self) -> Value {
        long_value(*self)
    }
    fn from_value(v: &Value) -> Result<Self, Error> {
        long(v).map(|v| v as i64).map_err(Error::Input)
    }
    fn present(&self) -> bool {
        *self != 0
    }
}
impl JsonField for u64 {
    fn to_value(&self) -> Value {
        long_value(*self)
    }
    fn from_value(v: &Value) -> Result<Self, Error> {
        long(v).map_err(Error::Input)
    }
    fn present(&self) -> bool {
        *self != 0
    }
}
fn long(value: &Value) -> Result<u64, &'static str> {
    if let Value::Number(_) = value {
        let n = number(value);
        Ok(if n.is_nan() {
            0
        } else if n <= -9223372036854775808.0 {
            i64::MIN as u64
        } else if n + 1.0 >= 9223372036854775808.0 {
            i64::MAX as u64
        } else {
            n.trunc() as i64 as u64
        })
    } else {
        crate::serialization::integer(value.clone())
    }
}
fn float_value(n: f64) -> Value {
    if plain_json() && !n.is_finite() {
        // JSON.stringify writes non-finite numbers as null.
        Value::Null
    } else if n.is_nan() {
        json!("NaN")
    } else if n == f64::INFINITY {
        json!("Infinity")
    } else if n == f64::NEG_INFINITY {
        json!("-Infinity")
    } else {
        json!(n)
    }
}
impl JsonField for f64 {
    fn to_value(&self) -> Value {
        float_value(*self)
    }
    fn from_value(v: &Value) -> Result<Self, Error> {
        Ok(number(v))
    }
    fn present(&self) -> bool {
        *self != 0.0
    }
}
impl JsonField for f32 {
    fn to_value(&self) -> Value {
        float_value(f64::from(*self))
    }
    fn from_value(v: &Value) -> Result<Self, Error> {
        Ok(number(v) as f32)
    }
    fn present(&self) -> bool {
        *self != 0.0
    }
}
impl<T: JsonField> JsonField for Option<T> {
    fn to_value(&self) -> Value {
        self.as_ref()
            .map(JsonField::to_value)
            .unwrap_or(Value::Null)
    }
    fn from_value(v: &Value) -> Result<Self, Error> {
        if v.is_null() {
            Ok(None)
        } else {
            T::from_value(v).map(Some)
        }
    }
    fn present(&self) -> bool {
        self.is_some()
    }
}
impl<T: JsonField> JsonField for Vec<T> {
    fn to_value(&self) -> Value {
        Value::Array(self.iter().map(JsonField::to_value).collect())
    }
    fn from_value(v: &Value) -> Result<Self, Error> {
        v.as_array()
            .ok_or(Error::Input("array"))?
            .iter()
            .map(T::from_value)
            .collect()
    }
    fn present(&self) -> bool {
        !self.is_empty()
    }
}
impl<T: JsonField> JsonField for Box<T> {
    fn to_value(&self) -> Value {
        self.as_ref().to_value()
    }
    fn from_value(v: &Value) -> Result<Self, Error> {
        T::from_value(v).map(Box::new)
    }
    fn present(&self) -> bool {
        self.as_ref().present()
    }
}
impl<K: JsonField + Eq + std::hash::Hash, V: JsonField> JsonField
    for std::collections::HashMap<K, V>
{
    fn to_value(&self) -> Value {
        let ordered = self
            .iter()
            .map(|(k, v)| (string(&k.to_value()), v.to_value()))
            .collect::<std::collections::BTreeMap<_, _>>();
        Value::Object(ordered.into_iter().collect())
    }
    fn from_value(value: &Value) -> Result<Self, Error> {
        object(value)?
            .iter()
            .map(|(k, v)| Ok((K::from_value(&Value::String(k.clone()))?, V::from_value(v)?)))
            .collect()
    }
    fn present(&self) -> bool {
        !self.is_empty()
    }
}
include!(concat!(env!("OUT_DIR"), "/proto_json.rs"));
