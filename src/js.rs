//! JavaScript semantics the TypeScript connector relies on.
use baileys_rs::proto_json::JsonField;
use serde_json::Value;

/// JavaScript truthiness of a JSON value.
pub fn truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_f64().is_some_and(|n| n != 0.0 && !n.is_nan()),
        Some(Value::String(s)) => !s.is_empty(),
        Some(_) => true,
    }
}

/// A field that is present and truthy, as `a?.b` in a condition.
pub fn get<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
    value.get(name).filter(|v| truthy(Some(v)))
}

/// `String(value)` for the scalar values that reach string contexts.
pub fn string(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::String(s) => s.clone(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
        other => String::from_value(other).unwrap_or_else(|_| other.to_string()),
    }
}

/// `JSON.stringify` of a JSON value: numbers print as JavaScript does.
pub fn stringify(value: &Value) -> String {
    match value {
        Value::Number(_) => string(value),
        Value::Array(values) => format!(
            "[{}]",
            values.iter().map(stringify).collect::<Vec<_>>().join(",")
        ),
        Value::Object(fields) => format!(
            "{{{}}}",
            fields
                .iter()
                .map(|(k, v)| format!("{}:{}", Value::String(k.clone()), stringify(v)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        other => other.to_string(),
    }
}

/// UTF-16 length, which is what JavaScript string lengths count.
pub fn utf16_len(value: &str) -> usize {
    value.encode_utf16().count()
}

/// `String.prototype.padEnd` with spaces.
pub fn pad_end(value: &str, width: usize) -> String {
    let len = utf16_len(value);
    if len >= width {
        value.into()
    } else {
        format!("{value}{}", " ".repeat(width - len))
    }
}

/// `String.prototype.slice(start, end)` on UTF-16 units. A cut through a
/// surrogate pair keeps the replacement character, as a lone surrogate
/// cannot be held in a Rust string.
pub fn slice(value: &str, start: usize, end: usize) -> String {
    let units: Vec<u16> = value.encode_utf16().collect();
    let end = end.min(units.len());
    let start = start.min(end);
    String::from_utf16_lossy(&units[start..end])
}

/// JavaScript's `\s` class.
pub fn is_space(c: char) -> bool {
    matches!(c, '\u{0009}'..='\u{000d}' | ' ' | '\u{00a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')
}

/// `Buffer.from(text, 'base64')`: both alphabets, padding optional, and
/// characters outside the alphabet skipped; decoding stops at the first `=`.
pub fn base64_lenient(text: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(text.len() * 3 / 4);
    let mut buffer = 0u32;
    let mut bits = 0;
    for c in text.chars() {
        let value = match c {
            'A'..='Z' => c as u32 - 'A' as u32,
            'a'..='z' => c as u32 - 'a' as u32 + 26,
            '0'..='9' => c as u32 - '0' as u32 + 52,
            '+' | '-' => 62,
            '/' | '_' => 63,
            '=' => break,
            _ => continue,
        };
        buffer = (buffer << 6) | value;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
            buffer &= (1 << bits) - 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn numbers_and_strings_follow_javascript() {
        assert_eq!(
            stringify(&json!({"a":1.0,"b":[1e21,0.5],"c":"x\n"})),
            r#"{"a":1,"b":[1e+21,0.5],"c":"x\n"}"#
        );
        assert_eq!(string(&json!(3.0)), "3");
        assert!(truthy(Some(&json!("x"))));
        assert!(!truthy(Some(&json!(0))));
        assert_eq!(utf16_len("😀"), 2);
        assert_eq!(pad_end("ab", 4), "ab  ");
        assert_eq!(base64_lenient("aGk"), b"hi");
        assert_eq!(base64_lenient("a G-_"), vec![0x68, 0x6f, 0xbf]);
        assert_eq!(base64_lenient("aGk=garbage"), b"hi");
    }
}
