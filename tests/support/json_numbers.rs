use serde_json::Value;
/// JavaScript has one number type; compare serde's integer/float values as JS numbers.
pub fn canonical(value: &Value) -> Value {
    match value {
        Value::Number(number) => {
            Value::Number(serde_json::Number::from_f64(number.as_f64().unwrap()).unwrap())
        }
        Value::Array(values) => Value::Array(values.iter().map(canonical).collect()),
        Value::Object(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), canonical(value)))
                .collect(),
        ),
        other => other.clone(),
    }
}
