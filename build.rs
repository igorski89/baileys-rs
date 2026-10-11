use std::{env, fs, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/WAProto.proto");
    println!("cargo:rerun-if-changed=proto/tokens.json");
    let mut config = prost_build::Config::new();
    config.boxed(".proto.EmbeddedContent.content.embeddedMessage");
    config.boxed(".proto.EmbeddedContent.content.embeddedMusic");
    for message in [
        ".proto.ADVSignedDeviceIdentity",
        ".proto.MessageKey",
        ".proto.Message.AppStateSyncKeyData",
        ".proto.Message.AppStateSyncKeyFingerprint",
    ] {
        config.type_attribute(
            message,
            "#[derive(serde::Serialize, serde::Deserialize)] #[serde(rename_all = \"camelCase\", default)]",
        );
    }
    for field in [
        "details",
        "accountSignatureKey",
        "accountSignature",
        "deviceSignature",
    ] {
        config.field_attribute(
            format!(".proto.ADVSignedDeviceIdentity.{field}"),
            "#[serde(default, with = \"crate::serialization::optional_bytes\")]",
        );
    }
    config.field_attribute(
        ".proto.Message.AppStateSyncKeyData.keyData",
        "#[serde(default, with = \"crate::serialization::optional_bytes\")]",
    );
    config.field_attribute(
        ".proto.Message.AppStateSyncKeyData.timestamp",
        "#[serde(default, with = \"crate::serialization::optional_i64\")]",
    );
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    // protobufjs accepts nonzero-first enums; protoc requires a zero value in proto3.
    // Keep the vendored source unchanged and add only synthetic zero variants for codegen.
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let schema = fs::read_to_string("proto/WAProto.proto")?;
    let mut normalized = String::new();
    let mut enum_name = None;
    for line in schema.lines() {
        let trimmed = line.trim();
        if let Some(name) = trimmed.strip_prefix("enum ") {
            enum_name = Some(
                name.split_whitespace()
                    .next()
                    .ok_or("invalid enum")?
                    .to_owned(),
            );
        } else if let Some(name) = enum_name.take() {
            let number = trimmed
                .split_once('=')
                .ok_or("invalid enum value")?
                .1
                .trim();
            if !number.starts_with("0;") {
                normalized.push_str(&format!("        {name}_UNSPECIFIED = 0;\n"));
            }
        }
        normalized.push_str(line);
        normalized.push('\n');
    }
    let normalized_path = out_dir.join("WAProto.proto");
    fs::write(&normalized_path, normalized)?;
    config.compile_protos(&[normalized_path], &[out_dir])?;
    let tokens: serde_json::Value = serde_json::from_slice(&fs::read("proto/tokens.json")?)?;
    let mut output = String::from("// Generated from upstream Baileys token tables.\n");
    for (key, name) in [("single", "SINGLE"), ("double", "DOUBLE")] {
        if key == "single" {
            output.push_str(&format!("pub const {name}: &[&str] = &[\n"));
            for token in tokens[key].as_array().ok_or("invalid tokens")? {
                output.push_str(&format!("{:?},\n", token.as_str().ok_or("invalid token")?));
            }
        } else {
            output.push_str(&format!("pub const {name}: &[&[&str]] = &[\n"));
            for dictionary in tokens[key].as_array().ok_or("invalid dictionaries")? {
                output.push_str("&[\n");
                for token in dictionary.as_array().ok_or("invalid dictionary")? {
                    output.push_str(&format!("{:?},\n", token.as_str().ok_or("invalid token")?));
                }
                output.push_str("],\n");
            }
        }
        output.push_str("];\n");
    }
    fs::write(
        PathBuf::from(env::var("OUT_DIR")?).join("tokens.rs"),
        output,
    )?;
    Ok(())
}
