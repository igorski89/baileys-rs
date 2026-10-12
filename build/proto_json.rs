use heck::{ToSnakeCase, ToUpperCamelCase};
use prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorSet,
    field_descriptor_proto::{Label, Type},
};
fn ident(s: &str) -> String {
    match s {
        "self" | "super" | "crate" | "Self" => format!("{s}_"),
        "type" | "enum" | "match" | "ref" | "box" | "async" | "move" | "loop" | "where" | "in"
        | "fn" | "mod" | "pub" | "use" | "impl" | "trait" | "struct" | "const" | "static"
        | "let" | "mut" | "as" | "dyn" | "await" | "try" | "yield" | "gen" => format!("r#{s}"),
        _ => s.into(),
    }
}
fn path(name: &str) -> String {
    let parts: Vec<_> = name.trim_start_matches('.').split('.').collect();
    format!(
        "crate::{}",
        parts
            .iter()
            .enumerate()
            .map(|(i, s)| if i + 1 == parts.len() {
                ident(&s.to_upper_camel_case())
            } else {
                ident(&s.to_snake_case())
            })
            .collect::<Vec<_>>()
            .join("::")
    )
}
fn optional(f: &FieldDescriptorProto) -> bool {
    f.proto3_optional.unwrap_or(false) || f.r#type == Some(Type::Message as i32)
}
fn parse(f: &FieldDescriptorProto, value: &str) -> String {
    if f.r#type == Some(Type::Bytes as i32) {
        format!("bytes_from_value({value})?")
    } else {
        let kind = Type::try_from(f.r#type.unwrap()).unwrap();
        let ty = match kind {
            Type::Message | Type::Group => path(f.type_name.as_ref().unwrap()),
            Type::String => "String".into(),
            Type::Bool => "bool".into(),
            Type::Double => "f64".into(),
            Type::Float => "f32".into(),
            Type::Int64 | Type::Sfixed64 | Type::Sint64 => "i64".into(),
            Type::Uint64 | Type::Fixed64 => "u64".into(),
            Type::Uint32 | Type::Fixed32 => "u32".into(),
            _ => "i32".into(),
        };
        format!("parse::<{ty}>({value})?")
    }
}
pub fn generate(fds: &FileDescriptorSet) -> String {
    let mut out = String::from("// Generated complete protobuf JSON codec.\n");
    for file in &fds.file {
        let prefix = format!(".{}", file.package.as_deref().unwrap_or("proto"));
        for message in &file.message_type {
            emit(message, &prefix, &mut out)
        }
    }
    let names = schema(fds);
    let names = names.as_array().unwrap();
    out.push_str("pub const MESSAGE_NAMES:&[&str]=&[");
    for name in names {
        out.push_str(&format!("{:?},", name["name"].as_str().unwrap()));
    }
    out.push_str("];\n");
    out.push_str("pub fn decode_named(name:&str,bytes:&[u8])->Result<Value,Error>{match name.trim_start_matches('.'){");
    for name in names {
        let name = name["name"].as_str().unwrap();
        let rust = path(name);
        out.push_str(&format!(
            "{name:?}=>Ok(JsonField::to_value(&<{rust} as prost::Message>::decode(bytes)?)),"
        ));
    }
    out.push_str("_=>Err(Error::UnknownMessage(name.into()))}}\n");
    out.push_str("pub fn encode_named(name:&str,value:&Value)->Result<Vec<u8>,Error>{match name.trim_start_matches('.'){");
    for name in names {
        let name = name["name"].as_str().unwrap();
        let rust = path(name);
        out.push_str(&format!(
            "{name:?}=>Ok(prost::Message::encode_to_vec(&parse::<{rust}>(value)?)),"
        ));
    }
    out.push_str("_=>Err(Error::UnknownMessage(name.into()))}}\n");
    out
}
pub fn schema(fds: &FileDescriptorSet) -> serde_json::Value {
    let mut maps = std::collections::HashMap::new();
    fn collect(
        m: &DescriptorProto,
        prefix: &str,
        maps: &mut std::collections::HashMap<String, DescriptorProto>,
    ) {
        let name = format!("{prefix}.{}", m.name.as_ref().unwrap());
        if m.options
            .as_ref()
            .is_some_and(|o| o.map_entry.unwrap_or(false))
        {
            maps.insert(name.clone(), m.clone());
        }
        for n in &m.nested_type {
            collect(n, &name, maps)
        }
    }
    for file in &fds.file {
        for m in &file.message_type {
            collect(
                m,
                &format!(".{}", file.package.as_deref().unwrap_or("proto")),
                &mut maps,
            )
        }
    }
    fn field(f: &FieldDescriptorProto) -> serde_json::Value {
        serde_json::json!({"name":f.json_name.as_ref().or(f.name.as_ref()),"type":Type::try_from(f.r#type.unwrap()).unwrap().as_str_name(),"typeName":f.type_name,"repeated":f.label==Some(Label::Repeated as i32),"optional":optional(f)})
    }
    fn visit(
        m: &DescriptorProto,
        prefix: &str,
        maps: &std::collections::HashMap<String, DescriptorProto>,
        out: &mut Vec<serde_json::Value>,
    ) {
        if m.options
            .as_ref()
            .is_some_and(|o| o.map_entry.unwrap_or(false))
        {
            return;
        }
        let name = format!("{prefix}.{}", m.name.as_ref().unwrap());
        let fields=m.field.iter().map(|f|{let mut value=field(f);if let Some(index)=f.oneof_index.filter(|_|!f.proto3_optional.unwrap_or(false)){value["oneof"]=serde_json::json!(m.oneof_decl[index as usize].name);}
if let Some(map)=f.type_name.as_ref().and_then(|name|maps.get(name)){value["map"]=serde_json::json!({"key":field(&map.field[0]),"value":field(&map.field[1])});}value}).collect::<Vec<_>>();
        out.push(serde_json::json!({"name":name.trim_start_matches('.'),"fields":fields}));
        for n in &m.nested_type {
            visit(n, &name, maps, out)
        }
    }
    let mut out = vec![];
    for file in &fds.file {
        for m in &file.message_type {
            visit(
                m,
                &format!(".{}", file.package.as_deref().unwrap_or("proto")),
                &maps,
                &mut out,
            )
        }
    }
    serde_json::Value::Array(out)
}
fn emit(message: &DescriptorProto, parent: &str, out: &mut String) {
    if message
        .options
        .as_ref()
        .is_some_and(|o| o.map_entry.unwrap_or(false))
    {
        return;
    }
    let fq = format!("{parent}.{}", message.name.as_ref().unwrap());
    let rust = path(&fq);
    if message.field.is_empty() {
        out.push_str(&format!("impl JsonMessage for {rust}{{}} impl JsonField for {rust}{{fn to_value(&self)->Value{{json!({{}})}} fn from_value(value:&Value)->Result<Self,Error>{{object(value)?;Ok(<Self as Default>::default())}}}}\n"));
        for nested in &message.nested_type {
            emit(nested, &fq, out)
        }
        return;
    }
    let mut fields = message.field.iter().collect::<Vec<_>>();
    fields.sort_by_key(|f| f.number);
    out.push_str(&format!("impl JsonMessage for {rust} {{}}\nimpl JsonField for {rust} {{fn to_value(&self)->Value {{let mut map=serde_json::Map::new();\n"));
    for f in &fields {
        let name = f.json_name.as_ref().or(f.name.as_ref()).unwrap();
        let member = ident(&f.name.as_ref().unwrap().to_snake_case());
        let real_oneof = f
            .oneof_index
            .filter(|_| !f.proto3_optional.unwrap_or(false));
        if real_oneof.is_some() {
            continue;
        }
        let expr = if f.r#type == Some(Type::Bytes as i32) {
            if f.label == Some(Label::Repeated as i32) {
                format!("Value::Array(self.{member}.iter().map(|v|bytes_to_value(v)).collect())")
            } else if optional(f) {
                format!("bytes_to_value(self.{member}.as_ref().unwrap())")
            } else {
                format!("bytes_to_value(&self.{member})")
            }
        } else if f.r#type == Some(Type::Fixed64 as i32) {
            if f.label == Some(Label::Repeated as i32) {
                format!("Value::Array(self.{member}.iter().map(|v|fixed64_value(*v)).collect())")
            } else if optional(f) {
                format!("fixed64_value(self.{member}.unwrap())")
            } else {
                format!("fixed64_value(self.{member})")
            }
        } else {
            format!("self.{member}.to_value()")
        };
        out.push_str(&format!(
            "if self.{member}.present() {{map.insert({name:?}.into(),{expr});}}\n"
        ));
    }
    for (index, oneof) in message.oneof_decl.iter().enumerate() {
        let choices: Vec<_> = fields
            .iter()
            .filter(|f| f.oneof_index == Some(index as i32) && !f.proto3_optional.unwrap_or(false))
            .collect();
        if choices.is_empty() {
            continue;
        }
        let name = oneof.name.as_ref().unwrap();
        let member = ident(&name.to_snake_case());
        let enum_path = path(&format!("{fq}.{name}"));
        out.push_str(&format!(
            "if let Some(choice)=&self.{member} {{match choice {{\n"
        ));
        for f in choices {
            let n = f.json_name.as_ref().or(f.name.as_ref()).unwrap();
            let variant = ident(&f.name.as_ref().unwrap().to_upper_camel_case());
            let expr: String = if f.r#type == Some(Type::Bytes as i32) {
                "bytes_to_value(value)".into()
            } else if f.r#type == Some(Type::Fixed64 as i32) {
                "fixed64_value(*value)".into()
            } else {
                "value.to_value()".into()
            };
            out.push_str(&format!(
                "{enum_path}::{variant}(value)=>{{map.insert({n:?}.into(),{expr});}},\n"
            ));
        }
        out.push_str("}}\n");
    }
    out.push_str("Value::Object(map)}\nfn from_value(value:&Value)->Result<Self,Error>{let map=object(value)?;let mut message=<Self as Default>::default();\n");
    for f in &fields {
        let name = f.json_name.as_ref().or(f.name.as_ref()).unwrap();
        let member = ident(&f.name.as_ref().unwrap().to_snake_case());
        let oneof = f
            .oneof_index
            .filter(|_| !f.proto3_optional.unwrap_or(false));
        let enum_field = f.r#type == Some(Type::Enum as i32);
        let infallible = enum_field && f.label != Some(Label::Repeated as i32);
        if infallible {
            let ep = path(f.type_name.as_ref().unwrap());
            out.push_str(&format!("if let Some(value)=map.get({name:?}).filter(|v|!v.is_null()) && let Some(parsed)=enum_value(value,|name|{ep}::from_str_name(name).map(|v|v as i32)) {{\n"));
            if let Some(index) = oneof {
                let group = message.oneof_decl[index as usize].name.as_ref().unwrap();
                let gm = ident(&group.to_snake_case());
                let enum_path = path(&format!("{fq}.{group}"));
                let variant = ident(&f.name.as_ref().unwrap().to_upper_camel_case());
                out.push_str(&format!(
                    "message.{gm}=Some({enum_path}::{variant}(into_oneof(parsed)));\n"
                ));
            } else {
                let wrap = if optional(f) {
                    "Some(parsed)"
                } else {
                    "parsed"
                };
                out.push_str(&format!("message.{member}={wrap};\n"));
            }
            out.push_str("}\n");
            continue;
        }
        out.push_str(&format!(
            "if let Some(value)=map.get({name:?}).filter(|v|!v.is_null()) {{\n"
        ));
        out.push_str("let parsed=(||->Result<_,Error>{\n");
        let expr = parse(f, "value");
        if let Some(index) = oneof {
            let group = message.oneof_decl[index as usize].name.as_ref().unwrap();
            let group_member = ident(&group.to_snake_case());
            let enum_path = path(&format!("{fq}.{group}"));
            let variant = ident(&f.name.as_ref().unwrap().to_upper_camel_case());
            out.push_str(&format!(
                "message.{group_member}=Some({enum_path}::{variant}(into_oneof({expr})));\n"
            ));
        } else if f.label == Some(Label::Repeated as i32) {
            if enum_field {
                let ep = path(f.type_name.as_ref().unwrap());
                out.push_str(&format!("message.{member}=value.as_array().ok_or(Error::Input(\"array\"))?.iter().map(|value|enum_value(value,|name|{ep}::from_str_name(name).map(|v|v as i32)).unwrap_or(0)).collect();\n"));
            } else if f.r#type == Some(Type::Bytes as i32) {
                out.push_str(&format!("message.{member}=value.as_array().ok_or(Error::Input(\"array\"))?.iter().map(bytes_from_value).collect::<Result<_,_>>()?;\n"));
            } else {
                out.push_str(&format!("message.{member}=parse(value)?;\n"));
            }
        } else if f.r#type == Some(Type::Bytes as i32) && optional(f) {
            out.push_str(&format!("message.{member}=Some({expr});\n"));
        } else {
            out.push_str(&format!("message.{member}=parse(value)?;\n"));
        }
        out.push_str(&format!(
            "Ok(())}})();parsed.map_err(|e|e.field({:?}))?;}}\n",
            format!("{fq}.{name}")
        ));
    }
    out.push_str("Ok(message)}}\n");
    for nested in &message.nested_type {
        emit(nested, &fq, out)
    }
}
