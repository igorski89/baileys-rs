//! Connection payloads, companion browser descriptions and pairing validation.
use crate::{
    auth::{Contact, Credentials, ProtocolAddress, SignalIdentity},
    binary::{BinaryNode, NodeContent},
    crypto, defaults,
    jid::Jid,
    proto,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use prost::Message;
use zeroize::Zeroizing;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Crypto(#[from] crypto::Error),
    #[error(transparent)]
    Protobuf(#[from] prost::DecodeError),
    #[error("invalid login JID")]
    Jid,
    #[error("invalid pairing stanza: {0}")]
    Pairing(&'static str),
    #[error("pairing authentication failed: {0}")]
    Authentication(&'static str),
    #[error("invalid pairing code or phone number")]
    PairingCode,
    #[error("signed prekey ID exceeds wire range")]
    PreKeyId,
}
/// The kernel release (`uname -r`, or the version `ver` prints on Windows); empty when unavailable.
fn os_release() -> String {
    let run = |program: &str, args: &[&str]| {
        let output = std::process::Command::new(program)
            .args(args)
            .output()
            .ok()?;
        String::from_utf8(output.stdout).ok()
    };
    if cfg!(windows) {
        run("cmd", &["/c", "ver"])
            .and_then(|text| {
                let version = text.rsplit("Version ").next()?.trim();
                Some(version.trim_end_matches(']').to_owned())
            })
            .unwrap_or_default()
    } else {
        run("uname", &["-r"])
            .map(|text| text.trim().to_owned())
            .unwrap_or_default()
    }
}
/// Whether a credential platform is one of the WhatsApp Business apps.
pub fn is_business_platform(platform: &str) -> bool {
    platform == "smbi" || platform == "smba"
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Browser(pub String, pub String, pub String);
impl Browser {
    pub fn macos(browser: impl Into<String>) -> Self {
        Self("Mac OS".into(), browser.into(), "14.4.1".into())
    }
    pub fn ubuntu(browser: impl Into<String>) -> Self {
        Self("Ubuntu".into(), browser.into(), "22.04.4".into())
    }
    pub fn windows(browser: impl Into<String>) -> Self {
        Self("Windows".into(), browser.into(), "10.0.22631".into())
    }
    pub fn baileys(browser: impl Into<String>) -> Self {
        Self("Baileys".into(), browser.into(), "6.5.0".into())
    }
    pub fn android(device: impl Into<String>) -> Self {
        Self(device.into(), "Android".into(), String::new())
    }
    /// The platform this process runs on and its kernel release, as upstream's `appropriate`.
    pub fn appropriate(browser: impl Into<String>) -> Self {
        let platform = match std::env::consts::OS {
            "aix" => "AIX",
            "macos" => "Mac OS",
            "windows" => "Windows",
            "android" => "Android",
            "freebsd" => "FreeBSD",
            "openbsd" => "OpenBSD",
            "solaris" | "illumos" => "Solaris",
            _ => "Ubuntu",
        };
        Self(platform.into(), browser.into(), os_release())
    }
    pub fn companion_platform_id(&self) -> u8 {
        match self.1.as_str() {
            "Desktop" => {
                if self.0 == "Windows" {
                    8
                } else {
                    7
                }
            }
            "Chrome" => 1,
            "Edge" => 2,
            "Firefox" => 3,
            "IE" => 4,
            "Opera" => 5,
            "Safari" => 6,
            _ => 9,
        }
    }
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PayloadConfig {
    pub version: [u32; 3],
    pub browser: Browser,
    pub sync_full_history: bool,
    pub country_code: String,
    pub push_name: Option<String>,
}
impl Default for PayloadConfig {
    fn default() -> Self {
        Self {
            version: defaults::WA_VERSION,
            browser: Browser::macos("Chrome"),
            sync_full_history: true,
            country_code: "US".into(),
            push_name: None,
        }
    }
}
fn client_payload(config: &PayloadConfig) -> proto::ClientPayload {
    use proto::client_payload::{self as p, user_agent as ua, web_info::WebSubPlatform};
    let android = config.browser.1.to_lowercase().contains("android");
    let web_sub_platform = if config.sync_full_history && config.browser.1 == "Desktop" {
        match config.browser.0.as_str() {
            "Mac OS" => WebSubPlatform::Darwin,
            "Windows" => WebSubPlatform::WinHybrid,
            _ => WebSubPlatform::WebBrowser,
        }
    } else {
        WebSubPlatform::WebBrowser
    };
    proto::ClientPayload {
        connect_type: Some(p::ConnectType::WifiUnknown as i32),
        connect_reason: Some(p::ConnectReason::UserActivated as i32),
        user_agent: Some(p::UserAgent {
            app_version: Some(ua::AppVersion {
                primary: Some(config.version[0]),
                secondary: Some(config.version[1]),
                tertiary: Some(config.version[2]),
                ..Default::default()
            }),
            platform: Some(if android {
                ua::Platform::Android
            } else {
                ua::Platform::Web
            } as i32),
            release_channel: Some(ua::ReleaseChannel::Release as i32),
            os_version: Some("0.1".into()),
            device: Some("Desktop".into()),
            os_build_number: Some("0.1".into()),
            locale_language_iso6391: Some("en".into()),
            mnc: Some("000".into()),
            mcc: Some("000".into()),
            locale_country_iso31661_alpha2: Some(config.country_code.clone()),
            ..Default::default()
        }),
        web_info: if android {
            None
        } else {
            Some(p::WebInfo {
                web_sub_platform: Some(web_sub_platform as i32),
                ..Default::default()
            })
        },
        push_name: config.push_name.clone().filter(|n| !n.is_empty()),
        ..Default::default()
    }
}
pub fn login_payload(jid: &str, config: &PayloadConfig) -> Result<proto::ClientPayload, Error> {
    let jid = Jid::parse(jid).ok_or(Error::Jid)?;
    let username = jid.user.parse::<u64>().map_err(|_| Error::Jid)?;
    Ok(proto::ClientPayload {
        passive: Some(true),
        pull: Some(true),
        username: Some(username),
        device: jid.device.map(u32::from),
        lid_db_migrated: Some(false),
        ..client_payload(config)
    })
}
pub fn registration_payload(
    credentials: &Credentials,
    config: &PayloadConfig,
) -> Result<proto::ClientPayload, Error> {
    use proto::{
        client_payload::DevicePairingRegistrationData,
        device_props::{self as d, PlatformType},
    };
    if credentials.signed_pre_key.key_id > 0xffffff {
        return Err(Error::PreKeyId);
    }
    let platform = config.browser.1.to_uppercase();
    let platform_type = if platform == "ANDROID" {
        PlatformType::AndroidPhone
    } else {
        PlatformType::from_str_name(&platform)
            .filter(|v| *v != PlatformType::Unknown)
            .unwrap_or(PlatformType::Chrome)
    };
    let props = proto::DeviceProps {
        os: Some(config.browser.0.clone()),
        platform_type: Some(platform_type as i32),
        require_full_sync: Some(config.sync_full_history),
        version: Some(d::AppVersion {
            primary: Some(10),
            secondary: Some(15),
            tertiary: Some(7),
            ..Default::default()
        }),
        history_sync_config: Some(d::HistorySyncConfig {
            storage_quota_mb: Some(10240),
            inline_initial_payload_in_e2_ee_msg: Some(true),
            support_call_log_history: Some(false),
            support_bot_user_agent_chat_history: Some(true),
            support_cag_reactions_and_polls: Some(true),
            support_biz_hosted_msg: Some(true),
            support_recent_sync_chunk_message_count_tuning: Some(true),
            support_hosted_group_msg: Some(true),
            support_fbid_bot_chat_history: Some(true),
            support_message_association: Some(true),
            support_group_history: Some(false),
            ..Default::default()
        }),
    };
    let version = config
        .version
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(".");
    Ok(proto::ClientPayload {
        passive: Some(false),
        pull: Some(false),
        device_pairing_data: Some(DevicePairingRegistrationData {
            build_hash: Some(crypto::md5(version.as_bytes()).to_vec()),
            device_props: Some(props.encode_to_vec()),
            e_regid: Some(
                u32::from(credentials.registration_id)
                    .to_be_bytes()
                    .to_vec(),
            ),
            e_keytype: Some(vec![5]),
            e_ident: Some(credentials.signed_identity_key.public.to_vec()),
            e_skey_id: Some(credentials.signed_pre_key.key_id.to_be_bytes()[1..].to_vec()),
            e_skey_val: Some(credentials.signed_pre_key.key_pair.public.to_vec()),
            e_skey_sig: Some(credentials.signed_pre_key.signature.clone()),
        }),
        ..client_payload(config)
    })
}
pub fn pairing_qr(reference: &str, credentials: &Credentials, browser: &Browser) -> String {
    format!(
        "https://wa.me/settings/linked_devices#{reference},{},{},{},{}",
        STANDARD.encode(credentials.noise_key.public),
        STANDARD.encode(credentials.signed_identity_key.public),
        credentials.adv_secret_key,
        browser.companion_platform_id()
    )
}
pub fn encode_signed_device_identity(
    account: &proto::AdvSignedDeviceIdentity,
    include_signature_key: bool,
) -> Vec<u8> {
    let mut account = account.clone();
    if !include_signature_key
        || account
            .account_signature_key
            .as_ref()
            .is_none_or(Vec::is_empty)
    {
        account.account_signature_key = None;
    }
    account.encode_to_vec()
}
pub struct PairingResult {
    pub account: proto::AdvSignedDeviceIdentity,
    pub me: Contact,
    pub identity: SignalIdentity,
    pub platform: Option<String>,
    pub reply: BinaryNode,
}
impl PairingResult {
    pub fn apply(&self, credentials: &mut Credentials) {
        credentials.account = Some(self.account.clone());
        credentials.me = Some(self.me.clone());
        credentials
            .signal_identities
            .get_or_insert_with(Vec::new)
            .push(self.identity.clone());
        credentials.platform = self.platform.clone();
    }
}
pub fn configure_successful_pairing(
    stanza: &BinaryNode,
    credentials: &Credentials,
) -> Result<PairingResult, Error> {
    let success = stanza
        .child("pair-success")
        .ok_or(Error::Pairing("missing pair-success"))?;
    let device = success
        .child("device")
        .ok_or(Error::Pairing("missing device"))?;
    let identity = success
        .child_buffer("device-identity")
        .ok_or(Error::Pairing("missing device identity"))?;
    let signed_hmac = proto::AdvSignedDeviceIdentityHmac::decode(identity)?;
    let details = signed_hmac
        .details
        .ok_or(Error::Pairing("missing signed details"))?;
    let hmac = signed_hmac.hmac.ok_or(Error::Pairing("missing HMAC"))?;
    let secret = Zeroizing::new(
        STANDARD
            .decode(&credentials.adv_secret_key)
            .map_err(|_| Error::Pairing("ADV secret encoding"))?,
    );
    let mut authenticated = Vec::new();
    if signed_hmac.account_type == Some(proto::AdvEncryptionType::Hosted as i32) {
        authenticated.extend([6, 5]);
    }
    authenticated.extend(&details);
    // Fixed length plus constant-time comparison for authentication tags.
    use hmac::Mac;
    let mut verifier = <hmac::Hmac<sha2::Sha256> as Mac>::new_from_slice(&secret)
        .map_err(|_| Error::Authentication("ADV secret"))?;
    verifier.update(&authenticated);
    verifier
        .verify_slice(&hmac)
        .map_err(|_| Error::Authentication("account HMAC"))?;
    let mut account = proto::AdvSignedDeviceIdentity::decode(details.as_slice())?;
    let device_details = account
        .details
        .as_deref()
        .ok_or(Error::Pairing("missing device details"))?;
    let device_identity = proto::AdvDeviceIdentity::decode(device_details)?;
    let account_key = account
        .account_signature_key
        .as_deref()
        .ok_or(Error::Pairing("missing account key"))?;
    let signature = account
        .account_signature
        .as_deref()
        .ok_or(Error::Pairing("missing account signature"))?;
    let mut message =
        if device_identity.device_type == Some(proto::AdvEncryptionType::Hosted as i32) {
            vec![6, 5]
        } else {
            vec![6, 0]
        };
    message.extend(device_details);
    message.extend(credentials.signed_identity_key.public);
    if !crypto::verify_signature(account_key, &message, signature) {
        return Err(Error::Authentication("account signature"));
    }
    let mut message = vec![6, 1];
    message.extend(device_details);
    message.extend(credentials.signed_identity_key.public);
    message.extend(account_key);
    account.device_signature = Some(credentials.signed_identity_key.sign(&message).to_vec());
    let key_index = device_identity
        .key_index
        .ok_or(Error::Pairing("missing key index"))?;
    let id = stanza
        .attrs
        .get("id")
        .ok_or(Error::Pairing("missing stanza ID"))?;
    let jid = device
        .attrs
        .get("jid")
        .filter(|s| !s.is_empty())
        .ok_or(Error::Pairing("missing device JID"))?;
    let lid = device
        .attrs
        .get("lid")
        .filter(|s| !s.is_empty())
        .ok_or(Error::Pairing("missing device LID"))?;
    let identity = SignalIdentity {
        identifier: ProtocolAddress {
            name: lid.clone(),
            device_id: 0,
        },
        identifier_key: crypto::signal_public_key(
            account
                .account_signature_key
                .as_deref()
                .ok_or(Error::Pairing("account key"))?,
        )?
        .to_vec(),
    };
    let reply = BinaryNode::new("iq")
        .attr("to", "@s.whatsapp.net")
        .attr("type", "result")
        .attr("id", id)
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("pair-device-sign").with_content(NodeContent::Children(vec![
                BinaryNode::new("device-identity")
                    .attr("key-index", key_index.to_string())
                    .with_content(NodeContent::Bytes(encode_signed_device_identity(
                        &account, false,
                    ))),
            ])),
        ]));
    Ok(PairingResult {
        account,
        me: Contact {
            id: jid.clone(),
            lid: Some(lid.clone()),
            name: success
                .child("biz")
                .and_then(|n| n.attrs.get("name"))
                .cloned(),
            ..Default::default()
        },
        identity,
        platform: success
            .child("platform")
            .and_then(|n| n.attrs.get("name"))
            .cloned(),
        reply,
    })
}
pub fn bytes_to_crockford(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTVWXYZ";
    let mut bits = 0u32;
    let mut count = 0;
    let mut result = String::new();
    for byte in bytes {
        bits = (bits << 8) | u32::from(*byte);
        count += 8;
        while count >= 5 {
            count -= 5;
            result.push(ALPHABET[((bits >> count) & 31) as usize] as char);
        }
    }
    if count > 0 {
        result.push(ALPHABET[((bits << (5 - count)) & 31) as usize] as char);
    }
    result
}
pub fn pairing_code_request(
    credentials: &mut Credentials,
    phone: &str,
    custom_code: Option<&str>,
    browser: &Browser,
    id: &str,
) -> Result<(String, BinaryNode), Error> {
    let mut random = [0; 5];
    getrandom::fill(&mut random).map_err(|_| crypto::Error::Random)?;
    let code = custom_code
        .map(str::to_owned)
        .unwrap_or_else(|| bytes_to_crockford(&random));
    let mut salt = [0; 32];
    let mut iv = [0; 16];
    getrandom::fill(&mut salt).map_err(|_| crypto::Error::Random)?;
    getrandom::fill(&mut iv).map_err(|_| crypto::Error::Random)?;
    let node =
        pairing_code_request_with_material(credentials, phone, &code, browser, id, &salt, &iv)?;
    Ok((code, node))
}
/// Deterministic variant for fixture verification; use fresh salt and IV per request.
pub fn pairing_code_request_with_material(
    credentials: &mut Credentials,
    phone: &str,
    code: &str,
    browser: &Browser,
    id: &str,
    salt: &[u8; 32],
    iv: &[u8; 16],
) -> Result<BinaryNode, Error> {
    if phone.is_empty()
        || !phone.bytes().all(|b| b.is_ascii_digit())
        || code.encode_utf16().count() != 8
    {
        return Err(Error::PairingCode);
    }
    let key = Zeroizing::new(crypto::derive_pairing_code_key(code, salt));
    let mut wrapped = salt.to_vec();
    wrapped.extend(iv);
    wrapped.extend(crypto::aes_encrypt_ctr(
        &credentials.pairing_ephemeral_key_pair.public,
        key.as_slice(),
        iv,
    )?);
    let jid = format!("{phone}@s.whatsapp.net");
    let node = BinaryNode::new("iq")
        .attr("to", "@s.whatsapp.net")
        .attr("type", "set")
        .attr("id", id)
        .attr("xmlns", "md")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("link_code_companion_reg")
                .attr("jid", &jid)
                .attr("stage", "companion_hello")
                .attr("should_show_push_notification", "true")
                .with_content(NodeContent::Children(vec![
                    BinaryNode::new("link_code_pairing_wrapped_companion_ephemeral_pub")
                        .with_content(NodeContent::Bytes(wrapped)),
                    BinaryNode::new("companion_server_auth_key_pub")
                        .with_content(NodeContent::Bytes(credentials.noise_key.public.to_vec())),
                    BinaryNode::new("companion_platform_id").with_content(NodeContent::Text(
                        browser.companion_platform_id().to_string(),
                    )),
                    BinaryNode::new("companion_platform_display")
                        .with_content(NodeContent::Text(format!("{} ({})", browser.1, browser.0))),
                    BinaryNode::new("link_code_pairing_nonce")
                        .with_content(NodeContent::Text("0".into())),
                ])),
        ]));
    credentials.pairing_code = Some(code.into());
    credentials.me = Some(Contact {
        id: jid,
        name: Some("~".into()),
        ..Default::default()
    });
    Ok(node)
}

pub struct PairingFinish {
    pub node: BinaryNode,
    pub adv_secret_key: String,
}
pub fn pairing_code_finish(
    credentials: &Credentials,
    notification: &BinaryNode,
    id: &str,
) -> Result<PairingFinish, Error> {
    let mut random = [0; 32];
    let mut salt = [0; 32];
    let mut iv = [0; 12];
    getrandom::fill(&mut random).map_err(|_| crypto::Error::Random)?;
    getrandom::fill(&mut salt).map_err(|_| crypto::Error::Random)?;
    getrandom::fill(&mut iv).map_err(|_| crypto::Error::Random)?;
    pairing_code_finish_with_material(credentials, notification, id, &random, &salt, &iv)
}
pub fn pairing_code_finish_with_material(
    credentials: &Credentials,
    notification: &BinaryNode,
    id: &str,
    random: &[u8; 32],
    salt: &[u8; 32],
    iv: &[u8; 12],
) -> Result<PairingFinish, Error> {
    let reg = notification
        .child("link_code_companion_reg")
        .ok_or(Error::Pairing("missing code registration"))?;
    let reference = reg
        .child_buffer("link_code_pairing_ref")
        .ok_or(Error::Pairing("missing code reference"))?;
    let primary_identity = reg
        .child_buffer("primary_identity_pub")
        .filter(|b| b.len() == 32)
        .ok_or(Error::Pairing("primary identity"))?;
    let wrapped = reg
        .child_buffer("link_code_pairing_wrapped_primary_ephemeral_pub")
        .filter(|b| b.len() == 80)
        .ok_or(Error::Pairing("wrapped primary key"))?;
    let code = credentials
        .pairing_code
        .as_deref()
        .ok_or(Error::Pairing("missing pairing code"))?;
    let key = Zeroizing::new(crypto::derive_pairing_code_key(code, &wrapped[..32]));
    let public = crypto::aes_decrypt_ctr(&wrapped[48..80], key.as_slice(), &wrapped[32..48])?;
    let companion_shared =
        Zeroizing::new(credentials.pairing_ephemeral_key_pair.shared_key(&public)?);
    let expanded = Zeroizing::new(crypto::hkdf(
        companion_shared.as_slice(),
        32,
        Some(salt),
        b"link_code_pairing_key_bundle_encryption_key",
    )?);
    let mut plaintext = Zeroizing::new(credentials.signed_identity_key.public.to_vec());
    plaintext.extend(primary_identity);
    plaintext.extend(random);
    let mut encrypted = salt.to_vec();
    encrypted.extend(iv);
    encrypted.extend(crypto::aes_encrypt_gcm(&plaintext, &expanded, iv, b"")?);
    let identity_shared = Zeroizing::new(
        credentials
            .signed_identity_key
            .shared_key(primary_identity)?,
    );
    let mut identity_payload = Zeroizing::new(companion_shared.to_vec());
    identity_payload.extend(identity_shared.as_slice());
    identity_payload.extend(random);
    let adv_secret = Zeroizing::new(crypto::hkdf(&identity_payload, 32, None, b"adv_secret")?);
    let jid = credentials
        .me_id()
        .map_err(|_| Error::Pairing("missing companion JID"))?;
    let node = BinaryNode::new("iq")
        .attr("to", "@s.whatsapp.net")
        .attr("type", "set")
        .attr("id", id)
        .attr("xmlns", "md")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("link_code_companion_reg")
                .attr("jid", jid)
                .attr("stage", "companion_finish")
                .with_content(NodeContent::Children(vec![
                    BinaryNode::new("link_code_pairing_wrapped_key_bundle")
                        .with_content(NodeContent::Bytes(encrypted)),
                    BinaryNode::new("companion_identity_public").with_content(NodeContent::Bytes(
                        credentials.signed_identity_key.public.to_vec(),
                    )),
                    BinaryNode::new("link_code_pairing_ref")
                        .with_content(NodeContent::Bytes(reference.to_vec())),
                ])),
        ]));
    Ok(PairingFinish {
        node,
        adv_secret_key: STANDARD.encode(adv_secret.as_slice()),
    })
}
