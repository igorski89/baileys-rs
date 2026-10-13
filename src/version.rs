//! Look up the current WhatsApp Web version, as upstream's `fetchLatest*Version` helpers do.
use crate::defaults::WA_VERSION;
use regex::Regex;
use std::sync::LazyLock;

pub const BAILEYS_DEFAULTS_URL: &str =
    "https://raw.githubusercontent.com/WhiskeySockets/Baileys/master/src/Defaults/index.ts";
pub const WA_WEB_URL: &str = "https://web.whatsapp.com/sw.js";
const USER_AGENT: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

/// A looked-up version; on any failure the built-in version with `is_latest` unset and the reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Latest {
    pub version: [u32; 3],
    pub is_latest: bool,
    pub error: Option<String>,
}
impl Latest {
    fn fallback(error: impl Into<String>) -> Self {
        Self {
            version: WA_VERSION,
            is_latest: false,
            error: Some(error.into()),
        }
    }
    fn current(version: [u32; 3]) -> Self {
        Self {
            version,
            is_latest: true,
            error: None,
        }
    }
}
/// The version on the seventh line of upstream's `Defaults/index.ts`.
pub fn parse_baileys_version(text: &str) -> Option<[u32; 3]> {
    static PATTERN: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"const version = \[([0-9]+),\s*([0-9]+),\s*([0-9]+)\]").unwrap()
    });
    let found = PATTERN.captures(text.split('\n').nth(6)?)?;
    Some([
        found[1].parse().ok()?,
        found[2].parse().ok()?,
        found[3].parse().ok()?,
    ])
}
/// The client revision in WhatsApp Web's service worker, as a `2.3000.x` version.
pub fn parse_wa_web_version(text: &str) -> Option<[u32; 3]> {
    static PATTERN: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#"\\?"client_revision\\?":\s*([0-9]+)"#).unwrap());
    Some([2, 3000, PATTERN.captures(text)?[1].parse().ok()?])
}
async fn page(request: reqwest::RequestBuilder, name: &str) -> Result<String, String> {
    let response = request.send().await.map_err(|error| error.to_string())?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!(
            "Failed to fetch {name}: {}",
            status.canonical_reason().unwrap_or_default()
        ));
    }
    response.text().await.map_err(|error| error.to_string())
}
/// The version the upstream repository currently defaults to.
pub async fn fetch_latest_baileys_version(client: &reqwest::Client) -> Latest {
    fetch_baileys_version_from(client, BAILEYS_DEFAULTS_URL).await
}
pub async fn fetch_baileys_version_from(client: &reqwest::Client, url: &str) -> Latest {
    match page(client.get(url), "latest Baileys version").await {
        Ok(text) => parse_baileys_version(&text).map_or_else(
            || Latest::fallback("Could not parse version from Defaults/index.ts"),
            Latest::current,
        ),
        Err(error) => Latest::fallback(error),
    }
}
/// The version WhatsApp Web currently serves.
pub async fn fetch_latest_wa_web_version(client: &reqwest::Client) -> Latest {
    fetch_wa_web_version_from(client, WA_WEB_URL).await
}
pub async fn fetch_wa_web_version_from(client: &reqwest::Client, url: &str) -> Latest {
    // Upstream sends only the headers needed to get past bot detection.
    let request = client
        .get(url)
        .header("sec-fetch-site", "none")
        .header(reqwest::header::USER_AGENT, USER_AGENT);
    match page(request, "sw.js").await {
        Ok(text) => parse_wa_web_version(&text).map_or_else(
            || Latest::fallback("Could not find client revision in the fetched content"),
            Latest::current,
        ),
        Err(error) => Latest::fallback(error),
    }
}
