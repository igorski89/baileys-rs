//! Link previews: URL detection, page metadata extraction and thumbnail selection,
//! following upstream's getUrlInfo and the link-preview-js parser it delegates to.
use crate::proto;
use futures_util::future::BoxFuture;
use regex::Regex;
use scraper::{Html, Selector};
use std::sync::OnceLock;

pub const THUMBNAIL_WIDTH_PX: u32 = 192;

/// A fetched page: its final URL after redirects, content type and UTF-8 body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page {
    pub url: String,
    pub content_type: Option<String>,
    pub body: String,
}
/// Metadata of an HTML page, with image URLs resolved against the page URL.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PageInfo {
    pub title: String,
    pub description: Option<String>,
    pub images: Vec<String>,
}
/// Preview data for one URL, as attached to an extended text message.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UrlInfo {
    pub canonical_url: String,
    pub matched_text: String,
    pub title: String,
    pub description: Option<String>,
    pub original_thumbnail_url: Option<String>,
    pub jpeg_thumbnail: Option<Vec<u8>>,
    pub high_quality_thumbnail: Option<proto::message::ImageMessage>,
}
/// Network and media access used while building a preview.
pub trait Source: Send + Sync {
    /// Fetch a page, following redirects.
    fn page<'a>(&'a self, url: &'a str) -> BoxFuture<'a, Result<Page, Error>>;
    /// Fetch an image and reduce it to a JPEG thumbnail of the given width.
    fn thumbnail<'a>(&'a self, url: &'a str, width: u32) -> BoxFuture<'a, Result<Vec<u8>, Error>>;
    /// Upload an image as a link thumbnail; no image message means no thumbnail.
    fn upload<'a>(
        &'a self,
        url: &'a str,
    ) -> BoxFuture<'a, Result<Option<proto::message::ImageMessage>, Error>>;
}
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("link preview request failed: {0}")]
    Fetch(String),
    #[error("link preview image failed: {0}")]
    Image(String),
    /// A high-quality preview was requested for a page without an image.
    #[error("link preview page has no image")]
    MissingImage,
}

// link-preview-js REGEX_VALID_URL without its address exclusions, which
// `public_address` applies to the dotted-quad alternative.
fn url_patterns() -> &'static (Regex, Regex) {
    static PATTERNS: OnceLock<(Regex, Regex)> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        let pattern = |host: &str| {
            Regex::new(&format!(
                r"(?i)^(?:(?:https?|ftp)://)(?:\S+(?::\S*)?@)?(?:{host})(?::[0-9]{{2,5}})?(?:[/?#]\S*)?$"
            ))
            .expect("URL pattern")
        };
        (
            pattern(
                r"(?P<a>[1-9][0-9]?|1[0-9][0-9]|2[01][0-9]|22[0-3])(?P<rest>(?:\.(?:1?[0-9]{1,2}|2[0-4][0-9]|25[0-5])){2}(?:\.(?:[1-9][0-9]?|1[0-9][0-9]|2[0-4][0-9]|25[0-4])))",
            ),
            pattern(
                r"(?:(?:[a-z\x{00a1}-\x{ffff}0-9]-*)*[a-z\x{00a1}-\x{ffff}0-9]+)(?:\.(?:[a-z\x{00a1}-\x{ffff}0-9]-*)*[a-z\x{00a1}-\x{ffff}0-9]+)*(?:\.(?:[a-z\x{00a1}-\x{ffff}]{2,}))\.?",
            ),
        )
    })
}
// Private, loopback and link-local networks are not previewed.
fn public_address(first: &str, rest: &str) -> bool {
    let second: u32 = rest[1..]
        .split('.')
        .next()
        .and_then(|octet| octet.parse().ok())
        .unwrap_or(0);
    !match first {
        "10" | "127" => true,
        "169" => second == 254,
        "192" => second == 168,
        "172" => (16..=31).contains(&second),
        _ => false,
    }
}
fn valid_url(token: &str) -> bool {
    let (address, name) = url_patterns();
    address
        .captures(token)
        .is_some_and(|found| public_address(&found["a"], &found["rest"]))
        || name.is_match(token)
}
/// The first space-separated token that is a previewable URL.
pub fn detect_url(text: &str) -> Option<&str> {
    text.split([' ', '\n']).find(|token| valid_url(token))
}

fn select<'a>(document: &'a Html, selector: &str) -> Vec<scraper::ElementRef<'a>> {
    match Selector::parse(selector) {
        Ok(selector) => document.select(&selector).collect(),
        Err(_) => vec![],
    }
}
// The content of the first matching meta tag, if that tag has one.
fn meta_content(document: &Html, name: &str, attribute: &str) -> Option<String> {
    select(document, &format!("meta[{attribute}='{name}']"))
        .first()?
        .value()
        .attr("content")
        .map(str::to_owned)
}
// JavaScript `a || b || c`: the first non-empty value, else whatever came last.
fn first_truthy(values: impl IntoIterator<Item = Option<String>>) -> Option<String> {
    let mut last = None;
    for value in values {
        last = value;
        if last.as_ref().is_some_and(|value| !value.is_empty()) {
            break;
        }
    }
    last
}
fn resolve(root: &str, reference: &str) -> String {
    url::Url::parse(root)
        .and_then(|root| root.join(reference))
        .map_or_else(|_| reference.to_owned(), String::from)
}
/// Extract preview metadata from a fetched page. Only textual and unknown
/// content types are parsed; media and application responses carry no title.
pub fn page_info(page: &Page) -> Option<PageInfo> {
    if let Some(content_type) = &page.content_type {
        let kind = content_type
            .split(';')
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        let has = |family: &str| kind.contains(family);
        if has("image/") || has("audio/") || has("video/") {
            return None;
        }
        if !has("text/") && has("application/") {
            return None;
        }
    }
    let document = Html::parse_document(&page.body);
    let mut title = first_truthy([
        meta_content(&document, "og:title", "property"),
        meta_content(&document, "og:title", "name"),
    ])
    .unwrap_or_default();
    if title.is_empty() {
        title = select(&document, "head > title")
            .iter()
            .flat_map(|title| title.text())
            .collect();
    }
    let description = first_truthy([
        meta_content(&document, "description", "name"),
        meta_content(&document, "Description", "name"),
        meta_content(&document, "og:description", "property"),
    ]);
    let mut tags = select(&document, "meta[property='og:image']");
    if tags.is_empty() {
        tags = select(&document, "meta[name='og:image']");
    }
    let mut images: Vec<String> = tags
        .iter()
        .filter_map(|tag| tag.value().attr("content"))
        .filter(|source| !source.is_empty())
        .map(|source| resolve(&page.url, source))
        .collect();
    if images.is_empty() {
        let linked = select(&document, "link[rel=image_src]")
            .first()
            .and_then(|link| link.value().attr("href"))
            .filter(|source| !source.is_empty());
        if let Some(source) = linked {
            images.push(resolve(&page.url, source));
        } else {
            let mut seen = vec![];
            for image in select(&document, "img") {
                let Some(source) = image.value().attr("src").filter(|s| !s.is_empty()) else {
                    continue;
                };
                if !seen.contains(&source) {
                    seen.push(source);
                    images.push(resolve(&page.url, source));
                }
            }
        }
    }
    Some(PageInfo {
        title,
        description,
        images,
    })
}
/// Build the preview for the first URL in `text`. No URL, no title or an
/// unsupported page yields no preview; fetch failures are errors, as is a
/// missing image when a high-quality thumbnail is requested. A failed plain
/// thumbnail is only logged.
pub async fn url_info(
    text: &str,
    thumbnail_width: u32,
    high_quality: bool,
    source: &dyn Source,
) -> Result<Option<UrlInfo>, Error> {
    let link = if text.starts_with("https://") || text.starts_with("http://") {
        text.to_owned()
    } else {
        format!("https://{text}")
    };
    let Some(url) = detect_url(&link) else {
        return Ok(None);
    };
    let page = source.page(url).await?;
    let Some(info) = page_info(&page).filter(|info| !info.title.is_empty()) else {
        return Ok(None);
    };
    let image = info.images.into_iter().next();
    let mut preview = UrlInfo {
        canonical_url: page.url,
        matched_text: text.to_owned(),
        title: info.title,
        description: info.description,
        original_thumbnail_url: image.clone(),
        ..Default::default()
    };
    if high_quality {
        let image = image.ok_or(Error::MissingImage)?;
        let uploaded = source.upload(&image).await?;
        preview.jpeg_thumbnail = uploaded
            .as_ref()
            .and_then(|message| message.jpeg_thumbnail.clone());
        preview.high_quality_thumbnail = uploaded;
    } else if let Some(image) = image {
        match source.thumbnail(&image, thumbnail_width).await {
            Ok(thumbnail) => preview.jpeg_thumbnail = Some(thumbnail),
            Err(error) => tracing::debug!(?error, url, "error in generating thumbnail"),
        }
    }
    Ok(Some(preview))
}
