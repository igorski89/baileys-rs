//! Business profile, catalog, collection and order stanzas with their result parsers.
use crate::{
    binary::{BinaryNode, NodeContent},
    jid::S_WHATSAPP_NET,
};

fn bytes(tag: &str, value: impl AsRef<[u8]>) -> BinaryNode {
    BinaryNode::new(tag).with_content(NodeContent::Bytes(value.as_ref().to_vec()))
}
fn text(tag: &str, value: &str) -> BinaryNode {
    BinaryNode::new(tag).with_content(NodeContent::Text(value.into()))
}
fn iq(kind: &str, xmlns: &str, content: BinaryNode) -> BinaryNode {
    BinaryNode::new("iq")
        .attr("to", S_WHATSAPP_NET)
        .attr("type", kind)
        .attr("xmlns", xmlns)
        .with_content(NodeContent::Children(vec![content]))
}
fn profile(content: Vec<BinaryNode>) -> BinaryNode {
    iq(
        "set",
        "w:biz",
        BinaryNode::new("business_profile")
            .attr("v", "3")
            .attr("mutation_type", "delta")
            .with_content(NodeContent::Children(content)),
    )
}
// Unary plus on a child string; a missing child is NaN.
fn number(node: Option<&BinaryNode>, tag: &str) -> f64 {
    node.and_then(|node| node.child_string(tag))
        .map_or(f64::NAN, |value| crate::privacy::number(&value))
}
fn string(node: Option<&BinaryNode>, tag: &str) -> Option<String> {
    node?.child_string(tag)
}

/// Opening hours of one weekday.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HoursDay {
    /// `sun` to `sat`.
    pub day: String,
    pub mode: HoursMode,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HoursMode {
    SpecificHours {
        open_time_in_minutes: String,
        close_time_in_minutes: String,
    },
    Open24h,
    AppointmentOnly,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hours {
    pub timezone: String,
    pub days: Vec<HoursDay>,
}
/// Business profile fields to change; unset fields are left as they are.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProfileUpdate {
    pub address: Option<String>,
    pub websites: Option<Vec<String>>,
    pub email: Option<String>,
    pub description: Option<String>,
    pub hours: Option<Hours>,
}
pub fn profile_update_query(update: &ProfileUpdate) -> BinaryNode {
    let mut content = vec![];
    for (tag, value) in [
        ("address", &update.address),
        ("email", &update.email),
        ("description", &update.description),
    ] {
        if let Some(value) = value {
            content.push(text(tag, value));
        }
    }
    for website in update.websites.iter().flatten() {
        content.push(text("website", website));
    }
    if let Some(hours) = &update.hours {
        let days = hours.days.iter().map(|day| {
            let node = BinaryNode::new("business_hours_config").attr("day_of_week", &day.day);
            match &day.mode {
                HoursMode::SpecificHours {
                    open_time_in_minutes,
                    close_time_in_minutes,
                } => node
                    .attr("mode", "specific_hours")
                    .attr("open_time", open_time_in_minutes)
                    .attr("close_time", close_time_in_minutes),
                HoursMode::Open24h => node.attr("mode", "open_24h"),
                HoursMode::AppointmentOnly => node.attr("mode", "appointment_only"),
            }
        });
        content.push(
            BinaryNode::new("business_hours")
                .attr("timezone", &hours.timezone)
                .with_content(NodeContent::Children(days.collect())),
        );
    }
    profile(content)
}
// String(number) for the integral identifiers and timestamps the upload returns.
fn js_number(value: Option<f64>) -> String {
    match value {
        None => "undefined".into(),
        Some(value) if value.is_nan() => "NaN".into(),
        Some(value) if value.fract() == 0.0 && value.abs() < 1e21 => format!("{value:.0}"),
        Some(value) => value.to_string(),
    }
}
/// Point the business profile at an uploaded cover photo.
pub fn cover_photo_update_query(
    fbid: Option<f64>,
    meta_hmac: Option<&str>,
    ts: Option<f64>,
) -> BinaryNode {
    let mut photo = BinaryNode::new("cover_photo")
        .attr("id", js_number(fbid))
        .attr("op", "update");
    if let Some(token) = meta_hmac {
        photo = photo.attr("token", token);
    }
    profile(vec![photo.attr("ts", js_number(ts))])
}
pub fn cover_photo_remove_query(id: &str) -> BinaryNode {
    profile(vec![
        BinaryNode::new("cover_photo")
            .attr("op", "delete")
            .attr("id", id),
    ])
}
/// One catalog page of a normalized business JID; the page size defaults to ten.
pub fn catalog_query(jid: &str, limit: Option<u64>, cursor: Option<&str>) -> BinaryNode {
    let limit = limit.filter(|limit| *limit != 0).unwrap_or(10);
    let mut content = vec![
        bytes("limit", limit.to_string()),
        bytes("width", "100"),
        bytes("height", "100"),
    ];
    if let Some(cursor) = cursor.filter(|cursor| !cursor.is_empty()) {
        content.push(text("after", cursor));
    }
    iq(
        "get",
        "w:biz:catalog",
        BinaryNode::new("product_catalog")
            .attr("jid", jid)
            .attr("allow_shop_source", "true")
            .with_content(NodeContent::Children(content)),
    )
}
pub fn collections_query(jid: &str, limit: u64) -> BinaryNode {
    let limit = limit.to_string();
    iq(
        "get",
        "w:biz:catalog",
        BinaryNode::new("collections")
            .attr("biz_jid", jid)
            .with_content(NodeContent::Children(vec![
                bytes("collection_limit", &limit),
                bytes("item_limit", &limit),
                bytes("width", "100"),
                bytes("height", "100"),
            ])),
    )
    .attr("smax_id", "35")
}
pub fn order_details_query(order_id: &str, token_base64: &str) -> BinaryNode {
    iq(
        "get",
        "fb:thrift_iq",
        BinaryNode::new("order")
            .attr("op", "get")
            .attr("id", order_id)
            .with_content(NodeContent::Children(vec![
                BinaryNode::new("image_dimensions").with_content(NodeContent::Children(vec![
                    bytes("width", "100"),
                    bytes("height", "100"),
                ])),
                bytes("token", token_base64),
            ])),
    )
    .attr("smax_id", "5")
}

/// Editable product fields; unset fields are not sent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProductFields {
    pub name: Option<String>,
    pub retailer_id: Option<String>,
    pub description: Option<String>,
    /// Price in thousandths of the currency unit.
    pub price: Option<u64>,
    pub currency: Option<String>,
    pub is_hidden: Option<bool>,
}
/// Country of origin of a new product; an update does not send one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin<'a> {
    Unchanged,
    Exempt,
    Country(&'a str),
}
/// The `product` node of a catalog edit, with images already uploaded.
pub fn product_node(
    product_id: Option<&str>,
    product: &ProductFields,
    image_urls: &[String],
    origin: Origin<'_>,
) -> BinaryNode {
    let mut content = vec![];
    for (tag, value) in [
        ("id", product_id),
        ("name", product.name.as_deref()),
        ("description", product.description.as_deref()),
        ("retailer_id", product.retailer_id.as_deref()),
    ] {
        if let Some(value) = value {
            content.push(bytes(tag, value));
        }
    }
    if !image_urls.is_empty() {
        let images = image_urls.iter().map(|url| {
            BinaryNode::new("image").with_content(NodeContent::Children(vec![bytes("url", url)]))
        });
        content
            .push(BinaryNode::new("media").with_content(NodeContent::Children(images.collect())));
    }
    if let Some(price) = product.price {
        content.push(bytes("price", price.to_string()));
    }
    if let Some(currency) = &product.currency {
        content.push(bytes("currency", currency));
    }
    let mut node = BinaryNode::new("product");
    match origin {
        Origin::Unchanged => {}
        Origin::Exempt => node = node.attr("compliance_category", "COUNTRY_ORIGIN_EXEMPT"),
        Origin::Country(code) => content.push(BinaryNode::new("compliance_info").with_content(
            NodeContent::Children(vec![bytes("country_code_origin", code)]),
        )),
    }
    if let Some(hidden) = product.is_hidden {
        node = node.attr("is_hidden", hidden.to_string());
    }
    node.with_content(NodeContent::Children(content))
}
fn catalog_edit(tag: &str, product: BinaryNode) -> BinaryNode {
    iq(
        "set",
        "w:biz:catalog",
        BinaryNode::new(tag)
            .attr("v", "1")
            .with_content(NodeContent::Children(vec![
                product,
                text("width", "100"),
                text("height", "100"),
            ])),
    )
}
pub fn product_update_query(product: BinaryNode) -> BinaryNode {
    catalog_edit("product_catalog_edit", product)
}
pub fn product_create_query(product: BinaryNode) -> BinaryNode {
    catalog_edit("product_catalog_add", product)
}
pub fn product_delete_query(product_ids: &[String]) -> BinaryNode {
    let products = product_ids.iter().map(|id| {
        BinaryNode::new("product").with_content(NodeContent::Children(vec![bytes("id", id)]))
    });
    iq(
        "set",
        "w:biz:catalog",
        BinaryNode::new("product_catalog_delete")
            .attr("v", "1")
            .with_content(NodeContent::Children(products.collect())),
    )
}
/// Number of deleted products; NaN when the server count is not numeric.
pub fn parse_deleted_count(result: &BinaryNode) -> f64 {
    result
        .child("product_catalog_delete")
        .and_then(|node| node.attrs.get("deleted_count"))
        .filter(|count| !count.is_empty())
        .map_or(0.0, |count| crate::privacy::number(count))
}

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct ImageUrls {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original: Option<String>,
}
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct ReviewStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub whatsapp: Option<String>,
}
/// A catalog product. The price is NaN when the server sends none.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Product {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub image_urls: ImageUrls,
    pub review_status: ReviewStatus,
    pub availability: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retailer_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub price: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
    pub is_hidden: bool,
}
pub fn parse_product(node: &BinaryNode) -> Product {
    let image = node.child("media").and_then(|media| media.child("image"));
    Product {
        id: node.child_string("id"),
        image_urls: ImageUrls {
            requested: string(image, "request_image_url"),
            original: string(image, "original_image_url"),
        },
        review_status: ReviewStatus {
            whatsapp: string(node.child("status_info"), "status"),
        },
        availability: "in stock",
        name: node.child_string("name"),
        retailer_id: node.child_string("retailer_id"),
        url: node.child_string("url"),
        description: node.child_string("description"),
        price: number(Some(node), "price"),
        currency: node.child_string("currency"),
        is_hidden: node.attrs.get("is_hidden").is_some_and(|v| v == "true"),
    }
}
/// The product returned inside a catalog add or edit result.
pub fn parse_edited_product(result: &BinaryNode, tag: &str) -> Option<Product> {
    result.child(tag)?.child("product").map(parse_product)
}
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Catalog {
    pub products: Vec<Product>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_cursor: Option<String>,
}
fn products(node: Option<&BinaryNode>) -> Vec<Product> {
    node.into_iter()
        .flat_map(|node| node.children("product"))
        .map(parse_product)
        .collect()
}
pub fn parse_catalog(result: &BinaryNode) -> Catalog {
    let catalog = result.child("product_catalog");
    Catalog {
        products: products(catalog),
        next_page_cursor: string(catalog.and_then(|catalog| catalog.child("paging")), "after"),
    }
}
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectionStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    pub can_appeal: bool,
}
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Collection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub products: Vec<Product>,
    pub status: CollectionStatus,
}
pub fn parse_collections(result: &BinaryNode) -> Vec<Collection> {
    result
        .child("collections")
        .into_iter()
        .flat_map(|node| node.children("collection"))
        .map(|collection| {
            let status = collection.child("status_info");
            Collection {
                id: collection.child_string("id"),
                name: collection.child_string("name"),
                products: products(Some(collection)),
                status: CollectionStatus {
                    status: string(status, "status"),
                    can_appeal: string(status, "can_appeal").is_some_and(|v| v == "true"),
                },
            }
        })
        .collect()
}
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct OrderPrice {
    pub total: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
}
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderProduct {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_url: Option<String>,
    pub price: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
    pub quantity: f64,
}
/// An order with its total. Numbers are NaN where the server sends none.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct OrderDetails {
    pub price: OrderPrice,
    pub products: Vec<OrderProduct>,
}
pub fn parse_order_details(result: &BinaryNode) -> OrderDetails {
    let order = result.child("order");
    let price = order.and_then(|order| order.child("price"));
    OrderDetails {
        price: OrderPrice {
            total: number(price, "total"),
            currency: string(price, "currency"),
        },
        products: order
            .into_iter()
            .flat_map(|order| order.children("product"))
            .map(|product| OrderProduct {
                id: product.child_string("id"),
                name: product.child_string("name"),
                image_url: string(product.child("image"), "url"),
                price: number(Some(product), "price"),
                currency: product.child_string("currency"),
                quantity: number(Some(product), "quantity"),
            })
            .collect(),
    }
}
