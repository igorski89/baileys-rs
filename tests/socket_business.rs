#[path = "support/json_numbers.rs"]
mod json_numbers;
#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::{Contact, SqliteAuthStore},
    binary::{self, BinaryNode},
    business::{Hours, HoursDay, HoursMode, ProductFields, ProfileUpdate},
    crypto::KeyPair,
    media::{self, Input, MediaType, PrepareOptions, UploadFunction, UploadResponse},
    socket::{Config, Error, ProductCreate, ProductUpdate, Socket},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use socket_peer::{Peer, ROOT_PRIVATE};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::TcpListener, time::timeout};
fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}
// Stands in for the media servers: records each upload and answers as the fixture's stub did.
#[derive(Clone, Default)]
struct Upload {
    calls: Arc<Mutex<Vec<Value>>>,
    response: Arc<Mutex<Value>>,
}
impl media::MediaUploader for Upload {
    fn upload_media<'a>(
        &'a self,
        path: std::path::PathBuf,
        kind: MediaType,
        hash: String,
        options: media::UploadOptions,
    ) -> futures_util::future::BoxFuture<'a, Result<UploadResponse, media::Error>> {
        Box::pin(async move {
            use sha2::Digest;
            let digest = sha2::Sha256::digest(std::fs::read(&path).unwrap());
            assert_eq!(STANDARD.encode(digest), hash);
            let kind = match kind {
                MediaType::CatalogImage => "product-catalog-image",
                MediaType::BusinessCover => "biz-cover-photo",
                _ => panic!("unexpected media type"),
            };
            self.calls.lock().unwrap().push(json!({
                "mediaType": kind,
                "hash": hash,
                "timeoutMs": options.timeout.map(|t| t.as_millis() as u64),
            }));
            let response = self.response.lock().unwrap().clone();
            let prefix: String = digest.iter().take(6).map(|b| format!("{b:02x}")).collect();
            Ok(UploadResponse {
                url: Some(format!("https://media.example/{kind}")),
                direct_path: Some(format!("/v/t/{prefix}")),
                meta_hmac: response["meta_hmac"].as_str().map(str::to_owned),
                ts: response["ts"].as_f64(),
                fbid: response["fbid"].as_f64(),
            })
        })
    }
}
fn images(value: &Value) -> Vec<Input> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|image| match image["url"].as_str() {
            Some(url) => Input::Url(url.into()),
            None => Input::bytes(STANDARD.decode(image["bytes"].as_str().unwrap()).unwrap()),
        })
        .collect()
}
fn fields(value: &Value) -> ProductFields {
    let text = |name: &str| value[name].as_str().map(str::to_owned);
    ProductFields {
        name: text("name"),
        retailer_id: text("retailerId"),
        description: text("description"),
        price: value["price"].as_u64(),
        currency: text("currency"),
        is_hidden: value["isHidden"].as_bool(),
    }
}
fn profile(value: &Value) -> ProfileUpdate {
    let text = |name: &str| value[name].as_str().map(str::to_owned);
    ProfileUpdate {
        address: text("address"),
        websites: value["websites"].as_array().map(|sites| {
            sites
                .iter()
                .map(|site| site.as_str().unwrap().to_owned())
                .collect()
        }),
        email: text("email"),
        description: text("description"),
        hours: value.get("hours").map(|hours| Hours {
            timezone: hours["timezone"].as_str().unwrap().into(),
            days: hours["days"]
                .as_array()
                .unwrap()
                .iter()
                .map(|day| HoursDay {
                    day: day["day"].as_str().unwrap().into(),
                    mode: match day["mode"].as_str().unwrap() {
                        "specific_hours" => HoursMode::SpecificHours {
                            open_time_in_minutes: day["openTimeInMinutes"].as_str().unwrap().into(),
                            close_time_in_minutes: day["closeTimeInMinutes"]
                                .as_str()
                                .unwrap()
                                .into(),
                        },
                        "open_24h" => HoursMode::Open24h,
                        _ => HoursMode::AppointmentOnly,
                    },
                })
                .collect(),
        }),
    }
}
async fn execute(socket: &Socket, case: &Value, options: &PrepareOptions) -> Result<Value, Error> {
    let args = case["args"].as_array().unwrap();
    let s = |i: usize| args[i].as_str().unwrap();
    // Raw result stanzas are identified by their children.
    let raw = |node: BinaryNode| json!(node.all_children().len());
    Ok(match case["method"].as_str().unwrap() {
        "updateBussinesProfile" => raw(socket.update_business_profile(&profile(&args[0])).await?),
        "updateCoverPhoto" => {
            let photo = Input::bytes(STANDARD.decode(args[0]["bytes"].as_str().unwrap()).unwrap());
            json!(socket.update_cover_photo(&photo, options).await?)
        }
        "removeCoverPhoto" => raw(socket.remove_cover_photo(s(0)).await?),
        "getCatalog" => json!(
            socket
                .get_catalog(
                    args[0]["jid"].as_str(),
                    args[0]["limit"].as_u64(),
                    args[0]["cursor"].as_str(),
                )
                .await?
        ),
        "getCollections" => json!({"collections": socket
            .get_collections(
                args.first().and_then(Value::as_str),
                args.get(1).and_then(Value::as_u64),
            )
            .await?}),
        "getOrderDetails" => json!(socket.get_order_details(s(0), s(1)).await?),
        "productUpdate" => {
            let update = ProductUpdate {
                fields: fields(&args[1]),
                images: images(&args[1]["images"]),
            };
            json!(socket.product_update(s(0), &update, options).await?)
        }
        "productCreate" => {
            let create = ProductCreate {
                fields: fields(&args[0]),
                origin_country_code: args[0]["originCountryCode"].as_str().map(str::to_owned),
                images: images(&args[0]["images"]),
            };
            json!(socket.product_create(&create, options).await?)
        }
        "productDelete" => {
            let ids: Vec<String> = args[0]
                .as_array()
                .unwrap()
                .iter()
                .map(|id| id.as_str().unwrap().to_owned())
                .collect();
            json!({"deleted": socket.product_delete(&ids).await?})
        }
        method => panic!("unknown method {method}"),
    })
}
#[tokio::test]
async fn business_operations_match_original_requests_over_encrypted_transport() {
    timeout(Duration::from_secs(20), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = Config {
            url: format!("ws://{}", listener.local_addr().unwrap()),
            initialize_session: false,
            fire_init_queries: false,
            announce_presence_on_connect: false,
            query_timeout: Duration::from_secs(3),
            noise: baileys_rs::noise::Config {
                certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
                ..Default::default()
            },
            ..Default::default()
        };
        let store = SqliteAuthStore::in_memory().unwrap();
        let mut creds = store.load_credentials().unwrap();
        creds.me = Some(Contact {
            id: "100:7@s.whatsapp.net".into(),
            ..Default::default()
        });
        store.save_credentials(&creds).unwrap();
        let fixture: Value = serde_json::from_str(include_str!("fixtures/business.json")).unwrap();
        let operations = fixture["operations"].as_array().unwrap().clone();
        assert_eq!(operations.len(), 33);
        let served = operations.clone();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            for case in served {
                for (wire, response) in case["requests"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .zip(case["responses"].as_array().unwrap())
                {
                    let mut request = peer.read().await;
                    let id = request.attrs.shift_remove("id").unwrap();
                    let expected = binary::decode(&hex(wire.as_str().unwrap())).unwrap();
                    assert_eq!(request, expected, "{}", case["method"]);
                    let mut result = binary::decode(&hex(response.as_str().unwrap())).unwrap();
                    result.attrs.insert("id".into(), id);
                    peer.send(result).await;
                }
            }
            peer.closed().await;
        });
        let (socket, _events) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let upload = Upload::default();
        let options = PrepareOptions {
            uploader: Some(UploadFunction::new(upload.clone())),
            ..Default::default()
        };
        for case in operations {
            upload.calls.lock().unwrap().clear();
            *upload.response.lock().unwrap() = case["uploaded"].clone();
            let result = execute(&socket, &case, &options).await;
            assert_eq!(result.is_err(), case["error"].as_bool().unwrap(), "{case}");
            let mut uploads = upload.calls.lock().unwrap().clone();
            uploads.sort_by_key(|call| call["hash"].as_str().unwrap().to_owned());
            assert_eq!(json!(uploads), case["uploads"], "{case}");
            let Ok(result) = result else { continue };
            let expected = match case["method"].as_str().unwrap() {
                "updateBussinesProfile" | "removeCoverPhoto" => json!(
                    binary::decode(&hex(case["responses"][0].as_str().unwrap()))
                        .unwrap()
                        .all_children()
                        .len()
                ),
                _ => case["expected"].clone(),
            };
            assert_eq!(
                json_numbers::canonical(&result),
                json_numbers::canonical(&expected),
                "{case}"
            );
        }
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
