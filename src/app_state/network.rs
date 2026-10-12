use super::{DecodedState, Error, KeyProvider};
use crate::{
    auth::LtHashState,
    binary::{BinaryNode, NodeContent},
    media, proto,
};
use futures_util::{TryStreamExt, future::BoxFuture, stream::FuturesUnordered};
use indexmap::IndexMap;
use prost::Message as _;
use std::sync::Arc;
use tokio::io::AsyncReadExt;

type FetchBlob = dyn Fn(proto::ExternalBlobReference) -> BoxFuture<'static, Result<Vec<u8>, Error>>
    + Send
    + Sync;

/// External encrypted app-state blobs, with an injectable transport.
#[derive(Clone)]
pub struct BlobDownloader(Arc<FetchBlob>);
impl std::fmt::Debug for BlobDownloader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobDownloader").finish_non_exhaustive()
    }
}
impl BlobDownloader {
    pub fn new<F, Fut>(fetch: F) -> Self
    where
        F: Fn(proto::ExternalBlobReference) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Vec<u8>, Error>> + Send + 'static,
    {
        Self(Arc::new(move |reference| Box::pin(fetch(reference))))
    }
    pub fn from_http(client: media::HttpClient, options: media::DownloadOptions) -> Self {
        Self::new(move |reference| {
            let client = client.clone();
            let options = options.clone();
            async move {
                let url = media::download_url(
                    reference.direct_path.as_deref(),
                    None,
                    options.host.as_deref(),
                )?;
                let keys = media::Keys::derive(
                    reference
                        .media_key
                        .as_deref()
                        .ok_or(media::Error::MissingMediaKey)?,
                    media::MediaType::AppState,
                )?;
                // Drain concurrently: the media worker writes through a bounded bridge.
                let (output, mut input) = tokio::io::duplex(64 * 1024);
                let download =
                    client.download_to(&url, keys, options.range, options.headers, output);
                let collect = async move {
                    let mut bytes = Vec::new();
                    input
                        .read_to_end(&mut bytes)
                        .await
                        .map_err(media::Error::from)?;
                    Ok::<_, media::Error>(bytes)
                };
                let (_, bytes) = tokio::try_join!(download, collect)?;
                Ok(bytes)
            }
        })
    }
    pub async fn download(
        &self,
        reference: proto::ExternalBlobReference,
    ) -> Result<Vec<u8>, Error> {
        (self.0)(reference).await
    }
    pub async fn download_patch(
        &self,
        reference: proto::ExternalBlobReference,
    ) -> Result<proto::SyncdMutations, Error> {
        Ok(proto::SyncdMutations::decode(
            self.download(reference).await?.as_slice(),
        )?)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CollectionResponse {
    pub patches: Vec<proto::SyncdPatch>,
    pub has_more_patches: bool,
    pub snapshot: Option<proto::SyncdSnapshot>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionRequest {
    pub name: String,
    pub version: u64,
    pub force_snapshot: bool,
}
pub fn collection_query(collections: &[CollectionRequest]) -> BinaryNode {
    let children = collections
        .iter()
        .map(|c| {
            BinaryNode::new("collection")
                .attr("name", &c.name)
                .attr("version", c.version.to_string())
                .attr(
                    "return_snapshot",
                    (c.force_snapshot || c.version == 0).to_string(),
                )
        })
        .collect();
    BinaryNode::new("iq")
        .attr("to", "s.whatsapp.net")
        .attr("xmlns", "w:sync:app:state")
        .attr("type", "set")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("sync").with_content(NodeContent::Children(children)),
        ]))
}
fn bytes(node: &BinaryNode) -> Result<Option<&[u8]>, Error> {
    match &node.content {
        None => Ok(None),
        Some(NodeContent::Bytes(bytes)) => Ok(Some(bytes)),
        _ => Err(Error::Input("collection content must be binary")),
    }
}
/// Collections fetch their snapshots concurrently, matching the source helper.
pub async fn extract_collections(
    result: &BinaryNode,
    downloader: &BlobDownloader,
) -> Result<IndexMap<String, CollectionResponse>, Error> {
    let Some(sync) = result.child("sync") else {
        return Ok(IndexMap::new());
    };
    let mut pending: FuturesUnordered<_> = sync
        .children("collection")
        .map(|collection| async move {
            let name = collection
                .attrs
                .get("name")
                .ok_or(Error::Input("collection name"))?
                .clone();
            let snapshot = match collection
                .child("snapshot")
                .map(bytes)
                .transpose()?
                .flatten()
            {
                Some(content) => {
                    let reference = proto::ExternalBlobReference::decode(content)?;
                    Some(proto::SyncdSnapshot::decode(
                        downloader.download(reference).await?.as_slice(),
                    )?)
                }
                None => None,
            };
            let mut patches = Vec::new();
            for node in collection
                .child("patches")
                .unwrap_or(collection)
                .children("patch")
            {
                if let Some(content) = bytes(node)? {
                    let mut patch = proto::SyncdPatch::decode(content)?;
                    if patch.version.is_none() {
                        let version = collection
                            .attrs
                            .get("version")
                            .and_then(|v| v.parse::<u64>().ok())
                            .and_then(|v| v.checked_add(1))
                            .ok_or(Error::Input("collection version"))?;
                        patch.version = Some(proto::SyncdVersion {
                            version: Some(version),
                        });
                    }
                    patches.push(patch);
                }
            }
            Ok::<_, Error>((
                name,
                CollectionResponse {
                    patches,
                    snapshot,
                    has_more_patches: collection
                        .attrs
                        .get("has_more_patches")
                        .is_some_and(|v| v == "true"),
                },
            ))
        })
        .collect();
    let mut collections = IndexMap::new();
    while let Some((name, response)) = pending.try_next().await? {
        collections.insert(name, response);
    }
    Ok(collections)
}

/// Download each external mutation list immediately before decoding its patch.
/// References remain attached as in upstream; successful mutation buffers are cleared.
pub async fn decode_patches_with_downloads(
    patches: &mut [proto::SyncdPatch],
    name: &str,
    initial: &LtHashState,
    provider: &KeyProvider,
    minimum_version: Option<u64>,
    validate: bool,
    downloader: &BlobDownloader,
) -> Result<DecodedState, Error> {
    super::codec::decode_patches_inner(
        patches,
        name,
        initial,
        provider,
        minimum_version,
        validate,
        Some(downloader),
    )
    .await
}
