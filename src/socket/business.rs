use super::{Error, Socket};
use crate::{
    binary::BinaryNode,
    business::{
        self, Catalog, Collection, OrderDetails, Origin, Product, ProductFields, ProfileUpdate,
    },
    media::{self, Input, MediaType, PrepareOptions},
};
use std::time::Duration;

/// A product to add to the catalog.
#[derive(Debug, Clone)]
pub struct ProductCreate {
    pub fields: ProductFields,
    /// ISO country code of origin; none marks the product as exempt.
    pub origin_country_code: Option<String>,
    pub images: Vec<Input>,
}
/// Changes to a catalog product, with the complete set of its images.
#[derive(Debug, Clone)]
pub struct ProductUpdate {
    pub fields: ProductFields,
    pub images: Vec<Input>,
}

impl Socket {
    pub async fn update_business_profile(
        &self,
        update: &ProfileUpdate,
    ) -> Result<BinaryNode, Error> {
        self.query(business::profile_update_query(update), None)
            .await
    }
    async fn upload_raw(
        &self,
        input: &Input,
        kind: MediaType,
        options: &PrepareOptions,
    ) -> Result<media::UploadResponse, Error> {
        let http = self
            .inner
            .media_http
            .get_or_try_init(|| async { media::HttpClient::new() })
            .await?;
        let uploader: &dyn media::MediaUploader =
            options.uploader.as_ref().map(|u| u.get()).unwrap_or(self);
        Ok(media::upload_raw(input, kind, http, uploader, options).await?)
    }
    /// Upload a cover photo and attach it to the business profile; returns its id.
    pub async fn update_cover_photo(
        &self,
        photo: &Input,
        options: &PrepareOptions,
    ) -> Result<Option<f64>, Error> {
        let upload = self
            .upload_raw(photo, MediaType::BusinessCover, options)
            .await?;
        self.query(
            business::cover_photo_update_query(upload.fbid, upload.meta_hmac.as_deref(), upload.ts),
            None,
        )
        .await?;
        Ok(upload.fbid)
    }
    pub async fn remove_cover_photo(&self, id: &str) -> Result<BinaryNode, Error> {
        self.query(business::cover_photo_remove_query(id), None)
            .await
    }
    // The given business, or the account itself, without its device.
    async fn business_jid(&self, jid: Option<&str>) -> Result<String, Error> {
        let jid = match jid.filter(|jid| !jid.is_empty()) {
            Some(jid) => jid.to_owned(),
            None => super::load_credentials(self.inner.store.clone())
                .await?
                .me
                .clone()
                .map(|me| me.id)
                .unwrap_or_default(),
        };
        Ok(crate::jid::normalized_user(&jid))
    }
    /// One page of a catalog; the account's own when no JID is given.
    pub async fn get_catalog(
        &self,
        jid: Option<&str>,
        limit: Option<u64>,
        cursor: Option<&str>,
    ) -> Result<Catalog, Error> {
        let jid = self.business_jid(jid).await?;
        let result = self
            .query(business::catalog_query(&jid, limit, cursor), None)
            .await?;
        Ok(business::parse_catalog(&result))
    }
    /// Catalog collections; upstream's default limit is 51.
    pub async fn get_collections(
        &self,
        jid: Option<&str>,
        limit: Option<u64>,
    ) -> Result<Vec<Collection>, Error> {
        let jid = self.business_jid(jid).await?;
        let result = self
            .query(business::collections_query(&jid, limit.unwrap_or(51)), None)
            .await?;
        Ok(business::parse_collections(&result))
    }
    pub async fn get_order_details(
        &self,
        order_id: &str,
        token_base64: &str,
    ) -> Result<OrderDetails, Error> {
        let result = self
            .query(business::order_details_query(order_id, token_base64), None)
            .await?;
        Ok(business::parse_order_details(&result))
    }
    // Images hosted by WhatsApp are kept; all others are uploaded concurrently.
    async fn product_image_urls(
        &self,
        images: &[Input],
        options: &PrepareOptions,
    ) -> Result<Vec<String>, Error> {
        let mut options = options.clone();
        options.upload.timeout = Some(Duration::from_secs(30));
        let options = &options;
        futures_util::future::try_join_all(images.iter().map(|image| async move {
            if let Input::Url(url) = image
                && url.contains(".whatsapp.net")
            {
                return Ok(url.clone());
            }
            let upload = self
                .upload_raw(image, MediaType::CatalogImage, options)
                .await?;
            Ok(media::direct_path_url(
                upload.direct_path.as_deref().unwrap_or("undefined"),
                None,
            ))
        }))
        .await
    }
    pub async fn product_update(
        &self,
        product_id: &str,
        update: &ProductUpdate,
        options: &PrepareOptions,
    ) -> Result<Product, Error> {
        let urls = self.product_image_urls(&update.images, options).await?;
        let node =
            business::product_node(Some(product_id), &update.fields, &urls, Origin::Unchanged);
        let result = self
            .query(business::product_update_query(node), None)
            .await?;
        business::parse_edited_product(&result, "product_catalog_edit").ok_or(Error::MissingProduct)
    }
    /// Add a product; it is visible unless marked hidden.
    pub async fn product_create(
        &self,
        create: &ProductCreate,
        options: &PrepareOptions,
    ) -> Result<Product, Error> {
        let fields = ProductFields {
            is_hidden: Some(create.fields.is_hidden.unwrap_or(false)),
            ..create.fields.clone()
        };
        let urls = self.product_image_urls(&create.images, options).await?;
        let origin = match &create.origin_country_code {
            Some(code) => Origin::Country(code),
            None => Origin::Exempt,
        };
        let result = self
            .query(
                business::product_create_query(business::product_node(
                    None, &fields, &urls, origin,
                )),
                None,
            )
            .await?;
        business::parse_edited_product(&result, "product_catalog_add").ok_or(Error::MissingProduct)
    }
    /// Delete products; returns the server's count of deleted products.
    pub async fn product_delete(&self, product_ids: &[String]) -> Result<f64, Error> {
        let result = self
            .query(business::product_delete_query(product_ids), None)
            .await?;
        Ok(business::parse_deleted_count(&result))
    }
}
