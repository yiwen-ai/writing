use axum::{
    extract::{Query, State},
    Extension,
};
use isolang::Language;
use serde::{Deserialize, Serialize};
use std::{convert::From, sync::Arc};
use validator::Validate;

use crate::db::{self, meili};

use axum_web::context::ReqContext;
use axum_web::erring::{valid_user, HTTPError, SuccessResponse};
use axum_web::object::PackObject;
use scylla_orm::ColumnsMap;

use super::{
    get_fields, token_from_xid, token_to_xid, validate_cbor_content, AppState, GIDPagination,
    QueryGidId, QueryId, SubscriptionInput, SubscriptionOutput, UpdateStatusInput,
    MAX_CREATION_CONTENT_LEN,
};

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct CreateCreationInput {
    pub gid: PackObject<xid::Id>,
    pub language: PackObject<Language>,
    #[validate(range(min = -1, max = 100000))]
    pub price: Option<i64>,
    #[validate(url)]
    pub original_url: Option<String>,
    pub genre: Option<Vec<String>>,
    #[validate(length(min = 1, max = 256))]
    pub title: String,
    #[validate(length(min = 0, max = 2048))]
    pub summary: Option<String>,
    #[validate(url)]
    pub cover: Option<String>,
    #[validate(length(min = 0, max = 5))]
    pub keywords: Option<Vec<String>>,
    #[validate(length(min = 0, max = 5))]
    pub labels: Option<Vec<String>>,
    #[validate(length(min = 0, max = 10))]
    pub authors: Option<Vec<String>>,
    #[validate(custom = "validate_cbor_content")]
    pub content: PackObject<Vec<u8>>,
    #[validate(url)]
    pub license: Option<String>,
    pub parent: Option<PackObject<xid::Id>>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct CreationOutput {
    pub id: PackObject<xid::Id>,
    pub gid: PackObject<xid::Id>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<i8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rating: Option<i8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<PackObject<Language>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creator: Option<PackObject<xid::Id>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genre: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cover: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keywords: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authors: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reviewers: Option<Vec<PackObject<xid::Id>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<PackObject<Vec<u8>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
}

impl CreationOutput {
    fn from<T>(val: db::Creation, to: &PackObject<T>) -> Self {
        let mut rt = Self {
            gid: to.with(val.gid),
            id: to.with(val.id),
            rating: val._rating,
            price: val._price,
            ..Default::default()
        };

        for v in val._fields {
            match v.as_str() {
                "status" => rt.status = Some(val.status),
                "version" => rt.version = Some(val.version),
                "language" => rt.language = Some(to.with(val.language)),
                "creator" => rt.creator = Some(to.with(val.creator)),
                "created_at" => rt.created_at = Some(val.created_at),
                "updated_at" => rt.updated_at = Some(val.updated_at),
                "original_url" => rt.original_url = Some(val.original_url.to_owned()),
                "genre" => rt.genre = Some(val.genre.to_owned()),
                "title" => rt.title = Some(val.title.to_owned()),
                "cover" => rt.cover = Some(val.cover.to_owned()),
                "keywords" => rt.keywords = Some(val.keywords.to_owned()),
                "labels" => rt.labels = Some(val.labels.to_owned()),
                "authors" => rt.authors = Some(val.authors.to_owned()),
                "reviewers" => rt.reviewers = Some(to.with_vec(val.reviewers.to_owned())),
                "summary" => rt.summary = Some(val.summary.to_owned()),
                "content" => rt.content = Some(to.with(val._content.to_owned())),
                "license" => rt.license = Some(val.license.to_owned()),
                _ => {}
            }
        }

        rt
    }
}

pub async fn create(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<CreateCreationInput>,
) -> Result<PackObject<SuccessResponse<CreationOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    if input.content.len() > MAX_CREATION_CONTENT_LEN {
        return Err(HTTPError::new(
            400,
            format!(
                "content length is too long, expected <= {}, got {}",
                MAX_CREATION_CONTENT_LEN,
                input.content.len()
            ),
        ));
    }

    valid_user(ctx.user)?;

    let gid = *input.gid.to_owned();
    let language = input.language.unwrap();
    ctx.set_kvs(vec![
        ("action", "create_creation".into()),
        ("gid", gid.to_string().into()),
        ("language", language.to_639_3().into()),
    ])
    .await;

    let mut price = input.price.unwrap_or(0);
    let parent = if let Some(parent) = input.parent {
        let mut doc = db::Collection::with_pk(*parent);
        doc.get_one(
            &app.scylla,
            vec![
                "gid".to_string(),
                "status".to_string(),
                "creation_price".to_string(),
            ],
            None,
        )
        .await?;
        if doc.gid != gid {
            return Err(HTTPError::new(
                403,
                format!("Collection {} is not belong to group {}", doc.id, gid),
            ));
        }
        if doc.status < 0 {
            return Err(HTTPError::new(
                400,
                format!("Collection {} is archived", doc.id),
            ));
        }

        let count = db::CollectionChildren::count_children(&app.scylla, doc.id).await?;
        if count >= db::MAX_COLLECTION_CHILDREN {
            return Err(HTTPError::new(
                400,
                format!(
                    "Parent collection can only have {} children",
                    db::MAX_COLLECTION_CHILDREN
                ),
            ));
        }
        if price == 0 {
            price = doc.creation_price;
        }
        Some(doc)
    } else {
        None
    };

    let mut doc = db::Creation {
        gid,
        id: xid::new(),
        language,
        creator: ctx.user,
        original_url: input.original_url.unwrap_or_default(),
        genre: input.genre.unwrap_or_default(),
        title: input.title,
        cover: input.cover.unwrap_or_default(),
        keywords: input.keywords.unwrap_or_default(),
        labels: input.labels.unwrap_or_default(),
        authors: input.authors.unwrap_or_default(),
        summary: input.summary.unwrap_or_default(),
        license: input.license.unwrap_or_default(),
        ..Default::default()
    };

    let ok = doc
        .save_with(&app.scylla, price, input.content.unwrap())
        .await?;
    ctx.set("created", ok.into()).await;

    let meili_start = ctx.start.elapsed().as_millis() as u64;
    if let Err(err) = app
        .meili
        .add_or_update(meili::Space::Group(doc.gid), vec![doc.to_meili()])
        .await
    {
        log::error!(target: "meilisearch",
            action = "add_or_update",
            space = "group",
            rid = ctx.rid,
            gid = doc.gid.to_string(),
            cid = doc.id.to_string(),
            kind = 0i8,
            elapsed = ctx.start.elapsed().as_millis() as u64 - meili_start;
            "{}", err.to_string(),
        );
    }

    if let Some(parent) = parent {
        let mut child = db::CollectionChildren {
            id: parent.id,
            cid: doc.id,
            kind: 0,
            ord: ctx.unix_ms as f64,
            ..Default::default()
        };
        let _ = child.save(&app.scylla).await;
    }

    Ok(to.with(SuccessResponse::new(CreationOutput::from(doc, &to))))
}

pub async fn get(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryGidId>,
) -> Result<PackObject<SuccessResponse<CreationOutput>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = *input.gid.to_owned();
    let id = *input.id.to_owned();

    ctx.set_kvs(vec![
        ("action", "get_creation".into()),
        ("gid", gid.to_string().into()),
        ("id", id.to_string().into()),
    ])
    .await;

    let mut idoc = db::CreationIndex::with_pk(id);
    idoc.get_one(&app.scylla).await?;
    let mut doc = db::Creation::with_pk(gid, id);
    doc.get_one(&app.scylla, get_fields(input.fields.clone()))
        .await?;
    doc._rating = Some(idoc.rating);
    doc._price = Some(idoc.price);
    Ok(to.with(SuccessResponse::new(CreationOutput::from(doc, &to))))
}

pub async fn list(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<GIDPagination>,
) -> Result<PackObject<SuccessResponse<Vec<CreationOutput>>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = input.gid.unwrap();
    let page_size = input.page_size.unwrap_or(10);
    ctx.set_kvs(vec![
        ("action", "list_creation".into()),
        ("gid", gid.to_string().into()),
    ])
    .await;

    let fields = input.fields.unwrap_or_default();
    let res = db::Creation::list_by_gid(
        &app.scylla,
        gid,
        fields,
        page_size,
        token_to_xid(&input.page_token),
        input.status,
    )
    .await?;

    Ok(to.with(SuccessResponse {
        total_size: None,
        next_page_token: to.with_option(token_from_xid(if res.len() >= page_size as usize {
            Some(res.last().unwrap().id)
        } else {
            None
        })),
        result: res
            .iter()
            .map(|r| CreationOutput::from(r.to_owned(), &to))
            .collect(),
    }))
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdateCreationInput {
    pub id: PackObject<xid::Id>,
    pub gid: PackObject<xid::Id>,
    pub updated_at: i64,
    #[validate(range(min = -1, max = 100000))]
    pub price: Option<i64>,
    #[validate(length(min = 1, max = 256))]
    pub title: Option<String>,
    #[validate(url)]
    pub cover: Option<String>,
    #[validate(length(min = 0, max = 5))]
    pub keywords: Option<Vec<String>>,
    #[validate(length(min = 0, max = 5))]
    pub labels: Option<Vec<String>>,
    #[validate(length(min = 0, max = 10))]
    pub authors: Option<Vec<String>>,
    #[validate(length(min = 0, max = 2048))]
    pub summary: Option<String>,
    #[validate(url)]
    pub license: Option<String>,
}

impl UpdateCreationInput {
    fn into(self) -> anyhow::Result<ColumnsMap> {
        let mut cols = ColumnsMap::new();
        if let Some(title) = self.title {
            cols.set_as("title", &title);
        }
        if let Some(cover) = self.cover {
            cols.set_as("cover", &cover);
        }
        if let Some(keywords) = self.keywords {
            cols.set_as("keywords", &keywords);
        }
        if let Some(labels) = self.labels {
            cols.set_as("labels", &labels);
        }
        if let Some(authors) = self.authors {
            cols.set_as("authors", &authors);
        }
        if let Some(summary) = self.summary {
            cols.set_as("summary", &summary);
        }
        if let Some(license) = self.license {
            cols.set_as("license", &license);
        }

        if cols.is_empty() && self.price.is_none() {
            return Err(HTTPError::new(400, "No fields to update".to_string()).into());
        }

        Ok(cols)
    }
}

pub async fn update(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<UpdateCreationInput>,
) -> Result<PackObject<SuccessResponse<CreationOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let id = *input.id.to_owned();
    let gid = *input.gid.to_owned();

    ctx.set_kvs(vec![
        ("action", "update_creation".into()),
        ("gid", gid.to_string().into()),
        ("id", id.to_string().into()),
    ])
    .await;

    let mut idoc = db::CreationIndex::with_pk(id);
    idoc.get_one(&app.scylla).await?;
    if idoc.gid != gid {
        return Err(HTTPError::new(
            403,
            format!("Creation {} is not belong to group {}", id, gid),
        ));
    }

    if let Some(price) = input.price {
        if idoc.price < 0 {
            return Err(HTTPError::new(
                400,
                format!("Creation {} is free forever", id),
            ));
        }

        idoc.price = price;
        let _ = idoc.update_field(&app.scylla, "price").await?;
    }

    let updated_at = input.updated_at;
    let cols = input.into()?;
    let mut doc = db::Creation::with_pk(gid, id);
    if !cols.is_empty() {
        let update_meili = cols.has("title") || cols.has("summary") || cols.has("keywords");
        let ok = doc.update(&app.scylla, cols, updated_at).await?;
        doc._fields = vec!["updated_at".to_string()]; // only return `updated_at` field.
        ctx.set("updated", ok.into()).await;

        if update_meili {
            let meili_start = ctx.start.elapsed().as_millis() as u64;
            if let Err(err) = app
                .meili
                .add_or_update(meili::Space::Group(doc.gid), vec![doc.to_meili()])
                .await
            {
                log::error!(target: "meilisearch",
                    action = "add_or_update",
                    space = "group",
                    rid = ctx.rid,
                    gid = doc.gid.to_string(),
                    cid = doc.id.to_string(),
                    kind = 0i8,
                    elapsed = ctx.start.elapsed().as_millis() as u64 - meili_start;
                    "{}", err.to_string(),
                );
            }
        }
    }

    Ok(to.with(SuccessResponse::new(CreationOutput::from(doc, &to))))
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdateCreationContentInput {
    pub gid: PackObject<xid::Id>,
    pub id: PackObject<xid::Id>,
    pub language: PackObject<Language>,
    #[validate(custom = "validate_cbor_content")]
    pub content: PackObject<Vec<u8>>,
    pub updated_at: i64,
}

pub async fn update_content(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<UpdateCreationContentInput>,
) -> Result<PackObject<SuccessResponse<CreationOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let id = input.id.unwrap();
    let gid = input.gid.unwrap();
    let language = input.language.unwrap();
    let content = input.content.unwrap();

    let mut doc = db::Creation::with_pk(gid, id);
    ctx.set_kvs(vec![
        ("action", "update_content".into()),
        ("gid", doc.gid.to_string().into()),
        ("id", doc.id.to_string().into()),
    ])
    .await;

    let ok = doc
        .update_content(&app.scylla, language, content, input.updated_at)
        .await?;

    ctx.set("updated", ok.into()).await;
    doc._fields = vec![
        "updated_at".to_string(),
        "language".to_string(),
        "version".to_string(),
    ];
    Ok(to.with(SuccessResponse::new(CreationOutput::from(doc, &to))))
}

pub async fn update_status(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<UpdateStatusInput>,
) -> Result<PackObject<SuccessResponse<CreationOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = input
        .gid
        .ok_or_else(|| HTTPError::new(400, "Missing required field `gid`".to_string()))?;
    let gid = *gid.to_owned();
    let id = *input.id.to_owned();

    let mut doc = db::Creation::with_pk(gid, id);
    ctx.set_kvs(vec![
        ("action", "update_status".into()),
        ("gid", doc.gid.to_string().into()),
        ("id", doc.id.to_string().into()),
    ])
    .await;

    let ok = doc
        .update_status(&app.scylla, input.status, input.updated_at)
        .await?;

    ctx.set("updated", ok.into()).await;
    doc._fields = vec!["updated_at".to_string(), "status".to_string()];
    Ok(to.with(SuccessResponse::new(CreationOutput::from(doc, &to))))
}

pub async fn delete(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryGidId>,
) -> Result<PackObject<SuccessResponse<bool>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = *input.gid.to_owned();
    let id = *input.id.to_owned();

    ctx.set_kvs(vec![
        ("action", "delete_creation".into()),
        ("gid", gid.to_string().into()),
        ("id", id.to_string().into()),
    ])
    .await;

    let mut doc = db::Creation::with_pk(gid, id);
    let res = doc.delete(&app.scylla).await?;

    let meili_start = ctx.start.elapsed().as_millis() as u64;
    if let Err(err) = app
        .meili
        .delete(meili::Space::Group(doc.gid), vec![doc.to_meili().id])
        .await
    {
        log::error!(target: "meilisearch",
            action = "delete",
            space = "group",
            rid = ctx.rid,
            gid = gid.to_string(),
            cid = id.to_string(),
            kind = 0i8,
            elapsed = ctx.start.elapsed().as_millis() as u64 - meili_start;
            "{}", err.to_string(),
        );
    }
    Ok(to.with(SuccessResponse::new(res)))
}

pub async fn get_subscription(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryId>,
) -> Result<PackObject<SuccessResponse<SubscriptionOutput>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let cid = *input.id.to_owned();

    ctx.set_kvs(vec![
        ("action", "get_creation_subscription".into()),
        ("cid", cid.to_string().into()),
    ])
    .await;

    let mut icreation = db::CreationIndex::with_pk(cid);
    icreation.get_one(&app.scylla).await?;
    let mut doc = db::CreationSubscription::with_pk(ctx.user, cid);
    doc.get_one(&app.scylla, vec![]).await?;
    Ok(to.with(SuccessResponse::new(SubscriptionOutput {
        uid: to.with(doc.uid),
        cid: to.with(doc.cid),
        gid: to.with(icreation.gid),
        txn: to.with(doc.txn),
        updated_at: doc.updated_at,
        expire_at: doc.expire_at,
    })))
}

pub async fn update_subscription(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<SubscriptionInput>,
) -> Result<PackObject<SuccessResponse<SubscriptionOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let uid = *input.uid.to_owned();
    let cid = *input.cid.to_owned();
    let txn = *input.txn.to_owned();

    ctx.set_kvs(vec![
        ("action", "update_creation_subscription".into()),
        ("uid", uid.to_string().into()),
        ("cid", cid.to_string().into()),
        ("txn", txn.to_string().into()),
    ])
    .await;

    let mut icreation = db::CreationIndex::with_pk(cid);
    icreation.get_one(&app.scylla).await?;
    if ctx.rating < icreation.rating {
        return Err(HTTPError::new(451, "Collection unavailable".to_string()));
    }
    // ensure published
    let _ = db::PublicationIndex::get_implicit_published(
        &app.scylla,
        cid,
        icreation.gid,
        Language::Und,
    )
    .await?;
    let mut doc = db::CreationSubscription::with_pk(ctx.user, cid);
    match doc.get_one(&app.scylla, vec![]).await {
        Ok(_) => {
            if doc.expire_at >= input.expire_at {
                return Err(HTTPError::new(
                    400,
                    "Subscription expire_at can only be extended".to_string(),
                ));
            }
            if doc.updated_at != input.updated_at {
                return Err(HTTPError::new(
                    409,
                    format!(
                        "Subscription updated_at conflict, expected updated_at {}, got {}",
                        doc.updated_at, input.updated_at
                    ),
                ));
            }
            doc.update(&app.scylla, txn, input.expire_at, input.updated_at)
                .await?;
            ctx.set("updated", true.into()).await;
        }
        Err(_) => {
            doc.txn = txn;
            doc.expire_at = input.expire_at;
            doc.save(&app.scylla).await?;
            ctx.set("created", true.into()).await;
        }
    }

    Ok(to.with(SuccessResponse::new(SubscriptionOutput {
        uid: to.with(doc.uid),
        cid: to.with(doc.cid),
        gid: to.with(icreation.gid),
        txn: to.with(doc.txn),
        updated_at: doc.updated_at,
        expire_at: doc.expire_at,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum_web::object::cbor_from_slice;
    use hex_literal::hex;
    use std::str::FromStr;

    #[test]
    fn create_creation_input_works() {
        let data = hex!("a4636769644c00000000000000004d5bfcb8657469746c656e6669727374206372656174696f6e67636f6e74656e745859a2647479706563646f6367636f6e74656e7481a3647479706569706172616772617068656174747273a16269646631323334353667636f6e74656e7481a264746578746b48656c6c6f20776f726c6464747970656474657874686c616e677561676563656e67");

        let obj: CreateCreationInput = cbor_from_slice(&data).unwrap();
        obj.validate().unwrap();
        assert_eq!(
            obj.gid.unwrap(),
            xid::Id::from_str(db::USER_JARVIS).unwrap()
        );
        assert_eq!(obj.language.unwrap(), Language::Eng);
    }
}
