use axum::{
    extract::{Query, State},
    Extension,
};
use isolang::Language;
use serde::{Deserialize, Serialize};
use std::{convert::From, sync::Arc};
use validator::Validate;

use crate::db;

use axum_web::context::ReqContext;
use axum_web::erring::{valid_user, HTTPError, SuccessResponse};
use axum_web::object::PackObject;
use scylla_orm::ColumnsMap;

use super::{
    get_fields, token_from_xid, token_to_xid, AppState, Pagination, QueryId, UpdateStatusInput,
};

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct CreateCollectionInput {
    pub cid: PackObject<xid::Id>,
    pub language: PackObject<Language>,
    #[validate(range(min = 1, max = 10000))]
    pub version: i16,
    pub genre: Option<Vec<String>>,
    #[validate(length(min = 4, max = 256))]
    pub title: String,
    #[validate(url)]
    pub cover: Option<String>,
    #[validate(length(min = 0, max = 2048))]
    pub summary: Option<String>,
    pub labels: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct CollectionOutput {
    pub uid: PackObject<xid::Id>,
    pub id: PackObject<xid::Id>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cid: Option<PackObject<xid::Id>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<PackObject<Language>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<i8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genre: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cover: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<Vec<String>>,
}

impl CollectionOutput {
    fn from<T>(val: db::Collection, to: &PackObject<T>) -> Self {
        let mut rt = Self {
            uid: to.with(val.uid),
            id: to.with(val.id),
            ..Default::default()
        };

        for v in val._fields {
            match v.as_str() {
                "cid" => rt.cid = Some(to.with(val.cid)),
                "language" => rt.language = Some(to.with(val.language)),
                "version" => rt.version = Some(val.version),
                "status" => rt.status = Some(val.status),
                "updated_at" => rt.updated_at = Some(val.updated_at),
                "genre" => rt.genre = Some(val.genre.to_owned()),
                "title" => rt.title = Some(val.title.to_owned()),
                "cover" => rt.cover = Some(val.cover.to_owned()),
                "summary" => rt.summary = Some(val.summary.to_owned()),
                "labels" => rt.labels = Some(val.labels.to_owned()),
                _ => {}
            }
        }

        rt
    }
}

pub async fn create(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<CreateCollectionInput>,
) -> Result<PackObject<SuccessResponse<CollectionOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let mut doc = db::Collection {
        uid: ctx.user,
        id: xid::new(),
        cid: input.cid.unwrap(),
        language: input.language.unwrap(),
        version: input.version,
        genre: input.genre.unwrap_or_default(),
        title: input.title,
        cover: input.cover.unwrap_or_default(),
        summary: input.summary.unwrap_or_default(),
        labels: input.labels.unwrap_or_default(),
        ..Default::default()
    };
    ctx.set_kvs(vec![
        ("action", "create_collection".into()),
        ("id", doc.id.to_string().into()),
        ("cid", doc.cid.to_string().into()),
        ("language", doc.language.to_name().into()),
        ("version", doc.version.into()),
    ])
    .await;

    let ok = doc.save(&app.scylla).await?;
    ctx.set("created", ok.into()).await;
    Ok(to.with(SuccessResponse::new(CollectionOutput::from(doc, &to))))
}

pub async fn get(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryId>,
) -> Result<PackObject<SuccessResponse<CollectionOutput>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let id = *input.id.to_owned();

    ctx.set_kvs(vec![
        ("action", "get_collection".into()),
        ("id", id.to_string().into()),
    ])
    .await;

    let mut doc = db::Collection::with_pk(ctx.user, id);
    doc.get_one(&app.scylla, get_fields(input.fields.clone()))
        .await?;
    Ok(to.with(SuccessResponse::new(CollectionOutput::from(doc, &to))))
}

pub async fn list(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<Pagination>,
) -> Result<PackObject<SuccessResponse<Vec<CollectionOutput>>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let page_size = input.page_size.unwrap_or(10);
    ctx.set_kvs(vec![
        ("action", "list_collection".into()),
        ("page_size", page_size.into()),
    ])
    .await;

    let fields = input.fields.unwrap_or_default();
    let res = db::Collection::find(
        &app.scylla,
        ctx.user,
        fields,
        page_size,
        token_to_xid(&input.page_token),
        input.status,
    )
    .await?;
    let next_page_token = if res.len() >= page_size as usize {
        to.with_option(token_from_xid(res.last().unwrap().id))
    } else {
        None
    };

    Ok(to.with(SuccessResponse {
        total_size: None,
        next_page_token,
        result: res
            .iter()
            .map(|r| CollectionOutput::from(r.to_owned(), &to))
            .collect(),
    }))
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdateCollectionInput {
    pub id: PackObject<xid::Id>,
    pub updated_at: i64,
    #[validate(range(min = 1, max = 10000))]
    pub version: Option<i16>,
    #[validate(length(min = 4, max = 256))]
    pub title: Option<String>,
    #[validate(url)]
    pub cover: Option<String>,
    #[validate(length(min = 0, max = 2048))]
    pub summary: Option<String>,
    #[validate(length(min = 0, max = 20))]
    pub labels: Option<Vec<String>>,
}

impl UpdateCollectionInput {
    fn into(self) -> anyhow::Result<ColumnsMap> {
        let mut cols = ColumnsMap::new();
        if let Some(version) = self.version {
            cols.set_as("version", &version);
        }
        if let Some(title) = self.title {
            cols.set_as("title", &title);
        }
        if let Some(cover) = self.cover {
            cols.set_as("cover", &cover);
        }
        if let Some(summary) = self.summary {
            cols.set_as("summary", &summary);
        }
        if let Some(labels) = self.labels {
            cols.set_as("labels", &labels);
        }

        if cols.is_empty() {
            return Err(HTTPError::new(400, "No fields to update".to_string()).into());
        }

        Ok(cols)
    }
}

pub async fn update(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<UpdateCollectionInput>,
) -> Result<PackObject<SuccessResponse<CollectionOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let id = *input.id.to_owned();
    let mut doc = db::Collection::with_pk(ctx.user, id);
    let updated_at = input.updated_at;
    let cols = input.into()?;
    ctx.set_kvs(vec![
        ("action", "update_creation".into()),
        ("id", doc.id.to_string().into()),
    ])
    .await;

    let ok = doc.update(&app.scylla, cols, updated_at).await?;
    ctx.set("updated", ok.into()).await;
    doc._fields = vec!["updated_at".to_string()]; // only return `updated_at` field.
    Ok(to.with(SuccessResponse::new(CollectionOutput::from(doc, &to))))
}

pub async fn update_status(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<UpdateStatusInput>,
) -> Result<PackObject<SuccessResponse<CollectionOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = input
        .gid
        .ok_or_else(|| HTTPError::new(400, "Missing required field `gid`".to_string()))?;

    let id = *input.id.to_owned();
    let gid = *gid.to_owned();
    let mut doc = db::Collection::with_pk(gid, id);
    ctx.set_kvs(vec![
        ("action", "update_status".into()),
        ("id", doc.id.to_string().into()),
    ])
    .await;

    let ok = doc
        .update_status(&app.scylla, input.status, input.updated_at)
        .await?;
    ctx.set("updated", ok.into()).await;

    doc._fields = vec!["updated_at".to_string(), "status".to_string()];
    Ok(to.with(SuccessResponse::new(CollectionOutput::from(doc, &to))))
}

pub async fn delete(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryId>,
) -> Result<PackObject<SuccessResponse<bool>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let id = *input.id.to_owned();

    ctx.set_kvs(vec![
        ("action", "delete_collection".into()),
        ("id", id.to_string().into()),
    ])
    .await;

    let mut doc = db::Collection::with_pk(ctx.user, id);
    let res = doc.delete(&app.scylla).await?;
    Ok(to.with(SuccessResponse::new(res)))
}
