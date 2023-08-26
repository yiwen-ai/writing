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

use super::{get_fields, token_from_xid, token_to_xid, AppState, Pagination, QueryCid, QueryId};

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct CreateCollectionInput {
    pub gid: PackObject<xid::Id>,
    pub cid: PackObject<xid::Id>,
    pub language: PackObject<Language>,
    #[validate(range(min = 1, max = 10000))]
    pub version: i16,
    #[validate(length(min = 4, max = 256))]
    pub title: String,
    #[validate(length(min = 0, max = 5))]
    pub labels: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct CollectionOutput {
    pub uid: PackObject<xid::Id>,
    pub id: PackObject<xid::Id>,
    pub gid: PackObject<xid::Id>,
    pub cid: PackObject<xid::Id>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<PackObject<Language>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<Vec<String>>,
}

impl CollectionOutput {
    fn from<T>(val: db::Collection, to: &PackObject<T>) -> Self {
        let mut rt = Self {
            uid: to.with(val.uid),
            id: to.with(val.id),
            gid: to.with(val.gid),
            cid: to.with(val.cid),
            ..Default::default()
        };

        for v in val._fields {
            match v.as_str() {
                "language" => rt.language = Some(to.with(val.language)),
                "version" => rt.version = Some(val.version),
                "updated_at" => rt.updated_at = Some(val.updated_at),
                "title" => rt.title = Some(val.title.to_owned()),
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

    let cid = input.cid.unwrap();
    let gid = input.gid.unwrap();
    let language = input.language.unwrap();
    let res =
        db::Collection::get_one_by_cid(&app.scylla, ctx.user, cid, gid, language, vec![]).await;

    if let Ok(mut doc) = res {
        ctx.set_kvs(vec![
            ("action", "create_collection".into()),
            ("id", doc.id.to_string().into()),
            ("gid", doc.gid.to_string().into()),
            ("cid", doc.cid.to_string().into()),
            ("language", doc.language.to_name().into()),
            ("version", input.version.into()),
            ("created", false.into()),
        ])
        .await;

        if doc.version >= input.version {
            return Ok(to.with(SuccessResponse::new(CollectionOutput::from(doc, &to))));
        }

        let updated_at = doc.updated_at;
        let cols = UpdateCollectionInput {
            id: to.with(doc.id),
            updated_at,
            version: Some(input.version),
            title: Some(input.title),
            labels: input.labels,
        }
        .into()?;
        ctx.set_kvs(vec![
            ("action", "update_collection".into()),
            ("id", doc.id.to_string().into()),
        ])
        .await;

        let ok = doc.update(&app.scylla, cols, updated_at).await?;
        ctx.set("updated", ok.into()).await;
        return Ok(to.with(SuccessResponse::new(CollectionOutput::from(doc, &to))));
    }

    let mut doc = db::Collection {
        uid: ctx.user,
        id: xid::new(),
        gid: gid,
        cid: cid,
        language: language,
        version: input.version,
        title: input.title,
        labels: input.labels.unwrap_or_default(),
        ..Default::default()
    };

    ctx.set_kvs(vec![
        ("action", "create_collection".into()),
        ("id", doc.id.to_string().into()),
        ("gid", doc.gid.to_string().into()),
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
    let res = db::Collection::list(
        &app.scylla,
        ctx.user,
        fields,
        page_size,
        token_to_xid(&input.page_token),
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

pub async fn get_by_cid(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryCid>,
) -> Result<PackObject<SuccessResponse<Vec<CollectionOutput>>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let cid = *input.cid.to_owned();

    ctx.set_kvs(vec![
        ("action", "get_collection_by_cid".into()),
        ("cid", cid.to_string().into()),
    ])
    .await;

    let res =
        db::Collection::list_by_cid(&app.scylla, ctx.user, cid, get_fields(input.fields.clone()))
            .await?;

    Ok(to.with(SuccessResponse {
        total_size: None,
        next_page_token: None,
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
        ("action", "update_collection".into()),
        ("id", doc.id.to_string().into()),
    ])
    .await;

    let ok = doc.update(&app.scylla, cols, updated_at).await?;
    ctx.set("updated", ok.into()).await;
    doc._fields = vec!["updated_at".to_string()]; // only return `updated_at` field.
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
