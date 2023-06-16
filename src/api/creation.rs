use axum::{
    extract::{Query, State},
    Extension,
};
use isolang::Language;
use serde::{Deserialize, Serialize};
use std::{convert::From, str::FromStr, sync::Arc};
use validator::Validate;

use crate::context::ReqContext;
use crate::db;
use crate::erring::{HTTPError, SuccessResponse};
use crate::object::TypedObject;

use scylla_orm::ColumnsMap;

use super::{validate_cbor, validate_language, validate_xid, AppState};

#[derive(Debug, Deserialize, Validate)]
pub struct CreateCreationInput {
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub gid: String,
    #[validate(length(min = 2), custom = "validate_language")]
    pub language: String,
    #[validate(url)]
    pub original_url: Option<String>,
    pub genre: Option<Vec<String>>,
    #[validate(length(min = 3, max = 512))]
    pub title: String,
    #[validate(length(min = 3, max = 1024))]
    pub description: Option<String>,
    #[validate(url)]
    pub cover: Option<String>,
    pub keywords: Option<Vec<String>>,
    pub labels: Option<Vec<String>>,
    pub authors: Option<Vec<String>>,
    #[validate(length(min = 10, max = 2048))]
    pub summary: Option<String>,
    #[validate(length(min = 16, max = 1048576), custom = "validate_cbor")] // 1MB
    #[serde(with = "serde_bytes")]
    pub content: Vec<u8>,
    #[validate(url)]
    pub license: Option<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct CreationOutput {
    pub id: TypedObject<xid::Id>,
    pub gid: TypedObject<xid::Id>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<i8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rating: Option<i8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<TypedObject<Language>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creator: Option<TypedObject<xid::Id>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_languages: Option<Vec<TypedObject<Language>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genre: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cover: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keywords: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authors: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reviewers: Option<Vec<TypedObject<xid::Id>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<TypedObject<Vec<u8>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
}

impl CreationOutput {
    fn from<T>(val: db::Creation, to: &TypedObject<T>) -> Self {
        let mut rt = Self {
            gid: to.with(val.gid),
            id: to.with(val.id),
            ..Default::default()
        };

        for v in val._fields {
            match v.as_str() {
                "status" => rt.status = Some(val.status),
                "rating" => rt.rating = Some(val.rating),
                "version" => rt.version = Some(val.version),
                "language" => rt.language = Some(to.with(val.language)),
                "creator" => rt.creator = Some(to.with(val.creator)),
                "created_at" => rt.created_at = Some(val.created_at),
                "updated_at" => rt.updated_at = Some(val.updated_at),
                "active_languages" => {
                    rt.active_languages = Some(to.with_set(val.active_languages.to_owned()))
                }
                "original_url" => rt.original_url = Some(val.original_url.to_owned()),
                "genre" => rt.genre = Some(val.genre.to_owned()),
                "title" => rt.title = Some(val.title.to_owned()),
                "description" => rt.description = Some(val.description.to_owned()),
                "cover" => rt.cover = Some(val.cover.to_owned()),
                "keywords" => rt.keywords = Some(val.keywords.to_owned()),
                "labels" => rt.labels = Some(val.labels.to_owned()),
                "authors" => rt.authors = Some(val.authors.to_owned()),
                "reviewers" => rt.reviewers = Some(to.with_vec(val.reviewers.to_owned())),
                "summary" => rt.summary = Some(val.summary.to_owned()),
                "content" => rt.content = Some(to.with(val.content.to_owned())),
                "license" => rt.license = Some(val.license.to_owned()),
                _ => {}
            }
        }

        rt
    }
}

pub async fn create_creation(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: TypedObject<CreateCreationInput>,
) -> Result<TypedObject<SuccessResponse<CreationOutput>>, HTTPError> {
    let (to, input) = to.unwrap_type();
    input.validate()?;

    let mut doc = db::Creation {
        gid: xid::Id::from_str(&input.gid).unwrap(),
        id: xid::new(),
        language: Language::from_str(&input.language).unwrap_or_default(),
        creator: ctx.user,
        original_url: input.original_url.unwrap_or_default(),
        genre: input.genre.unwrap_or_default(),
        title: input.title,
        description: input.description.unwrap_or_default(),
        cover: input.cover.unwrap_or_default(),
        keywords: input.keywords.unwrap_or_default(),
        labels: input.labels.unwrap_or_default(),
        authors: input.authors.unwrap_or_default(),
        summary: input.summary.unwrap_or_default(),
        content: input.content,
        license: input.license.unwrap_or_default(),
        ..Default::default()
    };

    doc.save(&app.scylla).await?;
    ctx.set_kvs(vec![
        ("action", "create_creation".into()),
        ("gid", doc.gid.to_string().into()),
        ("id", doc.id.to_string().into()),
    ])
    .await;
    Ok(to.with(SuccessResponse::new(CreationOutput::from(doc, &to))))
}

#[derive(Debug, Deserialize, Validate)]
pub struct QueryId {
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub id: String,
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub gid: Option<String>,
    pub fields: Option<String>,
}

pub async fn get_creation(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: TypedObject<()>,
    input: Query<QueryId>,
) -> Result<TypedObject<SuccessResponse<CreationOutput>>, HTTPError> {
    input.validate()?;

    let id = xid::Id::from_str(&input.id).unwrap(); // validated
    let gid = if let Some(gid) = &input.gid {
        xid::Id::from_str(gid).unwrap()
    } else {
        let mut idx = db::CreationIndex::with_pk(id);
        idx.get_one(&app.scylla).await?;
        idx.gid
    };

    ctx.set_kvs(vec![
        ("action", "get_creation".into()),
        ("gid", input.gid.clone().unwrap_or_default().into()),
        ("id", input.id.clone().into()),
    ])
    .await;

    let mut doc = db::Creation::with_pk(gid, id);
    let fields = input
        .fields
        .clone()
        .unwrap_or_default()
        .split(',')
        .map(|s| s.to_string())
        .collect();
    doc.get_one(&app.scylla, fields).await?;
    Ok(to.with(SuccessResponse::new(CreationOutput::from(doc, &to))))
}

#[derive(Debug, Deserialize, Validate)]
pub struct Pagination {
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub gid: String,
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub page_token: Option<String>,
    #[validate(range(min = 2, max = 1000))]
    pub page_size: Option<u16>,
    pub fields: Option<Vec<String>>,
}

pub async fn list_creation(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: TypedObject<Pagination>,
) -> Result<TypedObject<SuccessResponse<Vec<CreationOutput>>>, HTTPError> {
    let (to, input) = to.unwrap_type();
    input.validate()?;

    // let page_token: xid::Id = input.page_token.unwrap_or_default().into()?;
    let page_size = input.page_size.unwrap_or(10);
    let gid = xid::Id::from_str(&input.gid).unwrap(); // validated
    ctx.set_kvs(vec![
        ("action", "list_creation".into()),
        ("gid", gid.to_string().into()),
    ])
    .await;

    let fields = input.fields.unwrap_or_default();
    let page_token = input.page_token.map(|s| xid::Id::from_str(&s).unwrap());
    let res = db::Creation::find(&app.scylla, gid, fields, page_size, page_token).await?;
    let next_page_token = if res.len() >= page_size as usize {
        Some(res.last().unwrap().id.to_string())
    } else {
        None
    };

    Ok(to.with(SuccessResponse {
        total_size: None,
        next_page_token,
        result: res
            .iter()
            .map(|r| CreationOutput::from(r.to_owned(), &to))
            .collect(),
    }))
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdateCreationInput {
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub id: String,
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub gid: String,
    pub updated_at: i64,
    pub title: Option<String>,
    #[validate(length(min = 3, max = 1024))]
    pub description: Option<String>,
    #[validate(url)]
    pub cover: Option<String>,
    #[validate(length(min = 0, max = 10))]
    pub keywords: Option<Vec<String>>,
    #[validate(length(min = 0, max = 20))]
    pub labels: Option<Vec<String>>,
    #[validate(length(min = 0, max = 100))]
    pub authors: Option<Vec<String>>,
    #[validate(length(min = 10, max = 2048))]
    pub summary: Option<String>,
    #[validate(length(min = 16, max = 1048576), custom = "validate_cbor")] // 1MB
    pub content: Option<Vec<u8>>,
    #[validate(url)]
    pub license: Option<String>,
}

impl UpdateCreationInput {
    fn into(self) -> anyhow::Result<ColumnsMap> {
        let mut cols = ColumnsMap::new();
        if let Some(title) = self.title {
            cols.set_as("title", &title)?;
        }
        if let Some(description) = self.description {
            cols.set_as("description", &description)?;
        }
        if let Some(cover) = self.cover {
            cols.set_as("cover", &cover)?;
        }
        if let Some(keywords) = self.keywords {
            cols.set_as("keywords", &keywords)?;
        }
        if let Some(labels) = self.labels {
            cols.set_as("labels", &labels)?;
        }
        if let Some(authors) = self.authors {
            cols.set_as("authors", &authors)?;
        }
        if let Some(summary) = self.summary {
            cols.set_as("summary", &summary)?;
        }
        if let Some(content) = self.content {
            cols.set_as("content", &content)?;
        }
        if let Some(license) = self.license {
            cols.set_as("license", &license)?;
        }

        if cols.is_empty() {
            return Err(anyhow::Error::new(HTTPError::new(
                400,
                "No fields to update".to_string(),
            )));
        }

        Ok(cols)
    }
}

pub async fn update_creation(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: TypedObject<UpdateCreationInput>,
) -> Result<TypedObject<SuccessResponse<CreationOutput>>, HTTPError> {
    let (to, input) = to.unwrap_type();
    input.validate()?;

    let id = xid::Id::from_str(&input.id).unwrap(); // validated
    let gid = xid::Id::from_str(&input.gid).unwrap(); // validated
    let mut doc = db::Creation::with_pk(gid, id);
    let updated_at = input.updated_at;
    let cols = input.into()?;
    ctx.set_kvs(vec![
        ("action", "update_creation".into()),
        ("gid", doc.gid.to_string().into()),
        ("id", doc.id.to_string().into()),
    ])
    .await;

    let update_content = cols.has("content");
    let ok = doc.update(&app.scylla, cols, updated_at).await?;
    if !ok {
        return Err(HTTPError::new(409, "Creation update failed".to_string()));
    }

    doc._fields = vec!["updated_at".to_string()]; // only return `updated_at` field.
    if update_content {
        doc._fields.push("version".to_string());
    }

    Ok(to.with(SuccessResponse::new(CreationOutput::from(doc, &to))))
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdateStatusInput {
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub id: String,
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub gid: String,
    #[validate(range(min = -1, max = 2))]
    pub status: i8,
    pub updated_at: i64,
}

pub async fn update_status(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: TypedObject<UpdateStatusInput>,
) -> Result<TypedObject<SuccessResponse<CreationOutput>>, HTTPError> {
    let (to, input) = to.unwrap_type();
    input.validate()?;

    let id = xid::Id::from_str(&input.id).unwrap(); // validated
    let gid = xid::Id::from_str(&input.gid).unwrap(); // validated
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
    if !ok {
        return Err(HTTPError::new(409, "Creation update failed".to_string()));
    }

    doc._fields = vec!["updated_at".to_string(), "status".to_string()];
    Ok(to.with(SuccessResponse::new(CreationOutput::from(doc, &to))))
}

#[derive(Debug, Deserialize, Validate)]
pub struct QueryIdVersion {
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub id: String,
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub gid: String,
    #[validate(range(min = 1, max = 10000))]
    pub version: i16,
}

pub async fn delete_creation(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: TypedObject<()>,
    input: Query<QueryIdVersion>,
) -> Result<TypedObject<SuccessResponse<bool>>, HTTPError> {
    input.validate()?;

    let id = xid::Id::from_str(&input.id).unwrap(); // validated
    let gid = xid::Id::from_str(&input.gid).unwrap(); // validated

    ctx.set_kvs(vec![
        ("action", "delete_creation".into()),
        ("gid", input.gid.clone().into()),
        ("id", input.id.clone().into()),
    ])
    .await;

    let mut doc = db::Creation::with_pk(gid, id);
    let res = doc.delete(&app.scylla, input.version).await?;
    Ok(to.with(SuccessResponse::new(res)))
}
