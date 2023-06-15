use axum::{
    extract::{Query, State},
    Extension,
};
use isolang::Language;
use serde::{Deserialize, Serialize};
use std::{convert::From, str::FromStr, sync::Arc};
use validator::Validate;

use crate::context::{unix_ms, ReqContext};
use crate::db;
use crate::erring::{HTTPError, SuccessResponse};
use crate::object::{Object, ObjectType};

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
    pub content: Vec<u8>,
    #[validate(url)]
    pub license: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CreationOutput {
    pub id: Vec<u8>,
    pub gid: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<i8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rating: Option<i8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creator: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_languages: Option<Vec<String>>,
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
    pub reviewers: Option<Vec<Vec<u8>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
}

impl From<db::Creation> for CreationOutput {
    fn from(val: db::Creation) -> Self {
        let mut rt = Self {
            gid: val.gid.as_bytes().to_vec(),
            id: val.id.as_bytes().to_vec(),
            ..Default::default()
        };

        for v in val._fields {
            match v.as_str() {
                "status" => rt.status = Some(val.status),
                "rating" => rt.rating = Some(val.rating),
                "version" => rt.version = Some(val.version),
                "language" => rt.language = Some(val.language.to_name().to_string()),
                "creator" => rt.creator = Some(val.creator.as_bytes().to_vec()),
                "created_at" => rt.created_at = Some(val.created_at),
                "updated_at" => rt.updated_at = Some(val.updated_at),
                "active_languages" => {
                    rt.active_languages = Some(
                        val.active_languages
                            .iter()
                            .map(|l| l.to_name().to_string())
                            .collect(),
                    )
                }
                "original_url" => rt.original_url = Some(val.original_url.to_owned()),
                "genre" => rt.genre = Some(val.genre.to_owned()),
                "title" => rt.title = Some(val.title.to_owned()),
                "description" => rt.description = Some(val.description.to_owned()),
                "cover" => rt.cover = Some(val.cover.to_owned()),
                "keywords" => rt.keywords = Some(val.keywords.to_owned()),
                "labels" => rt.labels = Some(val.labels.to_owned()),
                "authors" => rt.authors = Some(val.authors.to_owned()),
                "reviewers" => {
                    rt.reviewers = Some(
                        val.reviewers
                            .iter()
                            .map(|r| r.as_bytes().to_vec())
                            .collect(),
                    )
                }
                "summary" => rt.summary = Some(val.summary.to_owned()),
                "content" => rt.content = Some(val.content.to_owned()),
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
    Object(ct, input): Object<CreateCreationInput>,
) -> Result<Object<SuccessResponse<CreationOutput>>, HTTPError> {
    input.validate()?;

    let now = (unix_ms() / 1000) as i64;
    let mut doc = db::Creation {
        gid: xid::Id::from_str(&input.gid).unwrap(),
        id: xid::new(),
        version: 1,
        language: Language::from_str(&input.language).unwrap_or_default(),
        creator: ctx.user,
        created_at: now,
        updated_at: now,
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
    Ok(Object(ct, SuccessResponse::new(doc.into())))
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
    ct: ObjectType,
    input: Query<QueryId>,
) -> Result<Object<SuccessResponse<CreationOutput>>, HTTPError> {
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
    Ok(Object(ct, SuccessResponse::new(doc.into())))
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
    Object(ct, input): Object<Pagination>,
) -> Result<Object<SuccessResponse<Vec<CreationOutput>>>, HTTPError> {
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

    Ok(Object(
        ct,
        SuccessResponse {
            total_size: None,
            next_page_token,
            result: res.iter().map(|r| r.to_owned().into()).collect(),
        },
    ))
}
