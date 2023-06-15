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
    pub status: i8,
    pub rating: i8,
    pub version: i16,
    pub language: String,
    pub creator: Vec<u8>,
    pub created_at: i64,
    pub updated_at: i64,
    pub active_languages: Vec<String>,
    pub original_url: String,
    pub genre: Vec<String>,
    pub title: String,
    pub description: String,
    pub cover: String,
    pub keywords: Vec<String>,
    pub labels: Vec<String>,
    pub authors: Vec<String>,
    pub reviewers: Vec<Vec<u8>>,
    pub summary: String,
    pub content: Vec<u8>,
    pub license: String,
}

impl From<db::Creation> for CreationOutput {
    fn from(val: db::Creation) -> Self {
        Self {
            id: val.id.as_bytes().to_vec(),
            gid: val.gid.as_bytes().to_vec(),
            status: val.status,
            rating: val.rating,
            version: val.version,
            language: val.language.to_name().to_string(),
            creator: val.creator.as_bytes().to_vec(),
            created_at: val.created_at,
            updated_at: val.updated_at,
            active_languages: val
                .active_languages
                .iter()
                .map(|l| l.to_name().to_string())
                .collect(),
            original_url: val.original_url,
            genre: val.genre,
            title: val.title,
            description: val.description,
            cover: val.cover,
            keywords: val.keywords,
            labels: val.labels,
            authors: val.authors,
            reviewers: val
                .reviewers
                .iter()
                .map(|r| r.as_bytes().to_vec())
                .collect(),
            summary: val.summary,
            content: val.content,
            license: val.license,
        }
    }
}

pub async fn create_creation(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    Object(ct, input): Object<CreateCreationInput>,
) -> Result<Object<SuccessResponse<CreationOutput>>, HTTPError> {
    input.validate()?;

    let now = (unix_ms() / 1000) as i64;
    let doc = db::Creation {
        id: xid::new(),
        gid: xid::Id::from_str(&input.gid).unwrap(),
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
        ("id", doc.id.to_string().into()),
    ])
    .await;
    Ok(Object(ct, SuccessResponse { result: doc.into() }))
}

#[derive(Debug, Deserialize, Validate)]
pub struct QueryId {
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub id: String,
}

pub async fn get_creation(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    ct: ObjectType,
    input: Query<QueryId>,
) -> Result<Object<SuccessResponse<CreationOutput>>, HTTPError> {
    input.validate()?;
    let id = xid::Id::from_str(&input.id).unwrap(); // validated
    ctx.set_kvs(vec![
        ("action", "get_creation".into()),
        ("id", id.to_string().into()),
    ])
    .await;

    let mut doc = db::Creation::with_pk(id);
    doc.get_one(&app.scylla, Vec::new()).await?;
    Ok(Object(ct, SuccessResponse { result: doc.into() }))
}

#[derive(Debug, Deserialize, Validate)]
pub struct Pagination {
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub gid: String,
    #[validate(length(equal = 20))]
    pub page_token: Option<String>,
    #[validate(range(min = 5, max = 1000))]
    pub page_size: Option<u16>,
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

    let res = db::Creation::find(&app.scylla, gid, Vec::new(), page_size).await?;
    Ok(Object(
        ct,
        SuccessResponse {
            result: res.iter().map(|r| r.to_owned().into()).collect(),
        },
    ))
}
