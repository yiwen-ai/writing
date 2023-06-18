use axum::{
    extract::{Query, State},
    Extension,
};
use isolang::Language;
use serde::{Deserialize, Serialize};
use std::{convert::From, str::FromStr, sync::Arc};
use validator::Validate;

use crate::db;
use axum_web::context::ReqContext;
use axum_web::erring::{HTTPError, SuccessResponse};
use axum_web::object::TypedObject;

use super::{validate_xid, AppState, QueryIdLanguageVersion, UpdatePublicationStatusInput};

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct CreatePublicationInput {
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub gid: String,
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub id: String,
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub cid: String,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct PublicationOutput {
    pub id: TypedObject<xid::Id>,
    pub language: TypedObject<Language>,
    pub version: i16,
    pub rating: i8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<i8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creator: Option<TypedObject<xid::Id>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
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
    pub authors: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<TypedObject<Vec<u8>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_languages: Option<Vec<TypedObject<Language>>>,
}

impl PublicationOutput {
    fn from<T>(val: db::Publication, to: &TypedObject<T>) -> Self {
        let mut rt = Self {
            id: to.with(val.id),
            language: to.with(val.language),
            version: val.version,
            rating: val._rating,
            ..Default::default()
        };

        for v in val._fields {
            match v.as_str() {
                "status" => rt.status = Some(val.status),
                "creator" => rt.creator = Some(to.with(val.creator)),
                "created_at" => rt.created_at = Some(val.created_at),
                "updated_at" => rt.updated_at = Some(val.updated_at),
                "model" => rt.model = Some(val.model.to_owned()),
                "original_url" => rt.original_url = Some(val.original_url.to_owned()),
                "genre" => rt.genre = Some(val.genre.to_owned()),
                "title" => rt.title = Some(val.title.to_owned()),
                "description" => rt.description = Some(val.description.to_owned()),
                "cover" => rt.cover = Some(val.cover.to_owned()),
                "keywords" => rt.keywords = Some(val.keywords.to_owned()),
                "authors" => rt.authors = Some(val.authors.to_owned()),
                "summary" => rt.summary = Some(val.summary.to_owned()),
                "content" => rt.content = Some(to.with(val.content.to_owned())),
                "license" => rt.license = Some(val.license.to_owned()),
                _ => {}
            }
        }

        if !val._active_languages.is_empty() {
            rt.active_languages = Some(to.with_set(val._active_languages));
        }

        rt
    }
}

pub async fn create(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: TypedObject<CreatePublicationInput>,
) -> Result<TypedObject<SuccessResponse<PublicationOutput>>, HTTPError> {
    let (to, input) = to.unwrap_type();
    input.validate()?;

    ctx.set_kvs(vec![
        ("action", "create_publication".into()),
        ("gid", input.gid.clone().into()),
        ("id", input.id.clone().into()),
        ("cid", input.cid.clone().into()),
    ])
    .await;

    let cid = xid::Id::from_str(&input.cid).unwrap();
    let mut index = db::CreationIndex::with_pk(cid);

    if index.get_one(&app.scylla).await.is_err() {
        return Err(HTTPError::new(
            404,
            format!("Creation not exists, cid({})", input.cid),
        ));
    }
    if index.rating == i8::MAX {
        return Err(HTTPError::new(
            451,
            format!("Creation is banned, cid({})", input.cid),
        ));
    }

    let mut draft = db::PublicationDraft {
        gid: xid::Id::from_str(&input.gid).unwrap(),
        id: xid::Id::from_str(&input.id).unwrap(),
        cid,
        ..Default::default()
    };
    if draft.get_one(&app.scylla, Vec::new()).await.is_err() {
        return Err(HTTPError::new(
            404,
            format!(
                "Publication draft not exists, gid({}), id({}), cid({})",
                draft.gid, draft.id, draft.cid
            ),
        ));
    }
    if draft.status != 1 {
        return Err(HTTPError::new(
            400,
            format!(
                "Publication draft status not match, gid({}), id({}), cid({}), expected 1, got {}",
                input.gid, input.id, input.cid, draft.status
            ),
        ));
    }

    let mut publication: db::Publication =
        db::Publication::save_from(&app.scylla, index.gid, draft).await?;

    publication._rating = index.rating;
    Ok(to.with(SuccessResponse::new(PublicationOutput::from(
        publication,
        &to,
    ))))
}

pub async fn get(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: TypedObject<()>,
    input: Query<QueryIdLanguageVersion>,
) -> Result<TypedObject<SuccessResponse<PublicationOutput>>, HTTPError> {
    input.validate()?;

    let id = xid::Id::from_str(&input.id).unwrap(); // validated
    let language = Language::from_str(&input.language).unwrap(); // validated

    ctx.set_kvs(vec![
        ("action", "get_publication".into()),
        ("id", input.id.clone().into()),
        ("language", input.language.clone().into()),
        ("version", input.version.into()),
    ])
    .await;

    let mut index = db::CreationIndex::with_pk(id);

    if index.get_one(&app.scylla).await.is_err() {
        return Err(HTTPError::new(
            404,
            format!("Creation not exists, cid({})", input.id),
        ));
    }
    if index.rating > ctx.rating {
        return Err(HTTPError::new(
            451,
            format!(
                "Publication rating not match, id({}), gid({}), expected(*), got({})",
                index.id, index.gid, ctx.rating
            ),
        ));
    }

    let mut doc = db::Publication::with_pk(id, language, input.version);
    let fields = input
        .fields
        .clone()
        .unwrap_or_default()
        .split(',')
        .map(|s| s.to_string())
        .collect();
    doc.get_one(&app.scylla, fields).await?;
    doc._rating = index.rating;

    let mut creation = db::Creation::with_pk(index.gid, index.id);
    let _ = creation
        .get_one(
            &app.scylla,
            vec!["language".to_string(), "active_languages".to_string()],
        )
        .await; // maybe deleted.
    if !creation.active_languages.is_empty() {
        doc._active_languages = creation.active_languages;
    }

    Ok(to.with(SuccessResponse::new(PublicationOutput::from(doc, &to))))
}

pub async fn update_status(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: TypedObject<UpdatePublicationStatusInput>,
) -> Result<TypedObject<SuccessResponse<PublicationOutput>>, HTTPError> {
    let (to, input) = to.unwrap_type();
    input.validate()?;

    let id = xid::Id::from_str(&input.id).unwrap(); // validated
    let language = Language::from_str(&input.language).unwrap(); // validated
    ctx.set_kvs(vec![
        ("action", "update_publication_status".into()),
        ("id", input.id.clone().into()),
        ("language", language.to_name().into()),
        ("version", input.version.into()),
    ])
    .await;

    let mut doc = db::Publication::with_pk(id, language, input.version);

    let ok = doc
        .update_status(&app.scylla, input.status, input.updated_at)
        .await?;
    if !ok {
        return Err(HTTPError::new(
            409,
            "Publication draft update failed".to_string(),
        ));
    }

    doc._fields = vec!["updated_at".to_string(), "status".to_string()];
    Ok(to.with(SuccessResponse::new(PublicationOutput::from(doc, &to))))
}

pub async fn delete(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: TypedObject<()>,
    input: Query<QueryIdLanguageVersion>,
) -> Result<TypedObject<SuccessResponse<bool>>, HTTPError> {
    input.validate()?;

    let id = xid::Id::from_str(&input.id).unwrap(); // validated
    let language = Language::from_str(&input.language).unwrap(); // validated

    ctx.set_kvs(vec![
        ("action", "delete_publication".into()),
        ("id", input.id.clone().into()),
        ("language", input.language.clone().into()),
        ("version", input.version.into()),
    ])
    .await;

    let mut doc = db::Publication::with_pk(id, language, input.version);
    let res = doc.delete(&app.scylla).await?;
    Ok(to.with(SuccessResponse::new(res)))
}
