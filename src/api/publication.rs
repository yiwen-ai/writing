use axum::{
    extract::{Query, State},
    Extension,
};
use isolang::Language;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use validator::Validate;

use crate::db;
use axum_web::context::ReqContext;
use axum_web::erring::{HTTPError, SuccessResponse};
use axum_web::object::PackObject;

use super::{AppState, QueryIdLanguageVersion, UpdatePublicationStatusInput};

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct CreatePublicationInput {
    pub gid: PackObject<xid::Id>,
    pub id: PackObject<xid::Id>,
    pub cid: PackObject<xid::Id>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct PublicationOutput {
    pub id: PackObject<xid::Id>,
    pub language: PackObject<Language>,
    pub version: i16,
    pub rating: i8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<i8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creator: Option<PackObject<xid::Id>>,
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
    pub content: Option<PackObject<Vec<u8>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_languages: Option<Vec<PackObject<Language>>>,
}

impl PublicationOutput {
    fn from<T>(val: db::Publication, to: &PackObject<T>) -> Self {
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
    to: PackObject<CreatePublicationInput>,
) -> Result<PackObject<SuccessResponse<PublicationOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    ctx.set_kvs(vec![
        ("action", "create_publication".into()),
        ("gid", input.gid.to_string().into()),
        ("id", input.id.to_string().into()),
        ("cid", input.cid.to_string().into()),
    ])
    .await;

    let cid = *input.cid.to_owned();
    let mut index = db::CreationIndex::with_pk(cid);

    if index.get_one(&app.scylla).await.is_err() {
        return Err(HTTPError::new(
            404,
            format!("Creation not exists, cid({})", input.cid.as_ref()),
        ));
    }
    if index.rating == i8::MAX {
        return Err(HTTPError::new(
            451,
            format!("Creation is banned, cid({})", input.cid.as_ref()),
        ));
    }

    let mut draft = db::PublicationDraft {
        gid: *input.gid.to_owned(),
        id: *input.id.to_owned(),
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
                *input.gid, *input.id, *input.cid, draft.status
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
    to: PackObject<()>,
    input: Query<QueryIdLanguageVersion>,
) -> Result<PackObject<SuccessResponse<PublicationOutput>>, HTTPError> {
    input.validate()?;

    let id = *input.id.to_owned();
    let language = *input.language.to_owned();

    ctx.set_kvs(vec![
        ("action", "get_publication".into()),
        ("id", input.id.to_string().into()),
        ("language", input.language.to_name().into()),
        ("version", input.version.into()),
    ])
    .await;

    let mut index = db::CreationIndex::with_pk(id);

    if index.get_one(&app.scylla).await.is_err() {
        return Err(HTTPError::new(
            404,
            format!("Creation not exists, cid({})", *input.id),
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
    to: PackObject<UpdatePublicationStatusInput>,
) -> Result<PackObject<SuccessResponse<PublicationOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let id = *input.id.to_owned();
    let language = *input.language.to_owned();
    ctx.set_kvs(vec![
        ("action", "update_publication_status".into()),
        ("id", input.id.to_string().into()),
        ("language", language.to_name().into()),
        ("version", input.version.into()),
    ])
    .await;

    let mut doc = db::Publication::with_pk(id, language, input.version);

    let ok = doc
        .update_status(&app.scylla, input.status, input.updated_at)
        .await?;
    ctx.set("updated", ok.into()).await;

    doc._fields = vec!["updated_at".to_string(), "status".to_string()];
    Ok(to.with(SuccessResponse::new(PublicationOutput::from(doc, &to))))
}

pub async fn delete(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryIdLanguageVersion>,
) -> Result<PackObject<SuccessResponse<bool>>, HTTPError> {
    input.validate()?;

    let id = *input.id.to_owned();
    let language = *input.language.to_owned();

    ctx.set_kvs(vec![
        ("action", "delete_publication".into()),
        ("id", input.id.to_string().into()),
        ("language", input.language.to_name().into()),
        ("version", input.version.into()),
    ])
    .await;

    let mut doc = db::Publication::with_pk(id, language, input.version);
    let res = doc.delete(&app.scylla).await?;
    Ok(to.with(SuccessResponse::new(res)))
}

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct BatchGetPublicationsInput {
    #[validate(range(min = -1, max = 2))]
    pub min_status: i8,
    pub fields: Option<Vec<String>>,
    #[validate(length(min = 1, max = 100))]
    pub pks: Vec<(PackObject<xid::Id>, PackObject<Language>, i16)>,
}

pub async fn batch_get(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<BatchGetPublicationsInput>,
) -> Result<PackObject<SuccessResponse<Vec<PublicationOutput>>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    ctx.set_kvs(vec![
        ("action", "batch_get".into()),
        ("min_status", input.min_status.into()),
        ("length", input.pks.len().into()),
    ])
    .await;

    let mut ids: Vec<xid::Id> = Vec::with_capacity(input.pks.len());
    for (id, _, version) in &input.pks {
        if *version < 0 {
            return Err(HTTPError::new(400, format!("Invalid version {}", version)));
        }

        ids.push(*id.to_owned());
    }

    let indexs = db::CreationIndex::batch_get(&app.scylla, ids, ctx.rating).await?;
    let ratings_map: HashMap<xid::Id, i8> = indexs.into_iter().map(|i| (i.id, i.rating)).collect();

    let min_status = input.min_status;
    let mut select_fields = db::Publication::select_fields(input.fields.unwrap_or_default(), true)?;
    let status = "status".to_string();
    if !select_fields.contains(&status) {
        select_fields.push(status);
    }

    let mut res: Vec<db::Publication> = Vec::with_capacity(ratings_map.len());
    for (id, language, version) in &input.pks {
        let id = *id.to_owned();
        if ratings_map.contains_key(&id) {
            let mut item = db::Publication::with_pk(id, *language.to_owned(), *version);
            if item
                .get_one(&app.scylla, select_fields.clone())
                .await
                .is_ok()
                && item.status >= min_status
            {
                item._rating = ratings_map.get(&id).unwrap().to_owned();
                res.push(item);
            }
        }
    }

    Ok(to.with(SuccessResponse::new(
        res.iter()
            .map(|r| PublicationOutput::from(r.to_owned(), &to))
            .collect(),
    )))
}
