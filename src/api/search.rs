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

use super::AppState;

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct SearchInput {
    pub gid: Option<PackObject<xid::Id>>,
    pub language: Option<PackObject<Language>>,
    #[validate(length(min = 0, max = 128))]
    pub q: String,
}

pub async fn search(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<SearchInput>,
) -> Result<PackObject<SuccessResponse<meili::SearchOutput>>, HTTPError> {
    input.validate()?;

    let gid = input.gid.to_owned().map(|v| v.unwrap());
    let lang = input.language.to_owned().map(|v| v.unwrap());
    let q: Vec<&str> = input.q.split_whitespace().into_iter().collect();
    let q = q.join(" ");

    ctx.set_kvs(vec![("action", "search".into()), ("q", q.as_str().into())])
        .await;

    let res = app
        .meili
        .search(meili::Space::Pub(gid), lang, &q, &to)
        .await?;
    Ok(to.with(SuccessResponse::new(res)))
}

pub async fn group_search(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<SearchInput>,
) -> Result<PackObject<SuccessResponse<meili::SearchOutput>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = input
        .gid
        .to_owned()
        .ok_or_else(|| HTTPError::new(400, "Missing required field `gid`".to_string()))?;
    let gid = *gid.to_owned();
    let lang = input.language.to_owned().map(|v| v.unwrap());
    let q = input.q.trim();

    ctx.set_kvs(vec![("action", "group_search".into()), ("q", q.into())])
        .await;

    let res = app
        .meili
        .search(meili::Space::Group(gid), lang, q, &to)
        .await?;
    Ok(to.with(SuccessResponse::new(res)))
}

pub async fn original_search(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<SearchInput>,
) -> Result<PackObject<SuccessResponse<meili::SearchOutput>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = input
        .gid
        .to_owned()
        .ok_or_else(|| HTTPError::new(400, "Missing required field `gid`".to_string()))?;
    let gid = *gid.to_owned();
    let q = input.q.trim();
    if q.is_empty() {
        return Err(HTTPError::new(
            400,
            "Missing required field `q`".to_string(),
        ));
    }

    ctx.set_kvs(vec![("action", "original_search".into()), ("q", q.into())])
        .await;

    let publications = db::Publication::list_published_by_url(
        &app.scylla,
        q.to_string(),
        vec!["title".to_string(), "summary".to_string()],
    )
    .await?;

    let creations = db::Creation::list_by_gid_url(
        &app.scylla,
        gid,
        q.to_string(),
        vec![
            "version".to_string(),
            "language".to_string(),
            "title".to_string(),
            "summary".to_string(),
        ],
    )
    .await?;

    let mut res = meili::SearchOutput {
        hits: Vec::with_capacity(publications.len() + creations.len()),
        languages: Default::default(),
    };
    for doc in publications {
        res.hits.push(meili::DocumentOutput {
            gid: to.with(doc.gid),
            cid: to.with(doc.cid),
            language: to.with(doc.language),
            version: doc.version,
            kind: 1,
            title: doc.title,
            summary: doc.summary,
        });
    }
    for doc in creations {
        res.hits.push(meili::DocumentOutput {
            gid: to.with(doc.gid),
            cid: to.with(doc.id),
            language: to.with(doc.language),
            version: doc.version,
            kind: 0,
            title: doc.title,
            summary: doc.summary,
        });
    }
    Ok(to.with(SuccessResponse::new(res)))
}
