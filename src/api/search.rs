use axum::{
    extract::{Query, State},
    Extension,
};
use isolang::Language;
use serde::{Deserialize, Serialize};
use std::{convert::From, sync::Arc};
use validator::Validate;

use crate::db::meili;

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

pub async fn search(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<SearchInput>,
) -> Result<PackObject<SuccessResponse<meili::SearchOutput>>, HTTPError> {
    input.validate()?;

    let gid = input.gid.to_owned().map(|v| v.unwrap());
    let lang = input.language.to_owned().map(|v| v.unwrap());
    let q = input.q.trim();

    ctx.set_kvs(vec![("action", "search".into()), ("q", q.into())])
        .await;

    let res = app
        .meili
        .search(meili::Space::Pub(gid), lang, q, &to)
        .await?;
    Ok(to.with(SuccessResponse::new(res)))
}
