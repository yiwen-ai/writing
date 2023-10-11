use axum::{
    extract::{Query, State},
    Extension,
};
use isolang::Language;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use validator::Validate;

use axum_web::context::ReqContext;
use axum_web::erring::{valid_user, HTTPError, SuccessResponse};
use axum_web::object::PackObject;
use scylla_orm::ColumnsMap;

use crate::api::{
    get_fields, segment_content, token_from_xid, token_to_xid, validate_cbor_content, AppState,
    GIDPagination, Pagination, QueryCid, QueryGidCid, SubscriptionInputOutput,
};
use crate::{db, db::meili};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct PublicationOutput {
    pub gid: PackObject<xid::Id>,
    pub cid: PackObject<xid::Id>,
    pub language: PackObject<Language>,
    pub version: i16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rating: Option<i8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<i64>,
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
    pub from_language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genre: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
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
    pub content_length: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial_content: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription: Option<SubscriptionInputOutput>,
}

impl PublicationOutput {
    fn from<T>(val: db::Publication, to: &PackObject<T>) -> Self {
        let mut rt = Self {
            gid: to.with(val.gid),
            cid: to.with(val.cid),
            language: to.with(val.language),
            version: val.version,
            rating: val._rating,
            price: val._price,
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
                "from_language" => {
                    rt.from_language = Some(val.from_language.to_639_3().to_string())
                }
                "genre" => rt.genre = Some(val.genre.to_owned()),
                "title" => rt.title = Some(val.title.to_owned()),
                "cover" => rt.cover = Some(val.cover.to_owned()),
                "keywords" => rt.keywords = Some(val.keywords.to_owned()),
                "authors" => rt.authors = Some(val.authors.to_owned()),
                "summary" => rt.summary = Some(val.summary.to_owned()),
                "content" => {
                    rt.content_length = Some(val._length);
                    if !val._content.is_empty() {
                        rt.content_length = Some(val._content.len() as i32);
                        rt.content = Some(to.with(val._content.to_owned()));
                    }
                }
                "license" => rt.license = Some(val.license.to_owned()),
                _ => {}
            }
        }

        rt
    }

    fn need_pay(&self, now_ms: i64) -> bool {
        if self.price.unwrap_or(0) <= 0 {
            return false;
        }
        match self.subscription {
            Some(ref sub) => sub.expire_at < now_ms,
            None => true,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct CreatePublicationInput {
    pub gid: PackObject<xid::Id>,
    pub cid: PackObject<xid::Id>,
    pub language: PackObject<isolang::Language>,
    #[validate(range(min = 1, max = 10000))]
    pub version: i16,
    pub draft: Option<PublicationDraftInput>,
}

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct PublicationDraftInput {
    pub gid: PackObject<xid::Id>,
    pub language: PackObject<isolang::Language>,
    #[validate(length(min = 2, max = 16))]
    pub model: String,
    #[validate(length(min = 4, max = 256))]
    pub title: String,
    #[validate(url)]
    pub cover: String,
    #[validate(length(min = 0, max = 5))]
    pub keywords: Vec<String>,
    #[validate(length(min = 4, max = 2048))]
    pub summary: String,
    #[validate(custom = "validate_cbor_content")]
    pub content: PackObject<Vec<u8>>,
}

pub async fn create(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<CreatePublicationInput>,
) -> Result<PackObject<SuccessResponse<PublicationOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    ctx.set_kvs(vec![
        ("action", "create_publication".into()),
        ("gid", input.gid.to_string().into()),
        ("cid", input.cid.to_string().into()),
        ("language", input.language.to_639_3().into()),
        ("version", input.version.into()),
    ])
    .await;

    let gid = input.gid.unwrap();
    let cid = input.cid.unwrap();
    let language = input.language.unwrap();

    let mut index = db::CreationIndex::with_pk(cid);
    if index.get_one(&app.scylla).await.is_err() {
        return Err(HTTPError::new(404, "Creation not exists".to_string()));
    }
    if index.rating == i8::MAX {
        return Err(HTTPError::new(451, "Creation is banned".to_string()));
    }

    let mut doc = if input.draft.is_none() {
        db::Publication::create_from_creation(&app.scylla, gid, cid, ctx.user).await?
    } else {
        let draft = input.draft.unwrap();
        let content = draft.content.unwrap();
        let user_gid = draft.gid.unwrap();
        if index.rating > ctx.rating && gid != user_gid {
            return Err(HTTPError::new(451, "Can not view publication".to_string()));
        }

        ctx.set_kvs(vec![
            ("draft.gid", user_gid.to_string().into()),
            ("draft.language", draft.language.to_639_3().into()),
            ("draft.model", draft.model.as_str().into()),
        ])
        .await;

        db::Publication::create_from_publication(
            &app.scylla,
            db::Publication::with_pk(gid, cid, language, input.version),
            db::Publication {
                gid: user_gid,
                cid,
                language: draft.language.unwrap(),
                version: input.version,
                model: draft.model,
                title: draft.title,
                cover: draft.cover,
                keywords: draft.keywords,
                summary: draft.summary,
                ..Default::default()
            },
            content,
        )
        .await?
    };

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
            cid = doc.cid.to_string(),
            kind = 1i8,
            elapsed = ctx.start.elapsed().as_millis() as u64 - meili_start;
            "{}", err.to_string(),
        );
    }

    doc._rating = Some(index.rating);
    doc._price = Some(index.price);
    let output = PublicationOutput::from(doc, &to);
    Ok(to.with(SuccessResponse::new(output)))
}

#[derive(Debug, Deserialize, Validate)]
pub struct QueryPublicationInputput {
    pub gid: PackObject<xid::Id>,
    pub cid: PackObject<xid::Id>,
    pub language: PackObject<isolang::Language>,
    #[validate(range(min = 1, max = 10000))]
    pub version: i16,
    pub fields: Option<String>,
    pub check_subscription: Option<bool>,
}

pub async fn get(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryPublicationInputput>,
) -> Result<PackObject<SuccessResponse<PublicationOutput>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = *input.gid.to_owned();
    let cid = *input.cid.to_owned();
    let language = *input.language.to_owned();

    ctx.set_kvs(vec![
        ("action", "get_publication".into()),
        ("gid", gid.to_string().into()),
        ("cid", cid.to_string().into()),
        ("language", language.to_639_3().into()),
        ("version", input.version.into()),
    ])
    .await;

    let mut index = db::CreationIndex::with_pk(cid);
    if index.get_one(&app.scylla).await.is_err() {
        return Err(HTTPError::new(404, "Creation not exists".to_string()));
    }
    if gid != index.gid && ctx.rating < index.rating {
        return Err(HTTPError::new(451, "Can not view publication".to_string()));
    }

    let mut doc = db::Publication::with_pk(gid, cid, language, input.version);
    doc.get_one(&app.scylla, get_fields(input.fields.clone()))
        .await?;

    doc._rating = Some(index.rating);
    doc._price = Some(index.price);
    let mut output = PublicationOutput::from(doc, &to);
    if input.check_subscription.unwrap_or(false) {
        output.subscription =
            try_get_subscription(&app.scylla, &index, &to, ctx.user, ctx.unix_ms as i64).await;
        if output.need_pay(ctx.unix_ms as i64) {
            let (ok, content) = segment_content(output.content, 0.6);
            output.content = content;
            output.partial_content = Some(ok);
        }
    }
    Ok(to.with(SuccessResponse::new(output)))
}

#[derive(Debug, Deserialize, Validate)]
pub struct ImplicitQueryPublicationInputput {
    pub cid: PackObject<xid::Id>,
    pub gid: Option<PackObject<xid::Id>>,
    pub language: Option<PackObject<isolang::Language>>,
    pub fields: Option<String>,
    pub check_subscription: Option<bool>,
}

pub async fn implicit_get(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<ImplicitQueryPublicationInputput>,
) -> Result<PackObject<SuccessResponse<PublicationOutput>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let cid = *input.cid.to_owned();
    let gid = input.gid.to_owned().unwrap_or_default().unwrap();
    let mut language = input.language.to_owned().unwrap_or_default().unwrap();
    if language == Language::Und {
        language = ctx.language.unwrap_or_default()
    }

    ctx.set_kvs(vec![
        ("action", "implicit_get_publication".into()),
        ("gid", gid.to_string().into()),
        ("cid", cid.to_string().into()),
        ("language", language.to_639_3().into()),
    ])
    .await;

    let mut index = db::CreationIndex::with_pk(cid);
    if index.get_one(&app.scylla).await.is_err() {
        return Err(HTTPError::new(404, "Creation not exists".to_string()));
    }
    if gid != index.gid && ctx.rating < index.rating {
        return Err(HTTPError::new(451, "Can not view publication".to_string()));
    }

    let mut doc = db::Publication::get_implicit_published(&app.scylla, gid, cid, language).await?;
    doc.get_one(&app.scylla, get_fields(input.fields.clone()))
        .await?;

    doc._rating = Some(index.rating);
    doc._price = Some(index.price);
    let mut output = PublicationOutput::from(doc, &to);
    if input.check_subscription.unwrap_or(false) {
        output.subscription =
            try_get_subscription(&app.scylla, &index, &to, ctx.user, ctx.unix_ms as i64).await;
        if output.need_pay(ctx.unix_ms as i64) {
            let (ok, content) = segment_content(output.content, 0.6);
            output.content = content;
            output.partial_content = Some(ok);
        }
    }

    Ok(to.with(SuccessResponse::new(output)))
}

pub async fn implicit_get_beta(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<ImplicitQueryPublicationInputput>,
) -> Result<PackObject<SuccessResponse<PublicationOutput>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let cid = *input.cid.to_owned();
    let gid = input.gid.to_owned().unwrap_or_default().unwrap();
    let mut language = input.language.to_owned().unwrap_or_default().unwrap();
    if language == Language::Und {
        language = ctx.language.unwrap_or_default()
    }

    ctx.set_kvs(vec![
        ("action", "implicit_get_publication".into()),
        ("gid", gid.to_string().into()),
        ("cid", cid.to_string().into()),
        ("language", language.to_639_3().into()),
    ])
    .await;

    let mut index = db::CreationIndex::with_pk(cid);
    if index.get_one(&app.scylla).await.is_err() {
        return Err(HTTPError::new(404, "Creation not exists".to_string()));
    }
    if gid != index.gid && ctx.rating < index.rating {
        return Err(HTTPError::new(451, "Can not view publication".to_string()));
    }

    let idoc =
        db::PublicationIndex::get_implicit_published(&app.scylla, cid, gid, language).await?;
    let mut doc: db::Publication = idoc.into();
    doc.get_one(&app.scylla, get_fields(input.fields.clone()))
        .await?;

    doc._rating = Some(index.rating);
    doc._price = Some(index.price);
    let mut output = PublicationOutput::from(doc, &to);
    if input.check_subscription.unwrap_or(false) {
        output.subscription =
            try_get_subscription(&app.scylla, &index, &to, ctx.user, ctx.unix_ms as i64).await;
        if output.need_pay(ctx.unix_ms as i64) {
            let (ok, content) = segment_content(output.content, 0.6);
            output.content = content;
            output.partial_content = Some(ok);
        }
    }
    Ok(to.with(SuccessResponse::new(output)))
}

pub async fn list(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<GIDPagination>,
) -> Result<PackObject<SuccessResponse<Vec<PublicationOutput>>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = input.gid.unwrap();
    let page_size = input.page_size.unwrap_or(10);
    ctx.set_kvs(vec![
        ("action", "list_publication".into()),
        ("gid", gid.to_string().into()),
        ("page_size", page_size.into()),
    ])
    .await;

    let fields = input.fields.unwrap_or_default();
    let res = db::Publication::list_by_gid(
        &app.scylla,
        gid,
        fields,
        page_size,
        token_to_xid(&input.page_token),
        input.status,
        ctx.language,
    )
    .await?;

    Ok(to.with(SuccessResponse {
        total_size: None,
        next_page_token: to.with_option(token_from_xid(if res.len() >= page_size as usize {
            Some(res.last().unwrap().cid)
        } else {
            None
        })),
        result: res
            .iter()
            .map(|r| PublicationOutput::from(r.to_owned(), &to))
            .collect(),
    }))
}

#[derive(Debug, Deserialize, Validate)]
pub struct GidsPagination {
    pub gids: Vec<PackObject<xid::Id>>,
    pub page_token: Option<PackObject<Vec<u8>>>,
    #[validate(range(min = 2, max = 1000))]
    pub page_size: Option<u16>,
    pub fields: Option<Vec<String>>,
}

pub async fn list_by_gids(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<GidsPagination>,
) -> Result<PackObject<SuccessResponse<Vec<PublicationOutput>>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    ctx.set_kvs(vec![
        ("action", "list_publications_by_gids".into()),
        ("gids", input.gids.len().into()),
    ])
    .await;

    let fields = input.fields.unwrap_or_default();
    let (res, next_page_token) = db::Publication::list_by_gids(
        &app.scylla,
        input.gids.into_iter().map(|v| v.unwrap()).collect(),
        fields,
        token_to_xid(&input.page_token),
        ctx.language,
    )
    .await?;

    Ok(to.with(SuccessResponse {
        total_size: None,
        next_page_token: to.with_option(token_from_xid(next_page_token)),
        result: res
            .iter()
            .map(|r| PublicationOutput::from(r.to_owned(), &to))
            .collect(),
    }))
}

pub async fn list_by_gids_beta(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<GidsPagination>,
) -> Result<PackObject<SuccessResponse<Vec<PublicationOutput>>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    ctx.set_kvs(vec![
        ("action", "list_publications_by_gids".into()),
        ("gids", input.gids.len().into()),
    ])
    .await;

    let fields = input.fields.unwrap_or_default();
    let (res, next_page_token) = db::PublicationIndex::list_by_gids(
        &app.scylla,
        input.gids.into_iter().map(|v| v.unwrap()).collect(),
        token_to_xid(&input.page_token),
        ctx.language,
    )
    .await?;

    let docs = db::Publication::batch_get(&app.scylla, res, fields).await?;
    Ok(to.with(SuccessResponse {
        total_size: None,
        next_page_token: to.with_option(token_from_xid(next_page_token)),
        result: docs
            .iter()
            .map(|r| PublicationOutput::from(r.to_owned(), &to))
            .collect(),
    }))
}

pub async fn list_latest(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<Pagination>,
) -> Result<PackObject<SuccessResponse<Vec<PublicationOutput>>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    ctx.set_kvs(vec![("action", "list_latest_publications".into())])
        .await;

    let fields = input.fields.unwrap_or_default();
    let (res, next_page_token) = db::PublicationIndex::list_latest(
        &app.scylla,
        token_to_xid(&input.page_token),
        ctx.language,
    )
    .await?;

    let docs = db::Publication::batch_get(&app.scylla, res, fields).await?;
    Ok(to.with(SuccessResponse {
        total_size: None,
        next_page_token: to.with_option(token_from_xid(next_page_token)),
        result: docs
            .iter()
            .map(|r| PublicationOutput::from(r.to_owned(), &to))
            .collect(),
    }))
}

pub async fn get_publish_list(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryGidCid>,
) -> Result<PackObject<SuccessResponse<Vec<PublicationOutput>>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = *input.gid.to_owned();
    let cid = *input.cid.to_owned();
    let status = input.status.unwrap_or(2);
    ctx.set_kvs(vec![
        ("action", "get_publish_list".into()),
        ("gid", gid.to_string().into()),
        ("cid", cid.to_string().into()),
        ("status", status.into()),
    ])
    .await;

    let mut index = db::CreationIndex::with_pk(cid);
    if index.get_one(&app.scylla).await.is_err() {
        return Err(HTTPError::new(404, "Creation not exists".to_string()));
    }
    if gid != index.gid && ctx.rating < index.rating {
        return Err(HTTPError::new(451, "Can not view publication".to_string()));
    }

    let docs = db::Publication::list_published_by_cid(&app.scylla, gid, cid, status).await?;

    ctx.set("total_size", docs.len().into()).await;
    Ok(to.with(SuccessResponse::new(
        docs.iter()
            .map(|r| PublicationOutput::from(r.to_owned(), &to))
            .collect(),
    )))
}

pub async fn get_publish_list_beta(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryGidCid>,
) -> Result<PackObject<SuccessResponse<Vec<PublicationOutput>>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = *input.gid.to_owned();
    let cid = *input.cid.to_owned();
    let status = input.status.unwrap_or(2);
    ctx.set_kvs(vec![
        ("action", "get_publish_list".into()),
        ("gid", gid.to_string().into()),
        ("cid", cid.to_string().into()),
        ("status", status.into()),
    ])
    .await;

    let mut index = db::CreationIndex::with_pk(cid);
    if index.get_one(&app.scylla).await.is_err() {
        return Err(HTTPError::new(404, "Creation not exists".to_string()));
    }
    if gid != index.gid && ctx.rating < index.rating {
        return Err(HTTPError::new(451, "Can not view publication".to_string()));
    }

    let published = db::PublicationIndex::list_published_by_cid(&app.scylla, cid).await?;
    let mut docs = db::Publication::batch_get(
        &app.scylla,
        published,
        vec![
            "status".to_string(),
            "updated_at".to_string(),
            "from_language".to_string(),
            "title".to_string(),
        ],
    )
    .await?;
    let res = db::Publication::list_non_publish_by_cid(&app.scylla, gid, cid, status).await?;

    docs.extend_from_slice(&res);
    ctx.set("total_size", docs.len().into()).await;
    Ok(to.with(SuccessResponse::new(
        docs.iter()
            .map(|r| PublicationOutput::from(r.to_owned(), &to))
            .collect(),
    )))
}

pub async fn count_publish(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<GIDPagination>,
) -> Result<PackObject<SuccessResponse<usize>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = input.gid.unwrap();
    ctx.set_kvs(vec![
        ("action", "count_publish".into()),
        ("gid", gid.to_string().into()),
    ])
    .await;

    let res = db::Publication::count_published_by_gid(&app.scylla, gid).await?;
    Ok(to.with(SuccessResponse::new(res)))
}

pub async fn count_publish_beta(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<GIDPagination>,
) -> Result<PackObject<SuccessResponse<usize>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = input.gid.unwrap();
    ctx.set_kvs(vec![
        ("action", "count_publish".into()),
        ("gid", gid.to_string().into()),
    ])
    .await;

    let res = db::PublicationIndex::count_published_by_gid(&app.scylla, gid).await?;
    Ok(to.with(SuccessResponse::new(res)))
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdatePublicationStatusInput {
    pub gid: PackObject<xid::Id>,
    pub cid: PackObject<xid::Id>,
    pub language: PackObject<isolang::Language>,
    #[validate(range(min = 1, max = 10000))]
    pub version: i16,
    pub updated_at: i64,
    #[validate(range(min = -1, max = 2))]
    pub status: i8,
}

pub async fn update_status(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<UpdatePublicationStatusInput>,
) -> Result<PackObject<SuccessResponse<PublicationOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = input.gid.unwrap();
    let cid = input.cid.unwrap();
    let language = input.language.unwrap();

    ctx.set_kvs(vec![
        ("action", "update_publication_status".into()),
        ("gid", gid.to_string().into()),
        ("cid", cid.to_string().into()),
        ("language", language.to_name().into()),
        ("version", input.version.into()),
    ])
    .await;

    let mut doc = db::Publication::with_pk(gid, cid, language, input.version);

    let ok = doc
        .update_status(&app.scylla, input.status, input.updated_at)
        .await?;
    ctx.set("updated", ok.into()).await;

    if input.status == 2 {
        // get full doc for meili
        doc.get_one(&app.scylla, vec![]).await?;
        let meili_start = ctx.start.elapsed().as_millis() as u64;
        if let Err(err) = app
            .meili
            .add_or_update(meili::Space::Pub(Some(gid)), vec![doc.to_meili()])
            .await
        {
            log::error!(target: "meilisearch",
                action = "add_or_update",
                space = "pub",
                rid = ctx.rid,
                gid = doc.gid.to_string(),
                cid = doc.cid.to_string(),
                kind = 1i8,
                elapsed = ctx.start.elapsed().as_millis() as u64 - meili_start;
                "{}", err.to_string(),
            );
        }
    }

    doc._fields = vec!["updated_at".to_string(), "status".to_string()];
    Ok(to.with(SuccessResponse::new(PublicationOutput::from(doc, &to))))
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdatePublicationInput {
    pub cid: PackObject<xid::Id>,
    pub gid: PackObject<xid::Id>,
    pub language: PackObject<isolang::Language>,
    #[validate(range(min = 1, max = 10000))]
    pub version: i16,
    pub updated_at: i64,
    #[validate(length(min = 2, max = 16))]
    pub model: Option<String>,
    #[validate(length(min = 4, max = 256))]
    pub title: Option<String>,
    #[validate(url)]
    pub cover: Option<String>,
    #[validate(length(min = 0, max = 5))]
    pub keywords: Option<Vec<String>>,
    #[validate(length(min = 4, max = 2048))]
    pub summary: Option<String>,
}

impl UpdatePublicationInput {
    fn into(self) -> anyhow::Result<ColumnsMap> {
        let mut cols = ColumnsMap::new();
        if let Some(model) = self.model {
            cols.set_as("model", &model);
        }
        if let Some(title) = self.title {
            cols.set_as("title", &title);
        }
        if let Some(cover) = self.cover {
            cols.set_as("cover", &cover);
        }
        if let Some(keywords) = self.keywords {
            cols.set_as("keywords", &keywords);
        }
        if let Some(summary) = self.summary {
            cols.set_as("summary", &summary);
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
    to: PackObject<UpdatePublicationInput>,
) -> Result<PackObject<SuccessResponse<PublicationOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let cid = *input.cid.to_owned();
    let gid = *input.gid.to_owned();
    let language = *input.language.to_owned();

    ctx.set_kvs(vec![
        ("action", "update_publication".into()),
        ("gid", gid.to_string().into()),
        ("cid", cid.to_string().into()),
        ("language", language.to_639_3().into()),
        ("version", input.version.into()),
    ])
    .await;

    let mut doc = db::Publication::with_pk(gid, cid, language, input.version);
    let updated_at = input.updated_at;
    let cols = input.into()?;

    let update_meili = cols.has("title") || cols.has("summary") || cols.has("keywords");
    let ok = doc.update(&app.scylla, cols, updated_at).await?;
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
                cid = doc.cid.to_string(),
                kind = 1i8,
                elapsed = ctx.start.elapsed().as_millis() as u64 - meili_start;
                "{}", err.to_string(),
            );
        }
    }

    doc._fields = vec!["updated_at".to_string()]; // only return `updated_at` field.
    Ok(to.with(SuccessResponse::new(PublicationOutput::from(doc, &to))))
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdatePublicationContentInput {
    pub cid: PackObject<xid::Id>,
    pub gid: PackObject<xid::Id>,
    pub language: PackObject<isolang::Language>,
    #[validate(range(min = 1, max = 10000))]
    pub version: i16,
    pub updated_at: i64,
    #[validate(custom = "validate_cbor_content")]
    pub content: PackObject<Vec<u8>>,
}

pub async fn update_content(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<UpdatePublicationContentInput>,
) -> Result<PackObject<SuccessResponse<PublicationOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = input.gid.unwrap();
    let cid = input.cid.unwrap();
    let language = input.language.unwrap();
    let content = input.content.unwrap();

    ctx.set_kvs(vec![
        ("action", "update_publication_status".into()),
        ("gid", gid.to_string().into()),
        ("cid", cid.to_string().into()),
        ("language", language.to_name().into()),
        ("version", input.version.into()),
    ])
    .await;

    let mut doc = db::Publication::with_pk(gid, cid, language, input.version);

    let ok = doc
        .update_content(&app.scylla, content, input.updated_at)
        .await?;
    ctx.set("updated", ok.into()).await;

    doc._fields = vec!["updated_at".to_string()];
    Ok(to.with(SuccessResponse::new(PublicationOutput::from(doc, &to))))
}

pub async fn delete(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryPublicationInputput>,
) -> Result<PackObject<SuccessResponse<bool>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = *input.gid.to_owned();
    let cid = *input.cid.to_owned();
    let language = *input.language.to_owned();

    ctx.set_kvs(vec![
        ("action", "delete_publication".into()),
        ("gid", gid.to_string().into()),
        ("cid", cid.to_string().into()),
        ("language", language.to_639_3().into()),
        ("version", input.version.into()),
    ])
    .await;

    let mut doc = db::Publication::with_pk(gid, cid, language, input.version);
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
            cid = cid.to_string(),
            kind = 1i8,
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
    input: Query<QueryCid>,
) -> Result<PackObject<SuccessResponse<SubscriptionInputOutput>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let cid = *input.cid.to_owned();

    ctx.set_kvs(vec![
        ("action", "get_creation_subscription".into()),
        ("cid", cid.to_string().into()),
    ])
    .await;

    let mut doc = db::CreationSubscription::with_pk(ctx.user, cid);
    doc.get_one(&app.scylla, vec![]).await?;
    Ok(to.with(SuccessResponse::new(SubscriptionInputOutput {
        uid: to.with(doc.uid),
        cid: to.with(doc.cid),
        txn: to.with(doc.txn),
        updated_at: doc.updated_at,
        expire_at: doc.expire_at,
    })))
}

pub async fn update_subscription(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<SubscriptionInputOutput>,
) -> Result<PackObject<SuccessResponse<SubscriptionInputOutput>>, HTTPError> {
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

    Ok(to.with(SuccessResponse::new(SubscriptionInputOutput {
        uid: to.with(doc.uid),
        cid: to.with(doc.cid),
        txn: to.with(doc.txn),
        updated_at: doc.updated_at,
        expire_at: doc.expire_at,
    })))
}

async fn try_get_subscription<T>(
    scylla: &db::scylladb::ScyllaDB,
    creation: &db::CreationIndex,
    to: &PackObject<T>,
    uid: xid::Id,
    now_ms: i64,
) -> Option<SubscriptionInputOutput> {
    if creation.price <= 0 || uid <= db::MIN_ID {
        return None;
    }

    let mut output = SubscriptionInputOutput::default();
    let mut doc = db::CreationSubscription::with_pk(uid, creation.id);
    if doc.get_one(scylla, vec![]).await.is_ok() {
        output = SubscriptionInputOutput {
            uid: to.with(doc.uid),
            cid: to.with(doc.cid),
            txn: to.with(doc.txn),
            updated_at: doc.updated_at,
            expire_at: doc.expire_at,
        };
        if output.expire_at >= now_ms {
            return Some(output);
        }
    }
    if let Ok(parents) = db::Collection::list_by_child(
        scylla,
        creation.id,
        vec!["gid".to_string()],
        Some(creation.gid),
        None,
    )
    .await
    {
        for parent in parents {
            let mut doc = db::CollectionSubscription::with_pk(uid, parent.id);
            if doc.get_one(scylla, vec![]).await.is_ok() && doc.expire_at > output.expire_at {
                output = SubscriptionInputOutput {
                    uid: to.with(doc.uid),
                    cid: to.with(doc.cid),
                    txn: to.with(doc.txn),
                    updated_at: doc.updated_at,
                    expire_at: doc.expire_at,
                };
                if output.expire_at >= now_ms {
                    return Some(output);
                }
            }
        }
    }

    if output.expire_at > 0 {
        return Some(output);
    }

    None
}
