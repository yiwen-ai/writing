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
use scylla_orm::ColumnsMap;

use super::{
    get_fields, token_from_xid, token_to_xid, AppState, Pagination, QueryGidId, UpdateStatusInput,
};

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct CreateCreationInput {
    pub gid: PackObject<xid::Id>,
    pub language: PackObject<Language>,
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
    pub content: PackObject<Vec<u8>>,
    #[validate(url)]
    pub license: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct CreationOutput {
    pub id: PackObject<xid::Id>,
    pub gid: PackObject<xid::Id>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<i8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rating: Option<i8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<PackObject<Language>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creator: Option<PackObject<xid::Id>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
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
    pub reviewers: Option<Vec<PackObject<xid::Id>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<PackObject<Vec<u8>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
}

impl CreationOutput {
    fn from<T>(val: db::Creation, to: &PackObject<T>) -> Self {
        let mut rt = Self {
            gid: to.with(val.gid),
            id: to.with(val.id),
            ..Default::default()
        };

        for v in val._fields {
            match v.as_str() {
                "status" => rt.status = Some(val.status),
                "version" => rt.version = Some(val.version),
                "language" => rt.language = Some(to.with(val.language)),
                "creator" => rt.creator = Some(to.with(val.creator)),
                "created_at" => rt.created_at = Some(val.created_at),
                "updated_at" => rt.updated_at = Some(val.updated_at),
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
                "content" => rt.content = Some(to.with(val._content.to_owned())),
                "license" => rt.license = Some(val.license.to_owned()),
                _ => {}
            }
        }

        rt
    }
}

pub async fn create(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<CreateCreationInput>,
) -> Result<PackObject<SuccessResponse<CreationOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let mut doc = db::Creation {
        gid: input.gid.unwrap(),
        id: xid::new(),
        language: input.language.unwrap(),
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
        license: input.license.unwrap_or_default(),
        ..Default::default()
    };
    ctx.set_kvs(vec![
        ("action", "create_creation".into()),
        ("gid", doc.gid.to_string().into()),
        ("id", doc.id.to_string().into()),
    ])
    .await;

    let ok = doc.save_with(&app.scylla, input.content.unwrap()).await?;
    ctx.set("created", ok.into()).await;
    app.meili
        .add_or_update(meili::Space::Group(doc.gid), vec![doc.to_meili()])
        .await?;
    Ok(to.with(SuccessResponse::new(CreationOutput::from(doc, &to))))
}

pub async fn get(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryGidId>,
) -> Result<PackObject<SuccessResponse<CreationOutput>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = *input.gid.to_owned();
    let id = *input.id.to_owned();

    ctx.set_kvs(vec![
        ("action", "get_creation".into()),
        ("gid", gid.to_string().into()),
        ("id", id.to_string().into()),
    ])
    .await;

    let mut doc = db::Creation::with_pk(gid, id);
    doc.get_one(&app.scylla, get_fields(input.fields.clone()))
        .await?;
    Ok(to.with(SuccessResponse::new(CreationOutput::from(doc, &to))))
}

pub async fn list(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<Pagination>,
) -> Result<PackObject<SuccessResponse<Vec<CreationOutput>>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = input.gid.unwrap();
    let page_size = input.page_size.unwrap_or(10);
    ctx.set_kvs(vec![
        ("action", "list_creation".into()),
        ("gid", gid.to_string().into()),
    ])
    .await;

    let fields = input.fields.unwrap_or_default();
    let res = db::Creation::list_by_gid(
        &app.scylla,
        gid,
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
            .map(|r| CreationOutput::from(r.to_owned(), &to))
            .collect(),
    }))
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdateCreationInput {
    pub id: PackObject<xid::Id>,
    pub gid: PackObject<xid::Id>,
    pub updated_at: i64,
    #[validate(length(min = 3, max = 512))]
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
    pub content: Option<PackObject<Vec<u8>>>,
    #[validate(url)]
    pub license: Option<String>,
}

impl UpdateCreationInput {
    fn into(self) -> anyhow::Result<ColumnsMap> {
        let mut cols = ColumnsMap::new();
        if let Some(title) = self.title {
            cols.set_as("title", &title);
        }
        if let Some(description) = self.description {
            cols.set_as("description", &description);
        }
        if let Some(cover) = self.cover {
            cols.set_as("cover", &cover);
        }
        if let Some(keywords) = self.keywords {
            cols.set_as("keywords", &keywords);
        }
        if let Some(labels) = self.labels {
            cols.set_as("labels", &labels);
        }
        if let Some(authors) = self.authors {
            cols.set_as("authors", &authors);
        }
        if let Some(summary) = self.summary {
            cols.set_as("summary", &summary);
        }
        if let Some(license) = self.license {
            cols.set_as("license", &license);
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
    to: PackObject<UpdateCreationInput>,
) -> Result<PackObject<SuccessResponse<CreationOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let id = *input.id.to_owned();
    let gid = *input.gid.to_owned();

    let mut doc = db::Creation::with_pk(gid, id);
    let updated_at = input.updated_at;
    let cols = input.into()?;
    ctx.set_kvs(vec![
        ("action", "update_creation".into()),
        ("gid", doc.gid.to_string().into()),
        ("id", doc.id.to_string().into()),
    ])
    .await;

    let ok = doc.update(&app.scylla, cols, updated_at).await?;
    ctx.set("updated", ok.into()).await;

    app.meili
        .add_or_update(meili::Space::Group(doc.gid), vec![doc.to_meili()])
        .await?;

    doc._fields = vec!["updated_at".to_string()]; // only return `updated_at` field.
    Ok(to.with(SuccessResponse::new(CreationOutput::from(doc, &to))))
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdateCreationContentInput {
    pub gid: PackObject<xid::Id>,
    pub id: PackObject<xid::Id>,
    pub language: PackObject<Language>,
    pub content: PackObject<Vec<u8>>,
    pub updated_at: i64,
}

pub async fn update_content(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<UpdateCreationContentInput>,
) -> Result<PackObject<SuccessResponse<CreationOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let id = input.id.unwrap();
    let gid = input.gid.unwrap();
    let language = input.language.unwrap();
    let content = input.content.unwrap();

    let mut doc = db::Creation::with_pk(gid, id);
    ctx.set_kvs(vec![
        ("action", "update_content".into()),
        ("gid", doc.gid.to_string().into()),
        ("id", doc.id.to_string().into()),
    ])
    .await;

    let ok = doc
        .update_content(&app.scylla, language, content, input.updated_at)
        .await?;

    ctx.set("updated", ok.into()).await;
    doc._fields = vec![
        "updated_at".to_string(),
        "language".to_string(),
        "version".to_string(),
    ];
    Ok(to.with(SuccessResponse::new(CreationOutput::from(doc, &to))))
}

pub async fn update_status(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<UpdateStatusInput>,
) -> Result<PackObject<SuccessResponse<CreationOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = input
        .gid
        .ok_or_else(|| HTTPError::new(400, "Missing required field `gid`".to_string()))?;
    let gid = *gid.to_owned();
    let id = *input.id.to_owned();

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

    ctx.set("updated", ok.into()).await;
    doc._fields = vec!["updated_at".to_string(), "status".to_string()];
    Ok(to.with(SuccessResponse::new(CreationOutput::from(doc, &to))))
}

pub async fn delete(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryGidId>,
) -> Result<PackObject<SuccessResponse<bool>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = *input.gid.to_owned();
    let id = *input.id.to_owned();

    ctx.set_kvs(vec![
        ("action", "delete_creation".into()),
        ("gid", gid.to_string().into()),
        ("id", id.to_string().into()),
    ])
    .await;

    let mut doc = db::Creation::with_pk(gid, id);
    let res = doc.delete(&app.scylla).await?;
    app.meili
        .delete(meili::Space::Group(doc.gid), vec![doc.to_meili().id])
        .await?;
    Ok(to.with(SuccessResponse::new(res)))
}
