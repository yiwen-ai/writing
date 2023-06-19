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
use axum_web::erring::{HTTPError, SuccessResponse};
use axum_web::object::PackObject;
use scylla_orm::ColumnsMap;

use super::{AppState, Pagination, QueryIdGid, QueryIdGidVersion, UpdateStatusInput};

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct CreatePublicationDraftInput {
    pub gid: PackObject<xid::Id>,
    pub cid: PackObject<xid::Id>,
    pub from_language: PackObject<isolang::Language>,
    pub language: PackObject<isolang::Language>,
    #[validate(range(min = 1, max = 10000))]
    pub version: i16,
    #[validate(length(min = 3, max = 16))]
    pub model: String,
    #[validate(length(min = 3, max = 512))]
    pub title: String,
    #[validate(length(min = 3, max = 1024))]
    pub description: Option<String>,
    #[validate(url)]
    pub cover: Option<String>,
    pub keywords: Option<Vec<String>>,
    pub authors: Option<Vec<String>>,
    #[validate(length(min = 10, max = 2048))]
    pub summary: Option<String>,
    pub content: PackObject<Vec<u8>>,
    #[validate(url)]
    pub license: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct PublicationDraftOutput {
    pub gid: PackObject<xid::Id>,
    pub id: PackObject<xid::Id>,
    pub cid: PackObject<xid::Id>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<PackObject<Language>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<i16>,
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
}

impl PublicationDraftOutput {
    fn from<T>(val: db::PublicationDraft, to: &PackObject<T>) -> Self {
        let mut rt = Self {
            gid: to.with(val.gid),
            id: to.with(val.id),
            cid: to.with(val.cid),
            ..Default::default()
        };

        for v in val._fields {
            match v.as_str() {
                "language" => rt.language = Some(to.with(val.language)),
                "version" => rt.version = Some(val.version),
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

        rt
    }
}

pub async fn create(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<CreatePublicationDraftInput>,
) -> Result<PackObject<SuccessResponse<PublicationDraftOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let from_language = *input.language.to_owned();

    let mut draft = db::PublicationDraft {
        gid: input.gid.unwrap(),
        id: xid::new(),
        cid: input.cid.unwrap(),
        language: input.language.unwrap(),
        version: input.version,
        creator: ctx.user,
        model: input.model,
        title: input.title,
        description: input.description.unwrap_or_default(),
        cover: input.cover.unwrap_or_default(),
        keywords: input.keywords.unwrap_or_default(),
        authors: input.authors.unwrap_or_default(),
        summary: input.summary.unwrap_or_default(),
        content: input.content.unwrap(),
        license: input.license.unwrap_or_default(),
        ..Default::default()
    };

    let mut index = db::CreationIndex::with_pk(draft.cid);
    if index.get_one(&app.scylla).await.is_err() {
        return Err(HTTPError::new(
            404,
            format!("Creation not exists, cid({})", draft.cid),
        ));
    }

    ctx.set_kvs(vec![
        ("action", "create_publication_draft".into()),
        ("gid", draft.gid.to_string().into()),
        ("id", draft.id.to_string().into()),
        ("cid", draft.cid.to_string().into()),
        ("language", draft.language.to_name().into()),
        ("version", input.version.into()),
    ])
    .await;

    if draft.gid == index.gid {
        // create draft from creation by owner
        let mut creation = db::Creation::with_pk(index.gid, index.id);
        if creation
            .get_one(
                &app.scylla,
                vec![
                    "status".to_string(),
                    "version".to_string(),
                    "language".to_string(),
                ],
            )
            .await
            .is_err()
        {
            return Err(HTTPError::new(
                404,
                format!("Creation not exists, cid({}), gid({})", index.id, index.gid),
            ));
        }

        if creation.language != from_language {
            return Err(HTTPError::new(
                400,
                format!(
                    "Creation language not match, cid({}), gid({}), expected({}), got({})",
                    index.id, index.gid, creation.language, from_language
                ),
            ));
        }

        if creation.version != draft.version {
            return Err(HTTPError::new(
                400,
                format!(
                    "Creation version not match, cid({}), gid({}), expected({}), got({})",
                    index.id, index.gid, creation.version, draft.version
                ),
            ));
        }

        if creation.status != 2 {
            return Err(HTTPError::new(
                400,
                format!(
                    "Creation is not approved, cid({}), gid({})",
                    index.id, index.gid
                ),
            ));
        }
    } else {
        // create draft from publication by any user
        if index.rating > ctx.rating {
            return Err(HTTPError::new(
                451,
                format!(
                    "Publication rating not match, cid({}), gid({}), expected(*), got({})",
                    index.id, index.gid, ctx.rating
                ),
            ));
        }

        let mut publication = db::Publication::with_pk(draft.cid, from_language, draft.version);
        if publication
            .get_one(&app.scylla, vec!["status".to_string()])
            .await
            .is_err()
        {
            return Err(HTTPError::new(
                404,
                format!(
                    "Publication not exists, cid({}), language {}, version {}",
                    index.id, from_language, draft.version
                ),
            ));
        }

        if publication.status != 2 {
            return Err(HTTPError::new(
                400,
                format!(
                    "Publication is not published, cid({}), language {}, version {}",
                    index.id, from_language, draft.version
                ),
            ));
        }
    }

    let ok = draft.save(&app.scylla).await?;
    if !ok {
        return Err(HTTPError::new(409, "Create draft failed".to_string()));
    }

    Ok(to.with(SuccessResponse::new(PublicationDraftOutput::from(
        draft, &to,
    ))))
}

pub async fn get(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryIdGid>,
) -> Result<PackObject<SuccessResponse<PublicationDraftOutput>>, HTTPError> {
    input.validate()?;

    let id = *input.id.to_owned();
    let gid = *input.gid.to_owned();

    ctx.set_kvs(vec![
        ("action", "get_publication_draft".into()),
        ("gid", gid.to_string().into()),
        ("id", id.to_string().into()),
    ])
    .await;

    let mut doc = db::PublicationDraft::with_pk(gid, id);
    let fields = input
        .fields
        .clone()
        .unwrap_or_default()
        .split(',')
        .map(|s| s.to_string())
        .collect();
    doc.get_one(&app.scylla, fields).await?;
    Ok(to.with(SuccessResponse::new(PublicationDraftOutput::from(doc, &to))))
}

pub async fn list(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<Pagination>,
) -> Result<PackObject<SuccessResponse<Vec<PublicationDraftOutput>>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let page_size = input.page_size.unwrap_or(10);
    let gid = *input.gid.to_owned(); // validated
    ctx.set_kvs(vec![
        ("action", "list_publication_draft".into()),
        ("gid", gid.to_string().into()),
    ])
    .await;

    let fields = input.fields.unwrap_or_default();
    let page_token = input.page_token.map(|s| s.unwrap());
    let res = db::PublicationDraft::find(
        &app.scylla,
        gid,
        fields,
        page_size,
        page_token,
        input.status,
    )
    .await?;
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
            .map(|r| PublicationDraftOutput::from(r.to_owned(), &to))
            .collect(),
    }))
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdatePublicationDraftInput {
    pub id: PackObject<xid::Id>,
    pub gid: PackObject<xid::Id>,
    pub updated_at: i64,
    pub language: Option<PackObject<isolang::Language>>,
    #[validate(length(min = 3, max = 16))]
    pub model: Option<String>,
    #[validate(length(min = 3, max = 512))]
    pub title: Option<String>,
    #[validate(length(min = 3, max = 1024))]
    pub description: Option<String>,
    #[validate(url)]
    pub cover: Option<String>,
    #[validate(length(min = 0, max = 10))]
    pub keywords: Option<Vec<String>>,
    #[validate(length(min = 0, max = 100))]
    pub authors: Option<Vec<String>>,
    #[validate(length(min = 10, max = 2048))]
    pub summary: Option<String>,
    pub content: Option<PackObject<Vec<u8>>>,
    #[validate(url)]
    pub license: Option<String>,
}

impl UpdatePublicationDraftInput {
    fn into(self) -> anyhow::Result<ColumnsMap> {
        let mut cols = ColumnsMap::new();
        if let Some(language) = self.language {
            cols.set_as("language", language.as_ref())?;
        }
        if let Some(model) = self.model {
            cols.set_as("model", &model)?;
        }
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
        if let Some(authors) = self.authors {
            cols.set_as("authors", &authors)?;
        }
        if let Some(summary) = self.summary {
            cols.set_as("summary", &summary)?;
        }
        if let Some(content) = self.content {
            cols.set_as("content", content.as_ref())?;
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

pub async fn update(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<UpdatePublicationDraftInput>,
) -> Result<PackObject<SuccessResponse<PublicationDraftOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let id = *input.id.to_owned();
    let gid = *input.gid.to_owned();
    let mut doc = db::PublicationDraft::with_pk(gid, id);
    let updated_at = input.updated_at;
    let cols = input.into()?;
    ctx.set_kvs(vec![
        ("action", "update_publication_draft".into()),
        ("gid", doc.gid.to_string().into()),
        ("id", doc.id.to_string().into()),
    ])
    .await;

    let ok = doc.update(&app.scylla, cols, updated_at).await?;
    if !ok {
        return Err(HTTPError::new(409, "Creation update failed".to_string()));
    }

    doc._fields = vec!["updated_at".to_string()]; // only return `updated_at` field.

    Ok(to.with(SuccessResponse::new(PublicationDraftOutput::from(doc, &to))))
}

pub async fn update_status(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<UpdateStatusInput>,
) -> Result<PackObject<SuccessResponse<PublicationDraftOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let id = *input.id.to_owned(); // validated
    let gid = *input.gid.to_owned(); // validated
    let mut doc = db::PublicationDraft::with_pk(gid, id);
    ctx.set_kvs(vec![
        ("action", "update_publication_draft_status".into()),
        ("gid", doc.gid.to_string().into()),
        ("id", doc.id.to_string().into()),
    ])
    .await;

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
    Ok(to.with(SuccessResponse::new(PublicationDraftOutput::from(doc, &to))))
}

pub async fn delete(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryIdGidVersion>,
) -> Result<PackObject<SuccessResponse<bool>>, HTTPError> {
    input.validate()?;

    let id = *input.id.to_owned();
    let gid = *input.gid.to_owned();

    ctx.set_kvs(vec![
        ("action", "delete_publication_draft".into()),
        ("gid", gid.to_string().into()),
        ("id", id.to_string().into()),
    ])
    .await;

    let mut doc = db::PublicationDraft::with_pk(gid, id);
    let res = doc.delete(&app.scylla, input.version).await?;
    Ok(to.with(SuccessResponse::new(res)))
}
