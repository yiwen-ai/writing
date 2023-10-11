use axum::{
    extract::{Query, State},
    Extension,
};
use isolang::Language;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, convert::From, sync::Arc};
use validator::Validate;

use axum_web::context::ReqContext;
use axum_web::erring::{valid_user, HTTPError, SuccessResponse};
use axum_web::object::PackObject;
use scylla_orm::ColumnsMap;

use super::{get_fields, AppState, QueryId};
use crate::db;

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct CreateMessageInput {
    pub attach_to: PackObject<xid::Id>,
    pub kind: String,
    pub context: String,
    pub language: PackObject<Language>,
    pub message: PackObject<Vec<u8>>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct MessageOutput {
    pub id: PackObject<xid::Id>,
    pub i18n_messages: HashMap<String, PackObject<Vec<u8>>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub attach_to: Option<PackObject<xid::Id>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<PackObject<Language>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<PackObject<Vec<u8>>>,
}

impl MessageOutput {
    fn from<T>(val: db::Message, to: &PackObject<T>) -> Self {
        let mut rt = Self {
            id: to.with(val.id),
            ..Default::default()
        };

        for v in val._fields {
            match v.as_str() {
                "attach_to" => rt.attach_to = Some(to.with(val.attach_to)),
                "kind" => rt.kind = Some(val.kind.clone()),
                "language" => rt.language = Some(to.with(val.language)),
                "version" => rt.version = Some(val.version),
                "created_at" => rt.created_at = Some(val.created_at),
                "updated_at" => rt.updated_at = Some(val.updated_at),
                "context" => rt.context = Some(val.context.to_owned()),
                "message" => rt.message = Some(to.with(val.message.to_owned())),
                _ => {}
            }
        }

        if !val._i18n_messages.is_empty() {
            rt.i18n_messages = val
                ._i18n_messages
                .iter()
                .map(|(k, v)| (k.to_owned(), to.with(v.to_owned())))
                .collect();
        }

        rt
    }
}

pub async fn create(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<CreateMessageInput>,
) -> Result<PackObject<SuccessResponse<MessageOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let attach_to = input.attach_to.unwrap();
    let language = input.language.unwrap();

    let mut doc = db::Message {
        id: xid::new(),
        attach_to,
        kind: input.kind,
        language,
        context: input.context,
        message: input.message.unwrap(),
        ..Default::default()
    };

    ctx.set_kvs(vec![
        ("action", "create_message".into()),
        ("attach_to", doc.attach_to.to_string().into()),
        ("kind", doc.kind.clone().into()),
        ("language", doc.language.to_name().into()),
    ])
    .await;

    doc.save(&app.scylla).await?;
    Ok(to.with(SuccessResponse::new(MessageOutput::from(doc, &to))))
}

pub async fn get(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryId>,
) -> Result<PackObject<SuccessResponse<MessageOutput>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let id = *input.id.to_owned();

    ctx.set_kvs(vec![
        ("action", "get_message".into()),
        ("id", id.to_string().into()),
    ])
    .await;

    let mut doc = db::Message::with_pk(id);
    doc.get_one(&app.scylla, get_fields(input.fields.clone()))
        .await?;
    Ok(to.with(SuccessResponse::new(MessageOutput::from(doc, &to))))
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdateMessageInput {
    pub id: PackObject<xid::Id>,
    #[validate(range(min = 1, max = 32767))]
    pub version: i16,
    #[validate(length(min = 0, max = 1024))]
    pub context: Option<String>,
    pub language: Option<PackObject<Language>>,
    pub message: Option<PackObject<Vec<u8>>>,
}

impl UpdateMessageInput {
    fn into(self) -> anyhow::Result<ColumnsMap> {
        let mut cols = ColumnsMap::new();
        if let Some(context) = self.context {
            cols.set_as("context", &context);
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
    to: PackObject<UpdateMessageInput>,
) -> Result<PackObject<SuccessResponse<MessageOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let id = *input.id.to_owned();
    let version = input.version;
    let mut doc = db::Message::with_pk(id);

    ctx.set_kvs(vec![
        ("action", "update_message".into()),
        ("id", doc.id.to_string().into()),
        ("version", version.to_string().into()),
    ])
    .await;

    let ok = if let Some(message) = input.message {
        let language = input.language.unwrap_or_default().to_639_3().to_string();
        ctx.set("language", language.clone().into()).await;
        doc.update_message(&app.scylla, language, message.unwrap(), version)
            .await?
    } else {
        let cols = input.into()?;
        doc.update(&app.scylla, cols, version).await?
    };
    ctx.set("updated", ok.into()).await;
    Ok(to.with(SuccessResponse::new(MessageOutput::from(doc, &to))))
}

pub async fn delete(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryId>,
) -> Result<PackObject<SuccessResponse<bool>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let id = *input.id.to_owned();

    ctx.set_kvs(vec![
        ("action", "delete_bookmark".into()),
        ("id", id.to_string().into()),
    ])
    .await;

    let mut doc = db::Bookmark::with_pk(ctx.user, id);
    let res = doc.delete(&app.scylla).await?;
    Ok(to.with(SuccessResponse::new(res)))
}
