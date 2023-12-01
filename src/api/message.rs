use axum::{
    extract::{Query, State},
    Extension,
};
use isolang::Language;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    convert::From,
    sync::Arc,
};
use validator::{Validate, ValidationError};

use axum_web::context::ReqContext;
use axum_web::erring::{valid_user, HTTPError, SuccessResponse};
use axum_web::object::PackObject;
use scylla_orm::ColumnsMap;

use super::{get_fields, AppState, QueryGidId, QueryId};
use crate::db;

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct CreateMessageInput {
    pub attach_to: PackObject<xid::Id>,
    pub kind: String,
    #[validate(length(min = 0, max = 4096))]
    pub context: String,
    pub language: PackObject<Language>,
    #[validate(custom = "validate_message")]
    pub message: PackObject<Vec<u8>>,
}

pub fn validate_message(data: &PackObject<Vec<u8>>) -> Result<(), ValidationError> {
    if data.len() > db::MAX_MESSAGE_LEN {
        return Err(ValidationError::new("message length is too long"));
    }

    let _ = db::MessageValue::try_from(data.unwrap_ref().as_slice())
        .map_err(|_| ValidationError::new("message is not a valid cbor"))?;
    Ok(())
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct MessageOutput {
    pub id: PackObject<xid::Id>,
    pub i18n_messages: HashMap<String, PackObject<Vec<u8>>>,
    pub languages: Vec<PackObject<Language>>,

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
    pub fn from<T>(val: db::Message, to: &PackObject<T>) -> Self {
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

        if !val.languages.is_empty() {
            rt.languages = val
                .languages
                .iter()
                .map(|v| to.with(v.to_owned()))
                .collect();
            rt.languages
                .sort_by(|a, b| a.to_639_3().partial_cmp(b.to_639_3()).unwrap());
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

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct UpdateMessageInput {
    pub id: PackObject<xid::Id>,
    #[validate(range(min = 1, max = 32767))]
    pub version: i16,
    pub gid: PackObject<xid::Id>,
    #[validate(length(min = 0, max = 4096))]
    pub context: Option<String>,
    pub language: Option<PackObject<Language>>,
    pub languages: Option<Vec<PackObject<Language>>>,
    #[validate(custom = "validate_message")]
    pub message: Option<PackObject<Vec<u8>>>,
}

impl UpdateMessageInput {
    pub fn into(self) -> anyhow::Result<ColumnsMap> {
        let mut cols = ColumnsMap::new();
        if let Some(context) = self.context {
            cols.set_as("context", &context);
        }
        if let Some(languages) = self.languages {
            let languages: HashSet<Language> = languages.into_iter().map(|v| v.unwrap()).collect();
            cols.set_as("languages", &languages);
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
    let gid = *input.gid.to_owned();
    let version = input.version;
    let mut doc = db::Message::with_pk(id);

    ctx.set_kvs(vec![
        ("action", "update_message".into()),
        ("id", doc.id.to_string().into()),
        ("version", version.to_string().into()),
    ])
    .await;

    doc.get_one(&app.scylla, vec!["attach_to".to_string()])
        .await?;
    if doc.attach_to != gid {
        return Err(HTTPError::new(
            403,
            "Message attach_to not match".to_string(),
        ));
    }

    let mut ok = false;
    if input.context.is_some() || input.languages.is_some() {
        let cols = input.clone().into()?;
        ok = doc.update(&app.scylla, cols, version).await?;
    }

    if let Some(message) = input.message {
        let language = *input.language.unwrap_or_default();
        ctx.set("language", language.to_639_3().into()).await;
        ok = doc
            .update_message(&app.scylla, language, &message, version)
            .await?;
    };
    ctx.set("updated", ok.into()).await;
    Ok(to.with(SuccessResponse::new(MessageOutput::from(doc, &to))))
}

pub async fn delete(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryGidId>,
) -> Result<PackObject<SuccessResponse<bool>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let id = *input.id.to_owned();
    let gid = *input.gid.to_owned();

    ctx.set_kvs(vec![
        ("action", "delete_message".into()),
        ("id", id.to_string().into()),
        ("gid", gid.to_string().into()),
    ])
    .await;

    let mut doc = db::Message::with_pk(id);
    let res = doc.delete(&app.scylla, gid).await?;
    Ok(to.with(SuccessResponse::new(res)))
}
