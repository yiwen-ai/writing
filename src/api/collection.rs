use axum::{
    extract::{Query, State},
    Extension,
};
use isolang::Language;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, convert::From, sync::Arc};

use validator::{Validate, ValidationError};

use crate::{db, db::meili};

use axum_web::context::ReqContext;
use axum_web::erring::{valid_user, HTTPError, SuccessResponse};
use axum_web::object::PackObject;
use scylla_orm::ColumnsMap;

use super::{
    get_fields, message, token_from_xid, token_to_xid, AppState, GIDPagination, IDGIDPagination,
    QueryGidCid, QueryGidId, QueryGidIdCid, QueryId, SubscriptionInput, SubscriptionOutput,
    UpdateStatusInput, RFP,
};

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct CreateCollectionInput {
    pub gid: PackObject<xid::Id>,
    pub language: PackObject<Language>,
    #[validate(length(min = 0, max = 512))]
    pub context: String,
    pub info: CollectionInfoInput,
    #[validate(url)]
    pub cover: Option<String>,
    #[validate(range(min = -1, max = 1000000))]
    pub price: Option<i64>,
    #[validate(range(min = -1, max = 100000))]
    pub creation_price: Option<i64>,
    pub parent: Option<PackObject<xid::Id>>,
}

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct CollectionInfoInput {
    #[validate(length(min = 1, max = 256))]
    pub title: Option<String>,
    #[validate(length(min = 1, max = 2048))]
    pub summary: Option<String>,
    #[validate(length(min = 0, max = 10))]
    pub keywords: Option<Vec<String>>,
    #[validate(length(min = 0, max = 10))]
    pub authors: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct CollectionOutput {
    pub id: PackObject<xid::Id>,
    pub gid: PackObject<xid::Id>,
    pub status: i8,
    pub rating: i8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cover: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creation_price: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<PackObject<Language>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub languages: Option<Vec<PackObject<Language>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<db::CollectionInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub i18n_info: Option<HashMap<String, db::CollectionInfo>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription: Option<SubscriptionOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rfp: Option<RFP>,
}

impl CollectionOutput {
    fn from<T>(val: db::Collection, to: &PackObject<T>) -> Self {
        let mut rt = Self {
            id: to.with(val.id),
            gid: to.with(val.gid),
            status: val.status,
            rating: val.rating,
            ..Default::default()
        };

        for v in val._fields {
            match v.as_str() {
                "updated_at" => rt.updated_at = Some(val.updated_at),
                "cover" => rt.cover = Some(val.cover.to_owned()),
                "price" => rt.price = Some(val.price),
                "creation_price" => rt.creation_price = Some(val.creation_price),
                _ => {}
            }
        }

        if let Some(msg) = val._info {
            let msg = message::MessageOutput::from(msg, to);
            rt.language = msg.language;
            rt.version = msg.version;
            rt.languages = Some(msg.languages);

            if let Ok(info) = db::CollectionInfo::from_message(&msg.message.unwrap_or_default()) {
                rt.info = Some(info);
            }
            let mut i18n_info = HashMap::new();
            for (k, v) in msg.i18n_messages {
                if let Ok(info) = db::CollectionInfo::from_message(&v) {
                    i18n_info.insert(k, info);
                }
            }
            rt.i18n_info = Some(i18n_info);
        }

        rt
    }
}

pub async fn create(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<CreateCollectionInput>,
) -> Result<PackObject<SuccessResponse<CollectionOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    if input.info.title.is_none() {
        return Err(HTTPError::new(400, "Title is required".to_string()));
    }

    let gid = input.gid.unwrap();
    let language = input.language.unwrap();
    ctx.set_kvs(vec![
        ("action", "create_collection".into()),
        ("gid", gid.to_string().into()),
        ("language", language.to_639_3().into()),
    ])
    .await;

    let price = input.price.unwrap_or(0);
    let creation_price = input.creation_price.unwrap_or(0);
    if creation_price > price {
        return Err(HTTPError::new(
            400,
            "Creation price cannot be greater than price".to_string(),
        ));
    }

    let info = db::CollectionInfo {
        title: input.info.title.unwrap_or_default(),
        summary: input.info.summary.unwrap_or_default(),
        keywords: input.info.keywords,
        authors: input.info.authors,
    };
    let info_msg: Vec<u8> = info.to_message()?;

    let parent = if let Some(parent) = input.parent {
        let mut doc = db::Collection::with_pk(*parent);
        doc.get_one(
            &app.scylla,
            vec!["gid".to_string(), "status".to_string()],
            None,
        )
        .await?;
        if doc.gid != gid {
            return Err(HTTPError::new(
                403,
                format!("Collection {} is not belong to group {}", doc.id, gid),
            ));
        }
        if doc.status < 0 {
            return Err(HTTPError::new(
                400,
                format!("Collection {} is archived", doc.id),
            ));
        }

        let count = db::CollectionChildren::count_children(&app.scylla, doc.id).await?;
        if count >= db::MAX_COLLECTION_CHILDREN {
            return Err(HTTPError::new(
                400,
                format!(
                    "Parent collection can only have {} children",
                    db::MAX_COLLECTION_CHILDREN
                ),
            ));
        }
        Some(doc)
    } else {
        None
    };

    let mut doc = db::Collection {
        id: xid::new(),
        gid,
        cover: input.cover.unwrap_or_default(),
        price,
        creation_price,
        ..Default::default()
    };

    doc.save(&app.scylla).await?;

    let mut msg = db::Message {
        id: xid::new(),
        attach_to: doc.id,
        kind: "collection.info".to_string(),
        language,
        context: input.context,
        message: info_msg,
        ..Default::default()
    };

    msg.save(&app.scylla).await?;
    doc.mid = msg.id;
    doc.update_field(&app.scylla, "mid").await?;

    let meili_start = ctx.start.elapsed().as_millis() as u64;
    let meili_doc = doc.to_meili(msg.language, &msg.message, msg.version, msg.updated_at)?;
    if let Err(err) = app
        .meili
        .add_or_update(meili::Space::Group(doc.gid), vec![meili_doc])
        .await
    {
        log::error!(target: "meilisearch",
            action = "add_or_update",
            space = "group",
            rid = ctx.rid,
            gid = doc.gid.to_string(),
            id = doc.id.to_string(),
            kind = 2i8,
            elapsed = ctx.start.elapsed().as_millis() as u64 - meili_start;
            "{}", err.to_string(),
        );
    }

    if let Some(parent) = parent {
        let mut child = db::CollectionChildren {
            id: parent.id,
            cid: doc.id,
            kind: 2,
            ord: ctx.unix_ms as f64,
            ..Default::default()
        };
        let _ = child.save(&app.scylla).await;
    }

    doc._info = Some(msg);
    Ok(to.with(SuccessResponse::new(CollectionOutput::from(doc, &to))))
}

pub async fn get(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryGidId>,
) -> Result<PackObject<SuccessResponse<CollectionOutput>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let id = *input.id.to_owned();
    let gid = *input.gid.to_owned();
    let status = input.status.unwrap_or(2);

    ctx.set_kvs(vec![
        ("action", "get_collection".into()),
        ("id", id.to_string().into()),
        ("gid", gid.to_string().into()),
    ])
    .await;

    let mut doc = db::Collection::with_pk(id);
    let fields = get_fields(input.fields.clone());
    doc.get_one(&app.scylla, fields, ctx.language).await?;
    if status == 2 && doc.rating > ctx.rating {
        return Err(HTTPError::new(451, "Collection unavailable".to_string()));
    }
    if status < 2 && doc.gid != gid {
        return Err(HTTPError::new(403, "Collection gid not match".to_string()));
    }
    let price = doc.price;
    let mut output = CollectionOutput::from(doc, &to);
    if price > 0 {
        if ctx.user > db::MIN_ID && *output.gid != gid {
            let mut subscription = db::CollectionSubscription::with_pk(ctx.user, id);
            if subscription.get_one(&app.scylla, vec![]).await.is_ok() {
                output.subscription = Some(SubscriptionOutput {
                    uid: to.with(subscription.uid),
                    cid: to.with(subscription.cid),
                    gid: to.with(*output.gid),
                    txn: to.with(subscription.txn),
                    updated_at: subscription.updated_at,
                    expire_at: subscription.expire_at,
                });
            }
        }
        match output.subscription {
            None => {
                output.rfp = Some(RFP {
                    creation: None,
                    collection: Some(price),
                })
            }
            Some(ref sub) => {
                if sub.expire_at < ctx.unix_ms as i64 {
                    output.rfp = Some(RFP {
                        creation: None,
                        collection: Some(price),
                    })
                }
            }
        }
    }
    Ok(to.with(SuccessResponse::new(output)))
}

pub async fn list(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<GIDPagination>,
) -> Result<PackObject<SuccessResponse<Vec<CollectionOutput>>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = *input.gid.to_owned();
    let page_size = input.page_size.unwrap_or(10);
    ctx.set_kvs(vec![
        ("action", "list_collection".into()),
        ("gid", gid.to_string().into()),
        ("page_size", page_size.into()),
    ])
    .await;

    let fields = input.fields.unwrap_or_default();
    let res = db::Collection::list_by_gid(
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
        next_page_token: to.with_option(token_from_xid(res.1)),
        result: res
            .0
            .iter()
            .map(|r| CollectionOutput::from(r.to_owned(), &to))
            .collect(),
    }))
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdateCollectionInput {
    pub id: PackObject<xid::Id>,
    pub gid: PackObject<xid::Id>,
    pub updated_at: i64,
    #[validate(url)]
    pub cover: Option<String>,
    #[validate(range(min = -1, max = 1000000))]
    pub price: Option<i64>,
    #[validate(range(min = -1, max = 100000))]
    pub creation_price: Option<i64>,
}

impl UpdateCollectionInput {
    fn into(self) -> anyhow::Result<ColumnsMap, HTTPError> {
        let mut cols = ColumnsMap::new();
        if let Some(cover) = self.cover {
            cols.set_as("cover", &cover);
        }
        if let Some(creation_price) = self.creation_price {
            cols.set_as("creation_price", &creation_price);
        }
        if let Some(price) = self.price {
            cols.set_as("price", &price);
            if price == -1 {
                cols.set_as("creation_price", &price);
            }
        }

        if cols.is_empty() {
            return Err(HTTPError::new(400, "No fields to update".to_string()));
        }

        Ok(cols)
    }
}

pub async fn update(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<UpdateCollectionInput>,
) -> Result<PackObject<SuccessResponse<CollectionOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let id = *input.id.to_owned();
    let gid = *input.gid.to_owned();
    let mut doc = db::Collection::with_pk(id);
    let updated_at = input.updated_at;
    let cols = input.into()?;
    ctx.set_kvs(vec![
        ("action", "update_collection".into()),
        ("id", doc.id.to_string().into()),
    ])
    .await;

    let ok = doc.update(&app.scylla, gid, cols, updated_at).await?;
    ctx.set("updated", ok.into()).await;
    doc._fields = vec!["updated_at".to_string()]; // only return `updated_at` field.
    Ok(to.with(SuccessResponse::new(CollectionOutput::from(doc, &to))))
}

pub fn validate_collection_info(data: &PackObject<Vec<u8>>) -> Result<(), ValidationError> {
    if data.len() > db::MAX_MESSAGE_LEN {
        return Err(ValidationError::new("message length is too long"));
    }

    let _ = db::CollectionInfo::from_message(data.unwrap_ref())
        .map_err(|_| ValidationError::new("message is not a valid cbor"))?;
    Ok(())
}

pub async fn get_info(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryGidId>,
) -> Result<PackObject<SuccessResponse<message::MessageOutput>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let id = *input.id.to_owned();
    let gid = *input.gid.to_owned();

    ctx.set_kvs(vec![
        ("action", "get_collection_info".into()),
        ("id", id.to_string().into()),
        ("gid", gid.to_string().into()),
    ])
    .await;

    let mut doc = db::Collection::with_pk(id);
    doc.get_one(
        &app.scylla,
        vec!["mid".to_string(), "gid".to_string()],
        None,
    )
    .await?;

    if doc.gid != gid {
        return Err(HTTPError::new(403, "Collection gid not match".to_string()));
    }
    let mut info = db::Message::with_pk(doc.mid);
    info.get_one(&app.scylla, get_fields(input.fields.clone()))
        .await?;
    Ok(to.with(SuccessResponse::new(message::MessageOutput::from(
        info, &to,
    ))))
}

pub async fn update_info(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<message::UpdateMessageInput>,
) -> Result<PackObject<SuccessResponse<message::MessageOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let id = *input.id.to_owned();
    let gid = *input.gid.to_owned();
    let version = input.version;

    ctx.set_kvs(vec![
        ("action", "update_collection_info".into()),
        ("id", id.to_string().into()),
        ("version", version.to_string().into()),
    ])
    .await;

    let mut doc = db::Collection::with_pk(id);
    doc.get_one(
        &app.scylla,
        vec!["gid".to_string(), "mid".to_string()],
        None,
    )
    .await?;
    if doc.gid != gid {
        return Err(HTTPError::new(403, "Collection gid not match".to_string()));
    }

    let mut info = db::Message::with_pk(doc.mid);
    if let Some(message) = input.message {
        validate_collection_info(&message)?;
        let language = *input.language.unwrap_or_default();
        ctx.set("language", language.to_639_3().into()).await;

        let ok = info
            .update_message(&app.scylla, language, &message, version)
            .await?;
        ctx.set("updated", ok.into()).await;

        let meili_start = ctx.start.elapsed().as_millis() as u64;
        let meili_doc = doc.to_meili(language, &message, info.version, info.updated_at)?;
        if let Err(err) = app
            .meili
            .add_or_update(meili::Space::Group(doc.gid), vec![meili_doc])
            .await
        {
            log::error!(target: "meilisearch",
                action = "add_or_update",
                space = "group",
                rid = ctx.rid,
                gid = doc.gid.to_string(),
                id = doc.id.to_string(),
                kind = 2i8,
                elapsed = ctx.start.elapsed().as_millis() as u64 - meili_start;
                "{}", err.to_string(),
            );
        }
    } else {
        let cols = input.into()?;
        let ok = info.update(&app.scylla, cols, version).await?;
        ctx.set("updated", ok.into()).await;
    }

    Ok(to.with(SuccessResponse::new(message::MessageOutput::from(
        info, &to,
    ))))
}

pub async fn update_status(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<UpdateStatusInput>,
) -> Result<PackObject<SuccessResponse<CollectionOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let id = *input.id.to_owned();
    let gid = *input.gid.unwrap_or_default();

    let mut doc = db::Collection::with_pk(id);
    ctx.set_kvs(vec![
        ("action", "update_collection_status".into()),
        ("id", doc.id.to_string().into()),
        ("gid", gid.to_string().into()),
    ])
    .await;

    let ok = doc
        .update_status(&app.scylla, gid, input.status, input.updated_at)
        .await?;

    ctx.set("updated", ok.into()).await;
    doc._fields = vec!["updated_at".to_string(), "status".to_string()];
    Ok(to.with(SuccessResponse::new(CollectionOutput::from(doc, &to))))
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
        ("action", "delete_collection".into()),
        ("id", id.to_string().into()),
        ("gid", gid.to_string().into()),
    ])
    .await;

    let mut doc = db::Collection::with_pk(id);
    let res = doc.delete(&app.scylla, gid).await?;
    Ok(to.with(SuccessResponse::new(res)))
}

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct AddChildrenInput {
    pub id: PackObject<xid::Id>,
    pub gid: PackObject<xid::Id>,
    #[validate(length(min = 1, max = 1000))]
    pub cids: Vec<PackObject<xid::Id>>,
    #[validate(range(min = 0, max = 1))]
    pub kind: i8,
}

pub async fn add_children(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<AddChildrenInput>,
) -> Result<PackObject<SuccessResponse<Vec<PackObject<xid::Id>>>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let id = *input.id.to_owned();
    let gid = *input.gid.to_owned();
    let cids: Vec<xid::Id> = input.cids.iter().map(|id| *id.to_owned()).collect();
    ctx.set_kvs(vec![
        ("action", "add_collection_children".into()),
        ("id", id.to_string().into()),
        (
            "cids",
            cids.iter()
                .map(|id| id.to_string())
                .collect::<Vec<String>>()
                .into(),
        ),
        ("kind", input.kind.into()),
    ])
    .await;

    let mut parent = db::Collection::with_pk(id);
    parent
        .get_one(
            &app.scylla,
            vec![
                "gid".to_string(),
                "rating".to_string(),
                "status".to_string(),
                "creation_price".to_string(),
            ],
            None,
        )
        .await?;
    if parent.status < 0 {
        return Err(HTTPError::new(
            400,
            "Parent collection is archived".to_string(),
        ));
    }
    if parent.gid != gid {
        return Err(HTTPError::new(403, "Collection gid not match".to_string()));
    }

    let count = db::CollectionChildren::count_children(&app.scylla, id).await?;
    if count + cids.len() > db::MAX_COLLECTION_CHILDREN {
        return Err(HTTPError::new(
            400,
            format!(
                "Parent collection can only have {} children",
                db::MAX_COLLECTION_CHILDREN
            ),
        ));
    }

    let ord = ctx.unix_ms as f64;
    let total = cids.len() as f64;
    let mut processed: Vec<xid::Id> = Vec::with_capacity(cids.len());

    match input.kind {
        0 | 1 => {
            for cid in cids {
                let mut child = db::CreationIndex::with_pk(cid);
                child.get_one(&app.scylla).await?;
                if child.rating > parent.rating {
                    continue;
                }

                if parent.gid == child.gid {
                    // update creation price
                    if parent.creation_price > 0 && child.price == 0 {
                        child.price = parent.creation_price;
                        child.update_field(&app.scylla, "price").await?;
                    }
                }

                if parent.gid != child.gid || input.kind == 1 {
                    // ensure the creation is published
                    let _ = db::PublicationIndex::get_implicit_published(
                        &app.scylla,
                        cid,
                        db::ZERO_ID,
                        Language::Und,
                    )
                    .await?;
                }

                let mut doc = db::CollectionChildren {
                    id,
                    cid,
                    kind: input.kind,
                    ord: ord + processed.len() as f64 / total,
                    ..Default::default()
                };
                let ok = doc.save(&app.scylla).await?;
                if ok {
                    processed.push(cid);
                }
            }
        }
        2 => {
            for cid in cids {
                if cid == id {
                    return Err(HTTPError::new(
                        400,
                        "Parent collection can not be its own child".to_string(),
                    ));
                }

                let mut child = db::Collection::with_pk(cid);
                child
                    .get_one(
                        &app.scylla,
                        vec![
                            "gid".to_string(),
                            "rating".to_string(),
                            "status".to_string(),
                        ],
                        None,
                    )
                    .await?;
                if child.rating > parent.rating {
                    continue;
                }
                if child.gid != parent.gid && child.status < 2 {
                    return Err(HTTPError::new(
                        400,
                        "Child collection is not published".to_string(),
                    ));
                }

                let mut doc = db::CollectionChildren {
                    id,
                    cid,
                    kind: input.kind,
                    ord: ord + processed.len() as f64 / total,
                    ..Default::default()
                };
                let ok = doc.save(&app.scylla).await?;
                if ok {
                    processed.push(cid);
                }
            }
        }
        _ => return Err(HTTPError::new(400, "Invalid collection kind".to_string())),
    }

    ctx.set("added", processed.len().into()).await;
    Ok(to.with(SuccessResponse::new(
        processed.into_iter().map(|id| to.with(id)).collect(),
    )))
}

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct UpdateChildrenInput {
    pub id: PackObject<xid::Id>,
    pub gid: PackObject<xid::Id>,
    pub cid: PackObject<xid::Id>,
    #[validate(range(min = 0))]
    pub ord: f64,
}

pub async fn update_child(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<UpdateChildrenInput>,
) -> Result<PackObject<SuccessResponse<bool>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let id = *input.id.to_owned();
    let gid = *input.gid.to_owned();
    let cid = *input.cid.to_owned();
    ctx.set_kvs(vec![
        ("action", "update_collection_children".into()),
        ("id", id.to_string().into()),
        ("gid", gid.to_string().into()),
        ("cid", cid.to_string().into()),
    ])
    .await;

    let mut parent = db::Collection::with_pk(id);
    parent
        .get_one(
            &app.scylla,
            vec!["gid".to_string(), "status".to_string()],
            None,
        )
        .await?;
    if parent.status < 0 {
        return Err(HTTPError::new(
            400,
            "Parent collection is archived".to_string(),
        ));
    }
    if parent.gid != gid {
        return Err(HTTPError::new(403, "Collection gid not match".to_string()));
    }

    let mut doc = db::CollectionChildren::with_pk(id, cid);
    let ok = doc.update_ord(&app.scylla, input.ord).await?;
    Ok(to.with(SuccessResponse::new(ok)))
}

pub async fn remove_child(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryGidIdCid>,
) -> Result<PackObject<SuccessResponse<bool>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let id = *input.id.to_owned();
    let gid = *input.gid.to_owned();
    let cid = *input.cid.to_owned();

    ctx.set_kvs(vec![
        ("action", "remove_collection_child".into()),
        ("id", id.to_string().into()),
        ("gid", gid.to_string().into()),
        ("cid", cid.to_string().into()),
    ])
    .await;

    let mut parent = db::Collection::with_pk(id);
    parent
        .get_one(
            &app.scylla,
            vec!["gid".to_string(), "status".to_string()],
            None,
        )
        .await?;
    if parent.status < 0 {
        return Err(HTTPError::new(
            400,
            "Parent collection is archived".to_string(),
        ));
    }
    if parent.gid != gid {
        return Err(HTTPError::new(403, "Collection gid not match".to_string()));
    }

    let mut doc = db::CollectionChildren::with_pk(id, cid);
    let ok = doc.delete(&app.scylla).await?;
    Ok(to.with(SuccessResponse::new(ok)))
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct CollectionChildrenOutput {
    pub parent: PackObject<xid::Id>,
    pub gid: PackObject<xid::Id>,
    pub cid: PackObject<xid::Id>,
    pub kind: i8,
    pub ord: f64,
    pub status: i8,
    pub rating: i8,
    pub updated_at: i64,
    pub cover: String,
    pub price: i64,
    pub language: PackObject<Language>,
    pub title: String,
    pub summary: String,
}

pub async fn list_children(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<IDGIDPagination>,
) -> Result<PackObject<SuccessResponse<Vec<CollectionChildrenOutput>>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let id = *input.id.to_owned();
    let gid = *input.gid.to_owned();
    let status = input.status.unwrap_or(0);
    let token = token_to_xid(&input.page_token);
    let page_size = input.page_size.unwrap_or(10) as usize;

    ctx.set_kvs(vec![
        ("action", "list_collection_child".into()),
        ("id", id.to_string().into()),
        ("gid", gid.to_string().into()),
        ("status", status.into()),
    ])
    .await;

    let language = ctx.language.unwrap_or_default();
    let mut children = db::CollectionChildren::list_children(&app.scylla, id).await?;
    let total = children.len();
    if let Some(cid) = token {
        if let Some(i) = children.iter().position(|v| v.cid == cid) {
            children = children.split_off(i + 1);
        } else {
            children.truncate(0);
        }
    }

    let has_next = children.len() > page_size;
    children.truncate(page_size);
    let next_page_token = to.with_option(if has_next {
        token_from_xid(Some(children.last().unwrap().cid))
    } else {
        None
    });

    let mut res: Vec<CollectionChildrenOutput> = Vec::with_capacity(children.len());

    for child in children {
        let mut output = CollectionChildrenOutput {
            parent: to.with(child.id),
            cid: to.with(child.cid),
            kind: child.kind,
            ord: child.ord,
            status: -2,
            ..Default::default()
        };

        match child.kind {
            2 => {
                let mut doc = db::Collection::with_pk(child.cid);
                if doc
                    .get_one(
                        &app.scylla,
                        vec![
                            "gid".to_string(),
                            "status".to_string(),
                            "rating".to_string(),
                            "info".to_string(),
                        ],
                        ctx.language,
                    )
                    .await
                    .is_ok()
                    && doc.status >= status
                {
                    if let Some((lang, info)) = doc.to_info(language) {
                        output.status = doc.status;
                        output.updated_at = doc._info.unwrap().updated_at;
                        output.language = to.with(lang);
                        output.title = info.title;
                        output.summary = info.summary;
                    }
                    output.gid = to.with(doc.gid);
                    output.rating = doc.rating;
                    output.cover = doc.cover;
                    output.price = doc.price;
                };
            }

            1 | 0 => {
                let mut icreation = db::CreationIndex::with_pk(child.cid);
                if icreation.get_one(&app.scylla).await.is_ok() {
                    output.rating = icreation.rating;
                    output.price = icreation.price;

                    match db::PublicationIndex::get_implicit_published(
                        &app.scylla,
                        child.cid,
                        db::ZERO_ID,
                        language,
                    )
                    .await
                    {
                        Ok(ipub) => {
                            let mut doc = db::Publication::with_pk(
                                ipub.gid,
                                ipub.cid,
                                ipub.language,
                                ipub.version,
                            );
                            doc.get_one(
                                &app.scylla,
                                vec![
                                    "status".to_string(),
                                    "title".to_string(),
                                    "summary".to_string(),
                                    "updated_at".to_string(),
                                ],
                            )
                            .await?;
                            output.gid = to.with(doc.gid);
                            output.status = 2;
                            output.updated_at = doc.updated_at;
                            output.language = to.with(doc.language);
                            output.title = doc.title;
                            output.summary = doc.summary;
                            output.kind = 1;
                        }
                        _ => {
                            // get for owner
                            if child.kind == 0 && icreation.gid == gid {
                                let mut doc = db::Creation::with_pk(icreation.gid, icreation.id);
                                doc.get_one(
                                    &app.scylla,
                                    vec![
                                        "status".to_string(),
                                        "title".to_string(),
                                        "summary".to_string(),
                                        "updated_at".to_string(),
                                    ],
                                )
                                .await?;
                                if doc.status >= status {
                                    output.gid = to.with(doc.gid);
                                    output.status = doc.status;
                                    output.updated_at = doc.updated_at;
                                    output.language = to.with(doc.language);
                                    output.title = doc.title;
                                    output.summary = doc.summary;
                                    output.kind = 0;
                                }
                            };
                        }
                    }
                }
            }
            _ => {}
        }

        if (output.status == 2 && output.rating <= ctx.rating)
            || (output.status >= status && *output.gid == gid)
        {
            res.push(output)
        }
    }

    Ok(to.with(SuccessResponse {
        total_size: Some(total as u64),
        next_page_token,
        result: res,
    }))
}

pub async fn list_by_child(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryGidCid>,
) -> Result<PackObject<SuccessResponse<Vec<CollectionOutput>>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let gid = *input.gid.to_owned();
    let cid = *input.cid.to_owned();
    let status = input.status.unwrap_or(0);
    let fields = get_fields(input.fields.to_owned());

    ctx.set_kvs(vec![
        ("action", "list_collection_by_child".into()),
        ("gid", gid.to_string().into()),
        ("cid", cid.to_string().into()),
    ])
    .await;

    let children = db::CollectionChildren::list_by_child(&app.scylla, cid).await?;
    let mut res: Vec<CollectionOutput> = Vec::with_capacity(children.len());

    for child in children {
        let mut doc = db::Collection::with_pk(child.id);
        if doc
            .get_one(&app.scylla, fields.clone(), ctx.language)
            .await
            .is_ok()
            && ((doc.status == 2 && doc.rating <= ctx.rating)
                || (doc.status >= status && doc.gid == gid))
        {
            let price = doc.price;
            let subscription = if price > 0 && ctx.user > db::MIN_ID && doc.gid != gid {
                let mut subscription = db::CollectionSubscription::with_pk(ctx.user, doc.id);
                if subscription.get_one(&app.scylla, vec![]).await.is_ok() {
                    Some(subscription)
                } else {
                    None
                }
            } else {
                None
            };

            let mut output = CollectionOutput::from(doc, &to);
            match subscription {
                Some(s) => {
                    if s.expire_at < ctx.unix_ms as i64 {
                        output.rfp = Some(RFP {
                            creation: None,
                            collection: Some(price),
                        })
                    }
                    output.subscription = Some(SubscriptionOutput {
                        uid: to.with(s.uid),
                        cid: to.with(s.cid),
                        gid: to.with(*output.gid),
                        txn: to.with(s.txn),
                        updated_at: s.updated_at,
                        expire_at: s.expire_at,
                    });
                }
                None => {
                    output.rfp = Some(RFP {
                        creation: None,
                        collection: Some(price),
                    })
                }
            }

            res.push(output)
        };
    }

    Ok(to.with(SuccessResponse::new(res)))
}

pub async fn get_subscription(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryId>,
) -> Result<PackObject<SuccessResponse<SubscriptionOutput>>, HTTPError> {
    input.validate()?;
    valid_user(ctx.user)?;

    let cid = *input.id.to_owned();

    ctx.set_kvs(vec![
        ("action", "get_collection_subscription".into()),
        ("cid", cid.to_string().into()),
    ])
    .await;

    let mut collection = db::Collection::with_pk(cid);
    collection
        .get_one(&app.scylla, vec!["status".to_string()], None)
        .await?;

    let mut doc = db::CollectionSubscription::with_pk(ctx.user, cid);
    doc.get_one(&app.scylla, vec![]).await?;
    Ok(to.with(SuccessResponse::new(SubscriptionOutput {
        uid: to.with(doc.uid),
        cid: to.with(doc.cid),
        gid: to.with(collection.gid),
        txn: to.with(doc.txn),
        updated_at: doc.updated_at,
        expire_at: doc.expire_at,
    })))
}

pub async fn update_subscription(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<SubscriptionInput>,
) -> Result<PackObject<SuccessResponse<SubscriptionOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    valid_user(ctx.user)?;

    let uid = *input.uid.to_owned();
    let cid = *input.cid.to_owned();
    let txn = *input.txn.to_owned();

    ctx.set_kvs(vec![
        ("action", "update_collection_subscription".into()),
        ("uid", uid.to_string().into()),
        ("cid", cid.to_string().into()),
        ("txn", txn.to_string().into()),
    ])
    .await;

    let mut collection = db::Collection::with_pk(cid);
    collection
        .get_one(
            &app.scylla,
            vec!["status".to_string(), "rating".to_string()],
            None,
        )
        .await?;
    if collection.status != 2 {
        return Err(HTTPError::new(
            400,
            "Collection is not published".to_string(),
        ));
    }
    if ctx.rating < collection.rating {
        return Err(HTTPError::new(451, "Collection unavailable".to_string()));
    }
    let mut doc = db::CollectionSubscription::with_pk(ctx.user, cid);
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

    Ok(to.with(SuccessResponse::new(SubscriptionOutput {
        uid: to.with(doc.uid),
        cid: to.with(doc.cid),
        gid: to.with(collection.gid),
        txn: to.with(doc.txn),
        updated_at: doc.updated_at,
        expire_at: doc.expire_at,
    })))
}
