use axum::extract::State;
use serde::{Deserialize, Serialize};
use std::{convert::From, str::FromStr, sync::Arc};
use validator::Validate;

use crate::db;
use crate::erring::{HTTPError, SuccessResponse};
use crate::object::Object;

use super::{validate_xid, AppState};

#[derive(Debug, Validate, Serialize, Deserialize)]
pub struct CreateCreationInput {
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub gid: String,
    pub original_url: String,
    pub title: String,
    pub description: String,
    pub cover: Option<String>,
    pub content: Vec<u8>,
    pub keywords: Option<Vec<String>>,
    pub labels: Option<Vec<String>>,
    pub authors: Option<Vec<String>>,
    pub summary: Option<String>,
    pub license: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CreationOutput {
    pub id: String,
    pub gid: String,
    pub status: i8,
    pub rating: i8,
    pub version: i16,
    pub language: String,
    pub creator: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub active_langs: Vec<String>,
    pub original_url: String,
    pub genre: Vec<String>,
    pub title: String,
    pub description: String,
    pub cover: String,
    pub keywords: Vec<String>,
    pub labels: Vec<String>,
    pub authors: Vec<String>,
    pub reviewers: Vec<String>,
    pub summary: String,
    pub content: Vec<u8>,
    pub license: String,
}

impl From<db::Creation> for CreationOutput {
    fn from(val: db::Creation) -> Self {
        Self {
            id: val.id.to_string(),
            gid: val.gid.to_string(),
            status: val.status,
            rating: val.rating,
            version: val.version,
            language: val.language.to_name().to_string(),
            creator: val.creator.to_string(),
            created_at: val.created_at,
            updated_at: val.updated_at,
            active_langs: val
                .active_languages
                .iter()
                .map(|l| l.to_name().to_string())
                .collect(),
            original_url: val.original_url,
            genre: val.genre,
            title: val.title,
            description: val.description,
            cover: val.cover,
            keywords: val.keywords,
            labels: val.labels,
            authors: val.authors,
            reviewers: val.reviewers.iter().map(|r| r.to_string()).collect(),
            summary: val.summary,
            content: val.content,
            license: val.license,
        }
    }
}

pub async fn create_creation(
    State(_app): State<Arc<AppState>>,
    Object(ct, input): Object<CreateCreationInput>,
) -> Result<Object<SuccessResponse<CreationOutput>>, HTTPError> {
    if let Err(err) = input.validate() {
        return Err(HTTPError {
            code: 400,
            message: format!("{:?}", err),
            data: None,
        });
    }

    let obj = db::Creation {
        id: xid::new(),
        gid: xid::Id::from_str(&input.gid).unwrap(),
        creator: xid::new(),
        ..Default::default()
    };

    // let did = xid_from_str(&input.did)?;
    // let lang = normalize_lang(&input.lang);
    // if Language::from_str(&lang).is_err() {
    //     return Err(HTTPError {
    //         code: 400,
    //         message: format!("unsupported language '{}'", &lang),
    //         data: None,
    //     });
    // }

    // let mut doc = db::Translating::new(did, input.version as i16, lang.clone());
    // doc.fill(&app.scylla, vec![])
    //     .await
    //     .map_err(HTTPError::from)?;

    Ok(Object(
        ct,
        SuccessResponse {
            result: CreationOutput::from(obj),
        },
    ))
}
