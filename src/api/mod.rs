use axum::extract::State;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::HashMap, str::FromStr, sync::Arc};
use validator::{Validate, ValidationError};

use axum_web::object::TypedObject;

use crate::db;

pub mod creation;
pub mod publication;
pub mod publication_draft;

pub const APP_NAME: &str = env!("CARGO_PKG_NAME");
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

pub static USER_JARVIS: &str = "0000000000000jarvis0"; // system user
pub static USER_ANON: &str = "000000000000000anon0"; // anonymous user

pub fn validate_xid(id: &str) -> Result<(), ValidationError> {
    let _ = xid::Id::from_str(id).map_err(|er| ValidationError {
        code: Cow::from("xid"),
        message: Some(Cow::from(format!("Invalid xid: {}, {:?}", id, er))),
        params: HashMap::new(),
    })?;

    Ok(())
}

pub fn validate_language(lang: &str) -> Result<(), ValidationError> {
    let _ = isolang::Language::from_str(lang).map_err(|er| ValidationError {
        code: Cow::from("isolang"),
        message: Some(Cow::from(format!("Invalid language: {}, {:?}", lang, er))),
        params: HashMap::new(),
    })?;

    Ok(())
}

pub fn validate_cbor(data: &[u8]) -> Result<(), ValidationError> {
    let _: ciborium::Value = ciborium::from_reader(data).map_err(|er| ValidationError {
        code: Cow::from("cbor"),
        message: Some(Cow::from(format!("Invalid CBOR data, {:?}", er))),
        params: HashMap::new(),
    })?;

    Ok(())
}

pub struct AppState {
    pub scylla: db::scylladb::ScyllaDB,
}

#[derive(Serialize, Deserialize)]
pub struct AppVersion {
    pub name: String,
    pub version: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct AppInfo {
    // https://docs.rs/scylla/latest/scylla/struct.Metrics.html
    pub scylla_latency_avg_ms: u64,
    pub scylla_latency_p99_ms: u64,
    pub scylla_latency_p90_ms: u64,
    pub scylla_errors_num: u64,
    pub scylla_queries_num: u64,
    pub scylla_errors_iter_num: u64,
    pub scylla_queries_iter_num: u64,
    pub scylla_retries_num: u64,
}

pub async fn version(
    to: TypedObject<()>,
    State(_): State<Arc<AppState>>,
) -> TypedObject<AppVersion> {
    to.with(AppVersion {
        name: APP_NAME.to_string(),
        version: APP_VERSION.to_string(),
    })
}

pub async fn healthz(
    to: TypedObject<()>,
    State(app): State<Arc<AppState>>,
) -> TypedObject<AppInfo> {
    let m = app.scylla.metrics();
    to.with(AppInfo {
        scylla_latency_avg_ms: m.get_latency_avg_ms().unwrap_or(0),
        scylla_latency_p99_ms: m.get_latency_percentile_ms(99.0f64).unwrap_or(0),
        scylla_latency_p90_ms: m.get_latency_percentile_ms(90.0f64).unwrap_or(0),
        scylla_errors_num: m.get_errors_num(),
        scylla_queries_num: m.get_queries_num(),
        scylla_errors_iter_num: m.get_errors_iter_num(),
        scylla_queries_iter_num: m.get_queries_iter_num(),
        scylla_retries_num: m.get_retries_num(),
    })
}

#[derive(Debug, Deserialize, Validate)]
pub struct QueryIdGid {
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub id: String,
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub gid: String,
    pub fields: Option<String>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct QueryIdGidVersion {
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub id: String,
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub gid: String,
    #[validate(range(min = 1, max = 10000))]
    pub version: i16,
}

#[derive(Debug, Deserialize, Validate)]
pub struct QueryIdLanguageVersion {
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub id: String,
    #[validate(length(min = 2), custom = "validate_language")]
    pub language: String,
    #[validate(range(min = 0, max = 10000))] // 0 means latest
    pub version: i16,
    pub fields: Option<String>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct Pagination {
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub gid: String,
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub page_token: Option<String>,
    #[validate(range(min = 2, max = 1000))]
    pub page_size: Option<u16>,
    #[validate(range(min = -1, max = 2))]
    pub status: Option<i8>,
    pub fields: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdateStatusInput {
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub id: String,
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub gid: String,
    #[validate(range(min = -1, max = 2))]
    pub status: i8,
    pub updated_at: i64,
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdatePublicationStatusInput {
    #[validate(length(equal = 20), custom = "validate_xid")]
    pub id: String,
    #[validate(length(min = 2), custom = "validate_language")]
    pub language: String,
    #[validate(range(min = 1, max = 10000))] // 0 means latest
    pub version: i16,
    #[validate(range(min = -1, max = 2))]
    pub status: i8,
    pub updated_at: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_xid() {
        assert!(validate_xid(USER_JARVIS).is_ok());
        assert!(validate_xid(USER_ANON).is_ok());

        let id = "00000000000000jarvis";
        let res = validate_xid("00000000000000jarvis");
        assert!(res.is_err());
        assert!(res.unwrap_err().message.unwrap().contains(id));
    }
}
