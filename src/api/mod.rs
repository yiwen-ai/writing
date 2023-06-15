use axum::extract::State;

use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::HashMap, str::FromStr, sync::Arc};
use validator::ValidationError;

use crate::db;

use crate::object::{Object, ObjectType};

pub mod creation;

pub const APP_NAME: &str = env!("CARGO_PKG_NAME");
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

pub static JARVIS: &str = "jarvis00000000000000";

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

#[derive(Serialize, Deserialize)]
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

pub async fn version(ct: ObjectType) -> Object<AppVersion> {
    Object(
        ct.or_default(),
        AppVersion {
            name: APP_NAME.to_string(),
            version: APP_VERSION.to_string(),
        },
    )
}

pub async fn healthz(State(app): State<Arc<AppState>>, ct: ObjectType) -> Object<AppInfo> {
    let m = app.scylla.metrics();
    Object(
        ct.or_default(),
        AppInfo {
            scylla_latency_avg_ms: m.get_latency_avg_ms().unwrap_or(0),
            scylla_latency_p99_ms: m.get_latency_percentile_ms(99.0f64).unwrap_or(0),
            scylla_latency_p90_ms: m.get_latency_percentile_ms(90.0f64).unwrap_or(0),
            scylla_errors_num: m.get_errors_num(),
            scylla_queries_num: m.get_queries_num(),
            scylla_errors_iter_num: m.get_errors_iter_num(),
            scylla_queries_iter_num: m.get_queries_iter_num(),
            scylla_retries_num: m.get_retries_num(),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_xid() {
        assert!(validate_xid(JARVIS).is_ok());

        let id = "00000000000000jarvis";
        let res = validate_xid("00000000000000jarvis");
        assert!(res.is_err());
        assert!(res.unwrap_err().message.unwrap().contains(id));
    }
}
