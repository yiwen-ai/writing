use axum::extract::State;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use validator::Validate;

use axum_web::object::PackObject;

use crate::db;

pub mod creation;
pub mod publication;
pub mod publication_draft;

pub const APP_NAME: &str = env!("CARGO_PKG_NAME");
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

pub static USER_JARVIS: &str = "0000000000000jarvis0"; // system user
pub static USER_ANON: &str = "000000000000000anon0"; // anonymous user

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

pub async fn version(to: PackObject<()>, State(_): State<Arc<AppState>>) -> PackObject<AppVersion> {
    to.with(AppVersion {
        name: APP_NAME.to_string(),
        version: APP_VERSION.to_string(),
    })
}

pub async fn healthz(to: PackObject<()>, State(app): State<Arc<AppState>>) -> PackObject<AppInfo> {
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
    pub id: PackObject<xid::Id>,
    pub gid: PackObject<xid::Id>,
    pub fields: Option<String>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct QueryIdGidVersion {
    pub id: PackObject<xid::Id>,
    pub gid: PackObject<xid::Id>,
    #[validate(range(min = 1, max = 10000))]
    pub version: i16,
}

#[derive(Debug, Deserialize, Validate)]
pub struct QueryIdLanguageVersion {
    pub id: PackObject<xid::Id>,
    pub language: PackObject<isolang::Language>,
    #[validate(range(min = 0, max = 10000))] // 0 means latest
    pub version: i16,
    pub fields: Option<String>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct Pagination {
    pub gid: PackObject<xid::Id>,
    pub page_token: Option<PackObject<xid::Id>>,
    #[validate(range(min = 2, max = 1000))]
    pub page_size: Option<u16>,
    #[validate(range(min = -1, max = 2))]
    pub status: Option<i8>,
    pub fields: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdateStatusInput {
    pub id: PackObject<xid::Id>,
    pub gid: PackObject<xid::Id>,
    #[validate(range(min = -1, max = 2))]
    pub status: i8,
    pub updated_at: i64,
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdatePublicationStatusInput {
    pub id: PackObject<xid::Id>,
    pub language: PackObject<isolang::Language>,
    #[validate(range(min = 1, max = 10000))] // 0 means latest
    pub version: i16,
    #[validate(range(min = -1, max = 2))]
    pub status: i8,
    pub updated_at: i64,
}
