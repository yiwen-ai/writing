use axum::extract::State;

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use validator::Validate;

use axum_web::object::{cbor_from_slice, cbor_to_vec, PackObject};

use crate::db;

pub mod bookmark;
pub mod creation;
pub mod publication;
pub mod search;

mod content;
pub use content::{
    validate_cbor_content, AttrValue, DocumentNode, PartialNode, MAX_CONTENT_LEN,
    MAX_CREATION_CONTENT_LEN,
};

pub const APP_NAME: &str = env!("CARGO_PKG_NAME");
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone)]
pub struct AppState {
    pub start_at: u64,
    pub scylla: Arc<db::scylladb::ScyllaDB>,
    pub meili: Arc<db::meili::MeiliSearch>,
}

#[derive(Serialize, Deserialize)]
pub struct AppVersion {
    pub name: String,
    pub version: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct AppInfo {
    pub start_at: u64,
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
        start_at: app.start_at,
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
pub struct QueryId {
    pub id: PackObject<xid::Id>,
    pub fields: Option<String>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct QueryCid {
    pub cid: PackObject<xid::Id>,
    pub fields: Option<String>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct QueryGidId {
    pub gid: PackObject<xid::Id>,
    pub id: PackObject<xid::Id>,
    pub fields: Option<String>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct QueryGidCid {
    pub gid: PackObject<xid::Id>,
    pub cid: PackObject<xid::Id>,
    pub status: Option<i8>,
    pub fields: Option<String>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct Pagination {
    pub page_token: Option<PackObject<Vec<u8>>>,
    #[validate(range(min = 2, max = 1000))]
    pub page_size: Option<u16>,
    #[validate(range(min = -1, max = 2))]
    pub status: Option<i8>,
    pub fields: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct GIDPagination {
    pub gid: PackObject<xid::Id>,
    pub page_token: Option<PackObject<Vec<u8>>>,
    #[validate(range(min = 2, max = 1000))]
    pub page_size: Option<u16>,
    #[validate(range(min = -1, max = 2))]
    pub status: Option<i8>,
    pub fields: Option<Vec<String>>,
}

pub fn token_to_xid(page_token: &Option<PackObject<Vec<u8>>>) -> Option<xid::Id> {
    match page_token.as_ref().map(|v| v.unwrap_ref()) {
        Some(v) => cbor_from_slice::<PackObject<xid::Id>>(v)
            .ok()
            .map(|v| v.unwrap()),
        _ => None,
    }
}

pub fn token_from_xid(id: Option<xid::Id>) -> Option<Vec<u8>> {
    match id {
        Some(id) => cbor_to_vec(&PackObject::Cbor(id)).ok(),
        _ => None,
    }
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdateStatusInput {
    pub id: PackObject<xid::Id>,
    pub gid: Option<PackObject<xid::Id>>,
    #[validate(range(min = -1, max = 2))]
    pub status: i8,
    pub updated_at: i64,
}

pub fn get_fields(fields: Option<String>) -> Vec<String> {
    if fields.is_none() {
        return vec![];
    }
    let fields = fields.unwrap();
    let fields = fields.trim();
    if fields.is_empty() {
        return vec![];
    }
    fields.split(',').map(|s| s.trim().to_string()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use faster_hex::hex_string;

    #[test]
    fn get_fields_works() {
        assert_eq!(get_fields(None), Vec::<String>::new());
        assert_eq!(get_fields(Some("".to_string())), Vec::<String>::new());
        assert_eq!(get_fields(Some(" ".to_string())), Vec::<String>::new());
        assert_eq!(get_fields(Some(" id".to_string())), vec!["id".to_string()]);
        assert_eq!(
            get_fields(Some("id, gid".to_string())),
            vec!["id".to_string(), "gid".to_string()]
        );
        assert_eq!(
            get_fields(Some("id,gid,version".to_string())),
            vec!["id".to_string(), "gid".to_string(), "version".to_string()]
        );
    }

    #[test]
    fn token_to_xid_works() {
        let input = xid::new();
        let v = token_from_xid(Some(input)).unwrap();
        assert_eq!(hex_string(&v).len(), 26);
        let rt = token_to_xid(&Some(PackObject::Cbor(v)));
        assert_eq!(rt, Some(input));
        let rt = token_to_xid(&Some(PackObject::Cbor(vec![0x41, 0x02])));
        assert_eq!(rt, None);
        let rt = token_to_xid(&None);
        assert_eq!(rt, None);
    }

    // #[test]
    // fn token_to_publication_works() {
    //     let input = (xid::new(), Language::Zho, 9i16);
    //     let v = token_from_publication(input).unwrap();
    //     assert_eq!(hex_string(&v).len(), 38);
    //     let rt = token_to_publication(&Some(PackObject::Cbor(v)));
    //     assert_eq!(rt, Some(input));
    //     let rt = token_to_publication(&Some(PackObject::Cbor(vec![0x41, 0x02])));
    //     assert_eq!(rt, None);
    //     let rt = token_to_publication(&None);
    //     assert_eq!(rt, None);
    // }
}
