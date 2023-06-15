use axum::{
    http::{HeaderMap, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde_json::Value;
use std::{collections::BTreeMap, str::FromStr, sync::Arc, time::Instant};
use tokio::sync::RwLock;
use uuid::Uuid;

pub use structured_logger::unix_ms;

pub struct ReqContext {
    pub rid: String,   // from x-request-id header
    pub user: xid::Id, // from x-user-id header
    pub unix_ms: u64,
    pub start: Instant,
    pub kv: RwLock<BTreeMap<String, Value>>,
}

impl ReqContext {
    pub fn new(rid: &str, user: xid::Id) -> Self {
        Self {
            rid: rid.to_string(),
            user,
            unix_ms: unix_ms(),
            start: Instant::now(),
            kv: RwLock::new(BTreeMap::new()),
        }
    }

    pub async fn set(&self, key: &str, value: Value) {
        let mut kv = self.kv.write().await;
        kv.insert(key.to_string(), value);
    }

    pub async fn set_kvs(&self, kvs: Vec<(&str, Value)>) {
        let mut kv = self.kv.write().await;
        for item in kvs {
            kv.insert(item.0.to_string(), item.1);
        }
    }
}

pub async fn middleware<B>(mut req: Request<B>, next: Next<B>) -> Response {
    let method = req.method().to_string();
    let uri = req.uri().to_string();
    let rid = extract_header(req.headers(), "x-request-id", || Uuid::new_v4().to_string());
    let user = extract_header(req.headers(), "x-user-id", || "".to_string());
    let uid = match xid::Id::from_str(&user) {
        Ok(id) => id,
        Err(err) => {
            return (
                StatusCode::UNAUTHORIZED,
                format!("Invalid x-user-id, {}", err),
            )
                .into_response()
        }
    };

    let ctx = Arc::new(ReqContext::new(&rid, uid));
    req.extensions_mut().insert(ctx.clone());

    let res = next.run(req).await;
    let kv = ctx.kv.read().await;
    let status = res.status().as_u16();
    log::info!(target: "api",
        method = method,
        uri = uri,
        rid = rid,
        user = user,
        status = status,
        start = ctx.unix_ms,
        elapsed = ctx.start.elapsed().as_millis() as u64,
        kv = log::as_serde!(*kv);
        "",
    );

    res
}

pub fn extract_header(hm: &HeaderMap, key: &str, or: impl FnOnce() -> String) -> String {
    match hm.get(key) {
        None => or(),
        Some(v) => match v.to_str() {
            Ok(s) => s.to_string(),
            Err(_) => or(),
        },
    }
}
