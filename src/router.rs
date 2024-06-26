use axum::{
    middleware,
    response::{IntoResponse, Response},
    routing, Router,
};
use std::sync::Arc;
use tower::ServiceBuilder;
use tower_http::{
    catch_panic::CatchPanicLayer,
    compression::{predicate::SizeAbove, CompressionLayer},
};

use axum_web::context;
use axum_web::encoding;
use axum_web::erring;

use crate::api;
use crate::conf;
use crate::db;

pub async fn todo() -> Response {
    (erring::HTTPError::new(501, "TODO".to_string())).into_response()
}

pub async fn new(cfg: conf::Conf) -> anyhow::Result<(Arc<api::AppState>, Router)> {
    let keyspace = if cfg.env == "test" {
        "writing_test"
    } else {
        "writing"
    };

    let scylla = db::scylladb::ScyllaDB::new(cfg.scylla, keyspace).await?;
    let meili = db::meili::MeiliSearch::new(cfg.meili).await?;

    let app_state = Arc::new(api::AppState {
        start_at: context::unix_ms(),
        scylla: Arc::new(scylla),
        meili: Arc::new(meili),
    });

    let mds = ServiceBuilder::new()
        .layer(CatchPanicLayer::new())
        .layer(middleware::from_fn(context::middleware))
        .layer(CompressionLayer::new().compress_when(SizeAbove::new(encoding::MIN_ENCODING_SIZE)));

    let app = Router::new()
        .route("/", routing::get(api::version))
        .route("/healthz", routing::get(api::healthz))
        .route("/v1/search", routing::get(api::search::search))
        .route(
            "/v1/search/in_group",
            routing::get(api::search::group_search),
        )
        .route(
            "/v1/search/by_original_url",
            routing::get(api::search::original_search),
        )
        .nest(
            "/v1/creation",
            Router::new()
                .route(
                    "/",
                    routing::post(api::creation::create)
                        .get(api::creation::get)
                        .patch(api::creation::update)
                        .delete(api::creation::delete),
                )
                .route("/list", routing::post(api::creation::list))
                .route(
                    "/update_status",
                    routing::patch(api::creation::update_status),
                )
                .route(
                    "/update_content",
                    routing::put(api::creation::update_content).patch(todo),
                )
                .route(
                    "/subscription",
                    routing::put(api::creation::update_subscription)
                        .get(api::creation::get_subscription),
                ),
        )
        .nest(
            "/v1/publication",
            Router::new()
                .route(
                    "/",
                    routing::post(api::publication::create)
                        .get(api::publication::get)
                        .patch(api::publication::update)
                        .delete(api::publication::delete),
                )
                .route(
                    "/implicit_get",
                    routing::get(api::publication::implicit_get),
                )
                .route("/publish", routing::get(api::publication::get_publish_list))
                .route(
                    "/count_publish",
                    routing::post(api::publication::count_publish),
                )
                .route("/list", routing::post(api::publication::list))
                .route(
                    "/list_by_gids",
                    routing::post(api::publication::list_by_gids),
                )
                .route("/list_latest", routing::post(api::publication::list_latest))
                .route(
                    "/update_status",
                    routing::patch(api::publication::update_status),
                )
                .route(
                    "/update_content",
                    routing::put(api::publication::update_content).patch(todo),
                ),
        )
        .nest(
            "/beta/publication",
            Router::new()
                .route(
                    "/implicit_get",
                    routing::get(api::publication::implicit_get),
                )
                .route("/publish", routing::get(api::publication::get_publish_list))
                .route(
                    "/count_publish",
                    routing::post(api::publication::count_publish),
                )
                .route(
                    "/list_by_gids",
                    routing::post(api::publication::list_by_gids),
                )
                .route("/list_latest", routing::post(api::publication::list_latest)),
        )
        .nest(
            "/v1/message",
            Router::new().route(
                "/",
                routing::post(api::message::create)
                    .get(api::message::get)
                    .patch(api::message::update)
                    .delete(api::message::delete),
            ),
        )
        .nest(
            "/v1/collection",
            Router::new()
                .route(
                    "/",
                    routing::post(api::collection::create)
                        .get(api::collection::get)
                        .patch(api::collection::update)
                        .delete(api::collection::delete),
                )
                .route(
                    "/info",
                    routing::get(api::collection::get_info).patch(api::collection::update_info),
                )
                .route(
                    "/update_status",
                    routing::patch(api::collection::update_status),
                )
                .route(
                    "/child",
                    routing::post(api::collection::add_children)
                        .patch(api::collection::update_child)
                        .delete(api::collection::remove_child),
                )
                .route("/list", routing::post(api::collection::list))
                .route("/list_latest", routing::post(api::collection::list_latest))
                .route(
                    "/list_children",
                    routing::post(api::collection::list_children),
                )
                .route(
                    "/list_by_child",
                    routing::get(api::collection::list_by_child),
                )
                .route(
                    "/subscription",
                    routing::put(api::collection::update_subscription)
                        .get(api::collection::get_subscription),
                ),
        )
        .nest(
            "/v1/bookmark",
            Router::new()
                .route(
                    "/",
                    routing::post(api::bookmark::create)
                        .get(api::bookmark::get)
                        .patch(api::bookmark::update)
                        .delete(api::bookmark::delete),
                )
                .route("/by_cid", routing::get(api::bookmark::get_by_cid))
                .route("/list", routing::post(api::bookmark::list)),
        )
        .nest(
            "/v1/sys",
            Router::new()
                .route("/creation", routing::patch(todo).delete(todo))
                .route("/publication", routing::patch(todo).delete(todo))
                .route("/publication/comment", routing::patch(todo).delete(todo))
                .route("/bookmark", routing::patch(todo).delete(todo)),
        )
        .route_layer(mds)
        .with_state(app_state.clone());

    Ok((app_state, app))
}

#[cfg(test)]
mod tests {
    use axum::http::{self, header::HeaderValue, StatusCode};
    use base64::{engine::general_purpose, Engine as _};
    use ciborium::cbor;
    use serde_json::json;
    use std::net::SocketAddr;
    use std::net::TcpListener;
    use std::str::FromStr;
    use tokio::sync::OnceCell;
    use tokio::time;

    use crate::conf;
    use axum_web::erring;

    use super::*;

    static SERVER: OnceCell<(SocketAddr, reqwest::Client)> = OnceCell::const_new();

    async fn get_server() -> &'static (SocketAddr, reqwest::Client) {
        SERVER
            .get_or_init(|| async {
                let cfg = conf::Conf::new().unwrap_or_else(|err| panic!("config error: {}", err));
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let addr = listener.local_addr().unwrap();
                let (_, app) = new(cfg).await.unwrap();

                tokio::spawn(async move {
                    let res = axum::Server::from_tcp(listener)
                        .unwrap()
                        .serve(app.into_make_service())
                        .await;
                    println!("server error: {:?}", res);
                });

                time::sleep(time::Duration::from_millis(100)).await;
                (
                    addr,
                    reqwest::ClientBuilder::new().gzip(true).build().unwrap(),
                )
            })
            .await
    }

    fn encode_cbor(val: &ciborium::Value) -> anyhow::Result<Vec<u8>> {
        let mut buf: Vec<u8> = Vec::new();
        ciborium::into_writer(val, &mut buf)?;
        Ok(buf)
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore]
    async fn test_all() {
        // problem: https://users.rust-lang.org/t/tokio-runtimes-and-tokio-oncecell/91351/5
        healthz_api_works().await;
        api_works_with_json_and_cbor().await;
    }

    async fn healthz_api_works() {
        let (addr, client) = get_server().await;
        println!("addr: {:?}", addr);

        // time::sleep(time::Duration::from_secs(100)).await;

        let res = client
            .get(format!("http://{}/healthz", addr))
            .header(
                http::header::ACCEPT,
                HeaderValue::from_static("application/json"),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get("content-type").unwrap(),
            "application/json"
        );
        let body = res.bytes().await.unwrap();
        let json_obj: api::AppInfo = serde_json::from_slice(&body).unwrap();

        let res = client
            .get(format!("http://{}/healthz", addr))
            .header(
                http::header::ACCEPT,
                HeaderValue::from_static("application/cbor"),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get("content-type").unwrap(),
            "application/cbor"
        );
        let body = res.bytes().await.unwrap();
        let cbor_obj: api::AppInfo = ciborium::from_reader(&body[..]).unwrap();

        assert_eq!(json_obj.start_at, cbor_obj.start_at);
    }

    async fn api_works_with_json_and_cbor() {
        let (addr, client) = get_server().await;

        let content = encode_cbor(
            &cbor!({
                "type" => "doc",
                "content" => [{
                    "type" => "heading",
                    "attrs" => {
                        "id" => "Y3T1Ik",
                        "level" => 1u8,
                    },
                    "content" => [{
                        "type" => "text",
                        "text" => "Hello World",
                    }],
                }],
            })
            .unwrap(),
        )
        .unwrap();

        let content_base64 = "omR0eXBlY2RvY2djb250ZW50gaNkdHlwZWdoZWFkaW5nZWF0dHJzomJpZGZZM1QxSWtlbGV2ZWwBZ2NvbnRlbnSBomR0eXBlZHRleHRkdGV4dGtIZWxsbyBXb3JsZA";
        assert_eq!(
            general_purpose::URL_SAFE_NO_PAD.encode(&content).as_str(),
            content_base64
        );

        let res = client
            .post(format!("http://{}/v1/creation", addr))
            .header(
                http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )
            .header("x-auth-user", db::USER_JARVIS)
            .json(&json!({
                "gid": "jarvis00000000000000",
                "language": "en",
                "title": "test json",
                "content": content_base64
            }))
            .send()
            .await
            .unwrap();
        if res.status() != StatusCode::OK {
            panic!("response: {:?}", res.text().await.unwrap());
        }

        assert_eq!(
            res.headers().get("content-type").unwrap(),
            "application/json"
        );
        let body = res.bytes().await.unwrap();
        let json_obj: erring::SuccessResponse<api::creation::CreationOutput> =
            serde_json::from_slice(&body).unwrap();
        let json_obj = json_obj.result;
        assert_eq!(json_obj.title.unwrap().as_str(), "test json");

        let req_body = encode_cbor(
            &cbor!({
                "gid" => ciborium::Value::Bytes(xid::Id::from_str("jarvis00000000000000").unwrap().as_bytes().to_vec()),
                "language" => "eng",
                "title" => "test cbor",
                "content" => ciborium::Value::Bytes(content),
            })
            .unwrap(),
        ).unwrap();

        let res = client
            .post(format!("http://{}/v1/creation", addr))
            .header(
                http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/cbor"),
            )
            .header("x-auth-user", db::USER_JARVIS)
            .body(req_body)
            .send()
            .await
            .unwrap();
        if res.status() != StatusCode::OK {
            panic!("response: {:?}", res.text().await.unwrap());
        }

        assert_eq!(
            res.headers().get("content-type").unwrap(),
            "application/cbor"
        );
        let body = res.bytes().await.unwrap();
        let cbor_obj: erring::SuccessResponse<api::creation::CreationOutput> =
            ciborium::from_reader(&body[..]).unwrap();
        let cbor_obj = cbor_obj.result;
        assert_eq!(cbor_obj.title.unwrap().as_str(), "test cbor");

        assert_eq!(json_obj.gid.unwrap(), cbor_obj.gid.unwrap());
        assert_ne!(json_obj.id.unwrap(), cbor_obj.id.unwrap());
        assert_eq!(
            json_obj.language.unwrap().unwrap(),
            cbor_obj.language.unwrap().unwrap()
        );
        assert_eq!(
            json_obj.content.unwrap().unwrap(),
            cbor_obj.content.unwrap().unwrap()
        );
    }
}
