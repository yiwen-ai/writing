use std::{net::SocketAddr, sync::Arc};

use axum::{http::header::HeaderName, middleware, routing::get, routing::post, Router};
use structured_logger::{async_json::new_writer, Builder};
use tokio::{
    io, signal,
};
use tower::ServiceBuilder;
use tower_http::{
    catch_panic::CatchPanicLayer, compression::CompressionLayer,
    propagate_header::PropagateHeaderLayer,
};

mod api;
mod conf;
mod db;

use axum_web::context;
use axum_web::erring;
use axum_web::object;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    let cfg = conf::Conf::new().unwrap_or_else(|err| panic!("config error: {}", err));

    Builder::with_level(cfg.log.level.as_str())
        .with_target_writer("*", new_writer(io::stdout()))
        .init();

    log::debug!("{:?}", cfg);

    let keyspace = if cfg.env == "test" {
        "writing_test"
    } else {
        "writing"
    };
    let scylla = db::scylladb::ScyllaDB::new(cfg.scylla, keyspace).await?;

    let app_state = Arc::new(api::AppState { scylla });

    let mds = ServiceBuilder::new()
        .layer(middleware::from_fn(context::middleware))
        .layer(CatchPanicLayer::new())
        .layer(CompressionLayer::new())
        .layer(PropagateHeaderLayer::new(HeaderName::from_static(
            "x-request-id",
        )));

    let app = Router::new()
        .route("/", get(api::version))
        .route("/healthz", get(api::healthz))
        .route("/v1/creation", post(api::creation::create_creation))
        .route_layer(mds)
        .with_state(app_state.clone());

    let addr = SocketAddr::from(([0, 0, 0, 0], cfg.server.port));
    log::info!(
        "{}@{} start {} at {}",
        api::APP_NAME,
        api::APP_VERSION,
        cfg.env,
        &addr
    );
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .with_graceful_shutdown(shutdown_signal(
            app_state.clone(),
            cfg.server.graceful_shutdown,
        ))
        .await?;

    Ok(())
}

async fn shutdown_signal(_app: Arc<api::AppState>, _wait_secs: usize) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    log::info!("signal received, Goodbye!");
}
