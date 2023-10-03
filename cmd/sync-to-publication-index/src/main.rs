use futures::stream::StreamExt;
use scylla_orm::{ColumnsMap, ToCqlVal};
use structured_logger::{async_json::new_writer, Builder};
use tokio::io;
use writing::{conf, db};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    Builder::with_level("debug")
        .with_target_writer("*", new_writer(io::stdout()))
        .init();

    let nodes = std::env::var("SCYLLA_NODES").expect(
        "env SCYLLA_NODES required:\nSCYLLA_NODES=127.0.0.1:9042 ./sync-to-publication-index",
    );

    let cfg = conf::ScyllaDB {
        nodes: nodes.split(',').map(|s| s.to_string()).collect(),
        username: "".to_string(),
        password: "".to_string(),
    };

    let sess = db::scylladb::ScyllaDB::new(cfg, "writing").await?;
    let publication_fields = vec![
        "gid".to_string(),
        "cid".to_string(),
        "language".to_string(),
        "version".to_string(),
        "status".to_string(),
        "model".to_string(),
        "from_language".to_string(),
    ];
    let query = format!("SELECT {} FROM publication", publication_fields.join(","));
    let mut stream = sess.stream(query, ()).await?;
    let mut total: usize = 0;
    let mut fixed: usize = 0;
    let mut synced: usize = 0;

    let update_mode_query =
        "UPDATE publication SET model=? WHERE gid=? AND cid=? AND language=? AND version=?";
    while let Some(row) = stream.next().await {
        let mut cols = ColumnsMap::with_capacity(publication_fields.len());
        cols.fill(row?, &publication_fields)?;
        let mut doc = db::Publication::default();
        doc.fill(&cols);
        total += 1;
        if doc.model != "gpt-3.5" && doc.model != "gpt-4" {
            let params = (
                "gpt-3.5",
                doc.gid.to_cql(),
                doc.cid.to_cql(),
                doc.language.to_cql(),
                doc.version,
            );
            sess.execute(update_mode_query, params).await?;
            fixed += 1;
        }

        if doc.status == 2 {
            let mut idoc = db::PublicationIndex {
                cid: doc.cid,
                language: doc.language,
                original: doc.language == doc.from_language,
                version: doc.version,
                gid: doc.gid,
                ..Default::default()
            };
            let res = idoc.upsert(&sess).await?;
            if res {
                synced += 1;
                println!("doc: {} {} {}", idoc.cid, idoc.language, idoc.version);
            }
        }
    }

    println!("total: {}, fixed: {}, synced: {}", total, fixed, synced);

    Ok(())
}
