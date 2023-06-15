use futures::{stream::StreamExt, Stream};
use scylla::{
    frame::value::ValueList,
    statement::{prepared_statement::PreparedStatement, Consistency, SerialConsistency},
    transport::{query_result::QueryResult, Compression, ExecutionProfile},
    Metrics, Session, SessionBuilder,
};
use std::{sync::Arc, time::Duration};

pub use scylla::{
    frame::response::result::{ColumnType, CqlValue, Row},
    query::Query,
};

pub use scylla_orm::{Ascii, ColumnsMap, CqlValueSerder};

use crate::conf;

pub struct ScyllaDB {
    session: Session,
}

impl ScyllaDB {
    pub async fn new(cfg: conf::ScyllaDB, keyspace: &str) -> anyhow::Result<Self> {
        // use tls https://github.com/scylladb/scylla-rust-driver/blob/main/examples/tls.rs

        let handle = ExecutionProfile::builder()
            .consistency(Consistency::LocalQuorum)
            .serial_consistency(Some(SerialConsistency::LocalSerial))
            .request_timeout(Some(Duration::from_secs(5)))
            .build()
            .into_handle();

        let session: Session = SessionBuilder::new()
            .known_nodes(&cfg.nodes)
            .user(cfg.username, cfg.password)
            .compression(Some(Compression::Lz4))
            .default_execution_profile_handle(handle)
            .build()
            .await?;

        session.use_keyspace(keyspace, false).await?;

        Ok(Self { session })
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.session.get_metrics()
    }

    pub async fn execute(
        &self,
        query: impl Into<Query>,
        params: impl ValueList,
    ) -> anyhow::Result<QueryResult> {
        let mut prepared: PreparedStatement = self.session.prepare(query).await?;

        prepared.set_consistency(Consistency::One);
        let res = self.session.execute(&prepared, params).await?;
        Ok(res)
    }

    pub async fn execute_iter(
        &self,
        query: impl Into<Query>,
        params: impl ValueList,
    ) -> anyhow::Result<Vec<Row>> {
        let mut prepared: PreparedStatement = self.session.prepare(query).await?;

        prepared.set_consistency(Consistency::One);
        let mut rows_stream = self.session.execute_iter(prepared, params).await?;

        let (capacity, _) = rows_stream.size_hint();
        let mut rows: Vec<Row> = Vec::with_capacity(capacity);
        while let Some(next_row) = rows_stream.next().await {
            rows.push(next_row?);
        }
        Ok(rows)
    }
}

pub fn extract_applied(res: QueryResult) -> bool {
    let res = res
        .single_row()
        .map(|r| r.columns[0].as_ref().and_then(|r| r.as_boolean()))
        .unwrap_or(Some(false));
    res == Some(true)
}
