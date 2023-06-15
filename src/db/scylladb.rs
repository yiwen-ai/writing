use scylla::{
    frame::value::ValueList,
    statement::{prepared_statement::PreparedStatement, Consistency, SerialConsistency},
    transport::{query_result::QueryResult, Compression, ExecutionProfile},
    Metrics, Session, SessionBuilder,
};

use std::{sync::Arc, time::Duration};

pub use scylla::{
    frame::response::result::{ColumnType, CqlValue},
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
}
