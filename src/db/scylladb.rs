use scylla::{
    cql_to_rust::{FromCqlVal, FromCqlValError},
    frame::response::result::Row,
    frame::value::ValueList,
    statement::{prepared_statement::PreparedStatement, Consistency, SerialConsistency},
    transport::{
        errors::QueryError, query_result::QueryResult, query_result::SingleRowError, Compression,
        ExecutionProfile,
    },
    Metrics, Session, SessionBuilder,
};
use serde::{de::DeserializeOwned, Serialize};

use std::{
    collections::{btree_map::Iter, BTreeMap},
    sync::Arc,
    time::Duration,
};

pub use scylla::{
    frame::response::result::{ColumnType, CqlValue},
    query::Query,
};

pub use super::scylla_helper::{Ascii, ColumnsMap, CqlValueSerder};

use crate::conf;
use crate::erring::HTTPError;

use super::ToAnyhowError;

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
        match self.session.execute(&prepared, params).await {
            Ok(result) => Ok(result),
            Err(err) => Err(err.to_anyhow_error()),
        }
    }
}

// TODO https://docs.rs/scylla/latest/scylla/transport/errors/enum.QueryError.html
impl ToAnyhowError for QueryError {
    fn to_anyhow_error(self) -> anyhow::Error {
        match self {
            QueryError::DbError(dberr, msg) => anyhow::Error::new(HTTPError {
                code: 500,
                message: msg,
                data: Some(serde_json::Value::String(dberr.to_string())),
            }),
            _ => anyhow::Error::new(HTTPError {
                code: 500,
                message: self.to_string(),
                data: None,
            }),
        }
    }
}

impl ToAnyhowError for SingleRowError {
    fn to_anyhow_error(self) -> anyhow::Error {
        anyhow::Error::new(HTTPError {
            code: 404,
            message: self.to_string(),
            data: None,
        })
    }
}

impl ToAnyhowError for FromCqlValError {
    fn to_anyhow_error(self) -> anyhow::Error {
        anyhow::Error::new(HTTPError {
            code: 422,
            message: self.to_string(),
            data: None,
        })
    }
}
