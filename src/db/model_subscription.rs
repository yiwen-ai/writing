use axum_web::context::unix_ms;
use axum_web::erring::HTTPError;
use scylla_orm::{ColumnsMap, CqlValue, ToCqlVal};
use scylla_orm_macros::CqlOrm;

use crate::db::{scylladb, scylladb::extract_applied, MAX_ID};

#[derive(Debug, Default, Clone, CqlOrm, PartialEq)]
pub struct CollectionSubscription {
    pub uid: xid::Id,
    pub cid: xid::Id,
    pub txn: xid::Id,
    pub updated_at: i64,
    pub expire_at: i64,

    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
}

#[derive(Debug, Default, Clone, CqlOrm, PartialEq)]
pub struct CreationSubscription {
    pub uid: xid::Id,
    pub cid: xid::Id,
    pub txn: xid::Id,
    pub updated_at: i64,
    pub expire_at: i64,

    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
}

impl CollectionSubscription {
    pub fn with_pk(uid: xid::Id, cid: xid::Id) -> Self {
        Self {
            uid,
            cid,
            ..Default::default()
        }
    }

    pub fn select_fields(select_fields: Vec<String>, with_pk: bool) -> anyhow::Result<Vec<String>> {
        if select_fields.is_empty() {
            return Ok(Self::fields());
        }

        let fields = Self::fields();
        for field in &select_fields {
            if !fields.contains(field) {
                return Err(HTTPError::new(400, format!("Invalid field: {}", field)).into());
            }
        }

        let mut select_fields = select_fields;
        if with_pk {
            let gid = "uid".to_string();
            if !select_fields.contains(&gid) {
                select_fields.push(gid);
            }
            let id = "cid".to_string();
            if !select_fields.contains(&id) {
                select_fields.push(id);
            }
            return Ok(select_fields);
        }

        Ok(select_fields)
    }

    pub async fn get_one(
        &mut self,
        db: &scylladb::ScyllaDB,
        select_fields: Vec<String>,
    ) -> anyhow::Result<()> {
        let fields = Self::select_fields(select_fields, false)?;
        self._fields = fields.clone();

        let query = format!(
            "SELECT {} FROM collection_subscription WHERE uid=? AND cid=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.uid.to_cql(), self.cid.to_cql());
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, &fields)?;
        self.fill(&cols);

        Ok(())
    }

    pub async fn save(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        self.updated_at = unix_ms() as i64;

        let fields = Self::fields();
        self._fields = fields.clone();

        let mut cols_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut vals_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut params: Vec<&CqlValue> = Vec::with_capacity(fields.len());
        let cols = self.to();

        for field in &fields {
            cols_name.push(field);
            vals_name.push("?");
            params.push(cols.get(field).unwrap());
        }

        let query = format!(
            "INSERT INTO collection_subscription ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                "Subscription save failed, please try again".to_string(),
            )
            .into());
        }

        Ok(true)
    }

    pub async fn update(
        &mut self,
        db: &scylladb::ScyllaDB,
        txn: xid::Id,
        expire_at: i64,
        updated_at: i64,
    ) -> anyhow::Result<bool> {
        let new_updated_at = unix_ms() as i64;
        let query = "UPDATE collection_subscription SET txn=?,expire_at=?,updated_at=? WHERE uid=? AND cid=? IF updated_at=?";
        let params = (
            txn.to_cql(),
            expire_at,
            new_updated_at,
            self.uid.to_cql(),
            self.cid.to_cql(),
            updated_at,
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                "Subscription update failed, please try again".to_string(),
            )
            .into());
        }

        self.txn = txn;
        self.expire_at = expire_at;
        self.updated_at = new_updated_at;
        Ok(true)
    }

    pub async fn list(
        db: &scylladb::ScyllaDB,
        uid: xid::Id,
        select_fields: Vec<String>,
        page_size: u16,
        page_token: Option<xid::Id>,
    ) -> anyhow::Result<Vec<Self>> {
        let fields = Self::select_fields(select_fields, true)?;

        let token = match page_token {
            Some(id) => id,
            None => MAX_ID,
        };

        let query = format!(
            "SELECT {} FROM collection_subscription WHERE uid=? AND cid<? LIMIT ? USING TIMEOUT 3s",
            fields.clone().join(",")
        );
        let params = (uid.to_cql(), token.to_cql(), page_size as i32);
        let rows = db.execute_iter(query, params).await?;

        let mut res: Vec<Self> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut doc = Self::default();
            let mut cols = ColumnsMap::with_capacity(fields.len());
            cols.fill(row, &fields)?;
            doc.fill(&cols);
            doc._fields = fields.clone();
            res.push(doc);
        }

        Ok(res)
    }
}

impl CreationSubscription {
    pub fn with_pk(uid: xid::Id, cid: xid::Id) -> Self {
        Self {
            uid,
            cid,
            ..Default::default()
        }
    }

    pub fn select_fields(select_fields: Vec<String>, with_pk: bool) -> anyhow::Result<Vec<String>> {
        if select_fields.is_empty() {
            return Ok(Self::fields());
        }

        let fields = Self::fields();
        for field in &select_fields {
            if !fields.contains(field) {
                return Err(HTTPError::new(400, format!("Invalid field: {}", field)).into());
            }
        }

        let mut select_fields = select_fields;
        if with_pk {
            let gid = "uid".to_string();
            if !select_fields.contains(&gid) {
                select_fields.push(gid);
            }
            let id = "cid".to_string();
            if !select_fields.contains(&id) {
                select_fields.push(id);
            }
            return Ok(select_fields);
        }

        Ok(select_fields)
    }

    pub async fn get_one(
        &mut self,
        db: &scylladb::ScyllaDB,
        select_fields: Vec<String>,
    ) -> anyhow::Result<()> {
        let fields = Self::select_fields(select_fields, false)?;
        self._fields = fields.clone();

        let query = format!(
            "SELECT {} FROM creation_subscription WHERE uid=? AND cid=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.uid.to_cql(), self.cid.to_cql());
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, &fields)?;
        self.fill(&cols);

        Ok(())
    }

    pub async fn save(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        self.updated_at = unix_ms() as i64;

        let fields = Self::fields();
        self._fields = fields.clone();

        let mut cols_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut vals_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut params: Vec<&CqlValue> = Vec::with_capacity(fields.len());
        let cols = self.to();

        for field in &fields {
            cols_name.push(field);
            vals_name.push("?");
            params.push(cols.get(field).unwrap());
        }

        let query = format!(
            "INSERT INTO creation_subscription ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                "Subscription save failed, please try again".to_string(),
            )
            .into());
        }

        Ok(true)
    }

    pub async fn update(
        &mut self,
        db: &scylladb::ScyllaDB,
        txn: xid::Id,
        expire_at: i64,
        updated_at: i64,
    ) -> anyhow::Result<bool> {
        let new_updated_at = unix_ms() as i64;
        let query = "UPDATE creation_subscription SET txn=?,expire_at=?,updated_at=? WHERE uid=? AND cid=? IF updated_at=?";
        let params = (
            txn.to_cql(),
            expire_at,
            new_updated_at,
            self.uid.to_cql(),
            self.cid.to_cql(),
            updated_at,
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                "Subscription update failed, please try again".to_string(),
            )
            .into());
        }

        self.txn = txn;
        self.expire_at = expire_at;
        self.updated_at = new_updated_at;
        Ok(true)
    }

    pub async fn list(
        db: &scylladb::ScyllaDB,
        uid: xid::Id,
        select_fields: Vec<String>,
        page_size: u16,
        page_token: Option<xid::Id>,
    ) -> anyhow::Result<Vec<Self>> {
        let fields = Self::select_fields(select_fields, true)?;

        let token = match page_token {
            Some(id) => id,
            None => MAX_ID,
        };

        let query = format!(
            "SELECT {} FROM creation_subscription WHERE uid=? AND cid<? LIMIT ? USING TIMEOUT 3s",
            fields.clone().join(",")
        );
        let params = (uid.to_cql(), token.to_cql(), page_size as i32);
        let rows = db.execute_iter(query, params).await?;

        let mut res: Vec<Self> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut doc = Self::default();
            let mut cols = ColumnsMap::with_capacity(fields.len());
            cols.fill(row, &fields)?;
            doc.fill(&cols);
            doc._fields = fields.clone();
            res.push(doc);
        }

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use crate::conf;
    use crate::db;
    use axum_web::erring;
    use tokio::sync::OnceCell;

    use super::*;

    static DB: OnceCell<db::scylladb::ScyllaDB> = OnceCell::const_new();

    async fn get_db() -> &'static db::scylladb::ScyllaDB {
        DB.get_or_init(|| async {
            let cfg = conf::Conf::new().unwrap_or_else(|err| panic!("config error: {}", err));
            let res = db::scylladb::ScyllaDB::new(cfg.scylla, "writing_test").await;
            res.unwrap()
        })
        .await
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore]
    async fn test_all() {
        collection_subscription_model_works().await;
        creation_subscription_model_works().await;
    }

    // #[tokio::test(flavor = "current_thread")]
    async fn collection_subscription_model_works() {
        let db = get_db().await;
        let uid = xid::new();
        let cid = xid::new();

        // create
        {
            let txn = xid::new();
            let expire_at = unix_ms() as i64 + 3600 * 1000;
            let mut doc = CollectionSubscription::with_pk(uid, cid);
            doc.txn = txn;
            doc.expire_at = expire_at;

            let res = doc.get_one(db, vec![]).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 404);

            assert!(doc.save(db).await.unwrap());
            let res = doc.save(db).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into(); // can not insert twice
            assert_eq!(err.code, 409);

            let mut doc2 = CollectionSubscription::with_pk(uid, cid);
            doc2.get_one(db, vec![]).await.unwrap();

            assert_eq!(doc2.txn, txn);
            assert_eq!(doc2.expire_at, expire_at);

            let mut doc3 = CollectionSubscription::with_pk(uid, cid);
            doc3.get_one(db, vec!["expire_at".to_string()])
                .await
                .unwrap();
            assert!(doc3.txn.is_zero());
            assert_eq!(doc3.updated_at, 0);
            assert_eq!(doc3.expire_at, expire_at);
        }

        // update
        {
            let txn = xid::new();
            let expire_at = unix_ms() as i64 + 3610 * 1000;
            let mut doc = CollectionSubscription::with_pk(uid, cid);
            let res = doc.update(db, txn, expire_at, 0).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 409); // updated_at is not match

            doc.get_one(db, vec![]).await.unwrap();

            let res = doc
                .update(db, txn, expire_at, doc.updated_at)
                .await
                .unwrap();
            assert!(res);
            assert_eq!(doc.txn, txn);
        }
    }

    async fn creation_subscription_model_works() {
        let db = get_db().await;
        let uid = xid::new();
        let cid = xid::new();

        // create
        {
            let txn = xid::new();
            let mut doc = CreationSubscription::with_pk(uid, cid);
            doc.txn = txn;
            doc.expire_at = unix_ms() as i64 + 3600 * 1000;

            let res = doc.get_one(db, vec![]).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 404);

            assert!(doc.save(db).await.unwrap());
            let res = doc.save(db).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into(); // can not insert twice
            assert_eq!(err.code, 409);

            let mut doc2 = CreationSubscription::with_pk(uid, cid);
            doc2.get_one(db, vec![]).await.unwrap();

            assert_eq!(doc2.txn, txn);
            assert_eq!(doc2.expire_at, doc.expire_at);

            let mut doc3 = CreationSubscription::with_pk(uid, cid);
            doc3.get_one(db, vec!["expire_at".to_string()])
                .await
                .unwrap();
            assert!(doc3.txn.is_zero());
            assert_eq!(doc3.updated_at, 0);
            assert_eq!(doc3.expire_at, doc.expire_at);
        }

        // update
        {
            let txn = xid::new();
            let expire_at = unix_ms() as i64 + 3610 * 1000;
            let mut doc = CreationSubscription::with_pk(uid, cid);
            let res = doc.update(db, txn, expire_at, 0).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 409); // updated_at is not match

            doc.get_one(db, vec![]).await.unwrap();

            let res = doc
                .update(db, txn, expire_at, doc.updated_at)
                .await
                .unwrap();
            assert!(res);
            assert_eq!(doc.txn, txn);
        }
    }
}
