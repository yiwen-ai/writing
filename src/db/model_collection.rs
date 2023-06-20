use isolang::Language;

use axum_web::context::unix_ms;
use axum_web::erring::HTTPError;
use scylla_orm::ColumnsMap;
use scylla_orm_macros::CqlOrm;

use crate::db::{
    scylladb,
    scylladb::{extract_applied, CqlValue, Query},
};

#[derive(Debug, Default, Clone, CqlOrm, PartialEq)]
pub struct Collection {
    pub uid: xid::Id,
    pub id: xid::Id,
    pub cid: xid::Id,
    pub language: Language,
    pub version: i16,
    pub status: i8,
    pub updated_at: i64,
    pub genre: Vec<String>,
    pub title: String,
    pub description: String,
    pub cover: String,
    pub summary: String,
    pub labels: Vec<String>,

    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
}

impl Collection {
    pub fn with_pk(uid: xid::Id, id: xid::Id) -> Self {
        Self {
            uid,
            id,
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

        if with_pk {
            let mut select_fields = select_fields;
            let gid = "uid".to_string();
            if !select_fields.contains(&gid) {
                select_fields.push(gid);
            }
            let id = "id".to_string();
            if !select_fields.contains(&id) {
                select_fields.push(id);
            }
            return Ok(select_fields);
        }

        Ok(select_fields)
    }

    pub fn valid_status(&self, status: i8) -> anyhow::Result<()> {
        if !(-1..=2).contains(&status) {
            return Err(HTTPError::new(400, format!("Invalid status, {}", status)).into());
        }

        Ok(())
    }

    pub async fn get_one(
        &mut self,
        db: &scylladb::ScyllaDB,
        select_fields: Vec<String>,
    ) -> anyhow::Result<()> {
        let fields = Self::select_fields(select_fields, false)?;
        self._fields = fields.clone();

        let query = format!(
            "SELECT {} FROM collection WHERE uid=? AND id=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.uid.as_bytes(), self.id.as_bytes());
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, fields)?;
        self.fill(&cols);

        Ok(())
    }

    pub async fn get_deleted(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<()> {
        let fields = Self::fields();
        self._fields = fields.clone();

        let query = format!(
            "SELECT {} FROM deleted_collection WHERE uid=? AND id=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.uid.as_bytes(), self.id.as_bytes());
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, fields)?;
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
        let cols = self.to()?;

        for field in &fields {
            cols_name.push(field);
            vals_name.push("?");
            params.push(cols.get(field).unwrap());
        }

        let query = format!(
            "INSERT INTO collection ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                format!("Collection {} save failed, please try again", self.id),
            )
            .into());
        }

        Ok(true)
    }

    pub async fn update_status(
        &mut self,
        db: &scylladb::ScyllaDB,
        status: i8,
        updated_at: i64,
    ) -> anyhow::Result<bool> {
        self.get_one(db, vec!["status".to_string(), "updated_at".to_string()])
            .await?;
        if self.updated_at != updated_at {
            return Err(HTTPError::new(
                409,
                format!(
                    "Collection updated_at conflict, expected updated_at {}, got {}",
                    self.updated_at, updated_at
                ),
            )
            .into());
        }

        self.valid_status(status)?;
        if self.status == status {
            return Ok(false); // no need to update
        }

        let new_updated_at = unix_ms() as i64;
        let query =
            "UPDATE collection SET status=?,updated_at=? WHERE uid=? AND id=? IF updated_at=?";
        let params = (
            status,
            new_updated_at,
            self.uid.as_bytes(),
            self.id.as_bytes(),
            updated_at,
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                format!(
                    "Collection update_status {} failed, please try again",
                    status
                ),
            )
            .into());
        }

        self.updated_at = new_updated_at;
        self.status = status;
        Ok(true)
    }

    pub async fn update(
        &mut self,
        db: &scylladb::ScyllaDB,
        cols: ColumnsMap,
        updated_at: i64,
    ) -> anyhow::Result<bool> {
        let valid_fields = vec![
            "version",
            "title",
            "description",
            "cover",
            "summary",
            "labels",
        ];
        let update_fields = cols.keys();
        for field in &update_fields {
            if !valid_fields.contains(&field.as_str()) {
                return Err(HTTPError::new(400, format!("Invalid field: {}", field)).into());
            }
        }

        self.get_one(db, vec!["status".to_string(), "updated_at".to_string()])
            .await?;
        if self.updated_at != updated_at {
            return Err(HTTPError::new(
                409,
                format!(
                    "Collection updated_at conflict, expected updated_at {}, got {}",
                    self.updated_at, updated_at
                ),
            )
            .into());
        }
        if self.status < 0 {
            return Err(HTTPError::new(
                409,
                format!("Collection can not be update, status {}", self.status),
            )
            .into());
        }

        let mut set_fields: Vec<String> = Vec::with_capacity(update_fields.len() + 1);
        let mut params: Vec<CqlValue> = Vec::with_capacity(update_fields.len() + 1 + 3);

        let new_updated_at = unix_ms() as i64;
        set_fields.push("updated_at=?".to_string());
        params.push(CqlValue::BigInt(new_updated_at));
        for field in &update_fields {
            set_fields.push(format!("{}=?", field));
            params.push(cols.get(field).unwrap().to_owned());
        }

        let query = format!(
            "UPDATE collection SET {} WHERE uid=? AND id=? IF updated_at=?",
            set_fields.join(",")
        );
        params.push(CqlValue::Blob(self.uid.as_bytes().to_vec()));
        params.push(CqlValue::Blob(self.id.as_bytes().to_vec()));
        params.push(CqlValue::BigInt(updated_at));

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                format!("Creation {} update failed, please try again", self.id),
            )
            .into());
        }

        self.updated_at = new_updated_at;
        Ok(true)
    }

    pub async fn delete(&mut self, db: &scylladb::ScyllaDB, version: i16) -> anyhow::Result<bool> {
        let res = self.get_one(db, vec!["version".to_string()]).await;
        if res.is_err() {
            return Ok(false); // already deleted
        }

        if self.version != version {
            return Err(HTTPError::new(
                409,
                format!(
                    "Collection version conflict, expected version {}, got {}",
                    self.version, version
                ),
            )
            .into());
        }

        self.get_one(db, Vec::new()).await?;
        self.updated_at = unix_ms() as i64;

        let fields = Self::fields();
        self._fields = fields.iter().map(|f| f.to_string()).collect();

        let mut cols_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut vals_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut insert_params: Vec<&CqlValue> = Vec::with_capacity(fields.len());
        let cols = self.to()?;

        for field in &fields {
            cols_name.push(field);
            vals_name.push("?");
            insert_params.push(cols.get(field).unwrap());
        }

        let insert_query = format!(
            "INSERT INTO deleted_collection ({}) VALUES ({})",
            cols_name.join(","),
            vals_name.join(","),
        );

        let delete_query = "DELETE FROM collection WHERE uid=? AND id=?";
        let delete_params = (self.uid.as_bytes(), self.id.as_bytes());

        let _ = db
            .batch(
                vec![insert_query.as_str(), delete_query],
                (insert_params, delete_params),
            )
            .await?;
        Ok(true)
    }

    pub async fn find(
        db: &scylladb::ScyllaDB,
        uid: xid::Id,
        select_fields: Vec<String>,
        page_size: u16,
        page_token: Option<xid::Id>,
        status: Option<i8>,
    ) -> anyhow::Result<Vec<Collection>> {
        let fields = Self::select_fields(select_fields, true)?;

        let rows = if let Some(id) = page_token {
            if status.is_none() {
                let query = Query::new(format!(
                "SELECT {} FROM collection WHERE uid=? AND id<? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(","))).with_page_size(page_size as i32);
                let params = (uid.as_bytes(), id.as_bytes(), page_size as i32);
                db.execute_paged(query, params, None).await?
            } else {
                let query = Query::new(format!(
                    "SELECT {} FROM collection WHERE uid=? AND id<? AND status=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                    fields.clone().join(","))).with_page_size(page_size as i32);
                let params = (
                    uid.as_bytes(),
                    id.as_bytes(),
                    status.unwrap(),
                    page_size as i32,
                );
                db.execute_paged(query, params, None).await?
            }
        } else if status.is_none() {
            let query = Query::new(format!(
                "SELECT {} FROM collection WHERE uid=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(page_size as i32);
            let params = (uid.as_bytes(), page_size as i32);
            db.execute_iter(query, params).await? // TODO: execute_iter or execute_paged?
        } else {
            let query = Query::new(format!(
                "SELECT {} FROM collection WHERE uid=? AND status=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            )).with_page_size(page_size as i32);
            let params = (uid.as_bytes(), status.unwrap(), page_size as i32);
            db.execute_iter(query, params).await?
        };

        let mut res: Vec<Collection> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut doc = Collection::default();
            let mut cols = ColumnsMap::with_capacity(fields.len());
            cols.fill(row, fields.clone())?;
            doc.fill(&cols);
            doc._fields = fields.clone();
            res.push(doc);
        }

        Ok(res)
    }
}

#[cfg(test)]
mod tests {

    use std::str::FromStr;

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
    async fn test_all() -> anyhow::Result<()> {
        collection_model_works().await?;
        collection_find_works().await?;

        Ok(())
    }

    // #[tokio::test(flavor = "current_thread")]
    async fn collection_model_works() -> anyhow::Result<()> {
        let db = get_db().await;
        let uid = xid::Id::from_str(db::USER_JARVIS).unwrap();
        let id = xid::new();
        let cid = xid::new();

        // valid_status
        {
            let mut doc = Collection::with_pk(uid, id);
            assert!(doc.valid_status(-2).is_err());
            assert!(doc.valid_status(-1).is_ok());
            assert!(doc.valid_status(0).is_ok());
            assert!(doc.valid_status(1).is_ok());
            assert!(doc.valid_status(2).is_ok());
            assert!(doc.valid_status(3).is_err());

            doc.status = -1;
            assert!(doc.valid_status(-2).is_err());
            assert!(doc.valid_status(-1).is_ok());
            assert!(doc.valid_status(0).is_ok());
            assert!(doc.valid_status(1).is_ok());
            assert!(doc.valid_status(2).is_ok());
            assert!(doc.valid_status(3).is_err());

            doc.status = 1;
            assert!(doc.valid_status(-2).is_err());
            assert!(doc.valid_status(-1).is_ok());
            assert!(doc.valid_status(0).is_ok());
            assert!(doc.valid_status(1).is_ok());
            assert!(doc.valid_status(2).is_ok());
            assert!(doc.valid_status(3).is_err());

            doc.status = 2;
            assert!(doc.valid_status(-2).is_err());
            assert!(doc.valid_status(-1).is_ok());
            assert!(doc.valid_status(0).is_ok());
            assert!(doc.valid_status(1).is_ok());
            assert!(doc.valid_status(2).is_ok());
            assert!(doc.valid_status(3).is_err());
        }

        // create
        {
            let mut doc = Collection::with_pk(uid, id);
            doc.cid = cid;
            doc.language = Language::Eng;
            doc.version = 1;
            doc.title = "Hello World".to_string();

            let res = doc.get_one(db, vec![]).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 404);

            assert!(doc.save(db).await?);
            let res = doc.save(db).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into(); // can not insert twice
            assert_eq!(err.code, 409);

            let mut doc2 = Collection::with_pk(uid, id);
            doc2.get_one(db, vec![]).await?;

            assert_eq!(doc2.cid, cid);
            assert_eq!(doc2.title.as_str(), "Hello World");
            assert_eq!(doc2.version, 1);
            assert_eq!(doc2.language, Language::Eng);

            let mut doc3 = Collection::with_pk(uid, id);
            doc3.get_one(db, vec!["cid".to_string(), "version".to_string()])
                .await?;
            assert_eq!(doc3.cid, cid);
            assert_eq!(doc3.title.as_str(), "");
            assert_eq!(doc3.version, 1);
            assert_eq!(doc3.language, Language::default());
        }

        // update
        {
            let mut doc = Collection::with_pk(uid, id);
            let mut cols = ColumnsMap::new();
            cols.set_as("status", &2i8)?;
            let res = doc.update(db, cols, 0).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 400); // status is not updatable

            let mut cols = ColumnsMap::new();
            cols.set_as("title", &"update title 1".to_string())?;
            let res = doc.update(db, cols, 1).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 409); // updated_at not match

            let mut cols = ColumnsMap::new();
            cols.set_as("title", &"title 1".to_string())?;
            let res = doc.update(db, cols, doc.updated_at).await?;
            assert!(res);

            let mut cols = ColumnsMap::new();
            cols.set_as("version", &2i16)?;
            cols.set_as("title", &"title 2".to_string())?;
            cols.set_as("description", &"description 2".to_string())?;
            cols.set_as("cover", &"cover 2".to_string())?;
            cols.set_as("summary", &"summary 2".to_string())?;
            cols.set_as("labels", &vec!["label 1".to_string()])?;

            let res = doc.update(db, cols, doc.updated_at).await?;
            assert!(res);
        }

        // update status
        {
            let mut doc = Collection::with_pk(uid, id);
            doc.get_one(db, vec![]).await?;

            let res = doc.update_status(db, 2, doc.updated_at - 1).await;
            assert!(res.is_err());

            let res = doc.update_status(db, 2, doc.updated_at).await?;
            assert!(res);

            let res = doc.update_status(db, 1, doc.updated_at).await?;
            assert!(res);

            let res = doc.update_status(db, 1, doc.updated_at).await?;
            assert!(!res);
        }

        // delete
        {
            let mut backup = Collection::with_pk(uid, id);
            backup.get_one(db, vec![]).await?;
            backup.updated_at = 0;

            let mut deleted = Collection::with_pk(uid, id);
            let res = deleted.get_deleted(db).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 404);

            let mut doc = Collection::with_pk(uid, id);
            let res = doc.delete(db, 0).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 409);

            let res = doc.delete(db, 2).await?;
            assert!(res);

            let res = doc.delete(db, 2).await?;
            assert!(!res); // already deleted

            deleted.get_deleted(db).await?;
            deleted.updated_at = 0;
            assert_eq!(deleted, backup);
        }

        Ok(())
    }

    // #[tokio::test(flavor = "current_thread")]
    async fn collection_find_works() -> anyhow::Result<()> {
        let db = get_db().await;
        let uid = xid::new();

        let mut docs: Vec<Collection> = Vec::new();
        for i in 0..10 {
            let mut doc = Collection::with_pk(uid, xid::new());
            doc.cid = xid::new();
            doc.language = Language::Eng;
            doc.version = 1;
            doc.title = format!("Hello World {}", i);
            doc.save(db).await?;

            docs.push(doc)
        }
        assert_eq!(docs.len(), 10);

        let latest = Collection::find(db, uid, Vec::new(), 1, None, None).await?;
        assert_eq!(latest.len(), 1);
        let mut latest = latest[0].to_owned();
        assert_eq!(latest.uid, docs.last().unwrap().uid);
        assert_eq!(latest.id, docs.last().unwrap().id);

        latest.update_status(db, 1, latest.updated_at).await?;
        let res = Collection::find(db, uid, vec!["title".to_string()], 100, None, None).await?;
        assert_eq!(res.len(), 10);

        let res = Collection::find(db, uid, vec!["title".to_string()], 100, None, Some(1)).await?;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].id, docs.last().unwrap().id);

        let res = Collection::find(db, uid, vec!["title".to_string()], 5, None, None).await?;
        assert_eq!(res.len(), 5);
        assert_eq!(res[4].id, docs[5].id);

        let res = Collection::find(
            db,
            uid,
            vec!["title".to_string()],
            5,
            Some(docs[5].id),
            None,
        )
        .await?;
        assert_eq!(res.len(), 5);
        assert_eq!(res[4].id, docs[0].id);

        let res = Collection::find(
            db,
            uid,
            vec!["title".to_string()],
            5,
            Some(docs[5].id),
            Some(1),
        )
        .await?;
        assert_eq!(res.len(), 0);

        Ok(())
    }
}
