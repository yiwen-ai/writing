use isolang::Language;

use axum_web::context::unix_ms;
use axum_web::erring::HTTPError;
use scylla_orm::{ColumnsMap, CqlValue, ToCqlVal};
use scylla_orm_macros::CqlOrm;

use crate::db::{scylladb, scylladb::extract_applied, MAX_ID};

#[derive(Debug, Default, Clone, CqlOrm, PartialEq)]
pub struct Bookmark {
    pub uid: xid::Id,
    pub id: xid::Id,
    pub kind: i8,
    pub cid: xid::Id,
    pub gid: xid::Id,
    pub language: Language,
    pub version: i16,
    pub updated_at: i64,
    pub title: String,
    pub labels: Vec<String>,
    pub payload: Vec<u8>,

    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
}

impl Bookmark {
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

        let mut select_fields = select_fields;
        let field = "kind".to_string();
        if !select_fields.contains(&field) {
            select_fields.push(field);
        }
        let field = "cid".to_string();
        if !select_fields.contains(&field) {
            select_fields.push(field);
        }
        let field = "gid".to_string();
        if !select_fields.contains(&field) {
            select_fields.push(field);
        }

        if with_pk {
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

    pub async fn get_one(
        &mut self,
        db: &scylladb::ScyllaDB,
        select_fields: Vec<String>,
    ) -> anyhow::Result<()> {
        let fields = Self::select_fields(select_fields, false)?;
        self._fields = fields.clone();

        let query = format!(
            "SELECT {} FROM bookmark WHERE uid=? AND id=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.uid.to_cql(), self.id.to_cql());
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
            "INSERT INTO bookmark ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(
                HTTPError::new(409, "Bookmark save failed, please try again".to_string()).into(),
            );
        }

        Ok(true)
    }

    pub async fn update(
        &mut self,
        db: &scylladb::ScyllaDB,
        cols: ColumnsMap,
        updated_at: i64,
    ) -> anyhow::Result<bool> {
        let valid_fields = ["version", "title", "gid", "language", "labels", "payload"];
        let update_fields = cols.keys();
        for field in &update_fields {
            if !valid_fields.contains(&field.as_str()) {
                return Err(HTTPError::new(400, format!("Invalid field: {}", field)).into());
            }
        }

        self.get_one(db, vec!["updated_at".to_string()]).await?;
        if self.updated_at != updated_at {
            return Err(HTTPError::new(
                409,
                format!(
                    "Bookmark updated_at conflict, expected {}, got {}",
                    self.updated_at, updated_at
                ),
            )
            .into());
        }

        let mut set_fields: Vec<String> = Vec::with_capacity(update_fields.len() + 1);
        let mut params: Vec<CqlValue> = Vec::with_capacity(update_fields.len() + 1 + 3);

        let new_updated_at = unix_ms() as i64;
        set_fields.push("updated_at=?".to_string());
        params.push(new_updated_at.to_cql());
        for field in &update_fields {
            set_fields.push(format!("{}=?", field));
            params.push(cols.get(field).unwrap().to_owned());
        }

        let query = format!(
            "UPDATE bookmark SET {} WHERE uid=? AND id=? IF updated_at=?",
            set_fields.join(",")
        );
        params.push(self.uid.to_cql());
        params.push(self.id.to_cql());
        params.push(updated_at.to_cql());

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                "Bookmark update failed, please try again".to_string(),
            )
            .into());
        }

        self.updated_at = new_updated_at;
        Ok(true)
    }

    pub async fn delete(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        let res = self.get_one(db, Vec::new()).await;
        if res.is_err() {
            return Ok(false); // already deleted
        }

        let query = "DELETE FROM bookmark WHERE uid=? AND id=?";
        let params = (self.uid.to_cql(), self.id.to_cql());
        let _ = db.execute(query, params).await?;

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
            "SELECT {} FROM bookmark WHERE uid=? AND id<? LIMIT ? USING TIMEOUT 3s",
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

    pub async fn list_by_cid(
        db: &scylladb::ScyllaDB,
        uid: xid::Id,
        cid: xid::Id,
        select_fields: Vec<String>,
    ) -> anyhow::Result<Vec<Self>> {
        let fields = Self::select_fields(select_fields, true)?;

        let query = format!(
            "SELECT {} FROM bookmark WHERE uid=? AND cid=? LIMIT ? USING TIMEOUT 3s",
            fields.clone().join(",")
        );
        let params = (uid.to_cql(), cid.to_cql(), 1000i32);
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

        res.sort_by(|a, b| b.version.partial_cmp(&a.version).unwrap());
        Ok(res)
    }

    pub async fn get_one_by_cid(
        db: &scylladb::ScyllaDB,
        uid: xid::Id,
        cid: xid::Id,
        gid: xid::Id,
        language: Language,
        select_fields: Vec<String>,
    ) -> anyhow::Result<Self> {
        let fields = Self::select_fields(select_fields, false)?;
        let query = format!(
            "SELECT {} FROM bookmark WHERE uid=? AND cid=? AND gid=? AND language=? LIMIT 1 ALLOW FILTERING",
            fields.join(",")
        );
        let params = (uid.to_cql(), cid.to_cql(), gid.to_cql(), language.to_cql());
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
        if res.len() == 1 {
            return Ok(res.remove(0));
        }

        Err(HTTPError::new(
            404,
            format!(
                "Bookmark not found, uid: {}, cid: {}, gid: {}, language: {}",
                uid, cid, gid, language
            ),
        )
        .into())
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
    async fn test_all() {
        bookmark_model_works().await;
    }

    // #[tokio::test(flavor = "current_thread")]
    async fn bookmark_model_works() {
        let db = get_db().await;
        let uid = xid::Id::from_str(db::USER_JARVIS).unwrap();
        let id = xid::new();
        let cid = xid::new();

        // create
        {
            let mut doc = Bookmark::with_pk(uid, id);
            doc.cid = cid;
            doc.language = Language::Eng;
            doc.version = 1;
            doc.title = "Hello World".to_string();

            let res = doc.get_one(db, vec![]).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 404);

            assert!(doc.save(db).await.unwrap());
            let res = doc.save(db).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into(); // can not insert twice
            assert_eq!(err.code, 409);

            let mut doc2 = Bookmark::with_pk(uid, id);
            doc2.get_one(db, vec![]).await.unwrap();

            assert_eq!(doc2.cid, cid);
            assert_eq!(doc2.title.as_str(), "Hello World");
            assert_eq!(doc2.version, 1);
            assert_eq!(doc2.language, Language::Eng);

            let mut doc3 = Bookmark::with_pk(uid, id);
            doc3.get_one(db, vec!["cid".to_string(), "version".to_string()])
                .await
                .unwrap();
            assert_eq!(doc3.cid, cid);
            assert_eq!(doc3.title.as_str(), "");
            assert_eq!(doc3.version, 1);
            assert_eq!(doc3.language, Language::default());
        }

        // update
        {
            let mut doc = Bookmark::with_pk(uid, id);
            let mut cols = ColumnsMap::new();
            cols.set_as("status", &2i8);
            let res = doc.update(db, cols, 0).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 400); // status is not updatable

            let mut cols = ColumnsMap::new();
            cols.set_as("title", &"update title 1".to_string());
            let res = doc.update(db, cols, 1).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 409); // updated_at not match

            let mut cols = ColumnsMap::new();
            cols.set_as("title", &"title 1".to_string());
            let res = doc.update(db, cols, doc.updated_at).await.unwrap();
            assert!(res);

            let mut cols = ColumnsMap::new();
            cols.set_as("version", &2i16);
            cols.set_as("title", &"title 2".to_string());
            cols.set_as("labels", &vec!["label 1".to_string()]);

            let res = doc.update(db, cols, doc.updated_at).await.unwrap();
            assert!(res);
        }

        // delete
        {
            let mut doc = Bookmark::with_pk(uid, id);
            let res = doc.delete(db).await.unwrap();
            assert!(res);

            let res = doc.delete(db).await.unwrap();
            assert!(!res); // already deleted
        }
    }
}
