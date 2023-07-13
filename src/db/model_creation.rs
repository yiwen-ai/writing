use isolang::Language;
use std::{
    collections::HashSet,
    time::{Duration, SystemTime},
};

use axum_web::context::unix_ms;
use axum_web::erring::HTTPError;
use scylla_orm::{ColumnsMap, CqlValue, ToCqlVal};
use scylla_orm_macros::CqlOrm;

use crate::db::{
    scylladb,
    scylladb::{extract_applied, Query},
};

#[derive(Debug, Default, Clone, CqlOrm)]
pub struct CreationIndex {
    pub id: xid::Id,
    pub gid: xid::Id,
    pub rating: i8,
    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
}

impl CreationIndex {
    pub fn with_pk(id: xid::Id) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }

    pub async fn get_one(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<()> {
        self._fields = Self::fields();

        let query = "SELECT gid,rating FROM creation_index WHERE id=? LIMIT 1";
        let params = (self.id.to_cql(),);
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(2);
        cols.fill(res, &vec!["gid".to_string(), "rating".to_string()])?;
        self.fill(&cols);

        Ok(())
    }

    pub async fn save(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        use std::ops::Sub;
        let now = SystemTime::now().sub(Duration::from_secs(10));
        if self.id.time() < now {
            return Err(HTTPError::new(400, format!("Invalid id {:?}", self.id)).into());
        }

        self._fields = Self::fields();
        let query = "INSERT INTO creation_index (id,gid,rating) VALUES (?,?,?) IF NOT EXISTS";
        let params = (self.id.to_cql(), self.gid.to_cql(), self.rating);
        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(
                HTTPError::new(409, format!("CreationIndex {} already exists", self.id)).into(),
            );
        }

        Ok(true)
    }

    pub async fn batch_get(
        db: &scylladb::ScyllaDB,
        ids: Vec<xid::Id>,
        max_rating: i8,
    ) -> anyhow::Result<Vec<CreationIndex>> {
        let fields: Vec<String> = Self::fields();

        let mut vals_name: Vec<&str> = Vec::with_capacity(ids.len());
        let mut params: Vec<CqlValue> = Vec::with_capacity(ids.len() + 1);

        for id in &ids {
            vals_name.push("?");
            params.push(id.to_cql());
        }

        let query = format!("SELECT id,gid,rating FROM creation_index WHERE id IN ({}) AND rating<=? ALLOW FILTERING", vals_name.join(","));
        params.push(max_rating.to_cql());
        let res = db.execute(query, params).await?;

        let rows = res.rows.unwrap_or_default();
        let mut res: Vec<CreationIndex> = Vec::with_capacity(rows.len());
        for r in rows {
            let mut cols = ColumnsMap::with_capacity(3);
            cols.fill(r, &fields)?;
            let mut item = CreationIndex::default();
            item.fill(&cols);
            res.push(item);
        }

        Ok(res)
    }
}

#[derive(Debug, Default, Clone, CqlOrm, PartialEq)]
pub struct Creation {
    pub gid: xid::Id,
    pub id: xid::Id,
    pub status: i8,
    pub rating: i8,
    pub version: i16,
    pub language: Language,
    pub creator: xid::Id,
    pub created_at: i64,
    pub updated_at: i64,
    pub active_languages: HashSet<Language>,
    pub original_url: String,
    pub genre: Vec<String>,
    pub title: String,
    pub description: String,
    pub cover: String,
    pub keywords: Vec<String>,
    pub labels: Vec<String>,
    pub authors: Vec<String>,
    pub reviewers: Vec<xid::Id>,
    pub summary: String,
    pub content: Vec<u8>,
    pub license: String,

    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
}

impl Creation {
    pub fn with_pk(gid: xid::Id, id: xid::Id) -> Self {
        Self {
            gid,
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
            let gid = "gid".to_string();
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

        match self.status {
            -1 if !(-1..=1).contains(&status) => Err(HTTPError::new(
                400,
                format!(
                    "Creation status is {}, expected update to 0 or 1, got {}",
                    self.status, status
                ),
            )
            .into()),
            0 if !(-1..=1).contains(&status) => Err(HTTPError::new(
                400,
                format!(
                    "Creation status is {}, expected update to -1 or 1, got {}",
                    self.status, status
                ),
            )
            .into()),
            1 if !(-1..=2).contains(&status) => Err(HTTPError::new(
                400,
                format!(
                    "Creation status is {}, expected update to -1, 0 or 2, got {}",
                    self.status, status
                ),
            )
            .into()),
            2 if !(-1..=2).contains(&status) => Err(HTTPError::new(
                400,
                format!(
                    "Creation status is {}, expected update to -1, 0 or 1, got {}",
                    self.status, status
                ),
            )
            .into()),
            _ => Ok(()),
        }
    }

    pub async fn get_one(
        &mut self,
        db: &scylladb::ScyllaDB,
        select_fields: Vec<String>,
    ) -> anyhow::Result<()> {
        let fields = Self::select_fields(select_fields, false)?;
        self._fields = fields.clone();

        let query = format!(
            "SELECT {} FROM creation WHERE gid=? AND id=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.gid.to_cql(), self.id.to_cql());
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, &fields)?;
        self.fill(&cols);

        Ok(())
    }

    pub async fn get_deleted(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<()> {
        let fields = Self::fields();
        self._fields = fields.clone();

        let query = format!(
            "SELECT {} FROM deleted_creation WHERE gid=? AND id=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.gid.to_cql(), self.id.to_cql());
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, &fields)?;
        self.fill(&cols);

        Ok(())
    }

    pub async fn save(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        let mut index = CreationIndex::with_pk(self.id);
        index.gid = self.gid;
        index.save(db).await?;

        let now = unix_ms() as i64;
        self.created_at = now;
        self.updated_at = now;
        self.version = 1;

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
            "INSERT INTO creation ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                format!("Creation {} save failed, please try again", self.id),
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
                    "Creation updated_at conflict, expected updated_at {}, got {}",
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
            "UPDATE creation SET status=?,updated_at=? WHERE gid=? AND id=? IF updated_at=?";
        let params = (
            status,
            new_updated_at,
            self.gid.to_cql(),
            self.id.to_cql(),
            updated_at,
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                format!("Creation update_status {} failed, please try again", status),
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
            "title",
            "description",
            "cover",
            "keywords",
            "labels",
            "authors",
            "summary",
            "content",
            "license",
        ];
        let update_fields = cols.keys();
        for field in &update_fields {
            if !valid_fields.contains(&field.as_str()) {
                return Err(HTTPError::new(400, format!("Invalid field: {}", field)).into());
            }
        }

        self.get_one(
            db,
            vec![
                "status".to_string(),
                "version".to_string(),
                "updated_at".to_string(),
            ],
        )
        .await?;
        if self.updated_at != updated_at {
            return Err(HTTPError::new(
                409,
                format!(
                    "Creation updated_at conflict, expected updated_at {}, got {}",
                    self.updated_at, updated_at
                ),
            )
            .into());
        }
        if self.status < 0 || self.status > 1 {
            return Err(HTTPError::new(
                409,
                format!("Creation can not be update, status {}", self.status),
            )
            .into());
        }

        let mut incr = 1usize; // should add `updated_at`
        if cols.has("content") {
            incr += 1; // should add `version`
        }

        let mut set_fields: Vec<String> = Vec::with_capacity(update_fields.len() + incr);
        let mut params: Vec<CqlValue> = Vec::with_capacity(update_fields.len() + incr + 3);

        let new_updated_at = unix_ms() as i64;
        let mut new_version = self.version;
        set_fields.push("updated_at=?".to_string());
        params.push(new_updated_at.to_cql());
        if cols.has("content") {
            set_fields.push("version=?".to_string());
            new_version += 1;
            params.push(new_version.to_cql());
        }
        for field in &update_fields {
            set_fields.push(format!("{}=?", field));
            params.push(cols.get(field).unwrap().to_owned());
        }

        let query = format!(
            "UPDATE creation SET {} WHERE gid=? AND id=? IF updated_at=?",
            set_fields.join(",")
        );
        params.push(self.gid.to_cql());
        params.push(self.id.to_cql());
        params.push(updated_at.to_cql());

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                format!("Creation {} update failed, please try again", self.id),
            )
            .into());
        }

        self.updated_at = new_updated_at;
        self.version = new_version;
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
                    "Creation version conflict, expected version {}, got {}",
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
        let cols = self.to();

        for field in &fields {
            cols_name.push(field);
            vals_name.push("?");
            insert_params.push(cols.get(field).unwrap());
        }

        let insert_query = format!(
            "INSERT INTO deleted_creation ({}) VALUES ({})",
            cols_name.join(","),
            vals_name.join(","),
        );

        let delete_query = "DELETE FROM creation WHERE gid=? AND id=?";
        let delete_params = (self.gid.to_cql(), self.id.to_cql());

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
        gid: xid::Id,
        select_fields: Vec<String>,
        page_size: u16,
        page_token: Option<xid::Id>,
        status: Option<i8>,
    ) -> anyhow::Result<Vec<Creation>> {
        let fields = Self::select_fields(select_fields, true)?;

        let rows = if let Some(id) = page_token {
            if status.is_none() {
                let query = Query::new(format!(
                "SELECT {} FROM creation WHERE gid=? AND id<? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(","))).with_page_size(page_size as i32);
                let params = (gid.to_cql(), id.to_cql(), page_size as i32);
                db.execute_paged(query, params, None).await?
            } else {
                let query = Query::new(format!(
                    "SELECT {} FROM creation WHERE gid=? AND id<? AND status=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                    fields.clone().join(","))).with_page_size(page_size as i32);
                let params = (gid.to_cql(), id.to_cql(), status.unwrap(), page_size as i32);
                db.execute_paged(query, params, None).await?
            }
        } else if status.is_none() {
            let query = Query::new(format!(
                "SELECT {} FROM creation WHERE gid=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(page_size as i32);
            let params = (gid.to_cql(), page_size as i32);
            db.execute_iter(query, params).await? // TODO: execute_iter or execute_paged?
        } else {
            let query = Query::new(format!(
                "SELECT {} FROM creation WHERE gid=? AND status=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            )).with_page_size(page_size as i32);
            let params = (gid.to_cql(), status.unwrap(), page_size as i32);
            db.execute_iter(query, params).await?
        };

        let mut res: Vec<Creation> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut doc = Creation::default();
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
    use ciborium::cbor;
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
        // problem: https://users.rust-lang.org/t/tokio-runtimes-and-tokio-oncecell/91351/5
        creation_index_model_works().await?;
        creation_model_works().await?;
        creation_find_works().await?;

        Ok(())
    }

    // #[tokio::test(flavor = "current_thread")]
    async fn creation_index_model_works() -> anyhow::Result<()> {
        let db = get_db().await;

        let mut c1 = CreationIndex {
            id: xid::new(),
            gid: xid::new(),
            ..Default::default()
        };
        c1.save(db).await?;

        let mut c2 = CreationIndex {
            id: xid::new(),
            gid: xid::new(),
            rating: 127,
            ..Default::default()
        };
        c2.save(db).await?;

        let mut c3 = CreationIndex {
            id: xid::new(),
            gid: xid::new(),
            rating: 3,
            ..Default::default()
        };
        c3.save(db).await?;

        let mut d1 = CreationIndex::with_pk(c1.id);
        d1.get_one(db).await?;
        assert_eq!(d1.gid, c1.gid);

        let docs = CreationIndex::batch_get(db, vec![c1.id, c3.id, c2.id], 3).await?;
        assert!(docs.len() == 2);

        let docs = CreationIndex::batch_get(db, vec![c1.id, c3.id, c2.id], 127).await?;
        assert!(docs.len() == 3);

        Ok(())
    }

    // #[tokio::test(flavor = "current_thread")]
    async fn creation_model_works() -> anyhow::Result<()> {
        let db = get_db().await;
        let gid = xid::Id::from_str(db::USER_JARVIS).unwrap();
        let cid = xid::new();

        // valid_status
        {
            let mut doc = Creation::with_pk(gid, cid);
            assert!(doc.valid_status(-2).is_err());
            assert!(doc.valid_status(-1).is_ok());
            assert!(doc.valid_status(0).is_ok());
            assert!(doc.valid_status(1).is_ok());
            assert!(doc.valid_status(2).is_err());
            assert!(doc.valid_status(3).is_err());

            doc.status = -1;
            assert!(doc.valid_status(-2).is_err());
            assert!(doc.valid_status(-1).is_ok());
            assert!(doc.valid_status(0).is_ok());
            assert!(doc.valid_status(1).is_ok());
            assert!(doc.valid_status(2).is_err());
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
            let mut doc = Creation::with_pk(gid, cid);
            doc.language = Language::Eng;
            doc.title = "Hello World".to_string();
            ciborium::into_writer(
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
                })?,
                &mut doc.content,
            )?;

            let res = doc.get_one(db, vec![]).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 404);

            assert!(doc.save(db).await?);
            let res = doc.save(db).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into(); // can not insert twice
            assert_eq!(err.code, 409);

            let mut doc2 = Creation::with_pk(gid, cid);
            doc2.get_one(db, vec![]).await?;
            // println!("doc: {:#?}", doc2);

            assert_eq!(doc2.title.as_str(), "Hello World");
            assert_eq!(doc2.version, 1);
            assert_eq!(doc2.language, Language::Eng);
            assert_eq!(doc2.content, doc.content);

            let mut doc3 = Creation::with_pk(gid, cid);
            doc3.get_one(db, vec!["gid".to_string(), "title".to_string()])
                .await?;
            assert_eq!(doc3.title.as_str(), "Hello World");
            assert_eq!(doc3.version, 0);
            assert_eq!(doc3.language, Language::default());
            assert_eq!(doc3._fields, vec!["gid", "title"]);
            assert!(doc3.content.is_empty());
        }

        // update
        {
            let mut doc = Creation::with_pk(gid, cid);
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
            let res = doc.update(db, cols, doc.updated_at).await?;
            assert!(res);
            assert_eq!(doc.version, 1);

            let mut cols = ColumnsMap::new();
            cols.set_as("title", &"title 2".to_string());
            cols.set_as("description", &"description 2".to_string());
            cols.set_as("cover", &"cover 2".to_string());
            cols.set_as("summary", &"summary 2".to_string());
            cols.set_as("keywords", &vec!["keyword".to_string()]);
            cols.set_as("labels", &vec!["label 1".to_string()]);
            cols.set_as("authors", &vec!["author 1".to_string()]);

            let mut content: Vec<u8> = Vec::new();
            ciborium::into_writer(
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
                            "text" => "Hello World 2",
                        }],
                    }],
                })?,
                &mut content,
            )?;
            cols.set_as("content", &content);
            cols.set_as("license", &"license 2".to_string());
            let res = doc.update(db, cols, doc.updated_at).await?;
            assert!(res);
            assert_eq!(doc.version, 2);
        }

        // update status
        {
            let mut doc = Creation::with_pk(gid, cid);
            doc.get_one(db, vec![]).await?;

            let res = doc.update_status(db, 2, doc.updated_at - 1).await;
            assert!(res.is_err());

            let res = doc.update_status(db, 2, doc.updated_at).await;
            assert!(res.is_err());

            let res = doc.update_status(db, 1, doc.updated_at).await?;
            assert!(res);

            let res = doc.update_status(db, 1, doc.updated_at).await?;
            assert!(!res);
        }

        // delete
        {
            let mut backup = Creation::with_pk(gid, cid);
            backup.get_one(db, vec![]).await?;
            backup.updated_at = 0;

            let mut deleted = Creation::with_pk(gid, cid);
            let res = deleted.get_deleted(db).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 404);

            let mut doc = Creation::with_pk(gid, cid);
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
    async fn creation_find_works() -> anyhow::Result<()> {
        let db = get_db().await;
        let gid = xid::new();
        let mut content: Vec<u8> = Vec::new();
        ciborium::into_writer(
            &cbor!({
                "type" => "doc",
                "content" => [],
            })?,
            &mut content,
        )?;

        let mut docs: Vec<Creation> = Vec::new();
        for i in 0..10 {
            let mut doc = Creation::with_pk(gid, xid::new());
            doc.language = Language::Eng;
            doc.title = format!("Hello World {}", i);
            doc.content = content.clone();
            doc.save(db).await?;

            docs.push(doc)
        }
        assert_eq!(docs.len(), 10);

        let latest = Creation::find(db, gid, Vec::new(), 1, None, None).await?;
        assert_eq!(latest.len(), 1);
        let mut latest = latest[0].to_owned();
        assert_eq!(latest.gid, docs.last().unwrap().gid);
        assert_eq!(latest.id, docs.last().unwrap().id);

        latest.update_status(db, 1, latest.updated_at).await?;
        let res = Creation::find(db, gid, vec!["title".to_string()], 100, None, None).await?;
        assert_eq!(res.len(), 10);

        let res = Creation::find(db, gid, vec!["title".to_string()], 100, None, Some(1)).await?;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].id, docs.last().unwrap().id);

        let res = Creation::find(db, gid, vec!["title".to_string()], 5, None, None).await?;
        assert_eq!(res.len(), 5);
        assert_eq!(res[4].id, docs[5].id);

        let res = Creation::find(
            db,
            gid,
            vec!["title".to_string()],
            5,
            Some(docs[5].id),
            None,
        )
        .await?;
        assert_eq!(res.len(), 5);
        assert_eq!(res[4].id, docs[0].id);

        let res = Creation::find(
            db,
            gid,
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
