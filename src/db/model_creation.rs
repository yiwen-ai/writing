use isolang::Language;
use scylla_orm::ColumnsMap;
use scylla_orm_macros::CqlOrm;
use std::collections::HashSet;
use std::time::{Duration, SystemTime};

use super::{
    scylladb,
    scylladb::{extract_applied, CqlValue, Query},
};
use crate::context::unix_ms;
use crate::erring::HTTPError;

#[derive(Debug, Default, Clone, CqlOrm)]
pub struct CreationIndex {
    pub id: xid::Id,
    pub gid: xid::Id,
    pub rating: i8,
    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
}

impl CreationIndex {
    pub fn with_pk(id: xid::Id) -> Self {
        CreationIndex {
            id,
            ..Default::default()
        }
    }

    pub async fn get_one(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<()> {
        self._fields = Self::fields().iter().map(|f| f.to_string()).collect();

        let query = "SELECT gid,rating FROM creation_index WHERE id=? LIMIT 1";
        let params = (self.id.as_bytes(),);
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(2);
        cols.fill(res, vec!["gid".to_string(), "rating".to_string()])?;
        self.fill(&cols);

        Ok(())
    }

    pub async fn save(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        use std::ops::Sub;
        let now = SystemTime::now().sub(Duration::from_secs(10));
        if self.id.time() < now {
            return Err(anyhow::Error::new(HTTPError::new(
                400,
                format!("Invalid id {:?}", self.id),
            )));
        }

        self._fields = Self::fields().iter().map(|f| f.to_string()).collect();
        let query = "INSERT INTO creation_index (id,gid,rating) VALUES (?,?,?) IF NOT EXISTS";
        let params = (self.id.as_bytes(), self.gid.as_bytes(), 0i8);
        let res = db.execute(query, params).await?;
        Ok(extract_applied(res))
    }
}

#[derive(Debug, Default, Clone, CqlOrm)]
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
        Creation {
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
                return Err(anyhow::Error::new(HTTPError::new(
                    400,
                    format!("Invalid field: {}", field),
                )));
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

    pub async fn get_one(
        &mut self,
        db: &scylladb::ScyllaDB,
        select_fields: Vec<String>,
    ) -> anyhow::Result<()> {
        let fields = Self::select_fields(select_fields, false)?;
        self._fields = fields.iter().map(|f| f.to_string()).collect();

        let query = format!(
            "SELECT {} FROM creation WHERE gid=? AND id=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.gid.as_bytes(), self.id.as_bytes());
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, fields)?;
        self.fill(&cols);

        Ok(())
    }

    pub async fn save(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        let mut index = CreationIndex::with_pk(self.id);
        index.gid = self.gid;
        let ok = index.save(db).await?;
        if !ok {
            return Err(anyhow::Error::new(HTTPError::new(
                409,
                format!(
                    "Creation already exists, gid({}), id({})",
                    self.gid, self.id
                ),
            )));
        }

        let now = (unix_ms() / 1000) as i64;
        self.created_at = now;
        self.updated_at = now;
        self.version = 1;

        let fields = Self::fields();
        self._fields = fields.iter().map(|f| f.to_string()).collect();

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
            "INSERT INTO creation ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        Ok(extract_applied(res))
    }

    pub async fn update(
        &mut self,
        db: &scylladb::ScyllaDB,
        cols: ColumnsMap,
        updated_at: i64,
    ) -> anyhow::Result<i64> {
        let valid_fields = vec![
            "title",
            "description",
            "cover",
            "summary",
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
                return Err(anyhow::Error::new(HTTPError::new(
                    400,
                    format!("Invalid field: {}", field),
                )));
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
            return Err(anyhow::Error::new(HTTPError::new(
                409,
                format!(
                    "Creation updated_at conflict, expected updated_at {}, got {}",
                    self.updated_at, updated_at
                ),
            )));
        }
        if self.status < 0 || self.status > 1 {
            return Err(anyhow::Error::new(HTTPError::new(
                409,
                format!("Creation can not be update, status {}", self.status),
            )));
        }

        let mut incr = 1usize; // should add `updated_at`
        if cols.has("content") {
            incr += 1; // should add `version`
        }

        let mut set_fields: Vec<String> = Vec::with_capacity(update_fields.len() + incr);
        let mut params: Vec<CqlValue> = Vec::with_capacity(update_fields.len() + incr + 3);

        let new_updated_at = (unix_ms() / 1000) as i64;
        let mut new_version = self.version;
        set_fields.push("updated_at=?".to_string());
        params.push(CqlValue::BigInt(new_updated_at));
        if cols.has("content") {
            set_fields.push("version=?".to_string());
            new_version += 1;
            params.push(CqlValue::SmallInt(new_version));
        }
        for field in &update_fields {
            set_fields.push(format!("{}=?", field));
            params.push(cols.get(field).unwrap().to_owned());
        }

        let query = format!(
            "UPDATE creation SET {} WHERE gid=? AND id=? IF updated_at=?",
            set_fields.join(",")
        );
        params.push(CqlValue::Blob(self.gid.as_bytes().to_vec()));
        params.push(CqlValue::Blob(self.id.as_bytes().to_vec()));
        params.push(CqlValue::BigInt(updated_at));

        let _ = db.execute(query, params).await?;
        self.updated_at = new_updated_at;
        self.version = new_version;
        Ok(new_updated_at)
    }

    pub async fn delete(&mut self, db: &scylladb::ScyllaDB, version: i16) -> anyhow::Result<bool> {
        let res = self.get_one(db, vec!["version".to_string()]).await;
        if res.is_err() {
            return Ok(false); // already deleted
        }

        if self.version != version {
            return Err(anyhow::Error::new(HTTPError::new(
                409,
                format!(
                    "Creation version conflict, expected version {}, got {}",
                    self.version, version
                ),
            )));
        }

        self.get_one(db, Vec::new()).await?;
        self.updated_at = (unix_ms() / 1000) as i64;

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
            "INSERT INTO deleted_creation ({}) VALUES ({})",
            cols_name.join(","),
            vals_name.join(","),
        );

        let delete_query = "DELETE FROM creation WHERE gid=? AND id=?";
        let delete_params = (self.gid.as_bytes(), self.id.as_bytes());

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
    ) -> anyhow::Result<Vec<Creation>> {
        let fields = Self::select_fields(select_fields, true)?;

        let rows = if let Some(id) = page_token {
            let query = Query::new(format!(
                "SELECT {} FROM creation WHERE gid=? AND id<? AND status>=0 ORDER BY id DESC LIMIT ? ALLOW FILTERING BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            )).with_page_size(page_size as i32);
            let params = (gid.as_bytes(), id.as_bytes(), page_size as i32);
            db.execute_paged(query, params, None).await?
        } else {
            let query = Query::new(format!(
                "SELECT {} FROM creation WHERE gid=? AND status>=0 ORDER BY id DESC LIMIT ? ALLOW FILTERING BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            )).with_page_size(page_size as i32);
            let params = (gid.as_bytes(), page_size as i32);
            db.execute_iter(query, params).await? // TODO: execute_iter or execute_paged?
        };

        let mut res: Vec<Creation> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut doc = Creation::default();
            let mut cols = ColumnsMap::with_capacity(fields.len());
            cols.fill(row, fields.clone())?;
            doc.fill(&cols);
            doc._fields = fields.iter().map(|f| f.to_string()).collect();
            res.push(doc);
        }

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use ciborium::cbor;
    use std::str::FromStr;
    use tokio::sync::OnceCell;

    use crate::{conf, erring};

    use super::*;

    static DB: OnceCell<scylladb::ScyllaDB> = OnceCell::const_new();

    async fn get_db() -> scylladb::ScyllaDB {
        let cfg = conf::Conf::new().unwrap_or_else(|err| panic!("config error: {}", err));
        let res = scylladb::ScyllaDB::new(cfg.scylla, "writing_test").await;
        res.unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore]
    async fn creation_model_works() -> anyhow::Result<()> {
        assert!(Creation::fields().contains(&"license".to_string()));
        assert!(!Creation::fields().contains(&"_fields".to_string()));

        let db = DB.get_or_init(get_db).await;
        let did = xid::new();
        let uid = xid::Id::from_str("jarvis00000000000000")?;

        // create
        {
            let mut doc = Creation::with_pk(uid, did);
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

            let mut doc2 = Creation::with_pk(uid, did);
            doc2.get_one(db, vec![]).await?;
            // println!("doc: {:#?}", doc2);

            assert_eq!(doc2.title.as_str(), "Hello World");
            assert_eq!(doc2.version, 1);
            assert_eq!(doc2.language, Language::Eng);
            assert_eq!(doc2.content, doc.content);

            let mut doc3 = Creation::with_pk(uid, did);
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
            let mut doc = Creation::with_pk(uid, did);
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
            assert!(res >= doc.updated_at);
            assert_eq!(doc.version, 1);

            let mut cols = ColumnsMap::new();
            cols.set_as("title", &"title 2".to_string())?;
            cols.set_as("description", &"description 2".to_string())?;
            cols.set_as("cover", &"cover 2".to_string())?;
            cols.set_as("summary", &"summary 2".to_string())?;
            cols.set_as("keywords", &vec!["keyword".to_string()])?;
            cols.set_as("labels", &vec!["label 1".to_string()])?;
            cols.set_as("authors", &vec!["author 1".to_string()])?;
            cols.set_as("summary", &"summary 2".to_string())?;

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
            cols.set_as("content", &content)?;
            cols.set_as("license", &"license 2".to_string())?;
            let res = doc.update(db, cols, doc.updated_at).await?;
            assert!(res >= doc.updated_at);
            assert_eq!(doc.version, 2);
        }

        // delete
        {
            let mut doc = Creation::with_pk(uid, did);
            let res = doc.delete(db, 0).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 409);

            let res = doc.delete(db, 2).await?;
            assert!(res);

            let res = doc.delete(db, 2).await?;
            assert!(!res); // already deleted
        }

        Ok(())
    }
}
