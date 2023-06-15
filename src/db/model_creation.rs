use std::collections::HashSet;
use std::time::{Duration, SystemTime};

use super::{
    scylladb,
    scylladb::{extract_applied, CqlValue},
};
use crate::erring::HTTPError;
use isolang::Language;
use scylla_orm::ColumnsMap;
use scylla_orm_macros::CqlOrm;

#[derive(Debug, Default, Clone, CqlOrm)]
pub struct CreationIndex {
    pub id: xid::Id,
    pub gid: xid::Id,
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

        let query = "SELECT gid FROM creation_index WHERE id=? LIMIT 1";
        let params = (self.id.as_bytes(),);
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(1);
        cols.fill(res, vec!["gid".to_string()])?;
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
        let query = "INSERT INTO creation_index (id,gid) VALUES (?,?) IF NOT EXISTS";
        let params = (self.id.as_bytes(), self.gid.as_bytes());
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
        use std::ops::Sub;
        let now = SystemTime::now().sub(Duration::from_secs(10));
        if self.id.time() < now {
            return Err(anyhow::Error::new(HTTPError::new(
                400,
                format!("Invalid id {:?}", self.id),
            )));
        }
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

    pub async fn find(
        db: &scylladb::ScyllaDB,
        gid: xid::Id,
        select_fields: Vec<String>,
        page_size: u16,
        page_token: Option<xid::Id>,
    ) -> anyhow::Result<Vec<Creation>> {
        let fields = Self::select_fields(select_fields, true)?;

        let rows = if let Some(id) = page_token {
            let query = format!(
                "SELECT {} FROM creation WHERE gid=? AND id<=? AND status>=0 ORDER BY id DESC LIMIT ? ALLOW FILTERING BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            );
            let params = (gid.as_bytes(), id.as_bytes(), page_size as i32);
            db.execute_iter(query, params).await?
        } else {
            let query = format!(
                "SELECT {} FROM creation WHERE gid=? AND status>=0 ORDER BY id DESC LIMIT ? ALLOW FILTERING BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            );
            let params = (gid.as_bytes(), page_size as i32);
            db.execute_iter(query, params).await?
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
    async fn creation_model_works() {
        assert!(Creation::fields().contains(&"license".to_string()));
        assert!(!Creation::fields().contains(&"_fields".to_string()));

        let db = DB.get_or_init(get_db).await;
        let did = xid::new();
        let uid = xid::Id::from_str("jarvis00000000000000").unwrap();
        let mut doc = Creation::with_pk(uid, did);
        doc.version = 1;
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
            })
            .unwrap(),
            &mut doc.content,
        )
        .unwrap();

        // println!("doc: {:?}", doc.content);
        // 0xa2647479706563646f6367636f6e74656e7481a364747970656768656164696e67656174747273a26269646659335431496b656c6576656c0167636f6e74656e7481a26474797065647465787464746578746b48656c6c6f20576f726c64

        let res = doc.get_one(db, vec![]).await;
        assert!(res.is_err());
        let err: erring::HTTPError = res.unwrap_err().into();
        assert_eq!(err.code, 404);

        assert!(doc.save(db).await.unwrap());
        let res = doc.save(db).await;
        assert!(res.is_err());
        let err: erring::HTTPError = res.unwrap_err().into(); // can not insert twice
        assert_eq!(err.code, 409);

        let mut doc2 = Creation::with_pk(uid, did);
        doc2.get_one(db, vec![]).await.unwrap();
        // println!("doc: {:#?}", doc2);

        assert_eq!(doc2.title.as_str(), "Hello World");
        assert_eq!(doc2.version, 1);
        assert_eq!(doc2.language, Language::Eng);
        assert_eq!(doc2.content, doc.content);

        let mut doc3 = Creation::with_pk(uid, did);
        doc3.get_one(db, vec!["gid".to_string(), "title".to_string()])
            .await
            .unwrap();
        assert_eq!(doc3.title.as_str(), "Hello World");
        assert_eq!(doc3.version, 0);
        assert_eq!(doc3.language, Language::default());
        assert_eq!(doc3._fields, vec!["gid", "title"]);
        assert!(doc3.content.is_empty());

        // println!("doc: {:#?}", doc3);
    }
}
