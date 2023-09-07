use isolang::Language;
use sha3::{Digest, Sha3_256};

use axum_web::context::unix_ms;
use axum_web::erring::HTTPError;
use scylla_orm::{ColumnsMap, CqlValue, ToCqlVal};
use scylla_orm_macros::CqlOrm;

use crate::db::{scylladb, scylladb::extract_applied};

#[derive(Debug, Default, Clone, CqlOrm)]
pub struct Content {
    pub id: xid::Id,
    pub gid: xid::Id,
    pub cid: xid::Id,
    pub status: i8,
    pub version: i16,
    pub language: Language,
    pub updated_at: i64,
    pub length: i32,
    pub hash: Vec<u8>,
    pub content: Vec<u8>,

    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
}

impl Content {
    pub fn with_pk(id: xid::Id) -> Self {
        Self {
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
            let id = "id".to_string();
            if !select_fields.contains(&id) {
                select_fields.push(id);
            }
            return Ok(select_fields);
        }

        Ok(select_fields)
    }

    pub fn valid_status(&self, status: i8) -> anyhow::Result<()> {
        if !(-1..=0).contains(&status) || !(-1..=0).contains(&self.status) {
            return Err(HTTPError::new(400, format!("Invalid status, {}", status)).into());
        }

        match self.status {
            -1 if !(-1..=0).contains(&status) => Err(HTTPError::new(
                400,
                format!(
                    "Content status is {}, expected update to 0, got {}",
                    self.status, status
                ),
            )
            .into()),
            0 if !(-1..=0).contains(&status) => Err(HTTPError::new(
                400,
                format!(
                    "Content status is {}, expected update to -1 or 1, got {}",
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
            "SELECT {} FROM content WHERE id=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.id.to_cql(),);
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, &fields)?;
        self.fill(&cols);

        if self.length == 0 {
            self.length = self.content.len() as i32;
        }

        Ok(())
    }

    pub async fn save(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        let fields = Self::fields();
        self._fields = fields.clone();

        self.length = self.content.len() as i32;
        let mut hasher = Sha3_256::new();
        hasher.update(&self.content);
        self.hash = hasher.finalize().to_vec();

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
            "INSERT INTO content ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(
                HTTPError::new(409, "Content save failed, please try again".to_string()).into(),
            );
        }

        Ok(true)
    }

    pub async fn update_content(
        &mut self,
        db: &scylladb::ScyllaDB,
        version: i16,
        language: Language,
        content: Vec<u8>,
    ) -> anyhow::Result<bool> {
        let new_updated_at = unix_ms() as i64;
        let length = content.len() as i32;
        let mut hasher = Sha3_256::new();
        hasher.update(&content);
        let hash: Vec<u8> = hasher.finalize().to_vec();

        let query =
            "UPDATE content SET updated_at=?,version=?,language=?,length=?,hash=?,content=? WHERE id=? IF EXISTS";
        let params = (
            new_updated_at,
            version,
            language.to_cql(),
            length,
            hash.to_cql(),
            content.to_cql(),
            self.id.to_cql(),
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                "Content update_content failed, please try again".to_string(),
            )
            .into());
        }

        self.updated_at = new_updated_at;
        self.version = version;
        self.language = language;
        self.length = length;
        self.hash = hash;
        self.content = content;
        Ok(true)
    }

    pub async fn update_status(
        &mut self,
        db: &scylladb::ScyllaDB,
        status: i8,
    ) -> anyhow::Result<bool> {
        self.get_one(db, vec!["status".to_string()]).await?;
        self.valid_status(status)?;
        if self.status == status {
            return Ok(false); // no need to update
        }

        let new_updated_at = unix_ms() as i64;
        let query = "UPDATE content SET status=?,updated_at=? WHERE id=? IF status=?";
        let params = (status, new_updated_at, self.id.to_cql(), self.status);

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                format!("Content update_status {} failed, please try again", status),
            )
            .into());
        }

        self.updated_at = new_updated_at;
        self.status = status;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use axum_web::object::cbor_to_vec;
    use ciborium::cbor;
    use std::str::FromStr;
    use tokio::sync::OnceCell;

    use crate::conf;
    use crate::db;
    use axum_web::erring;

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
        content_model_works().await;
    }

    // #[tokio::test(flavor = "current_thread")]
    async fn content_model_works() {
        let db = get_db().await;
        let id = xid::new();
        let gid = xid::Id::from_str(db::USER_JARVIS).unwrap();
        let cid = xid::new();

        // valid_status
        {
            let mut doc = Content::with_pk(id);
            assert!(doc.valid_status(-2).is_err());
            assert!(doc.valid_status(-1).is_ok());
            assert!(doc.valid_status(0).is_ok());
            assert!(doc.valid_status(1).is_err());

            doc.status = -1;
            assert!(doc.valid_status(-2).is_err());
            assert!(doc.valid_status(-1).is_ok());
            assert!(doc.valid_status(0).is_ok());
            assert!(doc.valid_status(1).is_err());

            doc.status = 1;
            assert!(doc.valid_status(-2).is_err());
            assert!(doc.valid_status(-1).is_err());
            assert!(doc.valid_status(0).is_err());
            assert!(doc.valid_status(1).is_err());
        }

        // create
        {
            let mut doc = Content::with_pk(id);
            doc.gid = gid;
            doc.cid = cid;
            doc.language = Language::Eng;
            doc.version = 1;
            doc.content = cbor_to_vec(
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
            )
            .unwrap();

            let res = doc.get_one(db, vec![]).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 404);

            assert!(doc.save(db).await.unwrap());
            let res = doc.save(db).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into(); // can not insert twice
            assert_eq!(err.code, 409);

            let mut doc2 = Content::with_pk(id);
            doc2.get_one(db, vec![]).await.unwrap();

            assert_eq!(doc2.cid, cid);
            assert_eq!(doc2.version, 1);
            assert_eq!(doc2.language, Language::Eng);
            assert_eq!(doc2.content, doc.content);

            let mut doc3 = Content::with_pk(id);
            doc3.get_one(db, vec!["cid".to_string(), "version".to_string()])
                .await
                .unwrap();
            assert_eq!(doc3.cid, cid);
            assert_eq!(doc3.version, 1);
            assert_eq!(doc3.language, Language::default());
        }

        // update_content
        {
            let mut doc = Content::with_pk(id);
            doc.get_one(db, vec![]).await.unwrap();

            let mut new_doc = Content::with_pk(xid::new());
            let res = new_doc
                .update_content(db, doc.version + 1, doc.language, doc.content.clone())
                .await;

            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into(); // can not insert twice
            assert_eq!(err.code, 409);

            let content: Vec<u8> = cbor_to_vec(
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
                            "text" => "Hello World2",
                        }],
                    }],
                })
                .unwrap(),
            )
            .unwrap();

            let res = doc
                .update_content(db, doc.version + 1, doc.language, content.clone())
                .await
                .unwrap();
            assert!(res);
            assert_eq!(doc.version, 2);
            assert_eq!(&doc.content, &content);

            let mut doc2 = Content::with_pk(id);
            doc2.get_one(db, vec![]).await.unwrap();

            assert_eq!(doc2.cid, cid);
            assert_eq!(doc2.version, 2);
            assert_eq!(&doc2.content, &content);

            // update again
            let res = doc
                .update_content(db, doc.version, doc.language, content)
                .await
                .unwrap();
            assert!(res);
        }

        // update status
        {
            let mut doc = Content::with_pk(id);
            doc.get_one(db, vec![]).await.unwrap();

            let res = doc.update_status(db, 1).await;
            assert!(res.is_err());

            let res = doc.update_status(db, 0).await.unwrap();
            assert!(!res);

            let res = doc.update_status(db, -1).await.unwrap();
            assert!(res);

            let res = doc.update_status(db, -1).await.unwrap();
            assert!(!res);
        }
    }
}
