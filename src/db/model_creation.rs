use isolang::Language;
use std::time::{Duration, SystemTime};

use axum_web::context::unix_ms;
use axum_web::erring::HTTPError;
use scylla_orm::{ColumnsMap, CqlValue, ToCqlVal};
use scylla_orm_macros::CqlOrm;

use crate::db::{
    meili,
    scylladb::{self, extract_applied},
    Content, MAX_ID,
};

#[derive(Debug, Default, Clone, CqlOrm)]
pub struct CreationIndex {
    pub id: xid::Id,
    pub gid: xid::Id,
    pub rating: i8,
    pub price: i64,
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

        let query = format!(
            "SELECT {} FROM creation_index WHERE id=? LIMIT 1",
            self._fields.join(",")
        );
        let params = (self.id.to_cql(),);
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(self._fields.len());
        cols.fill(res, &self._fields)?;
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
        let mut cols_name: Vec<&str> = Vec::with_capacity(self._fields.len());
        let mut vals_name: Vec<&str> = Vec::with_capacity(self._fields.len());
        let mut params: Vec<&CqlValue> = Vec::with_capacity(self._fields.len());
        let cols = self.to();

        for field in &self._fields {
            cols_name.push(field);
            vals_name.push("?");
            params.push(cols.get(field).unwrap());
        }

        let query = format!(
            "INSERT INTO creation_index ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(409, "CreationIndex already exists".to_string()).into());
        }

        Ok(true)
    }

    pub async fn batch_get(
        db: &scylladb::ScyllaDB,
        ids: Vec<xid::Id>,
        max_rating: i8,
    ) -> anyhow::Result<Vec<CreationIndex>> {
        let fields = Self::fields();

        let mut vals_name: Vec<&str> = Vec::with_capacity(ids.len());
        let mut params: Vec<CqlValue> = Vec::with_capacity(ids.len() + 1);

        for id in &ids {
            vals_name.push("?");
            params.push(id.to_cql());
        }

        let query = format!(
            "SELECT {} FROM creation_index WHERE id IN ({}) AND rating<=? ALLOW FILTERING",
            fields.join(","),
            vals_name.join(",")
        );
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

    pub async fn update_field(
        &mut self,
        db: &scylladb::ScyllaDB,
        field: &str,
    ) -> anyhow::Result<bool> {
        let query = format!("UPDATE creation_index SET {}=? WHERE id=? IF EXISTS", field);
        let params = match field {
            "rating" => (self.rating.to_cql(), self.id.to_cql()),
            "price" => (self.price.to_cql(), self.id.to_cql()),
            _ => return Err(HTTPError::new(400, format!("Invalid field: {}", field)).into()),
        };

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                format!(
                    "CreationIndex update_field {} failed, please try again",
                    field
                ),
            )
            .into());
        }

        Ok(true)
    }
}

#[derive(Debug, Default, Clone, CqlOrm, PartialEq)]
pub struct Creation {
    pub gid: xid::Id,
    pub id: xid::Id,
    pub status: i8,
    pub version: i16,
    pub language: Language,
    pub creator: xid::Id,
    pub created_at: i64,
    pub updated_at: i64,
    pub original_url: String,
    pub genre: Vec<String>,
    pub title: String,
    pub cover: String,
    pub keywords: Vec<String>,
    pub labels: Vec<String>,
    pub authors: Vec<String>,
    pub reviewers: Vec<xid::Id>,
    pub summary: String,
    pub content: xid::Id,
    pub license: String,

    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
    pub _rating: Option<i8>,  // 内容安全分级
    pub _price: Option<i64>,
    pub _length: i32, // 内容字节长度
    pub _content: Vec<u8>,
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

        let mut select_fields = select_fields;
        let field = "language".to_string(); // language 字段在 meilisearch 中用作 PK，必须存在
        if !select_fields.contains(&field) {
            select_fields.push(field);
        }
        let field = "version".to_string();
        if !select_fields.contains(&field) {
            select_fields.push(field);
        }
        let field = "status".to_string();
        if !select_fields.contains(&field) {
            select_fields.push(field);
        }

        if with_pk {
            let gid = "gid".to_string();
            if !select_fields.contains(&gid) {
                select_fields.push(gid);
            }
            let id = "id".to_string();
            if !select_fields.contains(&id) {
                select_fields.push(id);
            }
        }

        Ok(select_fields)
    }

    pub fn to_meili(&self) -> meili::Document {
        let mut doc = meili::Document::new(self.id, self.language, self.gid);
        doc.kind = 0;
        doc.version = self.version;
        doc.updated_at = self.updated_at;
        if !self.genre.is_empty() {
            doc.genre = Some(self.genre.clone());
        }
        if !self.title.is_empty() {
            doc.title = Some(self.title.clone());
        }
        if !self.keywords.is_empty() {
            doc.keywords = Some(self.keywords.clone());
        }
        if !self.authors.is_empty() {
            doc.authors = Some(self.authors.clone());
        }
        if !self.summary.is_empty() {
            doc.summary = Some(self.summary.clone());
        }
        doc
    }

    pub fn valid_status(&self, status: i8) -> anyhow::Result<()> {
        if !(-1..=2).contains(&status) || !(-1..=2).contains(&self.status) {
            return Err(HTTPError::new(400, format!("Invalid status, {}", status)).into());
        }

        match self.status {
            -1 if 0 != status => Err(HTTPError::new(
                400,
                format!(
                    "Creation status is {}, expected update to 0, got {}",
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
            2 if !(-1..=0).contains(&status) => Err(HTTPError::new(
                400,
                format!(
                    "Creation status is {}, expected update to -1, 0, got {}",
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

        if self._fields.contains(&"content".to_string()) {
            let mut doc = Content::with_pk(self.content);
            doc.get_one(db, vec!["content".to_string()]).await?;
            self._content = doc.content;
        }

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

    pub async fn save_with(
        &mut self,
        db: &scylladb::ScyllaDB,
        price: i64,
        content: Vec<u8>,
    ) -> anyhow::Result<bool> {
        let mut index = CreationIndex::with_pk(self.id);
        index.gid = self.gid;
        index.price = price;
        index.save(db).await?;

        let now = unix_ms() as i64;
        self.created_at = now;
        self.updated_at = now;
        self.version = 1;
        self.content = xid::new();

        let mut doc = Content {
            id: self.content,
            gid: self.gid,
            cid: self.id,
            version: self.version,
            language: self.language,
            updated_at: self.updated_at,
            content: content.clone(),
            ..Default::default()
        };
        doc.save(db).await?;

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
            return Err(
                HTTPError::new(409, "Creation save failed, please try again".to_string()).into(),
            );
        }

        self._content = content;
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
                    "Creation updated_at conflict, expected {}, got {}",
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

    // upgrade when creating publication.
    pub async fn upgrade_version(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<()> {
        let updated_at = unix_ms() as i64;
        let query = "UPDATE creation SET updated_at=?,version=? WHERE gid=? AND id=? IF version=?";
        let params = (
            updated_at,
            self.version + 1,
            self.gid.to_cql(),
            self.id.to_cql(),
            self.version,
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                "Creation upgrade version failed, please try again".to_string(),
            )
            .into());
        }

        self.updated_at = updated_at;
        self.version += 1;
        Ok(())
    }

    pub async fn update_content(
        &mut self,
        db: &scylladb::ScyllaDB,
        language: Language,
        content: Vec<u8>,
        updated_at: i64,
    ) -> anyhow::Result<bool> {
        self.get_one(
            db,
            vec![
                "status".to_string(),
                "version".to_string(),
                "updated_at".to_string(),
                "content".to_string(),
            ],
        )
        .await?;
        if self.updated_at != updated_at {
            return Err(HTTPError::new(
                409,
                format!(
                    "Creation updated_at conflict, expected {}, got {}",
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

        if language != Language::Und {
            self.language = language;
        }

        let mut doc = Content::with_pk(self.content);
        doc.update_content(db, self.version, self.language, content.clone())
            .await?;

        let query =
            "UPDATE creation SET updated_at=?,language=? WHERE gid=? AND id=? IF updated_at=?";
        let params = (
            doc.updated_at,
            self.language.to_cql(),
            self.gid.to_cql(),
            self.id.to_cql(),
            updated_at,
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                "Creation update_content failed, please try again".to_string(),
            )
            .into());
        }

        self.updated_at = doc.updated_at;
        self._content = content;
        Ok(true)
    }

    pub async fn update(
        &mut self,
        db: &scylladb::ScyllaDB,
        cols: ColumnsMap,
        updated_at: i64,
    ) -> anyhow::Result<bool> {
        let valid_fields = [
            "title", "cover", "keywords", "labels", "authors", "summary", "license",
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
                "updated_at".to_string(),
                "version".to_string(), // for meilisearch update
            ],
        )
        .await?;
        if self.updated_at != updated_at {
            return Err(HTTPError::new(
                409,
                format!(
                    "Creation updated_at conflict, expected {}, got {}",
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
                "Creation update failed, please try again".to_string(),
            )
            .into());
        }

        self.fill(&cols); // fill for meilisearch update
        self.updated_at = new_updated_at;
        Ok(true)
    }

    pub async fn delete(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        let res = self.get_one(db, Vec::new()).await;
        if res.is_err() {
            return Ok(false); // already deleted
        }

        if self.status != -1 {
            return Err(HTTPError::new(
                409,
                format!("Creation status conflict, expected -1, got {}", self.status),
            )
            .into());
        }

        let mut doc = Content::with_pk(self.content);
        doc.update_status(db, -1).await?;
        self.updated_at = doc.updated_at;

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

    pub async fn list_by_gid(
        db: &scylladb::ScyllaDB,
        gid: xid::Id,
        select_fields: Vec<String>,
        page_size: u16,
        page_token: Option<xid::Id>,
        status: Option<i8>,
    ) -> anyhow::Result<Vec<Creation>> {
        let fields = Self::select_fields(select_fields, true)?;

        let token = match page_token {
            Some(id) => id,
            None => MAX_ID,
        };

        let rows = if let Some(status) = status {
            let query = format!(
                "SELECT {} FROM creation WHERE gid=? AND status=? AND id<? LIMIT ? USING TIMEOUT 3s",
                fields.clone().join(","));
            let params = (gid.to_cql(), status, token.to_cql(), page_size as i32);
            db.execute_iter(query, params).await?
        } else {
            let query = format!(
                "SELECT {} FROM creation WHERE gid=? AND id<? AND status>=0 LIMIT ? ALLOW FILTERING USING TIMEOUT 3s",
                fields.clone().join(","));
            let params = (gid.to_cql(), token.to_cql(), page_size as i32);
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

    pub async fn list_by_gid_url(
        db: &scylladb::ScyllaDB,
        gid: xid::Id,
        url: String,
        select_fields: Vec<String>,
    ) -> anyhow::Result<Vec<Creation>> {
        let mut fields = Self::select_fields(select_fields, true)?;
        let field = "updated_at".to_string();
        if !fields.contains(&field) {
            fields.push(field)
        }

        let query = format!(
                "SELECT {} FROM creation WHERE gid=? AND original_url=? LIMIT 10 BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            );
        let params = (gid.to_cql(), url);
        let rows = db.execute_iter(query, params).await?;

        let mut res: Vec<Creation> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut doc = Creation::default();
            let mut cols = ColumnsMap::with_capacity(fields.len());
            cols.fill(row, &fields)?;
            doc.fill(&cols);
            doc._fields = fields.clone();
            res.push(doc);
        }

        res.sort_by(|a, b| b.updated_at.partial_cmp(&a.updated_at).unwrap());

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use ciborium::cbor;
    use std::str::FromStr;

    use crate::conf;
    use crate::db;
    use axum_web::{erring, object::cbor_to_vec};
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
        // problem: https://users.rust-lang.org/t/tokio-runtimes-and-tokio-oncecell/91351/5
        creation_index_model_works().await;
        creation_model_works().await;
        creation_find_works().await;
    }

    // #[tokio::test(flavor = "current_thread")]
    async fn creation_index_model_works() {
        let db = get_db().await;

        let mut c1 = CreationIndex {
            id: xid::new(),
            gid: xid::new(),
            ..Default::default()
        };
        c1.save(db).await.unwrap();

        let mut c2 = CreationIndex {
            id: xid::new(),
            gid: xid::new(),
            rating: 127,
            ..Default::default()
        };
        c2.save(db).await.unwrap();

        let mut c3 = CreationIndex {
            id: xid::new(),
            gid: xid::new(),
            rating: 3,
            ..Default::default()
        };
        c3.save(db).await.unwrap();

        let mut d1 = CreationIndex::with_pk(c1.id);
        d1.get_one(db).await.unwrap();
        assert_eq!(d1.gid, c1.gid);

        let docs = CreationIndex::batch_get(db, vec![c1.id, c3.id, c2.id], 3)
            .await
            .unwrap();
        assert!(docs.len() == 2);

        let docs = CreationIndex::batch_get(db, vec![c1.id, c3.id, c2.id], 127)
            .await
            .unwrap();
        assert!(docs.len() == 3);
    }

    // #[tokio::test(flavor = "current_thread")]
    async fn creation_model_works() {
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
            assert!(doc.valid_status(-1).is_err());
            assert!(doc.valid_status(0).is_ok());
            assert!(doc.valid_status(1).is_err());
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
            assert!(doc.valid_status(1).is_err());
            assert!(doc.valid_status(2).is_err());
            assert!(doc.valid_status(3).is_err());
        }

        // create
        {
            let mut doc = Creation::with_pk(gid, cid);
            doc.language = Language::Eng;
            doc.title = "Hello World".to_string();

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

            assert!(doc.save_with(db, 0, content.clone()).await.unwrap());
            let res = doc.save_with(db, 0, content.clone()).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into(); // can not insert twice
            assert_eq!(err.code, 409);

            let mut doc2 = Creation::with_pk(gid, cid);
            doc2.get_one(db, vec![]).await.unwrap();
            // println!("doc: {:#?}", doc2);

            assert_eq!(doc2.title.as_str(), "Hello World");
            assert_eq!(doc2.version, 1);
            assert_eq!(doc2.language, Language::Eng);
            assert_eq!(doc2.content, doc.content);
            assert_eq!(&doc2._content, &content);

            let mut doc3 = Creation::with_pk(gid, cid);
            doc3.get_one(db, vec!["gid".to_string(), "title".to_string()])
                .await
                .unwrap();
            assert_eq!(doc3.title.as_str(), "Hello World");
            assert_eq!(doc3.version, 1);
            assert_eq!(doc3.language, Language::Eng);
            assert_eq!(
                doc3._fields,
                vec!["gid", "title", "language", "version", "status"]
            );
            assert!(doc3._content.is_empty());
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
            let res = doc.update(db, cols, doc.updated_at).await.unwrap();
            assert!(res);

            let mut cols = ColumnsMap::new();
            cols.set_as("title", &"title 2".to_string());
            cols.set_as("cover", &"cover 2".to_string());
            cols.set_as("summary", &"summary 2".to_string());
            cols.set_as("keywords", &vec!["keyword".to_string()]);
            cols.set_as("labels", &vec!["label 1".to_string()]);
            cols.set_as("authors", &vec!["author 1".to_string()]);
            cols.set_as("license", &"license 2".to_string());
            let res = doc.update(db, cols, doc.updated_at).await.unwrap();
            assert!(res);
        }

        // update content
        {
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

            // update_content
            let mut doc = Creation::with_pk(gid, cid);
            doc.get_one(db, vec![]).await.unwrap();

            let res = doc
                .update_content(db, doc.language, content.clone(), doc.updated_at - 1)
                .await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 409); // updated_at not match

            doc.update_status(db, -1, doc.updated_at).await.unwrap();
            let res = doc
                .update_content(db, doc.language, content.clone(), doc.updated_at)
                .await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 409);

            doc.update_status(db, 0, doc.updated_at).await.unwrap();
            doc.update_status(db, 1, doc.updated_at).await.unwrap();
            doc.update_status(db, 2, doc.updated_at).await.unwrap();
            let res = doc
                .update_content(db, doc.language, content.clone(), doc.updated_at)
                .await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 409);

            doc.update_status(db, 0, doc.updated_at).await.unwrap();
            let res = doc
                .update_content(db, doc.language, content.clone(), doc.updated_at)
                .await
                .unwrap();
            assert!(res);
            assert_eq!(&doc._content, &content);

            let mut doc2 = Creation::with_pk(gid, cid);
            doc2.get_one(db, vec![]).await.unwrap();

            assert_eq!(doc2.content, doc.content);
            assert_eq!(&doc2._content, &content);
        }

        // update status
        {
            let mut doc = Creation::with_pk(gid, cid);
            doc.get_one(db, vec![]).await.unwrap();

            let res = doc.update_status(db, 2, doc.updated_at - 1).await;
            assert!(res.is_err());

            let res = doc.update_status(db, 2, doc.updated_at).await;
            assert!(res.is_err());

            let res = doc.update_status(db, 1, doc.updated_at).await.unwrap();
            assert!(res);

            let res = doc.update_status(db, 1, doc.updated_at).await.unwrap();
            assert!(!res);
        }

        // upgrade version
        {
            let mut doc = Creation::with_pk(gid, cid);
            doc.get_one(db, vec![]).await.unwrap();
            let version = doc.version;

            let res = doc.upgrade_version(db).await;
            assert!(res.is_ok());
            assert_eq!(doc.version, version + 1);

            let mut doc2 = Creation::with_pk(gid, cid);
            doc2.get_one(db, vec![]).await.unwrap();
            assert_eq!(doc2.version, version + 1);

            let res = doc.upgrade_version(db).await;
            assert!(res.is_ok());
            assert_eq!(doc.version, version + 2);

            doc.version -= 1;
            let res = doc.upgrade_version(db).await;
            assert!(res.is_err());
        }

        // delete
        {
            let mut backup = Creation::with_pk(gid, cid);
            backup.get_one(db, vec![]).await.unwrap();
            backup.updated_at = 0;
            backup._content = vec![];

            let mut deleted = Creation::with_pk(gid, cid);
            let res = deleted.get_deleted(db).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 404);

            let mut doc = Creation::with_pk(gid, cid);
            let res = doc.delete(db).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 409);

            doc.update_status(db, -1, doc.updated_at).await.unwrap();
            let res = doc.delete(db).await.unwrap();
            assert!(res);

            let res = doc.delete(db).await.unwrap();
            assert!(!res); // already deleted

            deleted.get_deleted(db).await.unwrap();
            deleted.updated_at = 0;
            backup.status = -1;
            assert_eq!(deleted, backup);
        }
    }

    // #[tokio::test(flavor = "current_thread")]
    async fn creation_find_works() {
        let db = get_db().await;
        let gid = xid::new();
        let mut content: Vec<u8> = Vec::new();
        ciborium::into_writer(
            &cbor!({
                "type" => "doc",
                "content" => [],
            })
            .unwrap(),
            &mut content,
        )
        .unwrap();

        let mut docs: Vec<Creation> = Vec::new();
        for i in 0..10 {
            let mut doc = Creation::with_pk(gid, xid::new());
            doc.language = Language::Eng;
            doc.title = format!("Hello World {}", i);
            doc.save_with(db, 0, content.clone()).await.unwrap();

            docs.push(doc)
        }
        assert_eq!(docs.len(), 10);

        let latest = Creation::list_by_gid(db, gid, Vec::new(), 1, None, None)
            .await
            .unwrap();
        assert_eq!(latest.len(), 1);
        let mut latest = latest[0].to_owned();
        assert_eq!(latest.gid, docs.last().unwrap().gid);
        assert_eq!(latest.id, docs.last().unwrap().id);

        latest
            .update_status(db, 1, latest.updated_at)
            .await
            .unwrap();
        let res = Creation::list_by_gid(db, gid, vec!["title".to_string()], 100, None, None)
            .await
            .unwrap();
        assert_eq!(res.len(), 10);

        let res = Creation::list_by_gid(db, gid, vec!["title".to_string()], 100, None, Some(1))
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].id, docs.last().unwrap().id);

        let res = Creation::list_by_gid(db, gid, vec!["title".to_string()], 5, None, None)
            .await
            .unwrap();
        assert_eq!(res.len(), 5);
        assert_eq!(res[4].id, docs[5].id);

        let res = Creation::list_by_gid(
            db,
            gid,
            vec!["title".to_string()],
            5,
            Some(docs[5].id),
            None,
        )
        .await
        .unwrap();
        assert_eq!(res.len(), 5);
        assert_eq!(res[4].id, docs[0].id);

        let res = Creation::list_by_gid(
            db,
            gid,
            vec!["title".to_string()],
            5,
            Some(docs[5].id),
            Some(1),
        )
        .await
        .unwrap();
        assert_eq!(res.len(), 0);
    }
}
