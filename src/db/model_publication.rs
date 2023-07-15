use isolang::Language;
use std::convert::From;

use scylla_orm::{ColumnsMap, CqlValue, ToCqlVal};
use scylla_orm_macros::CqlOrm;

use crate::db::{
    scylladb,
    scylladb::{extract_applied, Query},
    Content, Creation,
};
use axum_web::context::unix_ms;
use axum_web::erring::HTTPError;

#[derive(Debug, Default, Clone, CqlOrm, PartialEq)]
pub struct Publication {
    pub gid: xid::Id,
    pub cid: xid::Id,
    pub language: Language,
    pub version: i16,
    pub status: i8,
    pub creator: xid::Id,
    pub created_at: i64,
    pub updated_at: i64,
    pub model: String,
    pub original_url: String,
    pub genre: Vec<String>,
    pub title: String,
    pub description: String,
    pub cover: String,
    pub keywords: Vec<String>,
    pub authors: Vec<String>,
    pub summary: String,
    pub content: xid::Id,
    pub license: String,

    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
    pub _rating: i8,          // 内容安全分级
    pub _content: Vec<u8>,
}

impl From<Creation> for Publication {
    fn from(draft: Creation) -> Self {
        Self {
            gid: draft.gid,
            cid: draft.id,
            language: draft.language,
            version: draft.version,
            creator: draft.creator,
            original_url: draft.original_url,
            genre: draft.genre,
            title: draft.title,
            description: draft.description,
            cover: draft.cover,
            keywords: draft.keywords,
            authors: draft.authors,
            summary: draft.summary,
            content: draft.content,
            license: draft.license,
            ..Default::default()
        }
    }
}

impl Publication {
    pub fn with_pk(gid: xid::Id, cid: xid::Id, language: Language, version: i16) -> Self {
        Self {
            gid,
            cid,
            language,
            version,
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
            let cid = "cid".to_string();
            if !select_fields.contains(&cid) {
                select_fields.push(cid);
            }
            let language = "language".to_string();
            if !select_fields.contains(&language) {
                select_fields.push(language);
            }
            let version = "version".to_string();
            if !select_fields.contains(&version) {
                select_fields.push(version);
            }
            return Ok(select_fields);
        }

        Ok(select_fields)
    }

    pub fn valid_status(&self, status: i8) -> anyhow::Result<()> {
        if !(-1..=2).contains(&status) || !(-1..=2).contains(&self.status) {
            return Err(HTTPError::new(400, format!("Invalid status, {}", status)).into());
        }

        match self.status {
            -1 if 0 != status => Err(HTTPError::new(
                400,
                format!(
                    "Publication status is {}, expected update to 0, got {}",
                    self.status, status
                ),
            )
            .into()),
            0 if !(-1..=1).contains(&status) => Err(HTTPError::new(
                400,
                format!(
                    "Publication status is {}, expected update to -1 or 1, got {}",
                    self.status, status
                ),
            )
            .into()),
            1 if !(-1..=2).contains(&status) => Err(HTTPError::new(
                400,
                format!(
                    "Publication status is {}, expected update to -1, 0 or 2, got {}",
                    self.status, status
                ),
            )
            .into()),
            2 => Err(HTTPError::new(
                400,
                format!("Publication status is {}, can not be updated", self.status),
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
            "SELECT {} FROM publication WHERE gid=? AND cid=? AND language=? AND version=? LIMIT 1",
            fields.join(",")
        );
        let params = (
            self.gid.to_cql(),
            self.cid.to_cql(),
            self.language.to_cql(),
            self.version,
        );
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
            "SELECT {} FROM deleted_publication WHERE gid=? AND cid=? AND language=? AND version=? LIMIT 1",
            fields.join(",")
        );
        let params = (
            self.gid.to_cql(),
            self.cid.to_cql(),
            self.language.to_cql(),
            self.version,
        );
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, &fields)?;
        self.fill(&cols);

        Ok(())
    }

    pub async fn update_status(
        &mut self,
        db: &scylladb::ScyllaDB,
        status: i8,
        updated_at: i64,
    ) -> anyhow::Result<bool> {
        self.get_one(
            db,
            vec![
                "status".to_string(),
                "updated_at".to_string(),
                "language".to_string(),
            ],
        )
        .await?;
        if self.updated_at != updated_at {
            return Err(HTTPError::new(
                409,
                format!(
                    "Publication updated_at conflict, expected updated_at {}, got {}",
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
            "UPDATE publication SET status=?,updated_at=? WHERE gid=? AND cid=? AND language=? AND version=? IF updated_at=?";
        let params = (
            status,
            new_updated_at,
            self.gid.to_cql(),
            self.cid.to_cql(),
            self.language.to_cql(),
            self.version,
            updated_at,
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                format!(
                    "Publication update_status {} failed, please try again",
                    status
                ),
            )
            .into());
        }

        self.updated_at = new_updated_at;
        self.status = status;
        Ok(true)
    }

    pub async fn update_content(
        &mut self,
        db: &scylladb::ScyllaDB,
        content: Vec<u8>,
        updated_at: i64,
    ) -> anyhow::Result<bool> {
        self.get_one(
            db,
            vec![
                "status".to_string(),
                "version".to_string(),
                "language".to_string(),
                "updated_at".to_string(),
                "content".to_string(),
            ],
        )
        .await?;
        if self.updated_at != updated_at {
            return Err(HTTPError::new(
                409,
                format!(
                    "Publication updated_at conflict, expected updated_at {}, got {}",
                    self.updated_at, updated_at
                ),
            )
            .into());
        }

        if self.status != 0 {
            return Err(HTTPError::new(
                409,
                format!("Publication can not be update, status {}", self.status),
            )
            .into());
        }

        let mut doc = Content::with_pk(self.content);
        doc.update_content(db, self.version, self.language, content.clone())
            .await?;

        let query = "UPDATE publication SET updated_at=? WHERE gid=? AND cid=? AND language=? AND version=? IF updated_at=?";
        let params = (
            doc.updated_at,
            self.gid.to_cql(),
            self.cid.to_cql(),
            self.language.to_cql(),
            self.version,
            updated_at,
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                "Publication update_content failed, please try again".to_string(),
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
        let valid_fields = vec![
            "model",
            "title",
            "description",
            "cover",
            "keywords",
            "summary",
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
                    "Publication updated_at conflict, expected updated_at {}, got {}",
                    self.updated_at, updated_at
                ),
            )
            .into());
        }

        if self.status != 0 {
            return Err(HTTPError::new(
                409,
                format!("Publication can not be update, status {}", self.status),
            )
            .into());
        }

        let mut set_fields: Vec<String> = Vec::with_capacity(update_fields.len() + 1);
        let mut params: Vec<CqlValue> = Vec::with_capacity(update_fields.len() + 1 + 5);

        let new_updated_at = unix_ms() as i64;
        set_fields.push("updated_at=?".to_string());
        params.push(new_updated_at.to_cql());
        for field in &update_fields {
            set_fields.push(format!("{}=?", field));
            params.push(cols.get(field).unwrap().to_owned());
        }

        let query = format!(
            "UPDATE publication SET {} WHERE gid=? AND cid=? AND language=? AND version=? IF updated_at=?",
            set_fields.join(",")
        );
        params.push(self.gid.to_cql());
        params.push(self.cid.to_cql());
        params.push(self.language.to_cql());
        params.push(self.version.to_cql());
        params.push(updated_at.to_cql());

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                "Publication update failed, please try again".to_string(),
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

        if self.status != -1 {
            return Err(HTTPError::new(
                409,
                format!(
                    "Publication delete conflict, expected status -1, got {}",
                    self.status
                ),
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
            "INSERT INTO deleted_publication ({}) VALUES ({})",
            cols_name.join(","),
            vals_name.join(","),
        );

        let delete_query =
            "DELETE FROM publication WHERE gid=? AND cid=? AND language=? AND version=?";
        let delete_params = (
            self.gid.to_cql(),
            self.cid.to_cql(),
            self.language.to_cql(),
            self.version,
        );

        let _ = db
            .batch(
                vec![insert_query.as_str(), delete_query],
                (insert_params, delete_params),
            )
            .await?;
        Ok(true)
    }

    pub async fn create_from_creation(
        db: &scylladb::ScyllaDB,
        gid: xid::Id,
        cid: xid::Id,
        creator: xid::Id,
    ) -> anyhow::Result<Publication> {
        let mut creation = Creation::with_pk(gid, cid);
        creation.get_one(db, vec![]).await?;
        if creation.status != 2 {
            return Err(HTTPError::new(400, "Creation should be approved".to_string()).into());
        }

        let mut content = Content::with_pk(creation.content);
        content.get_one(db, vec![]).await?;
        content.id = xid::new();
        content.status = 0;
        content.updated_at = unix_ms() as i64;

        let mut doc: Publication = creation.clone().into();
        doc.created_at = content.updated_at;
        doc.updated_at = content.updated_at;
        doc.content = content.id;
        doc.creator = creator;

        let fields = Self::fields();
        doc._fields = fields.clone();

        let mut cols_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut vals_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut params: Vec<&CqlValue> = Vec::with_capacity(fields.len());
        let cols = doc.to();

        for field in &fields {
            cols_name.push(field);
            vals_name.push("?");
            params.push(cols.get(field).unwrap());
        }

        let query = format!(
            "INSERT INTO publication ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(409, "Publication exists".to_string()).into());
        }

        content.save(db).await?;
        creation.upgrade_version(db).await?;

        doc._content = content.content;
        Ok(doc)
    }

    pub async fn create_from_publication(
        db: &scylladb::ScyllaDB,
        src: Publication,
        draft: Publication,
        content: Vec<u8>,
    ) -> anyhow::Result<Publication> {
        let mut src = src;
        src.get_one(db, vec![]).await?;
        if draft.gid == src.gid {
            if src.status < 0 {
                return Err(
                    HTTPError::new(400, "Source publication is rejected".to_string()).into(),
                );
            }
        } else if src.status != 2 {
            return Err(
                HTTPError::new(400, "Source publication is not published".to_string()).into(),
            );
        }

        let mut content = Content {
            id: xid::new(),
            gid: draft.gid,
            cid: src.cid,
            version: src.version,
            language: draft.language,
            updated_at: unix_ms() as i64,
            content: content.clone(),
            ..Default::default()
        };

        let mut doc = draft;
        doc.cid = content.cid;
        doc.language = content.language;
        doc.version = content.version;
        doc.status = content.status;
        doc.created_at = content.updated_at;
        doc.updated_at = content.updated_at;
        doc.original_url = src.original_url;
        doc.genre = src.genre;
        doc.authors = src.authors;
        doc.content = content.id;
        doc.license = src.license;
        if doc.description.is_empty() {
            doc.description = src.description;
        }
        if doc.cover.is_empty() {
            doc.cover = src.cover;
        }
        if doc.keywords.is_empty() {
            doc.keywords = src.keywords;
        }

        let fields = Self::fields();
        doc._fields = fields.clone();

        let mut cols_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut vals_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut params: Vec<&CqlValue> = Vec::with_capacity(fields.len());
        let cols = doc.to();

        for field in &fields {
            cols_name.push(field);
            vals_name.push("?");
            params.push(cols.get(field).unwrap());
        }

        let query = format!(
            "INSERT INTO publication ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(409, "Publication exists".to_string()).into());
        }

        content.save(db).await?;
        doc._content = content.content;

        Ok(doc)
    }

    pub async fn list_by_gid(
        db: &scylladb::ScyllaDB,
        gid: xid::Id,
        select_fields: Vec<String>,
        page_size: u16,
        page_token: Option<xid::Id>,
        status: Option<i8>,
    ) -> anyhow::Result<Vec<Publication>> {
        let fields = Self::select_fields(select_fields, true)?;
        let mut page_token = page_token;
        let mut res: Vec<Publication> = Vec::with_capacity(page_size as usize);
        let query_size: i32 = match page_size {
            v if v <= 10 => 20,
            v if v <= 100 => v as i32 * 2,
            v => v as i32 + 100,
        };

        'label: loop {
            let rows = if let Some(cid) = page_token {
                if status.is_none() {
                    let query = Query::new(format!(
                "SELECT {} FROM publication WHERE gid=? AND cid<? GROUP BY cid LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(","))).with_page_size(query_size);
                    let params = (gid.to_cql(), cid.to_cql(), query_size);
                    db.execute_paged(query, params, None).await?
                } else {
                    let query = Query::new(format!(
                    "SELECT {} FROM publication WHERE gid=? AND cid<? AND status=? GROUP BY cid LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                    fields.clone().join(","))).with_page_size(query_size);
                    let params = (gid.to_cql(), cid.to_cql(), status.unwrap(), query_size);
                    db.execute_paged(query, params, None).await?
                }
            } else if status.is_none() {
                let query = Query::new(format!(
                "SELECT {} FROM publication WHERE gid=? GROUP BY cid LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(query_size);
                let params = (gid.to_cql(), query_size);
                db.execute_iter(query, params).await?
            } else {
                let query = Query::new(format!(
                "SELECT {} FROM publication WHERE gid=? AND status=? GROUP BY cid LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            )).with_page_size(query_size);
                let params = (gid.to_cql(), status.unwrap(), query_size);
                db.execute_iter(query, params).await?
            };

            if rows.is_empty() {
                break 'label;
            }

            for row in rows {
                let mut doc = Publication::default();
                let mut cols = ColumnsMap::with_capacity(fields.len());
                cols.fill(row, &fields)?;
                doc.fill(&cols);
                doc._fields = fields.clone();
                res.push(doc);
                if res.len() >= page_size as usize {
                    break 'label;
                }
            }
            page_token = Some(res.last().unwrap().cid);
        }

        Ok(res)
    }

    pub async fn list_by_gid_cid(
        db: &scylladb::ScyllaDB,
        gid: xid::Id,
        cid: xid::Id,
        select_fields: Vec<String>,
        status: Option<i8>,
    ) -> anyhow::Result<Vec<Publication>> {
        let fields = Self::select_fields(select_fields, true)?;
        let query_size = 1000i32;

        let rows = if status.is_none() {
            let query = Query::new(format!(
                "SELECT {} FROM publication WHERE gid=? AND cid=? GROUP BY cid LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(query_size);
            let params = (gid.to_cql(), cid.to_cql(), query_size);
            db.execute_iter(query, params).await?
        } else {
            let query = Query::new(format!(
                "SELECT {} FROM publication WHERE gid=? AND cid=? AND status=? GROUP BY cid LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            )).with_page_size(query_size);
            let params = (gid.to_cql(), cid.to_cql(), status.unwrap(), query_size);
            db.execute_iter(query, params).await?
        };

        let mut res: Vec<Publication> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut doc = Publication::default();
            let mut cols = ColumnsMap::with_capacity(fields.len());
            cols.fill(row, &fields)?;
            doc.fill(&cols);
            doc._fields = fields.clone();
            res.push(doc);
        }

        Ok(res)
    }

    pub async fn list_published_by_cid(
        db: &scylladb::ScyllaDB,
        cid: xid::Id,
    ) -> anyhow::Result<Vec<Publication>> {
        let fields = Self::select_fields(vec!["status".to_string()], true)?;
        let query_size = 1000i32;

        let query = Query::new(format!(
            "SELECT {} FROM publication WHERE cid=? AND status=? LIMIT ? ALLOW FILTERING BYPASS CACHE USING TIMEOUT 3s",
            fields.clone().join(","))).with_page_size(query_size);
        let params = (cid.to_cql(), 2i8, query_size);
        let rows = db.execute_paged(query, params, None).await?;

        let mut docs: Vec<Publication> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut doc = Publication::default();
            let mut cols = ColumnsMap::with_capacity(fields.len());
            cols.fill(row, &fields)?;
            doc.fill(&cols);
            doc._fields = fields.clone();
            docs.push(doc);
        }
        // TODO: filter by version
        // docs.sort_by(|a, b| b.version.partial_cmp(&a.version).unwrap());
        Ok(docs)
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
        publication_model_works().await;
        list_by_gid_works().await;
        list_published_by_cid_works().await;
    }

    // #[tokio::test(flavor = "current_thread")]
    async fn publication_model_works() {
        let db = get_db().await;
        let user = xid::new();
        let gid = xid::Id::from_str(db::USER_JARVIS).unwrap();
        let cid = xid::new();
        let language = Language::Zho;
        let version = 1i16;

        // valid_status
        {
            let mut doc = Publication::with_pk(gid, cid, language, version);
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
            assert!(doc.valid_status(-1).is_err());
            assert!(doc.valid_status(0).is_err());
            assert!(doc.valid_status(1).is_err());
            assert!(doc.valid_status(2).is_err());
            assert!(doc.valid_status(3).is_err());
        }

        // // create_from_creation
        {
            let mut creation = Creation::with_pk(gid, cid);
            creation.language = language;
            creation.title = "Hello World".to_string();
            creation.version = version;

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

            assert!(creation.save_with(db, content.clone()).await.unwrap());

            let res = Publication::create_from_creation(db, gid, cid, user).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 400);

            creation
                .update_status(db, 1i8, creation.updated_at)
                .await
                .unwrap();
            creation
                .update_status(db, 2i8, creation.updated_at)
                .await
                .unwrap();
            let doc = Publication::create_from_creation(db, gid, cid, user)
                .await
                .unwrap();
            assert_eq!(doc.gid, gid);
            assert_eq!(doc.cid, cid);
            assert_eq!(doc.creator, user);
            assert_eq!(doc.language, language);
            assert_eq!(&doc._content, &content);
            assert_ne!(doc.content, creation.content);
            creation.get_one(db, vec![]).await.unwrap();

            let mut c1 = Content::with_pk(creation.content);
            c1.get_one(db, vec![]).await.unwrap();
            let mut c2 = Content::with_pk(doc.content);
            c2.get_one(db, vec![]).await.unwrap();
            assert_eq!(c1.hash, c2.hash);
            assert_eq!(c1.content, c2.content);
            assert_eq!(c2.gid, doc.gid);
            assert_eq!(c2.cid, doc.cid);
            assert_eq!(c2.language, doc.language);
            assert_eq!(c2.version, doc.version);

            assert_eq!(creation.version, version + 1);

            let mut doc2 = Publication::with_pk(gid, cid, language, version);
            doc2.get_one(db, vec![]).await.unwrap();
            assert_eq!(doc2.gid, gid);
            assert_eq!(doc2.cid, cid);
            assert_eq!(doc2.creator, user);
            assert_eq!(doc2.language, language);
            assert_eq!(&doc2._content, &content);
            assert_eq!(doc2.content, doc.content);

            let mut doc3 = Publication::with_pk(gid, cid, language, version);
            doc3.get_one(db, vec!["cid".to_string(), "title".to_string()])
                .await
                .unwrap();
            assert_eq!(doc3.title.as_str(), "Hello World");
            assert_eq!(doc3.version, 1);
            assert_eq!(doc3.language, language);
            assert_eq!(doc3._fields, vec!["cid", "title"]);
            assert!(doc3.content.is_zero());
            assert!(doc3._content.is_empty());

            let doc = Publication::create_from_creation(db, gid, cid, user)
                .await
                .unwrap();
            assert_eq!(doc.version, version + 1);
        }

        // create_from_publication
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
                            "text" => "Hello World",
                        }],
                    }],
                })
                .unwrap(),
            )
            .unwrap();

            let src = Publication::with_pk(xid::new(), cid, language, version);
            let draft = Publication::with_pk(xid::new(), cid, language, version);
            let res =
                Publication::create_from_publication(db, src.clone(), draft.clone(), vec![]).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 404);

            let src = Publication::with_pk(gid, cid, language, version);
            let res =
                Publication::create_from_publication(db, src.clone(), draft.clone(), vec![]).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 400);

            let draft = Publication::with_pk(gid, cid, language, version);
            let res =
                Publication::create_from_publication(db, src.clone(), draft.clone(), vec![]).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 409);

            let mut draft = Publication::with_pk(gid, cid, Language::Eng, version);
            draft.creator = user;
            draft.title = "Hello World 2".to_string();
            let doc = Publication::create_from_publication(
                db,
                src.clone(),
                draft.clone(),
                content.clone(),
            )
            .await
            .unwrap();
            assert_eq!(doc.gid, gid);
            assert_eq!(doc.cid, cid);
            assert_eq!(doc.creator, user);
            assert_eq!(doc.language, Language::Eng);
            assert_eq!(&doc._content, &content);

            let mut src = Publication::with_pk(gid, cid, language, version);
            src.get_one(db, vec![]).await.unwrap();
            assert_ne!(doc.content, src.content);

            let mut c1 = Content::with_pk(src.content);
            c1.get_one(db, vec![]).await.unwrap();
            let mut c2 = Content::with_pk(doc.content);
            c2.get_one(db, vec![]).await.unwrap();
            assert_eq!(c1.hash, c2.hash);
            assert_eq!(c1.content, c2.content);
            assert_eq!(c2.gid, doc.gid);
            assert_eq!(c2.cid, doc.cid);
            assert_eq!(c2.language, doc.language);
            assert_eq!(c2.version, doc.version);

            let mut creation = Creation::with_pk(gid, cid);
            creation.get_one(db, vec![]).await.unwrap();
            assert_eq!(creation.version, version + 2);

            let mut doc2 = Publication::with_pk(gid, cid, Language::Eng, version);
            doc2.get_one(db, vec![]).await.unwrap();
            assert_eq!(doc2.gid, gid);
            assert_eq!(doc2.cid, cid);
            assert_eq!(doc2.creator, user);
            assert_eq!(doc2.language, Language::Eng);
            assert_eq!(&doc2._content, &content);
            assert_eq!(doc2.content, doc.content);

            let mut doc3 = Publication::with_pk(gid, cid, Language::Eng, version);
            doc3.get_one(db, vec!["cid".to_string(), "title".to_string()])
                .await
                .unwrap();
            assert_eq!(doc3.title.as_str(), "Hello World 2");
            assert_eq!(doc3.version, 1);
            assert_eq!(doc3.language, Language::Eng);
            assert_eq!(doc3._fields, vec!["cid", "title"]);
            assert!(doc3.content.is_zero());
            assert!(doc3._content.is_empty());
        }

        // update
        {
            let mut doc = Publication::with_pk(gid, cid, language, version);
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
            cols.set_as("model", &"GPT-4".to_string());
            cols.set_as("title", &"title 2".to_string());
            cols.set_as("description", &"description 2".to_string());
            cols.set_as("cover", &"cover 2".to_string());
            cols.set_as("keywords", &vec!["keyword".to_string()]);
            cols.set_as("summary", &"summary 2".to_string());
            let res = doc.update(db, cols, doc.updated_at).await.unwrap();
            assert!(res);
        }

        // // update status
        {
            let mut doc = Publication::with_pk(gid, cid, Language::Eng, version);
            doc.get_one(db, vec![]).await.unwrap();

            let res = doc.update_status(db, 2, doc.updated_at - 1).await;
            assert!(res.is_err());

            let res = doc.update_status(db, 2, doc.updated_at).await;
            assert!(res.is_err());

            let res = doc.update_status(db, 1, doc.updated_at).await.unwrap();
            assert!(res);

            let res = doc.update_status(db, 1, doc.updated_at).await.unwrap();
            assert!(!res);

            let res = doc.update_status(db, 2, doc.updated_at).await.unwrap();
            assert!(res);

            let res = doc.update_status(db, 1, doc.updated_at).await;
            assert!(res.is_err());
        }

        // delete
        {
            let mut backup = Publication::with_pk(gid, cid, language, version);
            backup.get_one(db, vec![]).await.unwrap();
            backup.updated_at = 0;
            backup._content = vec![];

            let mut deleted = Publication::with_pk(gid, cid, language, version);
            let res = deleted.get_deleted(db).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 404);

            let mut doc = Publication::with_pk(gid, cid, language, version);
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

            let mut doc = Publication::with_pk(gid, cid, Language::Eng, version);
            let res = doc.delete(db).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 409);
        }
    }

    // #[tokio::test(flavor = "current_thread")]
    async fn list_by_gid_works() {
        let db = get_db().await;
        let gid = xid::new();
        let content: Vec<u8> = cbor_to_vec(
            &cbor!({
                "type" => "doc",
                "content" => [],
            })
            .unwrap(),
        )
        .unwrap();

        let mut docs: Vec<Publication> = Vec::new();

        let mut creation = Creation::with_pk(gid, xid::new());
        creation.language = Language::Eng;
        creation.title = "Hello World".to_string();
        creation.version = 1;

        assert!(creation.save_with(db, content.clone()).await.unwrap());
        creation
            .update_status(db, 1i8, creation.updated_at)
            .await
            .unwrap();
        creation
            .update_status(db, 2i8, creation.updated_at)
            .await
            .unwrap();
        let doc = Publication::create_from_creation(db, gid, creation.id, creation.creator)
            .await
            .unwrap();
        docs.push(doc);

        let mut creation = Creation::with_pk(gid, xid::new());
        creation.language = Language::Zho;
        creation.title = "Hello World 1".to_string();
        creation.version = 1;

        assert!(creation.save_with(db, content.clone()).await.unwrap());
        creation
            .update_status(db, 1i8, creation.updated_at)
            .await
            .unwrap();
        creation
            .update_status(db, 2i8, creation.updated_at)
            .await
            .unwrap();
        let _ = Publication::create_from_creation(db, gid, creation.id, creation.creator)
            .await
            .unwrap();
        let _ = Publication::create_from_creation(db, gid, creation.id, creation.creator)
            .await
            .unwrap();
        let doc = Publication::create_from_creation(db, gid, creation.id, creation.creator)
            .await
            .unwrap();
        docs.push(doc); // version 3

        for i in 0..8 {
            let mut creation = Creation::with_pk(gid, xid::new());
            creation.language = Language::Zho;
            creation.title = format!("Hello World {}", i + 2);
            creation.version = 1;

            assert!(creation.save_with(db, content.clone()).await.unwrap());
            creation
                .update_status(db, 1i8, creation.updated_at)
                .await
                .unwrap();
            creation
                .update_status(db, 2i8, creation.updated_at)
                .await
                .unwrap();
            let _ = Publication::create_from_creation(db, gid, creation.id, creation.creator)
                .await
                .unwrap();
            let doc = Publication::create_from_creation(db, gid, creation.id, creation.creator)
                .await
                .unwrap();
            docs.push(doc); // version 2
        }

        assert_eq!(docs.len(), 10);

        let latest = Publication::list_by_gid(db, gid, Vec::new(), 1, None, None)
            .await
            .unwrap();
        assert_eq!(latest.len(), 1);
        let mut latest = latest[0].to_owned();
        assert_eq!(latest.gid, docs.last().unwrap().gid);
        assert_eq!(latest.cid, docs.last().unwrap().cid);
        assert_eq!(latest.language, docs.last().unwrap().language);
        assert_eq!(latest.version, docs.last().unwrap().version);
        assert_eq!(latest.version, 2i16);

        latest
            .update_status(db, 1, latest.updated_at)
            .await
            .unwrap();
        let res = Publication::list_by_gid(db, gid, vec!["title".to_string()], 100, None, None)
            .await
            .unwrap();
        assert_eq!(res.len(), 10);

        let res = Publication::list_by_gid(db, gid, vec!["title".to_string()], 100, None, Some(1))
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].cid, latest.cid);
        assert_eq!(res[0].version, 2i16);

        let res = Publication::list_by_gid(db, gid, vec!["title".to_string()], 5, None, None)
            .await
            .unwrap();
        assert_eq!(res.len(), 5);
        assert_eq!(res[4].gid, docs[5].gid);
        assert_eq!(res[4].cid, docs[5].cid);
        assert_eq!(res[4].language, docs[5].language);
        assert_eq!(res[4].version, docs[5].version);

        let res = Publication::list_by_gid(
            db,
            gid,
            vec!["title".to_string()],
            5,
            Some(docs[5].cid),
            None,
        )
        .await
        .unwrap();
        assert_eq!(res.len(), 5);
        assert_eq!(res[4].gid, docs[0].gid);
        assert_eq!(res[4].cid, docs[0].cid);
        assert_eq!(res[4].language, docs[0].language);
        assert_eq!(res[4].version, docs[0].version);

        let res = Publication::list_by_gid(
            db,
            gid,
            vec!["title".to_string()],
            5,
            Some(docs[5].cid),
            Some(1),
        )
        .await
        .unwrap();
        assert_eq!(res.len(), 0);
    }

    // #[tokio::test(flavor = "current_thread")]
    async fn list_published_by_cid_works() {
        let db = get_db().await;
        let gid = xid::Id::from_str(db::USER_JARVIS).unwrap();
        let cid = xid::new();
        let content: Vec<u8> = cbor_to_vec(
            &cbor!({
                "type" => "doc",
                "content" => [],
            })
            .unwrap(),
        )
        .unwrap();

        let mut creation = Creation::with_pk(gid, cid);
        creation.language = Language::Eng;
        creation.title = "Hello World".to_string();
        creation.version = 1;

        assert!(creation.save_with(db, content.clone()).await.unwrap());
        creation
            .update_status(db, 1i8, creation.updated_at)
            .await
            .unwrap();
        creation
            .update_status(db, 2i8, creation.updated_at)
            .await
            .unwrap();
        let _doc = Publication::create_from_creation(db, gid, creation.id, creation.creator)
            .await
            .unwrap();

        let mut doc = Publication::create_from_creation(db, gid, creation.id, creation.creator)
            .await
            .unwrap();

        doc.update_status(db, 1i8, doc.updated_at).await.unwrap();
        doc.update_status(db, 2i8, doc.updated_at).await.unwrap();

        let mut draft = doc.clone();
        draft.gid = xid::new();
        draft.language = Language::Aaa;
        draft.creator = xid::new();
        let mut doc2 =
            Publication::create_from_publication(db, doc.clone(), draft, content.clone())
                .await
                .unwrap();

        let mut draft = doc.clone();
        draft.gid = xid::new();
        draft.language = Language::Zho;
        draft.creator = xid::new();
        let mut doc3 =
            Publication::create_from_publication(db, doc.clone(), draft, content.clone())
                .await
                .unwrap();

        let res = Publication::list_published_by_cid(db, cid).await.unwrap();
        assert_eq!(res.len(), 1);

        doc2.update_status(db, 1i8, doc2.updated_at).await.unwrap();
        doc2.update_status(db, 2i8, doc2.updated_at).await.unwrap();
        let res = Publication::list_published_by_cid(db, cid).await.unwrap();
        assert_eq!(res.len(), 2);

        doc3.update_status(db, 1i8, doc3.updated_at).await.unwrap();
        doc3.update_status(db, 2i8, doc3.updated_at).await.unwrap();
        let res = Publication::list_published_by_cid(db, cid).await.unwrap();
        assert_eq!(res.len(), 3);
    }
}
