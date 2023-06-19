use isolang::Language;
use std::{collections::HashSet, convert::From};

use scylla_orm::{ColumnsMap, CqlValueSerder};
use scylla_orm_macros::CqlOrm;

use crate::db::{
    scylladb,
    scylladb::{extract_applied, CqlValue, Query},
};
use axum_web::context::unix_ms;
use axum_web::erring::HTTPError;

#[derive(Debug, Default, Clone, CqlOrm)]
pub struct PublicationDraft {
    pub gid: xid::Id,
    pub id: xid::Id,
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
    pub content: Vec<u8>,
    pub license: String,

    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
    pub _rating: i8,          // 内容安全分级
}

impl PublicationDraft {
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
        self._fields = fields.clone();

        let query = format!(
            "SELECT {} FROM publication_draft WHERE gid=? AND id=? LIMIT 1",
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
        let now = unix_ms() as i64;
        self.created_at = now;
        self.updated_at = now;

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
            "INSERT INTO publication_draft ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        Ok(extract_applied(res))
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
            return Err(anyhow::Error::new(HTTPError::new(
                409,
                format!(
                    "Publication draft updated_at conflict, expected updated_at {}, got {}",
                    self.updated_at, updated_at
                ),
            )));
        }
        if self.status == status {
            return Ok(true); // no need to update
        }

        match self.status {
            -1 if 0 != status => {
                return Err(anyhow::Error::new(HTTPError::new(
                    400,
                    format!(
                        "Publication draft status is {}, expected update to 0, got {}",
                        self.status, status
                    ),
                )));
            }
            0 if !(-1..=1).contains(&status) => {
                return Err(anyhow::Error::new(HTTPError::new(
                    400,
                    format!(
                        "Publication draft status is {}, expected update to -1 or 1, got {}",
                        self.status, status
                    ),
                )));
            }
            1 if !(-1..=2).contains(&status) => {
                return Err(anyhow::Error::new(HTTPError::new(
                    400,
                    format!(
                        "Publication draft status is {}, expected update to -1, 0 or 2, got {}",
                        self.status, status
                    ),
                )));
            }
            2 if !(0..=1).contains(&status) => {
                return Err(anyhow::Error::new(HTTPError::new(
                    400,
                    format!(
                        "Publication draft status is {}, expected update to 0 or 1, got {}",
                        self.status, status
                    ),
                )));
            }
            _ => {} // continue
        }

        let new_updated_at = unix_ms() as i64;
        let query =
            "UPDATE publication_draft SET status=?,updated_at=? WHERE gid=? AND id=? IF updated_at=?";
        let params = (
            status,
            new_updated_at,
            self.gid.as_bytes(),
            self.id.as_bytes(),
            updated_at,
        );

        let res = db.execute(query, params).await?;
        self.updated_at = new_updated_at;
        self.status = status;
        Ok(extract_applied(res))
    }

    pub async fn update(
        &mut self,
        db: &scylladb::ScyllaDB,
        cols: ColumnsMap,
        updated_at: i64,
    ) -> anyhow::Result<bool> {
        let valid_fields = vec![
            "language",
            "model",
            "title",
            "description",
            "cover",
            "keywords",
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

        self.get_one(db, vec!["status".to_string(), "updated_at".to_string()])
            .await?;
        if self.updated_at != updated_at {
            return Err(anyhow::Error::new(HTTPError::new(
                409,
                format!(
                    "Publication draft updated_at conflict, expected updated_at {}, got {}",
                    self.updated_at, updated_at
                ),
            )));
        }
        if self.status < 0 || self.status > 1 {
            return Err(anyhow::Error::new(HTTPError::new(
                409,
                format!(
                    "Publication draft can not be update, status {}",
                    self.status
                ),
            )));
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
            "UPDATE publication_draft SET {} WHERE gid=? AND id=? IF updated_at=?",
            set_fields.join(",")
        );
        params.push(CqlValue::Blob(self.gid.as_bytes().to_vec()));
        params.push(CqlValue::Blob(self.id.as_bytes().to_vec()));
        params.push(CqlValue::BigInt(updated_at));

        let res = db.execute(query, params).await?;
        self.updated_at = new_updated_at;
        Ok(extract_applied(res))
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
                    "Publication draft version conflict, expected version {}, got {}",
                    self.version, version
                ),
            )));
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
            "INSERT INTO deleted_publication_draft ({}) VALUES ({})",
            cols_name.join(","),
            vals_name.join(","),
        );

        let delete_query = "DELETE FROM publication_draft WHERE gid=? AND id=?";
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
        status: Option<i8>,
    ) -> anyhow::Result<Vec<PublicationDraft>> {
        let fields = Self::select_fields(select_fields, true)?;

        let rows = if let Some(id) = page_token {
            if status.is_none() {
                let query = Query::new(format!(
                "SELECT {} FROM publication_draft WHERE gid=? AND id<? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(","))).with_page_size(page_size as i32);
                let params = (gid.as_bytes(), id.as_bytes(), page_size as i32);
                db.execute_paged(query, params, None).await?
            } else {
                let query = Query::new(format!(
                    "SELECT {} FROM publication_draft WHERE gid=? AND id<? AND status=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                    fields.clone().join(","))).with_page_size(page_size as i32);
                let params = (
                    gid.as_bytes(),
                    id.as_bytes(),
                    status.unwrap(),
                    page_size as i32,
                );
                db.execute_paged(query, params, None).await?
            }
        } else if status.is_none() {
            let query = Query::new(format!(
                "SELECT {} FROM publication_draft WHERE gid=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(page_size as i32);
            let params = (gid.as_bytes(), page_size as i32);
            db.execute_iter(query, params).await?
        } else {
            let query = Query::new(format!(
                "SELECT {} FROM publication_draft WHERE gid=? AND status=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            )).with_page_size(page_size as i32);
            let params = (gid.as_bytes(), status.unwrap(), page_size as i32);
            db.execute_iter(query, params).await?
        };

        let mut res: Vec<PublicationDraft> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut doc = PublicationDraft::default();
            let mut cols = ColumnsMap::with_capacity(fields.len());
            cols.fill(row, fields.clone())?;
            doc.fill(&cols);
            doc._fields = fields.clone();
            res.push(doc);
        }

        Ok(res)
    }
}

#[derive(Debug, Default, Clone, CqlOrm)]
pub struct Publication {
    pub id: xid::Id,
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
    pub content: Vec<u8>,
    pub license: String,

    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
    pub _rating: i8,          // 内容安全分级
    pub _active_languages: HashSet<Language>,
}

impl From<PublicationDraft> for Publication {
    fn from(draft: PublicationDraft) -> Self {
        Self {
            id: draft.cid,
            language: draft.language,
            version: draft.version,
            status: draft.status,
            creator: draft.creator,
            created_at: draft.created_at,
            updated_at: draft.updated_at,
            model: draft.model,
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
    pub fn with_pk(id: xid::Id, language: Language, version: i16) -> Self {
        Self {
            id,
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
                return Err(anyhow::Error::new(HTTPError::new(
                    400,
                    format!("Invalid field: {}", field),
                )));
            }
        }

        if with_pk {
            let mut select_fields = select_fields;
            let id = "id".to_string();
            if !select_fields.contains(&id) {
                select_fields.push(id);
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

    pub async fn get_one(
        &mut self,
        db: &scylladb::ScyllaDB,
        select_fields: Vec<String>,
    ) -> anyhow::Result<()> {
        let fields = Self::select_fields(select_fields, false)?;
        self._fields = fields.clone();
        let res = if self.version > 0 {
            let query = format!(
                "SELECT {} FROM publication WHERE id=? AND language=? AND version=? LIMIT 1",
                fields.join(",")
            );
            let params = (self.id.as_bytes(), self.language.to_cql()?, self.version);
            db.execute(query, params).await?.single_row()?
        } else {
            let query = format!(
                "SELECT {} FROM publication WHERE id=? AND language=? LIMIT 1",
                fields.join(",")
            );
            let params = (self.id.as_bytes(), self.language.to_cql()?);
            db.execute(query, params).await?.single_row()?
        };

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, fields)?;
        self.fill(&cols);

        Ok(())
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
            return Err(anyhow::Error::new(HTTPError::new(
                409,
                format!(
                    "Publication updated_at conflict, expected updated_at {}, got {}",
                    self.updated_at, updated_at
                ),
            )));
        }
        if self.status == status {
            return Ok(true); // no need to update
        }

        match self.status {
            -1 if 0 != status => {
                return Err(anyhow::Error::new(HTTPError::new(
                    400,
                    format!(
                        "Publication status is {}, expected update to 0, got {}",
                        self.status, status
                    ),
                )));
            }
            0 if !(-1..=1).contains(&status) => {
                return Err(anyhow::Error::new(HTTPError::new(
                    400,
                    format!(
                        "Publication status is {}, expected update to -1 or 1, got {}",
                        self.status, status
                    ),
                )));
            }
            1 if !(-1..=2).contains(&status) => {
                return Err(anyhow::Error::new(HTTPError::new(
                    400,
                    format!(
                        "Publication status is {}, expected update to -1, 0 or 2, got {}",
                        self.status, status
                    ),
                )));
            }
            2 => {
                return Err(anyhow::Error::new(HTTPError::new(
                    400,
                    format!("Publication status is {}, can not be updated", self.status),
                )));
            }
            _ => {} // continue
        }

        let new_updated_at = unix_ms() as i64;
        let query =
            "UPDATE publication SET status=?,updated_at=? WHERE id=? AND language=? AND version=? IF updated_at=?";
        let params = (
            status,
            new_updated_at,
            self.id.as_bytes(),
            self.language.to_cql()?,
            self.version,
            updated_at,
        );

        let res = db.execute(query, params).await?;
        self.updated_at = new_updated_at;
        self.status = status;
        Ok(extract_applied(res))
    }

    pub async fn delete(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        let res = self.get_one(db, vec!["status".to_string()]).await;
        if res.is_err() {
            return Ok(false); // already deleted
        }

        if self.status == 2 {
            return Err(anyhow::Error::new(HTTPError::new(
                409,
                "Publication is published, can not be deleted".to_string(),
            )));
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
            "INSERT INTO deleted_publication ({}) VALUES ({})",
            cols_name.join(","),
            vals_name.join(","),
        );

        let delete_query = "DELETE FROM publication WHERE id=? AND language=? AND version=?";
        let delete_params = (self.id.as_bytes(), self.language.to_cql()?, self.version);

        let _ = db
            .batch(
                vec![insert_query.as_str(), delete_query],
                (insert_params, delete_params),
            )
            .await?;
        Ok(true)
    }

    pub async fn save_from(
        db: &scylladb::ScyllaDB,
        creation_gid: xid::Id,
        draft: PublicationDraft,
    ) -> anyhow::Result<Publication> {
        let now = unix_ms() as i64;
        let draft_gid = draft.gid;
        let draft_id = draft.id;
        let mut publication: Publication = draft.into();
        publication.created_at = now;
        publication.updated_at = now;

        let fields = Self::fields();
        publication._fields = fields.clone();

        let mut cols_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut vals_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut params: Vec<&CqlValue> = Vec::with_capacity(fields.len());
        let cols = publication.to()?;

        for field in &fields {
            cols_name.push(field);
            vals_name.push("?");
            params.push(cols.get(field).unwrap());
        }

        let query = format!(
            "INSERT INTO publication ({}) VALUES ({})", // always insert
            cols_name.join(","),
            vals_name.join(",")
        );

        let _ = db.execute(query, params).await?;

        // add language to creation
        let query =
            "UPDATE creation SET active_languages=active_languages+?,updated_at=? WHERE gid=? AND id=? IF EXISTS";
        let params = (
            publication.language.to_cql()?,
            now,
            creation_gid.as_bytes(),
            publication.id.as_bytes(),
        );
        let _ = db.execute(query, params).await?;

        // update draft status to 2: accepted.
        let query =
            "UPDATE publication_draft SET status=?,updated_at=? WHERE gid=? AND id=? IF EXISTS";
        let params = (2i8, now, draft_gid.as_bytes(), draft_id.as_bytes());
        let _ = db.execute(query, params).await?;

        Ok(publication)
    }
}
