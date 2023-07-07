use isolang::Language;
use std::{collections::HashSet, convert::From};

use scylla_orm::{ColumnsMap, CqlValue, ToCqlVal};
use scylla_orm_macros::CqlOrm;

use crate::db::{
    scylladb,
    scylladb::{extract_applied, Query},
};
use axum_web::context::unix_ms;
use axum_web::erring::HTTPError;

#[derive(Debug, Default, Clone, CqlOrm, PartialEq)]
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
            -1 if 0 != status => Err(HTTPError::new(
                400,
                format!(
                    "Publication draft status is {}, expected update to 0, got {}",
                    self.status, status
                ),
            )
            .into()),
            0 if !(-1..=1).contains(&status) => Err(HTTPError::new(
                400,
                format!(
                    "Publication draft status is {}, expected update to -1 or 1, got {}",
                    self.status, status
                ),
            )
            .into()),
            1 if !(-1..=2).contains(&status) => Err(HTTPError::new(
                400,
                format!(
                    "Publication draft status is {}, expected update to -1, 0 or 2, got {}",
                    self.status, status
                ),
            )
            .into()),
            2 if !(0..=2).contains(&status) => Err(HTTPError::new(
                400,
                format!(
                    "Publication draft status is {}, expected update to 0 or 1, got {}",
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
            "SELECT {} FROM publication_draft WHERE gid=? AND id=? LIMIT 1",
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
            "SELECT {} FROM deleted_publication_draft WHERE gid=? AND id=? LIMIT 1",
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
        let now = unix_ms() as i64;
        self.created_at = now;
        self.updated_at = now;

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
            "INSERT INTO publication_draft ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                format!(
                    "Publication draft {} save failed, please try again",
                    self.id
                ),
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
                    "Publication draft updated_at conflict, expected updated_at {}, got {}",
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
            "UPDATE publication_draft SET status=?,updated_at=? WHERE gid=? AND id=? IF updated_at=?";
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
                format!(
                    "Publication draft update_status {} failed, please try again",
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
                return Err(HTTPError::new(400, format!("Invalid field: {}", field)).into());
            }
        }

        self.get_one(db, vec!["status".to_string(), "updated_at".to_string()])
            .await?;
        if self.updated_at != updated_at {
            return Err(HTTPError::new(
                409,
                format!(
                    "Publication draft updated_at conflict, expected updated_at {}, got {}",
                    self.updated_at, updated_at
                ),
            )
            .into());
        }
        if self.status < 0 || self.status > 1 {
            return Err(HTTPError::new(
                409,
                format!(
                    "Publication draft can not be update, status {}",
                    self.status
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
            "UPDATE publication_draft SET {} WHERE gid=? AND id=? IF updated_at=?",
            set_fields.join(",")
        );
        params.push(self.gid.to_cql());
        params.push(self.id.to_cql());
        params.push(updated_at.to_cql());

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                format!(
                    "Publication draft {} update failed, please try again",
                    self.id
                ),
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
                    "Publication draft version conflict, expected version {}, got {}",
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
            "INSERT INTO deleted_publication_draft ({}) VALUES ({})",
            cols_name.join(","),
            vals_name.join(","),
        );

        let delete_query = "DELETE FROM publication_draft WHERE gid=? AND id=?";
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
    ) -> anyhow::Result<Vec<PublicationDraft>> {
        let fields = Self::select_fields(select_fields, true)?;

        let rows = if let Some(id) = page_token {
            if status.is_none() {
                let query = Query::new(format!(
                "SELECT {} FROM publication_draft WHERE gid=? AND id<? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(","))).with_page_size(page_size as i32);
                let params = (gid.to_cql(), id.to_cql(), page_size as i32);
                db.execute_paged(query, params, None).await?
            } else {
                let query = Query::new(format!(
                    "SELECT {} FROM publication_draft WHERE gid=? AND id<? AND status=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                    fields.clone().join(","))).with_page_size(page_size as i32);
                let params = (gid.to_cql(), id.to_cql(), status.unwrap(), page_size as i32);
                db.execute_paged(query, params, None).await?
            }
        } else if status.is_none() {
            let query = Query::new(format!(
                "SELECT {} FROM publication_draft WHERE gid=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(page_size as i32);
            let params = (gid.to_cql(), page_size as i32);
            db.execute_iter(query, params).await?
        } else {
            let query = Query::new(format!(
                "SELECT {} FROM publication_draft WHERE gid=? AND status=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            )).with_page_size(page_size as i32);
            let params = (gid.to_cql(), status.unwrap(), page_size as i32);
            db.execute_iter(query, params).await?
        };

        let mut res: Vec<PublicationDraft> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut doc = PublicationDraft::default();
            let mut cols = ColumnsMap::with_capacity(fields.len());
            cols.fill(row, &fields)?;
            doc.fill(&cols);
            doc._fields = fields.clone();
            res.push(doc);
        }

        Ok(res)
    }
}

#[derive(Debug, Default, Clone, CqlOrm, PartialEq)]
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
            creator: draft.creator,
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
                return Err(HTTPError::new(400, format!("Invalid field: {}", field)).into());
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

    pub fn valid_status(&self, status: i8) -> anyhow::Result<()> {
        if !(-1..=2).contains(&status) {
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
        let res = if self.version > 0 {
            let query = format!(
                "SELECT {} FROM publication WHERE id=? AND language=? AND version=? LIMIT 1",
                fields.join(",")
            );
            let params = (self.id.to_cql(), self.language.to_cql(), self.version);
            db.execute(query, params).await?.single_row()?
        } else {
            let query = format!(
                "SELECT {} FROM publication WHERE id=? AND language=? LIMIT 1",
                fields.join(",")
            );
            let params = (self.id.to_cql(), self.language.to_cql());
            db.execute(query, params).await?.single_row()?
        };

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, &fields)?;
        self.fill(&cols);

        Ok(())
    }

    pub async fn get_deleted(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<()> {
        let fields = Self::fields();
        self._fields = fields.clone();

        let res = if self.version > 0 {
            let query = format!(
                "SELECT {} FROM deleted_publication WHERE id=? AND language=? AND version=? LIMIT 1",
                fields.join(",")
            );
            let params = (self.id.to_cql(), self.language.to_cql(), self.version);
            db.execute(query, params).await?.single_row()?
        } else {
            let query = format!(
                "SELECT {} FROM deleted_publication WHERE id=? AND language=? LIMIT 1",
                fields.join(",")
            );
            let params = (self.id.to_cql(), self.language.to_cql());
            db.execute(query, params).await?.single_row()?
        };

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

        self.valid_status(status)?;
        if self.status == status {
            return Ok(false); // no need to update
        }

        let new_updated_at = unix_ms() as i64;
        let query =
            "UPDATE publication SET status=?,updated_at=? WHERE id=? AND language=? AND version=? IF updated_at=?";
        let params = (
            status,
            new_updated_at,
            self.id.to_cql(),
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

    pub async fn delete(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        let res = self.get_one(db, vec!["status".to_string()]).await;
        if res.is_err() {
            return Ok(false); // already deleted
        }

        if self.status == 2 {
            return Err(HTTPError::new(
                409,
                "Publication is published, can not be deleted".to_string(),
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
            "INSERT INTO deleted_publication ({}) VALUES ({})",
            cols_name.join(","),
            vals_name.join(","),
        );

        let delete_query = "DELETE FROM publication WHERE id=? AND language=? AND version=?";
        let delete_params = (self.id.to_cql(), self.language.to_cql(), self.version);

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
        let mut latest_draft = PublicationDraft::with_pk(draft.gid, draft.id);
        latest_draft
            .get_one(db, vec!["status".to_string(), "updated_at".to_string()])
            .await?;

        if latest_draft.status != 1 || latest_draft.updated_at != draft.updated_at {
            return Err(HTTPError::new(
                409,
                format!(
                    "Invalid publication draft, status or updated_at not match, gid({}), id({}), cid({})",
                    draft.gid, draft.id, draft.cid
                ),
            ).into());
        }

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
        let cols = publication.to();

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
            "UPDATE creation SET active_languages=active_languages+{?},updated_at=? WHERE gid=? AND id=? IF EXISTS";
        let params = (
            publication.language.to_cql(),
            now,
            creation_gid.to_cql(),
            publication.id.to_cql(),
        );
        let _ = db.execute(query, params).await?;

        // update draft status to 2: accepted.
        let query =
            "UPDATE publication_draft SET status=?,updated_at=? WHERE gid=? AND id=? IF EXISTS";
        let params = (2i8, now, draft_gid.to_cql(), draft_id.to_cql());
        let _ = db.execute(query, params).await?;

        Ok(publication)
    }
}

#[cfg(test)]
mod tests {
    use ciborium::cbor;
    use std::{str::FromStr, time::Duration};
    use tokio::time;

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
        publication_draft_model_works().await?;
        publication_draft_find_works().await?;
        publication_model_works().await?;

        Ok(())
    }

    // #[tokio::test(flavor = "current_thread")]
    async fn publication_draft_model_works() -> anyhow::Result<()> {
        let db = get_db().await;
        let gid = xid::Id::from_str(db::USER_JARVIS).unwrap();
        let id = xid::new();
        let cid = xid::new();

        // valid_status
        {
            let mut doc = PublicationDraft::with_pk(gid, id);
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
            assert!(doc.valid_status(0).is_ok());
            assert!(doc.valid_status(1).is_ok());
            assert!(doc.valid_status(2).is_ok());
            assert!(doc.valid_status(3).is_err());
        }

        // create
        {
            let mut doc = PublicationDraft::with_pk(gid, id);
            doc.cid = cid;
            doc.language = Language::Eng;
            doc.version = 2;
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

            let mut doc2 = PublicationDraft::with_pk(gid, id);
            doc2.get_one(db, vec![]).await?;

            assert_eq!(doc2.cid, cid);
            assert_eq!(doc2.title.as_str(), "Hello World");
            assert_eq!(doc2.version, 2);
            assert_eq!(doc2.language, Language::Eng);

            let mut doc3 = PublicationDraft::with_pk(gid, id);
            doc3.get_one(db, vec!["cid".to_string(), "title".to_string()])
                .await?;
            assert_eq!(doc3.title.as_str(), "Hello World");
            assert_eq!(doc3.version, 0);
            assert_eq!(doc3.language, Language::default());
            assert_eq!(doc3._fields, vec!["cid", "title"]);
        }

        // update
        {
            let mut doc = PublicationDraft::with_pk(gid, id);
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

            let mut cols = ColumnsMap::new();
            cols.set_as("model", &"GPT-4".to_string());
            cols.set_as("title", &"title 2".to_string());
            cols.set_as("description", &"description 2".to_string());
            cols.set_as("cover", &"cover 2".to_string());
            cols.set_as("keywords", &vec!["keyword".to_string()]);
            cols.set_as("authors", &vec!["author 1".to_string()]);
            cols.set_as("summary", &"summary 2".to_string());

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
        }

        // update status
        {
            let mut doc = PublicationDraft::with_pk(gid, id);
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
            let mut backup = PublicationDraft::with_pk(gid, id);
            backup.get_one(db, vec![]).await?;
            backup.updated_at = 0;

            let mut deleted = PublicationDraft::with_pk(gid, id);
            let res = deleted.get_deleted(db).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 404);

            let mut doc = PublicationDraft::with_pk(gid, id);
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
    async fn publication_draft_find_works() -> anyhow::Result<()> {
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

        let mut docs: Vec<PublicationDraft> = Vec::new();
        for i in 0..10 {
            let mut doc = PublicationDraft::with_pk(gid, xid::new());
            doc.cid = xid::new();
            doc.language = Language::Zho;
            doc.version = 1;
            doc.title = format!("Hello World {}", i);
            doc.content = content.clone();
            doc.save(db).await?;

            docs.push(doc)
        }
        assert_eq!(docs.len(), 10);

        let latest = PublicationDraft::find(db, gid, Vec::new(), 1, None, None).await?;
        assert_eq!(latest.len(), 1);
        let mut latest = latest[0].to_owned();
        assert_eq!(latest.gid, docs.last().unwrap().gid);
        assert_eq!(latest.id, docs.last().unwrap().id);

        latest.update_status(db, 1, latest.updated_at).await?;
        let res =
            PublicationDraft::find(db, gid, vec!["title".to_string()], 100, None, None).await?;
        assert_eq!(res.len(), 10);

        let res =
            PublicationDraft::find(db, gid, vec!["title".to_string()], 100, None, Some(1)).await?;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].id, docs.last().unwrap().id);

        let res = PublicationDraft::find(db, gid, vec!["title".to_string()], 5, None, None).await?;
        assert_eq!(res.len(), 5);
        assert_eq!(res[4].id, docs[5].id);

        let res = PublicationDraft::find(
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

        let res = PublicationDraft::find(
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

    // #[tokio::test(flavor = "current_thread")]
    async fn publication_model_works() -> anyhow::Result<()> {
        let db = get_db().await;
        let gid = xid::Id::from_str(db::USER_JARVIS).unwrap();
        let draft_id = xid::new();
        let cid = xid::new();

        // valid_status
        {
            let mut doc = Publication::with_pk(cid, Language::Zho, 1);
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

        // create
        {
            let mut doc = Publication::with_pk(cid, Language::Zho, 1);
            doc.title = "Hello World".to_string();

            let res = doc.get_one(db, vec![]).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 404);

            let mut creation = db::Creation::with_pk(gid, cid);
            doc.language = Language::Eng;
            doc.version = 1;
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
            assert!(creation.save(db).await?);

            let mut draft = PublicationDraft::with_pk(gid, draft_id);
            draft.cid = cid;
            draft.language = Language::Zho;
            draft.version = 1;
            draft.title = "你好，世界".to_string();
            assert!(draft.save(db).await?);

            let res = Publication::save_from(db, gid, draft.clone()).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 409);

            draft.update_status(db, 1, draft.updated_at).await?;
            time::sleep(Duration::from_millis(5)).await;

            let res = Publication::save_from(db, gid, draft).await?;
            assert_eq!(res.id, cid);
            assert_eq!(res.language, Language::Zho);
            assert_eq!(res.version, 1);
            assert_eq!(res.status, 0);

            creation.get_one(db, vec![]).await?;
            assert_eq!(creation.updated_at, res.created_at);
            assert_eq!(creation.active_languages, HashSet::from([Language::Zho]));

            let mut draft = PublicationDraft::with_pk(gid, draft_id);
            draft.get_one(db, vec![]).await?;
            assert_eq!(draft.updated_at, res.created_at);
            assert_eq!(draft.status, 2);

            draft.status = 1;
            let res = Publication::save_from(db, gid, draft.clone()).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 409);

            draft.update_status(db, 1, creation.updated_at).await?;
            draft.language = Language::Aaa;
            draft.version = 1;
            draft.title = "Hello World~~".to_string();
            creation.delete(db, 1).await?;
            time::sleep(Duration::from_millis(5)).await;
            let res = Publication::save_from(db, gid, draft).await?;
            assert_eq!(res.id, cid);
            assert_eq!(res.language, Language::Aaa);
            assert_eq!(res.version, 1);
            assert_eq!(res.status, 0);

            let ok = creation.get_one(db, vec![]).await;
            assert!(ok.is_err());
            let err: erring::HTTPError = ok.unwrap_err().into();
            assert_eq!(err.code, 404);

            let mut doc = Publication::with_pk(cid, Language::Zho, 1);
            doc.get_one(db, vec![]).await?;
            assert_eq!(doc.title.as_str(), "你好，世界");

            let mut doc = Publication::with_pk(cid, Language::Aaa, 1);
            doc.get_one(db, vec![]).await?;
            assert_eq!(doc.title.as_str(), "Hello World~~");
        }

        // update status
        {
            let mut doc = Publication::with_pk(cid, Language::Zho, 1);
            doc.get_one(db, vec![]).await?;

            let res = doc.update_status(db, 2, doc.updated_at - 1).await;
            assert!(res.is_err());

            let res = doc.update_status(db, 2, doc.updated_at).await;
            assert!(res.is_err());

            let res = doc.update_status(db, 1, doc.updated_at).await?;
            assert!(res);

            let res = doc.update_status(db, 1, doc.updated_at).await?;
            assert!(!res);

            let res = doc.update_status(db, 2, doc.updated_at).await?;
            assert!(res);

            let res = doc.update_status(db, 1, doc.updated_at).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 400);
        }

        // delete
        {
            let mut backup = Publication::with_pk(cid, Language::Aaa, 1);
            backup.get_one(db, vec![]).await?;
            backup.updated_at = 0;

            let mut deleted = Publication::with_pk(cid, Language::Aaa, 1);
            let res = deleted.get_deleted(db).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 404);

            let mut doc = Publication::with_pk(cid, Language::Zho, 1);
            let res = doc.delete(db).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 409);

            let mut doc = Publication::with_pk(cid, Language::Aaa, 1);

            let res = doc.delete(db).await?;
            assert!(res);
            let res = doc.delete(db).await?;
            assert!(!res); // already deleted

            deleted.get_deleted(db).await?;
            deleted.updated_at = 0;
            assert_eq!(deleted, backup);
        }

        Ok(())
    }
}
