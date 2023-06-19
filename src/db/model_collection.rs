use isolang::Language;


use axum_web::context::unix_ms;
use axum_web::erring::HTTPError;
use scylla_orm::{ColumnsMap};
use scylla_orm_macros::CqlOrm;

use crate::db::{
    scylladb,
    scylladb::{extract_applied, CqlValue, Query},
};

#[derive(Debug, Default, Clone, CqlOrm)]
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
                return Err(anyhow::Error::new(HTTPError::new(
                    400,
                    format!("Invalid field: {}", field),
                )));
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
                    "Collection updated_at conflict, expected updated_at {}, got {}",
                    self.updated_at, updated_at
                ),
            )));
        }

        if !(-1..=2).contains(&status) {
            return Err(anyhow::Error::new(HTTPError::new(
                400,
                format!("Invalid collection status, {}", status),
            )));
        }

        if self.status == status {
            return Ok(true); // no need to update
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
                    "Collection updated_at conflict, expected updated_at {}, got {}",
                    self.updated_at, updated_at
                ),
            )));
        }
        if self.status < 0 {
            return Err(anyhow::Error::new(HTTPError::new(
                409,
                format!("Collection can not be update, status {}", self.status),
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
            "UPDATE collection SET {} WHERE uid=? AND id=? IF updated_at=?",
            set_fields.join(",")
        );
        params.push(CqlValue::Blob(self.uid.as_bytes().to_vec()));
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
                    "Collection version conflict, expected version {}, got {}",
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
