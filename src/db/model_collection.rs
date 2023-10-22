use isolang::Language;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

use axum_web::context::unix_ms;
use axum_web::erring::HTTPError;
use scylla_orm::{ColumnsMap, CqlValue, FromCqlVal, ToCqlVal};
use scylla_orm_macros::CqlOrm;

use crate::db::{
    day_to_xid, meili, scylladb, scylladb::extract_applied, support_language, xid_day, Message,
    MessageTexts, MessageValue,
};

#[derive(Debug, Default, Clone, CqlOrm, PartialEq)]
pub struct Collection {
    pub day: i32,
    pub id: xid::Id,
    pub gid: xid::Id,
    pub status: i8,
    pub rating: i8,
    pub mid: xid::Id,
    pub cover: String,
    pub updated_at: i64,
    pub price: i64,
    pub creation_price: i64,

    pub _info: Option<Message>,
    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
}

#[derive(Debug, Default, Clone, CqlOrm, PartialEq)]
pub struct CollectionChildren {
    pub id: xid::Id,
    pub cid: xid::Id,
    pub kind: i8,
    pub ord: f64,

    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct CollectionInfo {
    pub title: String,
    pub summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keywords: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authors: Option<Vec<String>>,
}

impl CollectionInfo {
    pub fn from_message(data: &[u8]) -> anyhow::Result<Self, HTTPError> {
        match MessageValue::try_from(data) {
            Ok(MessageValue::Array(texts)) => {
                let mut res = Self::default();
                for v in texts {
                    match v.id.as_str() {
                        "title" => res.title = v.texts.first().cloned().unwrap_or_default(),
                        "summary" => res.summary = v.texts.first().cloned().unwrap_or_default(),
                        "keywords" => res.keywords = Some(v.texts),
                        "authors" => res.authors = Some(v.texts),
                        _ => {}
                    }
                }
                if res.title.is_empty() {
                    return Err(HTTPError::new(500, "Invalid message value".to_string()));
                }
                Ok(res)
            }
            Err(err) => Err(err),
            _ => Err(HTTPError::new(500, "Invalid message value".to_string())),
        }
    }

    pub fn to_message(&self) -> anyhow::Result<Vec<u8>, HTTPError> {
        let mut texts: Vec<MessageTexts> = Vec::with_capacity(2);
        texts.push(MessageTexts {
            id: "title".to_string(),
            texts: vec![self.title.clone()],
        });
        if !self.summary.is_empty() {
            texts.push(MessageTexts {
                id: "summary".to_string(),
                texts: vec![self.summary.clone()],
            });
        }
        if let Some(keywords) = &self.keywords {
            if !keywords.is_empty() {
                texts.push(MessageTexts {
                    id: "keywords".to_string(),
                    texts: keywords.clone(),
                });
            }
        }
        if let Some(authors) = &self.authors {
            if !authors.is_empty() {
                texts.push(MessageTexts {
                    id: "authors".to_string(),
                    texts: authors.clone(),
                });
            }
        }
        let msg = MessageValue::Array(texts);
        msg.try_into()
    }
}

impl CollectionChildren {
    pub fn with_pk(id: xid::Id, cid: xid::Id) -> Self {
        Self {
            id,
            cid,
            ..Default::default()
        }
    }

    pub async fn get_one(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<()> {
        let fields = Self::fields();
        self._fields = fields.clone();

        let query = format!(
            "SELECT {} FROM collection_children WHERE id=? AND cid=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.id.to_cql(), self.cid.to_cql());
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, &fields)?;
        self.fill(&cols);

        Ok(())
    }

    pub async fn save(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
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
            "INSERT INTO collection_children ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        Ok(extract_applied(res))
    }

    pub async fn update_ord(&mut self, db: &scylladb::ScyllaDB, ord: f64) -> anyhow::Result<bool> {
        let query = "UPDATE collection_children SET ord=? WHERE id=? AND cid=? IF EXISTS";
        let params = (ord, self.id.to_cql(), self.cid.to_cql());
        self.ord = ord;
        let res = db.execute(query, params).await?;
        Ok(extract_applied(res))
    }

    pub async fn delete(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        let query = "DELETE FROM collection_children WHERE id=? AND cid=? IF EXISTS";
        let params = (self.id.to_cql(), self.cid.to_cql());
        let res = db.execute(query, params).await?;
        Ok(extract_applied(res))
    }

    pub async fn cleanup(db: &scylladb::ScyllaDB, id: xid::Id) -> anyhow::Result<()> {
        let query = "SELECT id,cid FROM collection_children WHERE cid=?";
        let params = (id.to_cql(),);
        let rows = db.execute_iter(query, params.clone()).await?;

        let fields = vec!["id".to_string(), "cid".to_string()];
        for row in rows {
            let mut doc = Self::default();
            let mut cols = ColumnsMap::with_capacity(2);
            cols.fill(row, &fields)?;
            doc.fill(&cols);
            let _ = doc.delete(db).await;
        }

        let query = "DELETE FROM collection_children WHERE id=?";
        let _ = db.execute(query, params).await?;
        Ok(())
    }

    pub async fn list_children(db: &scylladb::ScyllaDB, id: xid::Id) -> anyhow::Result<Vec<Self>> {
        let fields = Self::fields();

        let query = format!(
            "SELECT {} FROM collection_children WHERE id=? LIMIT 10000 USING TIMEOUT 3s",
            fields.clone().join(",")
        );
        let params = (id.to_cql(),);
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
        res.sort_by(|a, b| a.ord.partial_cmp(&b.ord).unwrap());
        Ok(res)
    }

    pub async fn list_by_child(db: &scylladb::ScyllaDB, cid: xid::Id) -> anyhow::Result<Vec<Self>> {
        let fields = Self::fields();

        let query = format!(
            "SELECT {} FROM collection_children WHERE cid=? USING TIMEOUT 3s",
            fields.clone().join(",")
        );
        let params = (cid.to_cql(),);
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
        res.sort_by(|a, b| b.id.partial_cmp(&a.id).unwrap());
        Ok(res)
    }

    pub async fn count_children(db: &scylladb::ScyllaDB, id: xid::Id) -> anyhow::Result<usize> {
        let query = "SELECT cid FROM collection_children WHERE id=? USING TIMEOUT 3s";
        let params = (id.to_cql(),);
        let rows = db.execute_iter(query, params).await?;
        Ok(rows.len())
    }
}

impl Collection {
    pub fn with_pk(id: xid::Id) -> Self {
        Self {
            day: xid_day(id),
            id,
            ..Default::default()
        }
    }

    pub fn select_fields(select_fields: Vec<String>, with_pk: bool) -> anyhow::Result<Vec<String>> {
        if select_fields.is_empty() {
            return Ok(Self::fields());
        }

        let fields = Self::fields();
        let mut select_fields = select_fields;
        if let Some(i) = select_fields.iter().position(|s| s == "info") {
            select_fields.remove(i);
            let field = "mid".to_string();
            if !select_fields.contains(&field) {
                select_fields.push(field);
            }
            let field = "cover".to_string();
            if !select_fields.contains(&field) {
                select_fields.push(field);
            }
        }

        for field in &select_fields {
            if !fields.contains(field) {
                return Err(HTTPError::new(400, format!("Invalid field: {}", field)).into());
            }
        }

        let field = "gid".to_string();
        if !select_fields.contains(&field) {
            select_fields.push(field);
        }
        let field = "status".to_string();
        if !select_fields.contains(&field) {
            select_fields.push(field);
        }
        let field = "rating".to_string();
        if !select_fields.contains(&field) {
            select_fields.push(field);
        }
        let field = "price".to_string();
        if !select_fields.contains(&field) {
            select_fields.push(field);
        }

        if with_pk {
            let field = "day".to_string();
            if !select_fields.contains(&field) {
                select_fields.push(field);
            }
            let field = "id".to_string();
            if !select_fields.contains(&field) {
                select_fields.push(field);
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
                    "Collection status is {}, expected update to 0, got {}",
                    self.status, status
                ),
            )
            .into()),
            0 if !(-1..=2).contains(&status) => Err(HTTPError::new(
                400,
                format!(
                    "Collection status is {}, expected update to -1, 1 or 2, got {}",
                    self.status, status
                ),
            )
            .into()),
            1 if !(-1..=2).contains(&status) => Err(HTTPError::new(
                400,
                format!(
                    "Collection status is {}, expected update to -1, 0 or 2, got {}",
                    self.status, status
                ),
            )
            .into()),
            2 => Err(HTTPError::new(
                400,
                format!("Collection status is {}, can not be updated", self.status),
            )
            .into()),
            _ => Ok(()),
        }
    }

    pub fn to_meili(
        &self,
        language: Language,
        message: &[u8],
        version: i16,
        updated_at: i64,
    ) -> anyhow::Result<meili::Document> {
        let info = CollectionInfo::from_message(message)?;
        let mut doc = meili::Document::new(self.id, language, self.gid);
        doc.kind = 2;
        doc.version = version;
        doc.updated_at = updated_at;
        doc.title = Some(info.title);
        doc.summary = Some(info.summary);

        doc.keywords = info.keywords;
        doc.authors = info.authors;
        Ok(doc)
    }

    pub fn to_info(&self, language: Language) -> Option<(Language, CollectionInfo)> {
        if let Some(msg) = &self._info {
            let (lang, data) = if let Some(data) = msg._i18n_messages.get(language.to_639_3()) {
                (language, data)
            } else {
                (msg.language, &msg.message)
            };
            if let Ok(info) = CollectionInfo::from_message(data) {
                return Some((lang, info));
            }
        }
        None
    }

    pub async fn get_one(
        &mut self,
        db: &scylladb::ScyllaDB,
        select_fields: Vec<String>,
        language: Option<Language>,
    ) -> anyhow::Result<()> {
        let fields = Self::select_fields(select_fields, false)?;
        self._fields = fields.clone();
        self.day = xid_day(self.id);

        let query = format!(
            "SELECT {} FROM collection WHERE day=? AND id=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.day, self.id.to_cql());
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, &fields)?;
        self.fill(&cols);

        if self._fields.contains(&"mid".to_string()) {
            let mut msg = Message::with_pk(self.mid);
            let mut msg_fields = vec!["language".to_string(), "message".to_string()];
            if let Some(lang) = language {
                let lang = lang.to_639_3();
                if support_language(lang) {
                    msg_fields.push(lang.to_string());
                }
            }
            msg.get_one(db, msg_fields).await?;
            self._info = Some(msg);
        }

        Ok(())
    }

    pub async fn save(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        let fields = Self::fields();
        self._fields = fields.clone();
        self.updated_at = unix_ms() as i64;
        self.day = xid_day(self.id);

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
            "INSERT INTO collection ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                "Collection save failed, please try again".to_string(),
            )
            .into());
        }

        Ok(true)
    }

    pub async fn update_status(
        &mut self,
        db: &scylladb::ScyllaDB,
        gid: xid::Id,
        status: i8,
        updated_at: i64,
    ) -> anyhow::Result<bool> {
        self.get_one(
            db,
            vec![
                "gid".to_string(),
                "status".to_string(),
                "updated_at".to_string(),
            ],
            None,
        )
        .await?;
        if self.updated_at != updated_at {
            return Err(HTTPError::new(
                409,
                format!(
                    "Collection updated_at conflict, expected {}, got {}",
                    self.updated_at, updated_at
                ),
            )
            .into());
        }
        if self.gid != gid {
            return Err(HTTPError::new(
                403,
                format!(
                    "Collection gid conflict, expected {}, got {}",
                    gid, self.gid
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
            "UPDATE collection SET status=?,updated_at=? WHERE day=? AND id=? IF updated_at=?";
        let params = (
            status,
            new_updated_at,
            self.day,
            self.id.to_cql(),
            updated_at,
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                format!(
                    "Collection update_status {} failed, please try again",
                    status
                ),
            )
            .into());
        }

        self.updated_at = new_updated_at;
        self.status = status;
        Ok(true)
    }

    pub async fn update_field(
        &mut self,
        db: &scylladb::ScyllaDB,
        field: &str,
    ) -> anyhow::Result<bool> {
        let query = format!(
            "UPDATE collection SET {}=? WHERE day=? AND id=? IF EXISTS",
            field
        );
        let params = match field {
            "rating" => (self.rating.to_cql(), self.day, self.id.to_cql()),
            "mid" => (self.mid.to_cql(), self.day, self.id.to_cql()),
            _ => return Err(HTTPError::new(400, format!("Invalid field: {}", field)).into()),
        };

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                format!("Collection update_field {} failed, please try again", field),
            )
            .into());
        }

        Ok(true)
    }

    pub async fn update(
        &mut self,
        db: &scylladb::ScyllaDB,
        gid: xid::Id,
        cols: ColumnsMap,
        updated_at: i64,
    ) -> anyhow::Result<bool> {
        let valid_fields = ["cover", "price", "creation_price"];
        let update_fields = cols.keys();
        for field in &update_fields {
            if !valid_fields.contains(&field.as_str()) {
                return Err(HTTPError::new(400, format!("Invalid field: {}", field)).into());
            }
        }

        self.get_one(
            db,
            vec![
                "gid".to_string(),
                "updated_at".to_string(),
                "price".to_string(),
                "creation_price".to_string(),
            ],
            None,
        )
        .await?;
        if self.updated_at != updated_at {
            return Err(HTTPError::new(
                409,
                format!(
                    "Collection updated_at conflict, expected {}, got {}",
                    self.updated_at, updated_at
                ),
            )
            .into());
        }
        if self.gid != gid {
            return Err(HTTPError::new(
                403,
                format!(
                    "Collection gid conflict, expected {}, got {}",
                    gid, self.gid
                ),
            )
            .into());
        }

        if self.price < 0 && cols.has("price") {
            return Err(HTTPError::new(
                400,
                format!("Collection price is {}, can not be updated", self.price),
            )
            .into());
        }
        if self.creation_price < 0 && cols.has("creation_price") {
            return Err(HTTPError::new(
                400,
                format!(
                    "Collection creation_price is {}, can not be updated",
                    self.price
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
            "UPDATE collection SET {} WHERE day=? AND id=? IF updated_at=?",
            set_fields.join(",")
        );
        params.push(self.day.to_cql());
        params.push(self.id.to_cql());
        params.push(updated_at.to_cql());

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                "Collection update failed, please try again".to_string(),
            )
            .into());
        }

        self.updated_at = new_updated_at;
        self._fields = vec!["updated_at".to_string()];
        Ok(true)
    }

    pub async fn delete(&mut self, db: &scylladb::ScyllaDB, gid: xid::Id) -> anyhow::Result<bool> {
        let res = self
            .get_one(
                db,
                vec!["status".to_string(), "mid".to_string(), "gid".to_string()],
                None,
            )
            .await;
        if res.is_err() {
            return Ok(false); // already deleted
        }
        if self.gid != gid {
            return Err(HTTPError::new(
                403,
                format!(
                    "Collection gid conflict, expected {}, got {}",
                    gid, self.gid
                ),
            )
            .into());
        }

        if self.status != -1 {
            return Err(HTTPError::new(
                409,
                format!(
                    "Collection status conflict, expected -1, got {}",
                    self.status
                ),
            )
            .into());
        }

        CollectionChildren::cleanup(db, self.id).await?;
        let query = "DELETE FROM collection WHERE day=? AND id=?";
        let params = (self.day, self.id.to_cql());
        let _ = db.execute(query, params).await?;
        let _ = Message::with_pk(self.mid).delete(db, self.id).await;

        Ok(true)
    }

    pub async fn list_by_gid(
        db: &scylladb::ScyllaDB,
        gid: xid::Id,
        select_fields: Vec<String>,
        page_size: u16,
        page_token: Option<xid::Id>,
        status: Option<i8>,
        language: Option<Language>,
    ) -> anyhow::Result<(Vec<Self>, Option<xid::Id>)> {
        let fields = Self::select_fields(select_fields, true)?;

        let mut res: Vec<Self> = Vec::new();
        let status = status.unwrap_or(0);
        let query = match status {
            v if v == -1 || v == 2 => {
                format!(
                    "SELECT {} FROM collection WHERE day=? AND gid=? AND status=? LIMIT 1000 ALLOW FILTERING USING TIMEOUT 3s",
                fields.clone().join(",")
                )
            }
            _ => {
                format!(
                    "SELECT {} FROM collection WHERE day=? AND gid=? AND status>=? LIMIT 1000 ALLOW FILTERING USING TIMEOUT 3s",
                    fields.clone().join(",")
                )
            }
        };

        let mut day = if let Some(id) = page_token {
            xid_day(id) - 1
        } else {
            (unix_ms() / (1000 * 3600 * 24)) as i32
        };

        let mut i = 0i8;
        while day > 19650 && i < 30 {
            let params = (day, gid.to_cql(), status);
            let rows = db.execute_iter(query.as_str(), params).await?;

            for row in rows {
                let mut doc = Self::default();
                let mut cols = ColumnsMap::with_capacity(fields.len());
                cols.fill(row, &fields)?;
                doc.fill(&cols);
                doc._fields = fields.clone();
                res.push(doc);
            }

            if res.len() >= page_size as usize {
                break;
            }
            i += 1;
            day -= 1;
        }

        let next = if day > 19650 {
            Some(day_to_xid(day))
        } else {
            None
        };
        res.sort_by(|a, b| b.id.partial_cmp(&a.id).unwrap());

        if fields.contains(&"mid".to_string()) {
            let mut msg_fields = vec!["language".to_string(), "message".to_string()];
            if let Some(lang) = language {
                let lang = lang.to_639_3();
                if support_language(lang) {
                    msg_fields.push(lang.to_string());
                }
            }

            for doc in &mut res {
                let mut msg = Message::with_pk(doc.mid);
                msg.get_one(db, msg_fields.clone()).await?;
                doc._info = Some(msg);
            }
        }

        Ok((res, next))
    }

    pub async fn list_latest(
        db: &scylladb::ScyllaDB,
        select_fields: Vec<String>,
        page_token: Option<xid::Id>,
        language: Option<Language>,
    ) -> anyhow::Result<(Vec<Self>, Option<xid::Id>)> {
        let fields = Self::select_fields(select_fields, true)?;

        let mut res: Vec<Self> = Vec::new();
        let query = format!(
                "SELECT {} FROM collection WHERE day=? AND status=2 LIMIT 1000 ALLOW FILTERING USING TIMEOUT 3s",
                fields.clone().join(",")
            );

        let mut day = if let Some(id) = page_token {
            xid_day(id) - 1
        } else {
            (unix_ms() / (1000 * 3600 * 24)) as i32
        };

        let min = (unix_ms() / (1000 * 3600 * 24)) as i32 - 30;
        while day > min {
            let params = (day,);
            let rows = db.execute_iter(query.as_str(), params).await?;
            for row in rows {
                let mut doc = Self::default();
                let mut cols = ColumnsMap::with_capacity(fields.len());
                cols.fill(row, &fields)?;
                doc.fill(&cols);
                doc._fields = fields.clone();
                res.push(doc);
            }

            if (page_token.is_none() && res.len() >= 6) || (page_token.is_some() && res.len() >= 3)
            {
                break;
            }

            day -= 1;
        }

        let next = if res.is_empty() {
            None
        } else {
            Some(res.last().unwrap().id)
        };
        res.sort_by(|a, b| b.id.partial_cmp(&a.id).unwrap());

        if fields.contains(&"mid".to_string()) {
            let mut msg_fields = vec!["language".to_string(), "message".to_string()];
            if let Some(lang) = language {
                let lang = lang.to_639_3();
                if support_language(lang) {
                    msg_fields.push(lang.to_string());
                }
            }

            for doc in &mut res {
                let mut msg = Message::with_pk(doc.mid);
                msg.get_one(db, msg_fields.clone()).await?;
                doc._info = Some(msg);
            }
        }

        Ok((res, next))
    }

    pub async fn list_by_child(
        db: &scylladb::ScyllaDB,
        cid: xid::Id,
        select_fields: Vec<String>,
        gid: Option<xid::Id>,
        language: Option<Language>,
    ) -> anyhow::Result<Vec<Self>> {
        let query = "SELECT id FROM collection_children WHERE cid=? USING TIMEOUT 3s".to_string();

        let params = (cid.to_cql(),);
        let rows = db.execute_iter(query, params).await?;

        let mut res: Vec<Self> = Vec::with_capacity(rows.len());
        let mut ids: Vec<xid::Id> = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(Some(v)) = row.columns.first() {
                let id = xid::Id::from_cql(v)?;
                ids.push(id);
            }
        }
        if ids.is_empty() {
            return Ok(res);
        }

        for id in ids {
            let mut doc = Self::with_pk(id);
            doc.get_one(db, select_fields.clone(), language).await?;
            if let Some(gid) = gid {
                if doc.gid != gid {
                    continue;
                }
            }
            res.push(doc);
        }

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use ciborium::cbor;

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

    #[test]
    fn collection_info_works() {
        let data: Vec<u8> = cbor_to_vec(
            &cbor!([{
                "id" => "title",
                "texts" => ["Hello World"],
            }, {
                "id" => "keywords",
                "texts" => ["Hello", "World"],
            }])
            .unwrap(),
        )
        .unwrap();
        let info = CollectionInfo::from_message(data.as_slice()).unwrap();
        assert_eq!(info.title.as_str(), "Hello World");
        assert_eq!(info.summary.len(), 0);
        assert_eq!(
            info.keywords,
            Some(vec!["Hello".to_string(), "World".to_string()])
        );
        assert_eq!(info.authors, None);

        let data2: Vec<u8> = info.to_message().unwrap();
        assert_eq!(data2, data);

        let data: Vec<u8> = cbor_to_vec(
            &cbor!({
                "id" => "title",
                "texts" => "Hello World",
            })
            .unwrap(),
        )
        .unwrap();

        assert!(CollectionInfo::from_message(data.as_slice()).is_err());
        let data: Vec<u8> = cbor_to_vec(
            &cbor!({
                "id" => "title",
                "texts" => [],
            })
            .unwrap(),
        )
        .unwrap();
        assert!(CollectionInfo::from_message(data.as_slice()).is_err());
        assert!(CollectionInfo::from_message(vec![].as_slice()).is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore]
    async fn test_all() {
        collection_model_works().await;
        collection_children_model_works().await;
    }

    // #[tokio::test(flavor = "current_thread")]
    async fn collection_model_works() {
        let db = get_db().await;
        let id = xid::new();
        let mid = xid::new();
        let gid = xid::new();
        let message: Vec<u8> = cbor_to_vec(
            &cbor!([{
                "id" => "title",
                "texts" => ["Hello World"],
            }])
            .unwrap(),
        )
        .unwrap();

        let mut msg = Message::with_pk(mid);
        msg.attach_to = id;
        msg.kind = "collection.info".to_string();
        msg.language = Language::Eng;
        msg.version = 1;
        msg.context = "Hello World".to_string();
        msg.message = message.clone();
        msg.save(db).await.unwrap();

        // valid_status
        {
            let mut doc = Collection::with_pk(id);
            assert!(doc.valid_status(-2).is_err());
            assert!(doc.valid_status(-1).is_ok());
            assert!(doc.valid_status(0).is_ok());
            assert!(doc.valid_status(1).is_ok());
            assert!(doc.valid_status(2).is_ok());
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
            let mut doc = Collection::with_pk(id);
            doc.gid = gid;
            doc.mid = mid;

            let res = doc.get_one(db, vec![], None).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 404);

            assert!(doc.save(db).await.unwrap());
            let res = doc.save(db).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into(); // can not insert twice
            assert_eq!(err.code, 409);

            let mut doc2 = Collection::with_pk(id);
            doc2.get_one(db, vec![], None).await.unwrap();

            assert_eq!(doc2.gid, gid);
            assert_eq!(doc2.mid, mid);
            assert!(doc2._info.is_some());
            let (lang, info) = doc2.to_info(Language::Und).unwrap();
            assert_eq!(lang, Language::Eng);
            assert_eq!(info.title.as_str(), "Hello World");

            let mut doc3 = Collection::with_pk(id);
            doc3.get_one(db, vec!["info".to_string()], None)
                .await
                .unwrap();
            assert_eq!(doc3.gid, gid);
            assert_eq!(doc3.updated_at, 0);
            assert!(doc3._info.is_some());
            let (lang, info) = doc3.to_info(Language::Eng).unwrap();
            assert_eq!(lang, Language::Eng);
            assert_eq!(info.title.as_str(), "Hello World");

            let msg = doc3._info.as_ref().unwrap();
            let meili_doc = doc3
                .to_meili(msg.language, &msg.message, msg.version, msg.updated_at)
                .unwrap();
            assert_eq!(meili_doc.kind, 2i8);
            assert_eq!(meili_doc.version, 1);
            assert_eq!(meili_doc.updated_at, msg.updated_at);
            assert_eq!(meili_doc.title, Some("Hello World".to_string()));
        }

        // update
        {
            let mut doc = Collection::with_pk(id);
            let mut cols = ColumnsMap::new();
            cols.set_as("version", &2i16);
            let res = doc.update(db, gid, cols, 0).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 400); // version is not updatable

            let mut cols = ColumnsMap::new();
            cols.set_as("price", &50i64);
            let res = doc.update(db, gid, cols, 1).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 409); // updated_at not match

            let mut cols = ColumnsMap::new();
            cols.set_as("price", &50i64);
            cols.set_as("creation_price", &-1i64);
            doc.get_one(db, vec![], None).await.unwrap();
            let res = doc.update(db, gid, cols, doc.updated_at).await.unwrap();
            assert!(res);
            doc.get_one(db, vec![], None).await.unwrap();
            assert_eq!(doc.price, 50);
            assert_eq!(doc.creation_price, -1);

            let mut cols = ColumnsMap::new();
            cols.set_as("creation_price", &10i64);
            let res = doc.update(db, gid, cols, doc.updated_at).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 400); // creation_price is not updatable
        }

        // update_status
        {
            let mut doc = Collection::with_pk(id);
            doc.get_one(db, vec![], None).await.unwrap();

            let res = doc.update_status(db, gid, 2, 1).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 409); // version not match

            let res = doc.update_status(db, gid, 2, doc.updated_at).await.unwrap();
            assert!(res); // no need to update
            let res = doc.update_status(db, gid, 1, doc.updated_at).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 400); // version not match
        }

        // update_field
        {
            let mut doc = Collection::with_pk(id);
            doc.get_one(db, vec![], None).await.unwrap();
            assert_eq!(doc.rating, 0);

            doc.rating = 127;
            let res = doc.update_field(db, "gid").await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 400); // gid not match

            let res = doc.update_field(db, "rating").await.unwrap();
            assert!(res);

            let mut doc2 = Collection::with_pk(id);
            doc2.get_one(db, vec![], None).await.unwrap();
            assert_eq!(doc2.rating, 127);
        }

        // delete
        {
            let mut doc = Collection::with_pk(id);
            let res = doc.delete(db, gid).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 409); // version not match

            let mut doc = Collection::with_pk(xid::new());
            let res = doc.delete(db, gid).await.unwrap();
            assert!(!res);
        }
    }

    // #[tokio::test(flavor = "current_thread")]
    async fn collection_children_model_works() {
        let db = get_db().await;
        let mid = xid::new();
        let gid = xid::new();
        let message: Vec<u8> = cbor_to_vec(
            &cbor!([{
                "id" => "title",
                "texts" => ["Hello World"],
            }])
            .unwrap(),
        )
        .unwrap();

        let mut parent = Collection::with_pk(xid::new());
        let mut msg = Message::with_pk(mid);
        msg.attach_to = parent.id;
        msg.kind = "collection.info".to_string();
        msg.language = Language::Eng;
        msg.version = 1;
        msg.context = "Hello World".to_string();
        msg.message = message.clone();
        msg.save(db).await.unwrap();

        parent.gid = gid;
        parent.mid = mid;
        parent.save(db).await.unwrap();

        let mut child1 = CollectionChildren {
            id: parent.id,
            cid: xid::new(),
            kind: 0,
            ord: unix_ms() as f64,
            ..Default::default()
        };
        let res = child1.save(db).await.unwrap();
        assert!(res);

        let res = child1.save(db).await.unwrap();
        assert!(!res);

        assert_eq!(
            CollectionChildren::count_children(db, parent.id)
                .await
                .unwrap(),
            1
        );

        let mut child2 = CollectionChildren {
            id: parent.id,
            cid: xid::new(),
            kind: 1,
            ord: unix_ms() as f64,
            ..Default::default()
        };
        let res = child2.save(db).await.unwrap();
        assert!(res);

        let mut child3 = CollectionChildren {
            id: parent.id,
            cid: xid::new(),
            kind: 2,
            ord: unix_ms() as f64,
            ..Default::default()
        };
        let res = child3.save(db).await.unwrap();
        assert!(res);

        assert_eq!(
            CollectionChildren::count_children(db, parent.id)
                .await
                .unwrap(),
            3
        );

        let children = CollectionChildren::list_children(db, parent.id)
            .await
            .unwrap();
        assert_eq!(children.len(), 3);
        assert_eq!(
            children.iter().map(|v| v.cid).collect::<Vec<xid::Id>>(),
            vec![child1.cid, child2.cid, child3.cid]
        );

        child2.update_ord(db, 0f64).await.unwrap();
        let children = CollectionChildren::list_children(db, parent.id)
            .await
            .unwrap();
        assert_eq!(children.len(), 3);
        assert_eq!(
            children.iter().map(|v| v.cid).collect::<Vec<xid::Id>>(),
            vec![child2.cid, child1.cid, child3.cid]
        );
        let res = child2.delete(db).await.unwrap();
        assert!(res);
        let res = child2.delete(db).await.unwrap();
        assert!(!res);

        let children = CollectionChildren::list_children(db, parent.id)
            .await
            .unwrap();
        assert_eq!(children.len(), 2);
        assert_eq!(
            children.iter().map(|v| v.cid).collect::<Vec<xid::Id>>(),
            vec![child1.cid, child3.cid]
        );

        CollectionChildren::cleanup(db, child1.cid).await.unwrap();
        let children = CollectionChildren::list_children(db, parent.id)
            .await
            .unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(
            children.iter().map(|v| v.cid).collect::<Vec<xid::Id>>(),
            vec![child3.cid]
        );

        let res = parent.delete(db, gid).await;
        assert!(res.is_err());
        let err: erring::HTTPError = res.unwrap_err().into();
        assert_eq!(err.code, 409);
        parent
            .update_status(db, gid, -1, parent.updated_at)
            .await
            .unwrap();
        parent.delete(db, gid).await.unwrap();
        let children = CollectionChildren::list_children(db, parent.id)
            .await
            .unwrap();
        assert_eq!(children.len(), 0);
        assert_eq!(
            CollectionChildren::count_children(db, parent.id)
                .await
                .unwrap(),
            0
        );

        let res = parent.get_one(db, vec![], None).await;
        assert!(res.is_err());
        let err: erring::HTTPError = res.unwrap_err().into();
        assert_eq!(err.code, 404);
        let res = msg.get_one(db, vec![]).await;
        assert!(res.is_err());
        let err: erring::HTTPError = res.unwrap_err().into();
        assert_eq!(err.code, 404);
    }
}
