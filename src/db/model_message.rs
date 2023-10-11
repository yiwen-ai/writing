use isolang::Language;
use std::collections::HashMap;

use axum_web::context::unix_ms;
use axum_web::erring::HTTPError;
use scylla_orm::{ColumnsMap, CqlValue, ToCqlVal};
use scylla_orm_macros::CqlOrm;

use crate::db::{scylladb, scylladb::extract_applied, xid_day};

pub static LANGUAGES: [&str; 158] = [
    "abk", "aar", "afr", "aka", "sqi", "amh", "ara", "arg", "hye", "asm", "ava", "aze", "bam",
    "bak", "eus", "bel", "ben", "bis", "bos", "bre", "bul", "mya", "cat", "cha", "che", "zho",
    "chu", "chv", "cor", "cos", "hrv", "ces", "dan", "div", "nld", "dzo", "eng", "epo", "est",
    "ewe", "fao", "fin", "fra", "ful", "glg", "lug", "kat", "deu", "guj", "hat", "hau", "heb",
    "hin", "hun", "isl", "ibo", "ind", "ina", "iku", "gle", "ita", "jpn", "jav", "kal", "kan",
    "kas", "kaz", "khm", "kik", "kin", "kir", "kor", "kua", "kur", "lao", "lav", "lim", "lin",
    "lit", "lub", "ltz", "mkd", "mlg", "msa", "mal", "mlt", "glv", "mri", "mar", "ell", "mon",
    "nav", "nep", "nde", "sme", "nor", "nno", "nya", "oci", "ori", "orm", "oss", "pan", "fas",
    "pol", "por", "pus", "que", "ron", "roh", "run", "rus", "smo", "sag", "san", "gla", "srp",
    "sna", "iii", "snd", "sin", "slk", "slv", "som", "nbl", "sot", "spa", "sun", "swa", "ssw",
    "swe", "tgl", "tah", "tgk", "tam", "tat", "tel", "tha", "bod", "tir", "ton", "tso", "tsn",
    "tur", "tuk", "uig", "ukr", "urd", "uzb", "ven", "vie", "cym", "fry", "wol", "xho", "yid",
    "yor", "zul",
];

#[derive(Debug, Default, Clone, CqlOrm, PartialEq)]
pub struct Message {
    pub day: i32,
    pub id: xid::Id,
    pub attach_to: xid::Id,
    pub kind: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub context: String,
    pub language: Language,
    pub version: i16,
    pub message: Vec<u8>,

    pub _i18n_messages: HashMap<String, Vec<u8>>,
    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
}

impl Message {
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
        if let Some(i) = select_fields.iter().position(|s| s == "i18n") {
            select_fields.remove(i);
            let field = "message".to_string();
            if !select_fields.contains(&field) {
                select_fields.push(field);
            }
            for l in LANGUAGES {
                select_fields.push(l.to_string());
            }
        }

        for field in &select_fields {
            if !fields.contains(field) && !LANGUAGES.contains(&field.as_str()) {
                return Err(HTTPError::new(400, format!("Invalid field: {}", field)).into());
            }
        }

        let field = "language".to_string();
        if !select_fields.contains(&field) {
            select_fields.push(field);
        }
        let field = "version".to_string();
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

    pub fn fill_languages(&mut self, cols: &scylla_orm::ColumnsMap) {
        for lang in LANGUAGES.iter() {
            if cols.has(lang) {
                self._i18n_messages
                    .insert(lang.to_string(), cols.get_as(lang).unwrap_or_default());
            }
        }
    }

    pub async fn get_one(
        &mut self,
        db: &scylladb::ScyllaDB,
        select_fields: Vec<String>,
    ) -> anyhow::Result<()> {
        let fields = Self::select_fields(select_fields, false)?;
        self._fields = fields.clone();
        self.day = xid_day(self.id);

        let query = format!(
            "SELECT {} FROM message WHERE day=? AND id=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.day, self.id.to_cql());
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, &fields)?;
        self.fill(&cols);
        self.fill_languages(&cols);

        Ok(())
    }

    pub async fn batch_get(
        db: &scylladb::ScyllaDB,
        list: Vec<xid::Id>,
        select_fields: Vec<String>,
    ) -> anyhow::Result<Vec<Self>> {
        let fields = Self::select_fields(select_fields, false)?;

        let query = format!(
            "SELECT {} FROM message WHERE day=? AND id=? LIMIT 1",
            fields.join(",")
        );
        let mut res: Vec<Self> = Vec::with_capacity(list.len());
        for v in list {
            let mut doc = Self::with_pk(v);
            let row = db
                .execute(query.as_str(), (doc.day, doc.id.to_cql()))
                .await?
                .single_row()?;
            let mut cols = ColumnsMap::with_capacity(fields.len());
            cols.fill(row, &fields)?;
            doc.fill(&cols);
            doc.fill_languages(&cols);
            doc._fields = fields.clone();
            res.push(doc);
        }

        Ok(res)
    }

    pub async fn save(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        let fields = Self::fields();
        self._fields = fields.clone();
        self.updated_at = unix_ms() as i64;
        self.created_at = self.updated_at;
        self.version = 1;
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
            "INSERT INTO message ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(
                HTTPError::new(409, "Message save failed, please try again".to_string()).into(),
            );
        }

        Ok(true)
    }

    pub async fn update(
        &mut self,
        db: &scylladb::ScyllaDB,
        cols: ColumnsMap,
        version: i16,
    ) -> anyhow::Result<bool> {
        let valid_fields = ["context"];
        let update_fields = cols.keys();
        for field in &update_fields {
            if !valid_fields.contains(&field.as_str()) {
                return Err(HTTPError::new(400, format!("Invalid field: {}", field)).into());
            }
        }

        self.get_one(db, vec!["version".to_string()]).await?;
        if self.version != version {
            return Err(HTTPError::new(
                409,
                format!(
                    "Message version conflict, expected version {}, got {}",
                    self.version, version
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
            "UPDATE message SET {} WHERE day=? AND id=? IF version=?",
            set_fields.join(",")
        );
        params.push(self.day.to_cql());
        params.push(self.id.to_cql());
        params.push(version.to_cql());

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(
                HTTPError::new(409, "Message update failed, please try again".to_string()).into(),
            );
        }

        self.updated_at = new_updated_at;
        self._fields = vec!["updated_at".to_string()];
        Ok(true)
    }

    pub async fn update_message(
        &mut self,
        db: &scylladb::ScyllaDB,
        lang: String,
        message: Vec<u8>,
        version: i16,
    ) -> anyhow::Result<bool> {
        if !LANGUAGES.contains(&lang.as_str()) {
            return Err(HTTPError::new(400, format!("Invalid language: {}", lang)).into());
        }

        self.get_one(db, vec!["version".to_string(), "language".to_string()])
            .await?;
        if self.version != version {
            return Err(HTTPError::new(
                409,
                format!(
                    "Message version conflict, expected version {}, got {}",
                    self.version, version
                ),
            )
            .into());
        }

        let new_updated_at = unix_ms() as i64;
        let res = if lang == self.language.to_639_3() {
            if version == 32767 {
                return Err(HTTPError::new(
                    400,
                    format!("Message version overflow, got {}", version),
                )
                .into());
            }

            let query = format!(
                "UPDATE message SET updated_at=?,message=?,version=? WHERE day=? AND id=? IF version=?",
            );
            let params = (
                new_updated_at,
                message.to_cql(),
                version + 1,
                self.day,
                self.id.to_cql(),
                version,
            );
            self.version += 1;
            self.updated_at = new_updated_at;
            self._fields = vec!["updated_at".to_string(), "version".to_string()];
            db.execute(query, params).await?
        } else {
            let query = format!(
                "UPDATE message SET updated_at=?,{}=? WHERE day=? AND id=? IF version=?",
                lang
            );
            let params = (
                new_updated_at,
                message.to_cql(),
                self.day,
                self.id.to_cql(),
                version,
            );
            self.updated_at = new_updated_at;
            self._fields = vec!["updated_at".to_string()];
            db.execute(query, params).await?
        };

        if !extract_applied(res) {
            return Err(
                HTTPError::new(409, "Message update failed, please try again".to_string()).into(),
            );
        }
        Ok(true)
    }

    pub async fn delete(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        let res = self.get_one(db, vec!["version".to_string()]).await;
        if res.is_err() {
            return Ok(false); // already deleted
        }

        let query = "DELETE FROM message WHERE day=? AND id=?";
        let params = (self.day, self.id.to_cql());
        let _ = db.execute(query, params).await?;

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
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

    #[tokio::test(flavor = "current_thread")]
    #[ignore]
    async fn test_all() {
        message_model_works().await;
    }

    // #[tokio::test(flavor = "current_thread")]
    async fn message_model_works() {
        let db = get_db().await;
        let id = xid::new();
        let gid = xid::new();
        let message: Vec<u8> = cbor_to_vec(
            &cbor!([{
                "id" => "title",
                "texts" => ["Hello World"],
            }])
            .unwrap(),
        )
        .unwrap();

        // create
        {
            let mut doc = Message::with_pk(id);
            doc.attach_to = gid;
            doc.kind = "group.message".to_string();
            doc.language = Language::Eng;
            doc.version = 1;
            doc.context = "Hello World".to_string();
            doc.message = message.clone();

            let res = doc.get_one(db, vec![]).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 404);

            assert!(doc.save(db).await.unwrap());
            let res = doc.save(db).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into(); // can not insert twice
            assert_eq!(err.code, 409);

            let mut doc2 = Message::with_pk(id);
            doc2.get_one(db, vec![]).await.unwrap();

            assert_eq!(doc2.attach_to, gid);
            assert_eq!(doc2.context.as_str(), "Hello World");
            assert_eq!(doc2.version, 1);
            assert_eq!(doc2.language, Language::Eng);
            assert_eq!(doc2.message, message);

            let mut doc3 = Message::with_pk(id);
            doc3.get_one(db, vec!["language".to_string()])
                .await
                .unwrap();
            assert_eq!(doc3.id, id);
            assert_eq!(doc3.context.as_str(), "");
            assert_eq!(doc3.version, 1);
            assert_eq!(doc3.language, Language::Eng);
        }

        // update
        {
            let mut doc = Message::with_pk(id);
            let mut cols = ColumnsMap::new();
            cols.set_as("version", &2i16);
            let res = doc.update(db, cols, 0).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 400); // status is not updatable

            let mut cols = ColumnsMap::new();
            cols.set_as("context", &"update context 1".to_string());
            let res = doc.update(db, cols, 2).await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 409); // version not match

            let mut cols = ColumnsMap::new();
            cols.set_as("context", &"context 1".to_string());
            let res = doc.update(db, cols, 1).await.unwrap();
            assert!(res);
            assert_eq!(doc.version, 1);

            let mut cols = ColumnsMap::new();
            cols.set_as("context", &"context 2".to_string());

            let res = doc.update(db, cols, doc.version).await.unwrap();
            assert!(res);
        }

        // update_message
        {
            let mut doc = Message::with_pk(id);
            doc.get_one(db, vec!["i18n".to_string()]).await.unwrap();
            assert_eq!(doc.id, id);
            assert_eq!(doc.context.as_str(), "");
            assert_eq!(doc.version, 1);
            assert_eq!(doc.language, Language::Eng);
            assert_eq!(doc.message, message);
            assert!(doc._fields.contains(&"zho".to_string()));
            assert!(doc._i18n_messages.is_empty());

            let res = doc
                .update_message(db, "zho".to_string(), message.clone(), 2)
                .await;
            assert!(res.is_err());
            let err: erring::HTTPError = res.unwrap_err().into();
            assert_eq!(err.code, 409); // version not match

            doc.update_message(db, "zho".to_string(), message.clone(), 1)
                .await
                .unwrap();

            let mut doc2 = Message::with_pk(id);
            doc2.get_one(db, vec!["i18n".to_string()]).await.unwrap();
            assert_eq!(doc2.id, id);
            assert_eq!(doc2.context.as_str(), "");
            assert_eq!(doc2.version, 1);
            assert_eq!(doc2.language, Language::Eng);
            assert_eq!(doc2.message, message);
            assert!(doc2._fields.contains(&"zho".to_string()));
            assert!(doc2._i18n_messages.len() == 1);
            assert_eq!(doc2._i18n_messages.get("zho").unwrap(), &message);

            doc2.update_message(db, "eng".to_string(), message.clone(), 1)
                .await
                .unwrap();
            assert_eq!(doc2.version, 2);
            let res = doc2
                .update_message(db, "eng".to_string(), message.clone(), 1)
                .await;
            assert!(res.is_err());
        }

        // delete
        {
            let mut doc = Message::with_pk(id);
            let res = doc.delete(db).await.unwrap();
            assert!(res);

            let res = doc.delete(db).await.unwrap();
            assert!(!res); // already deleted
        }
    }
}
