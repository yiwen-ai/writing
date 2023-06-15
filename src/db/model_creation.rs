use ciborium::cbor;
use std::collections::HashSet;

use super::{scylladb, scylladb::CqlValue, ToAnyhowError};
use scylla_orm_macros::CqlOrm;

pub use isolang::Language;

#[derive(Debug, Default, Clone, CqlOrm)]
pub struct Creation {
    pub id: xid::Id,
    pub gid: xid::Id,
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
}

impl Creation {
    pub fn with_pk(id: xid::Id) -> Self {
        Creation {
            id,
            ..Default::default()
        }
    }

    pub async fn get_one(
        &mut self,
        db: &scylladb::ScyllaDB,
        select_fields: Vec<&str>,
    ) -> anyhow::Result<()> {
        let fields = if select_fields.is_empty() {
            Self::fields()
        } else {
            select_fields
        };

        let query = format!(
            "SELECT {} FROM creation WHERE id=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.id.as_bytes(),);
        let res = db.execute(query, params).await?.single_row();

        if let Err(err) = res {
            return Err(err.to_anyhow_error());
        }
        let mut cols = scylladb::ColumnsMap::with_capacity(fields.len());
        cols.fill(res.unwrap(), fields)?;
        self.fill(&cols);

        Ok(())
    }

    pub async fn save(&self, db: &scylladb::ScyllaDB) -> anyhow::Result<()> {
        let fields = Self::fields();
        let mut cols_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut vals_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut params: Vec<&CqlValue> = Vec::with_capacity(fields.len());
        let cols = self.to()?;

        for field in fields {
            cols_name.push(field);
            vals_name.push("?");
            params.push(cols.get(field).unwrap());
        }

        let query = format!(
            "INSERT INTO creation ({}) VALUES ({}) USING TTL 0",
            cols_name.join(","),
            vals_name.join(",")
        );

        let _ = db.execute(query, params).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
        let db = DB.get_or_init(get_db).await;
        let did = xid::new();
        let uid = xid::Id::from_str("jarvis00000000000000").unwrap();
        let mut doc = Creation::with_pk(did);
        doc.gid = uid;
        doc.version = 1;
        doc.language = Language::Eng;
        doc.title = "Hello World".to_string();
        ciborium::into_writer(
            &cbor!({
                "id" => "abcdef",
                "texts" => vec!["hello world","你好，世界"],
            })
            .unwrap(),
            &mut doc.content,
        )
        .unwrap();

        let res = doc.get_one(db, vec![]).await;
        assert!(res.is_err());
        assert_eq!(erring::HTTPError::from(res.unwrap_err()).code, 404);

        doc.save(db).await.unwrap();

        let mut doc2 = Creation::with_pk(did);
        doc2.get_one(db, vec![]).await.unwrap();
        println!("doc: {:#?}", doc2);

        assert_eq!(doc2.title.as_str(), "Hello World");
        assert_eq!(doc2.version, 1);
        assert_eq!(doc2.language, Language::Eng);
        assert_eq!(doc2.content, doc.content);

        let mut doc3 = Creation::with_pk(did);
        doc3.get_one(db, vec!["gid", "title"]).await.unwrap();
        assert_eq!(doc3.title.as_str(), "Hello World");
        assert_eq!(doc3.gid, uid);
        assert_eq!(doc3.version, 0);
        assert_eq!(doc3.language, Language::default());
        assert!(doc3.content.is_empty());

        println!("doc: {:#?}", doc3);
    }
}
