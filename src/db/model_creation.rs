use super::{scylladb, scylladb::CqlValue, ToAnyhowError};
use ciborium::cbor;
use std::collections::HashSet;

pub use isolang::Language;

#[derive(Debug, Default, Clone)]
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
    pub active_langs: HashSet<Language>,
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
    pub fn all_fields() -> Vec<&'static str> {
        vec![
            "id",
            "gid",
            "status",
            "rating",
            "version",
            "language",
            "creator",
            "created_at",
            "updated_at",
            "active_langs",
            "original_url",
            "genre",
            "title",
            "description",
            "cover",
            "keywords",
            "labels",
            "authors",
            "reviewers",
            "summary",
            "content",
            "license",
        ]
    }

    pub fn fill(&mut self, cols: &scylladb::ColumnsMap) {
        if cols.has("id") {
            self.id = cols.get_as("id").unwrap_or_default();
        }
        if cols.has("gid") {
            self.gid = cols.get_as("gid").unwrap_or_default();
        }
        if cols.has("status") {
            self.status = cols.get_as("status").unwrap_or_default();
        }
        if cols.has("rating") {
            self.rating = cols.get_as("rating").unwrap_or_default();
        }
        if cols.has("version") {
            self.version = cols.get_as("version").unwrap_or_default();
        }
        if cols.has("language") {
            self.language = cols.get_as("language").unwrap_or_default();
        }
        if cols.has("creator") {
            self.creator = cols.get_as("creator").unwrap_or_default();
        }
        if cols.has("created_at") {
            self.created_at = cols.get_as("created_at").unwrap_or_default();
        }
        if cols.has("updated_at") {
            self.updated_at = cols.get_as("updated_at").unwrap_or_default();
        }
        if cols.has("active_langs") {
            self.active_langs = cols.get_as("active_langs").unwrap_or_default();
        }
        if cols.has("original_url") {
            self.original_url = cols.get_as("original_url").unwrap_or_default();
        }
        if cols.has("genre") {
            self.genre = cols.get_as("genre").unwrap_or_default();
        }
        if cols.has("title") {
            self.title = cols.get_as("title").unwrap_or_default();
        }
        if cols.has("description") {
            self.description = cols.get_as("description").unwrap_or_default();
        }
        if cols.has("cover") {
            self.cover = cols.get_as("cover").unwrap_or_default();
        }
        if cols.has("keywords") {
            self.keywords = cols.get_as("keywords").unwrap_or_default();
        }
        if cols.has("labels") {
            self.labels = cols.get_as("labels").unwrap_or_default();
        }
        if cols.has("authors") {
            self.authors = cols.get_as("authors").unwrap_or_default();
        }
        if cols.has("reviewers") {
            self.reviewers = cols.get_as("reviewers").unwrap_or_default();
        }
        if cols.has("summary") {
            self.summary = cols.get_as("summary").unwrap_or_default();
        }
        if cols.has("content") {
            self.content = cols.get_as("content").unwrap_or_default();
        }
        if cols.has("license") {
            self.license = cols.get_as("license").unwrap_or_default();
        }
    }

    pub fn to(&self) -> anyhow::Result<scylladb::ColumnsMap> {
        let mut cols = scylladb::ColumnsMap::with_capacity(22);
        cols.set_as("id", &self.id)?;
        cols.set_as("gid", &self.gid)?;
        cols.set_as("status", &self.status)?;
        cols.set_as("rating", &self.rating)?;
        cols.set_as("version", &self.version)?;
        cols.set_as("language", &self.language)?;
        cols.set_as("creator", &self.creator)?;
        cols.set_as("created_at", &self.created_at)?;
        cols.set_as("updated_at", &self.updated_at)?;
        cols.set_as("active_langs", &self.active_langs)?;
        cols.set_as("original_url", &self.original_url)?;
        cols.set_as("genre", &self.genre)?;
        cols.set_as("title", &self.title)?;
        cols.set_as("description", &self.description)?;
        cols.set_as("cover", &self.cover)?;
        cols.set_as("keywords", &self.keywords)?;
        cols.set_as("labels", &self.labels)?;
        cols.set_as("authors", &self.authors)?;
        cols.set_as("reviewers", &self.reviewers)?;
        cols.set_as("summary", &self.summary)?;
        cols.set_as("content", &self.content)?;
        cols.set_as("license", &self.license)?;
        Ok(cols)
    }

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
            Self::all_fields()
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
        let all_fields = Self::all_fields();
        let mut cols_name: Vec<&str> = Vec::with_capacity(all_fields.len());
        let mut vals_name: Vec<&str> = Vec::with_capacity(all_fields.len());
        let mut params: Vec<&CqlValue> = Vec::with_capacity(all_fields.len());
        let cols = self.to()?;

        for field in all_fields {
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
