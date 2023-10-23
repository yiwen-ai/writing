use base64::{engine::general_purpose, Engine as _};
use isolang::Language;
use meilisearch_sdk::{
    client::Client,
    indexes::Index,
    search::{SearchQuery, Selectors},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use axum_web::object::{cbor_from_slice, cbor_to_vec, PackObject};

use crate::conf;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Document {
    pub id: String, // base64_raw_url(cbor([cid,language,gid]))
    pub gid: String,
    pub language: String,
    pub kind: i8, // 0: creation, 1: publication, 2: collection
    pub version: i16,

    #[serde(default)]
    pub updated_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genre: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keywords: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authors: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

type TripleId = (
    PackObject<xid::Id>,
    PackObject<Language>,
    PackObject<xid::Id>,
);

impl Document {
    pub fn new(cid: xid::Id, lang: Language, gid: xid::Id) -> Self {
        let to = PackObject::Cbor(());
        let data = cbor_to_vec(&(to.with(cid), to.with(lang), to.with(gid))).unwrap_or_default();
        let id = general_purpose::URL_SAFE_NO_PAD.encode(data);
        Self {
            id,
            gid: gid.to_string(),
            language: lang.to_639_3().to_string(),
            ..Default::default()
        }
    }

    pub fn extract_id(&self) -> TripleId {
        let data = general_purpose::URL_SAFE_NO_PAD
            .decode(&self.id)
            .unwrap_or_default();
        let res: TripleId = cbor_from_slice(&data).unwrap_or_default();
        res
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DocumentOutput {
    pub gid: PackObject<xid::Id>,
    pub cid: PackObject<xid::Id>,
    pub language: PackObject<Language>,
    pub version: i16,
    pub updated_at: i64,
    pub kind: i8, // 0: creation, 1: publication
    pub title: String,
    pub summary: String,
}

impl DocumentOutput {
    fn from(val: Document, to: &PackObject<()>) -> Self {
        let (cid, language, gid) = val.extract_id();
        Self {
            gid: to.with(gid.unwrap()),
            cid: to.with(cid.unwrap()),
            language: to.with(language.unwrap()),
            version: val.version,
            updated_at: val.updated_at,
            kind: val.kind,
            title: val.title.unwrap_or_default(),
            summary: val.summary.unwrap_or_default(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SearchOutput {
    pub hits: Vec<DocumentOutput>,
    pub languages: HashMap<String, usize>,
}

// Indexe settings
// {
//     "rankingRules": [
//         "words",
//         "typo",
//         "proximity",
//         "attribute",
//         "sort",
//         "exactness",
//         "updated_at:desc"
//     ],
//     "searchableAttributes": [
//         "title",
//         "keywords",
//         "summary"
//     ],
//     "displayedAttributes": [
//         "id",
//         "gid",
//         "language",
//         "kind",
//         "version",
//         "updated_at",
//         "title",
//         "summary"
//     ],
//     "sortableAttributes": [
//         "updated_at"
//     ],
//     "filterableAttributes": [
//         "gid",
//         "language"
//     ],
//     "pagination": {
//         "maxTotalHits": 20
//     },
//     "faceting": {
//         "maxValuesPerFacet": 100
//     }
// }

pub struct MeiliSearch {
    icreation: Index,
    ipublication: Index,
    cli: Client,
}

pub enum Space {
    Group(xid::Id),
    Pub(Option<xid::Id>),
}

impl MeiliSearch {
    #[cfg(test)]
    pub async fn new(cfg: conf::Meili) -> anyhow::Result<Self> {
        let client = Client::new(cfg.url, Some(cfg.api_key));

        Ok(Self {
            icreation: client.index("creation"),
            ipublication: client.index("publication"),
            cli: client,
        })
    }

    #[cfg(not(test))]
    pub async fn new(cfg: conf::Meili) -> anyhow::Result<Self> {
        let client = Client::new(cfg.url, Some(cfg.api_key));
        let _ = client.get_stats().await?;

        Ok(Self {
            icreation: client.index("creation"),
            ipublication: client.index("publication"),
            cli: client,
        })
    }

    pub async fn search(
        &self,
        space: Space,
        lang: Option<Language>,
        q: &str,
        to: &PackObject<()>,
    ) -> anyhow::Result<SearchOutput> {
        let (mut sq, gid) = match space {
            Space::Group(gid) => (SearchQuery::new(&self.icreation), Some(gid)),
            Space::Pub(some_gid) => (SearchQuery::new(&self.ipublication), some_gid),
        };

        let limit = if q.is_empty() { 1000 } else { 20 };
        sq.with_query(q).with_limit(limit);
        let mut filters: Vec<&str> = Vec::new();
        let f = if let Some(gid) = gid {
            format!("gid = {}", gid)
        } else {
            "".to_string()
        };

        if !f.is_empty() {
            filters.push(&f);
        }

        let f = if let Some(lang) = lang {
            format!("language = {}", lang.to_639_3())
        } else {
            "".to_string()
        };
        if !f.is_empty() {
            filters.push(&f);
        }

        if !filters.is_empty() {
            sq.with_array_filter(filters);
        }

        let res = sq.with_facets(Selectors::All).execute::<Document>().await?;
        Ok(SearchOutput {
            hits: res
                .hits
                .into_iter()
                .map(|d| DocumentOutput::from(d.result, to))
                .collect(),
            languages: res
                .facet_distribution
                .map_or_else(|| None, |mut m| m.remove("language"))
                .unwrap_or_default(),
        })
    }

    #[cfg(test)]
    pub async fn add_or_update(&self, _space: Space, _docs: Vec<Document>) -> anyhow::Result<()> {
        Ok(())
    }

    #[cfg(test)]
    pub async fn delete(&self, _space: Space, _ids: Vec<String>) -> anyhow::Result<()> {
        Ok(())
    }

    #[cfg(not(test))]
    pub async fn add_or_update(&self, space: Space, docs: Vec<Document>) -> anyhow::Result<()> {
        match space {
            Space::Group(_) => self.icreation.add_or_update(&docs, Some("id")).await?,
            Space::Pub(_) => self.ipublication.add_or_update(&docs, Some("id")).await?,
        };

        Ok(())
    }

    #[cfg(not(test))]
    pub async fn delete(&self, space: Space, ids: Vec<String>) -> anyhow::Result<()> {
        match space {
            Space::Group(_) => self.icreation.delete_documents(&ids).await?,
            Space::Pub(_) => self.ipublication.delete_documents(&ids).await?,
        };
        Ok(())
    }
}
