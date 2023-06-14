use isolang::Language;
use std::collections::HashSet;

use scylla_orm_macros::CqlOrm;

#[derive(Debug, Default, Clone, CqlOrm)]
pub struct Creation {
    #[cql(blob)]
    pub id: xid::Id,
    pub status: i8,
    #[cql(ascii)]
    pub language: Language,
    #[cql(set<ascii>, rename = "active_languages")]
    pub active_langs: HashSet<Language>,
    pub authors: Vec<String>,
    #[cql(list<blob>)]
    pub reviewers: Vec<xid::Id>,
    pub content: Vec<u8>,
}

fn main() {
    println!("TODO!");
}
