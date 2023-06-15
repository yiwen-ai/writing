use isolang::Language;
use std::collections::HashSet;

use scylla_orm_macros::CqlOrm;

#[derive(Debug, Default, Clone, CqlOrm)]
pub struct Creation {
    pub id: xid::Id,
    pub status: i8,
    pub language: Language,
    pub _langs: HashSet<Language>,
    pub authors: Vec<String>,
    pub reviewers: Vec<xid::Id>,
    pub content: Vec<u8>,
}

fn main() {
    println!("TODO!");
}
