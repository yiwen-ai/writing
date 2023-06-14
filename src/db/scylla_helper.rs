use scylla::{frame::response::result::CqlValue, frame::response::result::Row};
use serde::{de::DeserializeOwned, Serialize};

use std::{
    collections::{hash_map::Iter, HashMap, HashSet},
    str::FromStr,
};

use crate::erring::HTTPError;

pub trait CqlValueSerder: Sized {
    fn from_cql(val: &CqlValue) -> anyhow::Result<Self>;
    fn to_cql(&self) -> anyhow::Result<CqlValue>;
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash, Default)]
pub struct Ascii(pub String);

#[derive(Debug, Default, PartialEq)]
pub struct ColumnsMap(HashMap<String, CqlValue>);

impl CqlValueSerder for Ascii {
    fn from_cql(val: &CqlValue) -> anyhow::Result<Self> {
        match val {
            CqlValue::Ascii(val) => Ok(Ascii(val.to_owned())),
            _ => Err(anyhow::Error::new(HTTPError {
                code: 500,
                message: format!(
                    "CqlValueSerder::from_cql for Ascii: expected Ascii value, got {:?}",
                    val
                ),
                data: None,
            })),
        }
    }

    fn to_cql(&self) -> anyhow::Result<CqlValue> {
        if self.0.is_ascii() {
            Ok(CqlValue::Ascii(self.0.to_owned()))
        } else {
            Err(anyhow::Error::new(HTTPError {
                code: 500,
                message: format!(
                    "CqlValueSerder::to_cql for Ascii: expected ascii string, got {:?}",
                    self.0
                ),
                data: None,
            }))
        }
    }
}

impl CqlValueSerder for String {
    fn from_cql(val: &CqlValue) -> anyhow::Result<Self> {
        match val {
            CqlValue::Text(val) => Ok(val.to_owned()),
            _ => Err(anyhow::Error::new(HTTPError {
                code: 500,
                message: format!(
                    "CqlValueSerder::from_cql for String: expected Text value, got {:?}",
                    val
                ),
                data: None,
            })),
        }
    }

    fn to_cql(&self) -> anyhow::Result<CqlValue> {
        Ok(CqlValue::Text(self.to_string()))
    }
}

impl CqlValueSerder for i8 {
    fn from_cql(val: &CqlValue) -> anyhow::Result<Self> {
        match val {
            CqlValue::TinyInt(val) => Ok(val.to_owned()),
            _ => Err(anyhow::Error::new(HTTPError {
                code: 500,
                message: format!(
                    "CqlValueSerder::from_cql for i8: expected TinyInt value, got {:?}",
                    val
                ),
                data: None,
            })),
        }
    }

    fn to_cql(&self) -> anyhow::Result<CqlValue> {
        Ok(CqlValue::TinyInt(self.to_owned()))
    }
}

impl CqlValueSerder for i16 {
    fn from_cql(val: &CqlValue) -> anyhow::Result<Self> {
        match val {
            CqlValue::SmallInt(val) => Ok(val.to_owned()),
            _ => Err(anyhow::Error::new(HTTPError {
                code: 500,
                message: format!(
                    "CqlValueSerder::from_cql for i16: expected SmallInt value, got {:?}",
                    val
                ),
                data: None,
            })),
        }
    }

    fn to_cql(&self) -> anyhow::Result<CqlValue> {
        Ok(CqlValue::SmallInt(self.to_owned()))
    }
}

impl CqlValueSerder for i32 {
    fn from_cql(val: &CqlValue) -> anyhow::Result<Self> {
        match val {
            CqlValue::Int(val) => Ok(val.to_owned()),
            _ => Err(anyhow::Error::new(HTTPError {
                code: 500,
                message: format!(
                    "CqlValueSerder::from_cql for i32: expected Int value, got {:?}",
                    val
                ),
                data: None,
            })),
        }
    }

    fn to_cql(&self) -> anyhow::Result<CqlValue> {
        Ok(CqlValue::Int(self.to_owned()))
    }
}

impl CqlValueSerder for i64 {
    fn from_cql(val: &CqlValue) -> anyhow::Result<Self> {
        match val {
            CqlValue::BigInt(val) => Ok(val.to_owned()),
            _ => Err(anyhow::Error::new(HTTPError {
                code: 500,
                message: format!(
                    "CqlValueSerder::from_cql for i64: expected BigInt value, got {:?}",
                    val
                ),
                data: None,
            })),
        }
    }

    fn to_cql(&self) -> anyhow::Result<CqlValue> {
        Ok(CqlValue::BigInt(self.to_owned()))
    }
}

impl CqlValueSerder for f32 {
    fn from_cql(val: &CqlValue) -> anyhow::Result<Self> {
        match val {
            CqlValue::Float(val) => Ok(val.to_owned()),
            _ => Err(anyhow::Error::new(HTTPError {
                code: 500,
                message: format!(
                    "CqlValueSerder::from_cql for i64: expected Float value, got {:?}",
                    val
                ),
                data: None,
            })),
        }
    }

    fn to_cql(&self) -> anyhow::Result<CqlValue> {
        Ok(CqlValue::Float(self.to_owned()))
    }
}

impl CqlValueSerder for xid::Id {
    fn from_cql(val: &CqlValue) -> anyhow::Result<Self> {
        match val {
            CqlValue::Blob(val) => {
                if val.len() != 12 {
                    Err(anyhow::Error::new(HTTPError {
                        code: 500,
                        message: format!(
                            "CqlValueSerder::from_cql for xid::Id: expected value length 12, got {:?}",
                            val.len()
                        ),
                        data: None,
                    }))
                } else {
                    let mut bytes = [0u8; 12];
                    bytes.copy_from_slice(val);
                    Ok(xid::Id(bytes))
                }
            }
            _ => Err(anyhow::Error::new(HTTPError {
                code: 500,
                message: format!(
                    "CqlValueSerder::from_cql for xid::Id: expected Blob value, got {:?}",
                    val
                ),
                data: None,
            })),
        }
    }

    fn to_cql(&self) -> anyhow::Result<CqlValue> {
        Ok(CqlValue::Blob(self.as_bytes().to_vec()))
    }
}

impl CqlValueSerder for isolang::Language {
    fn from_cql(val: &CqlValue) -> anyhow::Result<Self> {
        match val {
            CqlValue::Ascii(val) => match isolang::Language::from_str(val) {
                Ok(lang) => Ok(lang),
                Err(err) => Err(anyhow::Error::new(HTTPError {
                    code: 500,
                    message: format!(
                        "CqlValueSerder::from_cql for isolang::Language: parse {:?} error: {:?}",
                        val, err,
                    ),
                    data: None,
                })),
            },
            _ => Err(anyhow::Error::new(HTTPError {
                code: 500,
                message: format!(
                    "CqlValueSerder::from_cql for isolang::Language: expected Ascii value, got {:?}",
                    val
                ),
                data: None,
            })),
        }
    }

    fn to_cql(&self) -> anyhow::Result<CqlValue> {
        Ok(CqlValue::Ascii(self.to_639_3().to_string()))
    }
}

impl CqlValueSerder for Vec<u8> {
    fn from_cql(val: &CqlValue) -> anyhow::Result<Self> {
        match val {
            CqlValue::Blob(val) => Ok(val.to_owned()),
            _ => Err(anyhow::Error::new(HTTPError {
                code: 500,
                message: format!(
                    "CqlValueSerder::from_cql for Vec<u8>: expected Blob value, got {:?}",
                    val
                ),
                data: None,
            })),
        }
    }

    fn to_cql(&self) -> anyhow::Result<CqlValue> {
        Ok(CqlValue::Blob(self.to_owned()))
    }
}

impl CqlValueSerder for CqlValue {
    fn from_cql(val: &CqlValue) -> anyhow::Result<Self> {
        Ok(val.to_owned())
    }

    fn to_cql(&self) -> anyhow::Result<CqlValue> {
        Ok(self.to_owned())
    }
}

impl<T: CqlValueSerder> CqlValueSerder for Vec<T> {
    fn from_cql(val: &CqlValue) -> anyhow::Result<Self> {
        match val {
            CqlValue::List(list) => {
                let mut rt = Vec::with_capacity(list.len());
                for item in list {
                    rt.push(T::from_cql(item)?);
                }
                Ok(rt)
            }
            _ => Err(anyhow::Error::new(HTTPError {
                code: 500,
                message: format!(
                    "CqlValueSerder::from_cql for Vec<T>: expected List value, got {:?}",
                    val
                ),
                data: None,
            })),
        }
    }

    fn to_cql(&self) -> anyhow::Result<CqlValue> {
        let mut rt: Vec<CqlValue> = Vec::with_capacity(self.len());
        for item in self {
            rt.push(item.to_cql()?);
        }
        Ok(CqlValue::List(rt))
    }
}

impl<T: CqlValueSerder + std::cmp::Eq + std::hash::Hash> CqlValueSerder for HashSet<T> {
    fn from_cql(val: &CqlValue) -> anyhow::Result<Self> {
        match val {
            CqlValue::Set(list) => {
                let mut rt = HashSet::with_capacity(list.len());
                for item in list {
                    rt.insert(T::from_cql(item)?);
                }
                Ok(rt)
            }
            _ => Err(anyhow::Error::new(HTTPError {
                code: 500,
                message: format!(
                    "CqlValueSerder::from_cql for Vec<T>: expected Set value, got {:?}",
                    val
                ),
                data: None,
            })),
        }
    }

    fn to_cql(&self) -> anyhow::Result<CqlValue> {
        let mut rt: Vec<CqlValue> = Vec::with_capacity(self.len());
        for item in self {
            rt.push(item.to_cql()?);
        }
        Ok(CqlValue::Set(rt))
    }
}

impl<T: CqlValueSerder> CqlValueSerder for HashMap<String, T> {
    fn from_cql(val: &CqlValue) -> anyhow::Result<Self> {
        match val {
            CqlValue::Map(list) => {
                let mut rt = HashMap::with_capacity(list.len());
                for item in list {
                    rt.insert(CqlValueSerder::from_cql(&item.0)?, T::from_cql(&item.1)?);
                }
                Ok(rt)
            }
            _ => Err(anyhow::Error::new(HTTPError {
                code: 500,
                message: format!(
                    "CqlValueSerder::from_cql for Vec<T>: expected Set value, got {:?}",
                    val
                ),
                data: None,
            })),
        }
    }

    fn to_cql(&self) -> anyhow::Result<CqlValue> {
        let mut rt: Vec<(CqlValue, CqlValue)> = Vec::with_capacity(self.len());
        for item in self {
            rt.push((item.0.to_cql()?, item.1.to_cql()?));
        }
        Ok(CqlValue::Map(rt))
    }
}

impl ColumnsMap {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(HashMap::with_capacity(capacity))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn has(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    pub fn get(&self, key: &str) -> Option<&CqlValue> {
        match self.0.get(key) {
            Some(v) => Some(v),
            None => None,
        }
    }

    pub fn iter(&self) -> Iter<'_, String, CqlValue> {
        self.0.iter()
    }

    pub fn get_as<T: CqlValueSerder>(&self, key: &str) -> anyhow::Result<T> {
        match self.0.get(key) {
            Some(v) => T::from_cql(v),
            None => Err(anyhow::Error::new(HTTPError {
                code: 500,
                message: format!("ColumnsMap::get_as: value for {:?} is null", key),
                data: None,
            })),
        }
    }

    pub fn set_as<T: CqlValueSerder>(&mut self, key: &str, val: &T) -> anyhow::Result<()> {
        self.0.insert(key.to_string(), val.to_cql()?);
        Ok(())
    }

    pub fn get_from_cbor<T: DeserializeOwned>(&self, key: &str) -> anyhow::Result<T> {
        let data = self.get_as::<Vec<u8>>(key)?;
        let val: T = ciborium::from_reader(&data[..])?;
        Ok(val)
    }

    pub fn set_in_cbor<T: ?Sized + Serialize>(&mut self, key: &str, val: &T) -> anyhow::Result<()> {
        let mut buf: Vec<u8> = Vec::new();
        ciborium::into_writer(val, &mut buf)?;
        self.0.insert(key.to_string(), CqlValue::Blob(buf));
        Ok(())
    }

    pub fn append_map<T: CqlValueSerder>(
        &mut self,
        map_name: &str,
        key: &str,
        val: T,
    ) -> anyhow::Result<()> {
        let mut map: HashMap<String, CqlValue> = self.get_as(map_name).unwrap_or_default();

        map.insert(key.to_string(), val.to_cql()?);
        self.0.insert(map_name.to_string(), map.to_cql()?);
        Ok(())
    }

    pub fn fill(&mut self, row: Row, fields: Vec<&str>) -> anyhow::Result<()> {
        if row.columns.len() != fields.len() {
            return Err(anyhow::Error::new(HTTPError {
                code: 500,
                message: format!(
                    "ColumnsMap::fill: row.columns.len({}) != fields.len({})",
                    row.columns.len(),
                    fields.len()
                ),
                data: None,
            }));
        }
        for (i, val) in row.columns.iter().enumerate() {
            if let Some(v) = val {
                self.0.insert(fields[i].to_owned(), v.to_owned());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn columns_map_works() {
        let mut map = ColumnsMap::new();

        assert_eq!(map.len(), 0);
        assert!(!map.has("user"));
        assert_eq!(map.get("user"), None);
        assert_eq!(map.get_as::<String>("user").is_err(), true);

        map.set_as("user", &Ascii("jarvis".to_string())).unwrap();
        assert_eq!(map.len(), 1);
        assert!(map.has("user"));
        assert_eq!(
            map.get("user"),
            Some(&CqlValue::Ascii("jarvis".to_string()))
        );
        assert_eq!(
            map.get_as::<Ascii>("user").unwrap(),
            Ascii("jarvis".to_string())
        );

        map.set_as("user", &Ascii("jarvis2".to_string())).unwrap();
        assert_eq!(map.len(), 1);
        assert!(map.has("user"));
        assert_eq!(
            map.get("user"),
            Some(&CqlValue::Ascii("jarvis2".to_string()))
        );
        assert_eq!(
            map.get_as::<Ascii>("user").unwrap(),
            Ascii("jarvis2".to_string())
        );

        assert!(!map.has("embeddings"));
        assert_eq!(map.get("embeddings"), None);
        map.set_as("embeddings", &vec![0.1f32, 0.2f32]).unwrap();
        assert!(map.has("embeddings"));
        assert_eq!(map.len(), 2);
        assert_eq!(
            map.get_as::<Vec<f32>>("embeddings").unwrap(),
            vec![0.1f32, 0.2f32],
        );

        assert!(!map.has("tokens"));
        assert_eq!(map.get("tokens"), None);
        map.append_map("tokens", "ada2", 999i32).unwrap();
        assert!(map.has("tokens"));
        assert_eq!(map.len(), 3);
        assert_eq!(
            map.get_as::<HashMap<String, i32>>("tokens").unwrap(),
            HashMap::from([("ada2".to_string(), 999i32)]),
        );

        map.append_map("tokens", "gpt4", 1999i32).unwrap();
        assert_eq!(map.len(), 3);
        assert_eq!(
            map.get_as::<HashMap<String, i32>>("tokens").unwrap(),
            HashMap::from([("ada2".to_string(), 999i32), ("gpt4".to_string(), 1999i32)]),
        );

        map.append_map("tokens", "ada2", 1999i32).unwrap();
        assert_eq!(map.len(), 3);
        assert_eq!(
            map.get_as::<HashMap<String, i32>>("tokens").unwrap(),
            HashMap::from([("ada2".to_string(), 1999i32), ("gpt4".to_string(), 1999i32)]),
        );

        assert!(!map.has("cbor"));
        assert_eq!(map.get("cbor"), None);
        assert_eq!(map.get_as::<Vec<u8>>("cbor").is_err(), true);
        assert!(map.set_in_cbor("cbor", &vec![1i64, 2i64, 3i64]).is_ok()); // CBOR: 0x83010203
        assert!(map.has("cbor"));
        assert_eq!(map.len(), 4);
        assert_eq!(
            map.get_as::<Vec<u8>>("cbor").unwrap(),
            vec![0x83, 0x01, 0x02, 0x03],
        );
        assert_eq!(map.get_as::<String>("cbor").is_err(), true);

        let mut row: Row = Row {
            columns: Vec::new(),
        };

        let mut fields: Vec<&str> = Vec::new();
        for (k, v) in map.iter() {
            fields.push(k);
            row.columns.push(Some(v.to_owned()));
        }

        assert_eq!(fields.len(), 4);
        let mut map2 = ColumnsMap::new();
        assert!(map2
            .fill(
                Row {
                    columns: Vec::new(),
                },
                fields.clone()
            )
            .is_err());
        assert_ne!(map2, map);

        assert!(map2.fill(row, fields).is_ok());
        assert_eq!(map2, map);
    }
}
