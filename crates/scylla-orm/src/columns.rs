use scylla::{frame::response::result::CqlValue, frame::response::result::Row};

use std::collections::{hash_map::Iter, HashMap};

use crate::CqlValueSerder;

#[derive(Debug, Default, PartialEq)]
pub struct ColumnsMap(HashMap<String, CqlValue>);

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
            None => Err(anyhow::Error::msg(format!(
                "ColumnsMap::get_as: value for {:?} is null",
                key
            ))),
        }
    }

    pub fn set_as<T: CqlValueSerder>(&mut self, key: &str, val: &T) -> anyhow::Result<()> {
        self.0.insert(key.to_string(), val.to_cql()?);
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
            return Err(anyhow::Error::msg(format!(
                "ColumnsMap::fill: row.columns.len({}) != fields.len({})",
                row.columns.len(),
                fields.len()
            )));
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
    use crate::Ascii;
    use serde::{de::DeserializeOwned, Serialize};

    impl ColumnsMap {
        pub fn get_from_cbor<T: DeserializeOwned>(&self, key: &str) -> anyhow::Result<T> {
            let data = self.get_as::<Vec<u8>>(key)?;
            let val: T = ciborium::from_reader(&data[..])?;
            Ok(val)
        }

        pub fn set_in_cbor<T: ?Sized + Serialize>(
            &mut self,
            key: &str,
            val: &T,
        ) -> anyhow::Result<()> {
            let mut buf: Vec<u8> = Vec::new();
            ciborium::into_writer(val, &mut buf)?;
            self.0.insert(key.to_string(), CqlValue::Blob(buf));
            Ok(())
        }
    }

    #[test]
    fn columns_map_works() {
        let mut map = ColumnsMap::new();

        assert_eq!(map.len(), 0);
        assert!(!map.has("user"));
        assert_eq!(map.get("user"), None);
        assert!(map.get_as::<String>("user").is_err());

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
        assert!(map.get_as::<Vec<u8>>("cbor").is_err());
        assert!(map.set_in_cbor("cbor", &vec![1i64, 2i64, 3i64]).is_ok()); // CBOR: 0x83010203
        assert!(map.has("cbor"));
        assert_eq!(map.len(), 4);
        assert_eq!(
            map.get_as::<Vec<u8>>("cbor").unwrap(),
            vec![0x83, 0x01, 0x02, 0x03],
        );
        assert!(map.get_as::<String>("cbor").is_err());

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
