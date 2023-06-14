use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

pub use scylla::frame::response::result::CqlValue;

pub trait CqlValueSerder: Sized {
    fn from_cql(val: &CqlValue) -> anyhow::Result<Self>;
    fn to_cql(&self) -> anyhow::Result<CqlValue>;
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash, Default)]
pub struct Ascii(pub String);

impl CqlValueSerder for Ascii {
    fn from_cql(val: &CqlValue) -> anyhow::Result<Self> {
        match val {
            CqlValue::Ascii(val) => Ok(Ascii(val.to_owned())),
            _ => Err(anyhow::Error::msg(format!(
                "CqlValueSerder::from_cql for Ascii: expected Ascii value, got {:?}",
                val
            ))),
        }
    }

    fn to_cql(&self) -> anyhow::Result<CqlValue> {
        if self.0.is_ascii() {
            Ok(CqlValue::Ascii(self.0.to_owned()))
        } else {
            Err(anyhow::Error::msg(format!(
                "CqlValueSerder::to_cql for Ascii: expected ascii string, got {:?}",
                self.0
            )))
        }
    }
}

impl CqlValueSerder for String {
    fn from_cql(val: &CqlValue) -> anyhow::Result<Self> {
        match val {
            CqlValue::Text(val) => Ok(val.to_owned()),
            _ => Err(anyhow::Error::msg(format!(
                "CqlValueSerder::from_cql for String: expected Text value, got {:?}",
                val
            ))),
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
            _ => Err(anyhow::Error::msg(format!(
                "CqlValueSerder::from_cql for i8: expected TinyInt value, got {:?}",
                val
            ))),
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
            _ => Err(anyhow::Error::msg(format!(
                "CqlValueSerder::from_cql for i16: expected SmallInt value, got {:?}",
                val
            ))),
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
            _ => Err(anyhow::Error::msg(format!(
                "CqlValueSerder::from_cql for i32: expected Int value, got {:?}",
                val
            ))),
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
            _ => Err(anyhow::Error::msg(format!(
                "CqlValueSerder::from_cql for i64: expected BigInt value, got {:?}",
                val
            ))),
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
            _ => Err(anyhow::Error::msg(format!(
                "CqlValueSerder::from_cql for i64: expected Float value, got {:?}",
                val
            ))),
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
                    Err(anyhow::Error::msg(format!(
                        "CqlValueSerder::from_cql for xid::Id: expected value length 12, got {:?}",
                        val.len()
                    )))
                } else {
                    let mut bytes = [0u8; 12];
                    bytes.copy_from_slice(val);
                    Ok(xid::Id(bytes))
                }
            }
            _ => Err(anyhow::Error::msg(format!(
                "CqlValueSerder::from_cql for xid::Id: expected Blob value, got {:?}",
                val
            ))),
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
                Err(err) => Err(anyhow::Error::msg(format!(
                    "CqlValueSerder::from_cql for isolang::Language: parse {:?} error: {:?}",
                    val, err,
                ))),
            },
            _ => Err(anyhow::Error::msg(format!(
                "CqlValueSerder::from_cql for isolang::Language: expected Ascii value, got {:?}",
                val
            ))),
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
            _ => Err(anyhow::Error::msg(format!(
                "CqlValueSerder::from_cql for Vec<u8>: expected Blob value, got {:?}",
                val
            ))),
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
            _ => Err(anyhow::Error::msg(format!(
                "CqlValueSerder::from_cql for Vec<T>: expected List value, got {:?}",
                val
            ))),
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
            _ => Err(anyhow::Error::msg(format!(
                "CqlValueSerder::from_cql for Vec<T>: expected Set value, got {:?}",
                val
            ))),
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
            _ => Err(anyhow::Error::msg(format!(
                "CqlValueSerder::from_cql for Vec<T>: expected Set value, got {:?}",
                val
            ))),
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
