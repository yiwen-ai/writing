use serde::{de, Deserialize, Serialize};
use std::{collections::BTreeMap, fmt};
use validator::ValidationError;

use axum_web::object::{cbor_from_slice, PackObject};

pub const MAX_CONTENT_LEN: usize = 1024 * 1024;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct DocumentNode {
    #[serde(rename = "type")]
    pub itype: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attrs: Option<BTreeMap<String, AttrValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub marks: Option<Vec<PartialNode>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<Vec<DocumentNode>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct PartialNode {
    #[serde(rename = "type")]
    pub itype: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attrs: Option<BTreeMap<String, AttrValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum AttrValue {
    Integer(i64),
    Float(f64),
    Bool(bool),
    Text(String),
    Null,
}

pub fn validate_cbor_content(content: &PackObject<Vec<u8>>) -> Result<(), ValidationError> {
    if content.len() > MAX_CONTENT_LEN {
        return Err(ValidationError::new("content length is too long"));
    }

    let _: DocumentNode = cbor_from_slice(content.unwrap_ref())
        .map_err(|_| ValidationError::new("content is not a valid cbor"))?;
    Ok(())
}

impl Serialize for AttrValue {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ::serde::Serializer,
    {
        match self {
            AttrValue::Null => serializer.serialize_unit(),
            AttrValue::Bool(b) => serializer.serialize_bool(*b),
            AttrValue::Integer(n) => n.serialize(serializer),
            AttrValue::Float(n) => n.serialize(serializer),
            AttrValue::Text(s) => serializer.serialize_str(s),
        }
    }
}

impl<'de> Deserialize<'de> for AttrValue {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<AttrValue, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> de::Visitor<'de> for ValueVisitor {
            type Value = AttrValue;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("any valid JSON value")
            }

            #[inline]
            fn visit_bool<E>(self, value: bool) -> Result<AttrValue, E> {
                Ok(AttrValue::Bool(value))
            }

            #[inline]
            fn visit_i64<E>(self, value: i64) -> Result<AttrValue, E> {
                Ok(AttrValue::Integer(value))
            }

            #[inline]
            fn visit_u64<E>(self, value: u64) -> Result<AttrValue, E>
            where
                E: de::Error,
            {
                if value < u64::MAX {
                    Ok(AttrValue::Integer(value as i64))
                } else {
                    Err(de::Error::custom("integer overflow"))
                }
            }

            #[inline]
            fn visit_f64<E>(self, value: f64) -> Result<AttrValue, E> {
                Ok(AttrValue::Float(value))
            }

            #[inline]
            fn visit_str<E>(self, value: &str) -> Result<AttrValue, E>
            where
                E: serde::de::Error,
            {
                self.visit_string(String::from(value))
            }

            #[inline]
            fn visit_string<E>(self, value: String) -> Result<AttrValue, E> {
                Ok(AttrValue::Text(value))
            }

            #[inline]
            fn visit_none<E>(self) -> Result<AttrValue, E> {
                Ok(AttrValue::Null)
            }

            #[inline]
            fn visit_some<D>(self, deserializer: D) -> Result<AttrValue, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                Deserialize::deserialize(deserializer)
            }

            #[inline]
            fn visit_unit<E>(self) -> Result<AttrValue, E> {
                Ok(AttrValue::Null)
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum_web::object::cbor_to_vec;

    #[test]
    fn validate_cbor_content_works() {
        let json_data = std::fs::read("sample/content.json").unwrap();
        let json_content: DocumentNode = serde_json::from_slice(&json_data).unwrap();

        let cbor_data = cbor_to_vec(&json_content).unwrap();
        let cbor_content: DocumentNode = cbor_from_slice(&cbor_data).unwrap();
        assert_eq!(json_content, cbor_content);

        println!(
            "json len: {}, cbor len: {}",
            json_data.len(),
            cbor_data.len()
        );

        let json_value1: serde_json::Value = serde_json::from_slice(&json_data).unwrap();
        let json_value2: serde_json::Value =
            serde_json::from_slice(&serde_json::to_vec(&cbor_content).unwrap()).unwrap();
        assert_eq!(json_value1, json_value2);

        let cbor_content2: DocumentNode =
            cbor_from_slice(&cbor_to_vec(&cbor_content).unwrap()).unwrap();
        assert_eq!(cbor_content, cbor_content2);

        validate_cbor_content(&PackObject::Cbor(cbor_data)).unwrap();
    }
}
