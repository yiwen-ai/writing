use async_trait::async_trait;
use axum::{
    body::HttpBody,
    extract::{FromRequest, FromRequestParts},
    http::{
        header::{self, HeaderMap, HeaderValue},
        request::{Parts, Request},
        StatusCode,
    },
    response::{IntoResponse, Response},
    BoxError,
};
use base64::{engine::general_purpose, Engine as _};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{
    de::{self, DeserializeOwned},
    ser::Serializer,
    Deserialize, Serialize,
};
use std::{collections::HashSet, error::Error, fmt, ops::Deref, str::FromStr};

use crate::encoding::Encoding;
use crate::erring::HTTPError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypedObject<T> {
    Json(T),
    Cbor(T),
}

impl<S> TypedObject<S> {
    pub fn unwrap(self) -> S {
        match self {
            TypedObject::Json(v) => v,
            TypedObject::Cbor(v) => v,
        }
    }

    pub fn unwrap_type(self) -> (TypedObject<()>, S) {
        match self {
            TypedObject::Json(v) => (TypedObject::Json(()), v),
            TypedObject::Cbor(v) => (TypedObject::Cbor(()), v),
        }
    }

    pub fn unit(&self) -> TypedObject<()> {
        match self {
            TypedObject::Json(_) => TypedObject::Json(()),
            TypedObject::Cbor(_) => TypedObject::Cbor(()),
        }
    }

    pub fn with<T>(&self, v: T) -> TypedObject<T> {
        match self {
            TypedObject::Json(_) => TypedObject::Json(v),
            TypedObject::Cbor(_) => TypedObject::Cbor(v),
        }
    }

    pub fn with_vec<T>(&self, vv: Vec<T>) -> Vec<TypedObject<T>> {
        match self {
            TypedObject::Json(_) => vv.into_iter().map(TypedObject::Json).collect(),
            TypedObject::Cbor(_) => vv.into_iter().map(TypedObject::Cbor).collect(),
        }
    }

    pub fn with_set<T>(&self, vv: HashSet<T>) -> Vec<TypedObject<T>> {
        match self {
            TypedObject::Json(_) => vv.into_iter().map(TypedObject::Json).collect(),
            TypedObject::Cbor(_) => vv.into_iter().map(TypedObject::Cbor).collect(),
        }
    }
}

impl<T: Default> Default for TypedObject<T> {
    fn default() -> Self {
        TypedObject::Json(T::default())
    }
}

impl<T> Deref for TypedObject<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            TypedObject::Json(ref v) => v,
            TypedObject::Cbor(ref v) => v,
        }
    }
}

impl Serialize for TypedObject<()> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_none()
    }
}

impl Serialize for TypedObject<&[u8]> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            TypedObject::Json(v) => {
                serializer.serialize_str(general_purpose::URL_SAFE_NO_PAD.encode(v).as_str())
            }
            TypedObject::Cbor(v) => serializer.serialize_bytes(v),
        }
    }
}

impl Serialize for TypedObject<Vec<u8>> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            TypedObject::Json(v) => {
                serializer.serialize_str(general_purpose::URL_SAFE_NO_PAD.encode(v).as_str())
            }
            TypedObject::Cbor(v) => serializer.serialize_bytes(v),
        }
    }
}

impl Serialize for TypedObject<xid::Id> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            TypedObject::Json(v) => serializer.serialize_str(v.to_string().as_str()),
            TypedObject::Cbor(v) => serializer.serialize_bytes(v.as_bytes()),
        }
    }
}

impl Serialize for TypedObject<isolang::Language> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            TypedObject::Json(v) => serializer.serialize_str(v.to_name()),
            TypedObject::Cbor(v) => serializer.serialize_str(v.to_639_3()),
        }
    }
}

struct TypedObjectBytesVisitor;

impl<'de> de::Visitor<'de> for TypedObjectBytesVisitor {
    type Value = TypedObject<Vec<u8>>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a byte array or a no pad base64url string")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(TypedObject::Cbor(v.to_vec()))
    }

    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(TypedObject::Cbor(v.to_vec()))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(TypedObject::Cbor(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let v = general_purpose::URL_SAFE_NO_PAD
            .decode(v)
            .map_err(de::Error::custom)?;
        Ok(TypedObject::Json(v))
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let v = general_purpose::URL_SAFE_NO_PAD
            .decode(v)
            .map_err(de::Error::custom)?;
        Ok(TypedObject::Json(v))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let v = general_purpose::URL_SAFE_NO_PAD
            .decode(v)
            .map_err(de::Error::custom)?;
        Ok(TypedObject::Json(v))
    }
}

impl<'de> Deserialize<'de> for TypedObject<Vec<u8>> {
    fn deserialize<D>(deserializer: D) -> Result<TypedObject<Vec<u8>>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_any(TypedObjectBytesVisitor)
    }
}

struct TypedObjectLanguageVisitor;

impl<'de> de::Visitor<'de> for TypedObjectLanguageVisitor {
    type Value = TypedObject<isolang::Language>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a ISO 639-1, ISO 639-3 or English language name string")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let id = isolang::Language::from_str(v).map_err(de::Error::custom)?;
        match v.len() {
            3 => Ok(TypedObject::Cbor(id)),
            _ => Ok(TypedObject::Json(id)),
        }
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let id = isolang::Language::from_str(v).map_err(de::Error::custom)?;
        match v.len() {
            3 => Ok(TypedObject::Cbor(id)),
            _ => Ok(TypedObject::Json(id)),
        }
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let id = isolang::Language::from_str(&v).map_err(de::Error::custom)?;
        match v.len() {
            3 => Ok(TypedObject::Cbor(id)),
            _ => Ok(TypedObject::Json(id)),
        }
    }
}

impl<'de> Deserialize<'de> for TypedObject<isolang::Language> {
    fn deserialize<D>(deserializer: D) -> Result<TypedObject<isolang::Language>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_str(TypedObjectLanguageVisitor)
    }
}

struct TypedObjectXidVisitor;

impl<'de> de::Visitor<'de> for TypedObjectXidVisitor {
    type Value = TypedObject<xid::Id>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a 12 bytes array or a 20 length xid string")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if v.len() != 12 {
            Err(de::Error::custom(format!(
                "expected value length 12, got {:?}",
                v.len()
            )))
        } else {
            let mut bytes = [0u8; 12];
            bytes.copy_from_slice(v);
            Ok(TypedObject::Cbor(xid::Id(bytes)))
        }
    }

    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if v.len() != 12 {
            Err(de::Error::custom(format!(
                "expected value length 12, got {:?}",
                v.len()
            )))
        } else {
            let mut bytes = [0u8; 12];
            bytes.copy_from_slice(v);
            Ok(TypedObject::Cbor(xid::Id(bytes)))
        }
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if v.len() != 12 {
            Err(de::Error::custom(format!(
                "expected value length 12, got {:?}",
                v.len()
            )))
        } else {
            let mut bytes = [0u8; 12];
            bytes.copy_from_slice(&v);
            Ok(TypedObject::Cbor(xid::Id(bytes)))
        }
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let id = xid::Id::from_str(v).map_err(de::Error::custom)?;
        Ok(TypedObject::Json(id))
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let id = xid::Id::from_str(v).map_err(de::Error::custom)?;
        Ok(TypedObject::Json(id))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let id = xid::Id::from_str(&v).map_err(de::Error::custom)?;
        Ok(TypedObject::Json(id))
    }
}

impl<'de> Deserialize<'de> for TypedObject<xid::Id> {
    fn deserialize<D>(deserializer: D) -> Result<TypedObject<xid::Id>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_any(TypedObjectXidVisitor)
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for TypedObject<()>
where
    S: Send + Sync,
{
    type Rejection = HTTPError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        match get_content_type(&parts.headers) {
            Ok(ot) => Ok(ot),
            Err(mut ct) => {
                if let Some(accept) = parts.headers.get(header::ACCEPT) {
                    if let Ok(accept) = accept.to_str() {
                        if accept.contains("application/cbor") {
                            return Ok(TypedObject::Cbor(()));
                        }
                        if accept.contains("application/json") {
                            return Ok(TypedObject::Json(()));
                        }
                        ct = accept.to_string();
                    }
                }

                Err(HTTPError::new(
                    StatusCode::UNSUPPORTED_MEDIA_TYPE.as_u16(),
                    format!("Unsupported media type, {}", ct),
                ))
            }
        }
    }
}

#[async_trait]
impl<T, S, B> FromRequest<S, B> for TypedObject<T>
where
    T: DeserializeOwned + Send + Sync,
    B: HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
    S: Send + Sync,
{
    type Rejection = HTTPError;

    async fn from_request(req: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        let headers = req.headers();
        let ct = get_content_type(headers).map_err(|ct| {
            HTTPError::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE.as_u16(),
                format!("Unsupported media type, {}", ct),
            )
        })?;

        let enc = Encoding::from_header_value(headers.get(header::CONTENT_ENCODING));
        let mut bytes = Bytes::from_request(req, state).await.map_err(|err| {
            HTTPError::new(
                StatusCode::BAD_REQUEST.as_u16(),
                format!("Invalid body, {}", err),
            )
        })?;

        if !enc.identity() {
            bytes = enc
                .decode_all(bytes.reader())
                .map_err(|err| {
                    HTTPError::new(
                        StatusCode::BAD_REQUEST.as_u16(),
                        format!("Invalid body, {}", err),
                    )
                })?
                .into();
        }

        match ct {
            TypedObject::Json(_) => {
                let value: T = serde_json::from_slice(&bytes).map_err(|err| HTTPError {
                    code: StatusCode::BAD_REQUEST.as_u16(),
                    message: format!("Invalid JSON body, {}", err),
                    data: None,
                })?;
                Ok(TypedObject::Json(value))
            }
            TypedObject::Cbor(_) => {
                let value: T = ciborium::from_reader(&bytes[..]).map_err(|err| HTTPError {
                    code: StatusCode::BAD_REQUEST.as_u16(),
                    message: format!("Invalid CBOR body, {}", err),
                    data: None,
                })?;
                Ok(TypedObject::Cbor(value))
            }
        }
    }
}

fn get_content_type(headers: &HeaderMap) -> Result<TypedObject<()>, String> {
    let content_type = if let Some(content_type) = headers.get(header::CONTENT_TYPE) {
        content_type
    } else {
        return Err("".to_string());
    };

    let content_type = if let Ok(content_type) = content_type.to_str() {
        content_type
    } else {
        return Err("".to_string());
    };

    if let Ok(mime) = content_type.parse::<mime::Mime>() {
        if mime.type_() == "application" {
            if mime.subtype() == "cbor" || mime.suffix().map_or(false, |name| name == "cbor") {
                return Ok(TypedObject::Cbor(()));
            } else if mime.subtype() == "json" || mime.suffix().map_or(false, |name| name == "json")
            {
                return Ok(TypedObject::Json(()));
            }
        }
    }

    Err(content_type.to_string())
}

impl<T> IntoResponse for TypedObject<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        // Use a small initial capacity of 128 bytes like serde_json::to_vec
        // https://docs.rs/serde_json/1.0.82/src/serde_json/ser.rs.html#2189
        let mut buf = BytesMut::with_capacity(128).writer();
        let res: Result<Response, Box<dyn Error>> = match self {
            TypedObject::Json(v) => match serde_json::to_writer(&mut buf, &v) {
                Ok(()) => Ok((
                    [(
                        header::CONTENT_TYPE,
                        HeaderValue::from_static(mime::APPLICATION_JSON.as_ref()),
                    )],
                    buf.into_inner().freeze(),
                )
                    .into_response()),
                Err(err) => Err(Box::new(err)),
            },
            TypedObject::Cbor(v) => match ciborium::into_writer(&v, &mut buf) {
                Ok(()) => Ok((
                    [(
                        header::CONTENT_TYPE,
                        HeaderValue::from_static("application/cbor"),
                    )],
                    buf.into_inner().freeze(),
                )
                    .into_response()),
                Err(err) => Err(Box::new(err)),
            },
        };

        match res {
            Ok(res) => res,
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static(mime::TEXT_PLAIN_UTF_8.as_ref()),
                )],
                err.to_string(),
            )
                .into_response(),
        }
    }
}
