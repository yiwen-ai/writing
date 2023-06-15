use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use scylla::transport::query_result::SingleRowError;
use serde::Serialize;
use std::{convert::From, error::Error, fmt, fmt::Debug};
use validator::{ValidationError, ValidationErrors};

/// ErrorResponse is the response body for error.
#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: HTTPError,
}

/// SuccessResponse is the response body for success.
#[derive(Serialize)]
pub struct SuccessResponse<S: Serialize> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
    pub result: S,
}

impl<S: Serialize> SuccessResponse<S> {
    pub fn new(result: S) -> Self {
        SuccessResponse {
            total_size: None,
            next_page_token: None,
            result,
        }
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct HTTPError {
    pub code: u16,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl HTTPError {
    pub fn new(code: u16, message: String) -> Self {
        HTTPError {
            code,
            message,
            data: None,
        }
    }
}

impl fmt::Display for HTTPError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string(self).unwrap_or(self.message.clone())
        )
    }
}

impl Error for HTTPError {}

impl IntoResponse for HTTPError {
    fn into_response(self) -> Response {
        let status = if self.code < 400 {
            StatusCode::INTERNAL_SERVER_ERROR
        } else {
            StatusCode::from_u16(self.code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
        };

        let body = Json(ErrorResponse { error: self });
        (status, body).into_response()
    }
}

impl From<anyhow::Error> for HTTPError {
    fn from(err: anyhow::Error) -> Self {
        match err.downcast::<Self>() {
            Ok(err) => err,
            Err(sel) => match sel.downcast::<SingleRowError>() {
                Ok(err) => HTTPError::new(404, format!("{:?}", err)),
                Err(sel) => HTTPError::new(500, format!("{:?}", sel)),
            },
        }
    }
}

impl From<ValidationError> for HTTPError {
    fn from(err: ValidationError) -> Self {
        HTTPError::new(400, format!("{:?}", err))
    }
}

impl From<ValidationErrors> for HTTPError {
    fn from(err: ValidationErrors) -> Self {
        HTTPError::new(400, format!("{:?}", err))
    }
}
