use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use std::{error::Error, fmt, fmt::Debug};

/// ErrorResponse is the response body for error.
#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: HTTPError,
}

/// SuccessResponse is the response body for success.
#[derive(Serialize)]
pub struct SuccessResponse<S: Serialize> {
    pub result: S,
}

#[derive(Serialize, Debug, Clone)]
pub struct HTTPError {
    pub code: u16,
    pub message: String,
    pub data: Option<serde_json::Value>,
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

impl HTTPError {
    pub fn from(err: anyhow::Error) -> Self {
        match err.downcast::<HTTPError>() {
            Ok(err) => err,
            Err(err) => Self {
                code: 500,
                message: err.to_string(),
                data: None,
            },
        }
    }
}
