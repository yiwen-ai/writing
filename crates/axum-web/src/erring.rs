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

// impl From<HTTPError> for anyhow::Error {
//     fn from(err: HTTPError) -> Self {
//         anyhow::Error::new(err)
//     }
// }

impl From<anyhow::Error> for HTTPError {
    fn from(err: anyhow::Error) -> Self {
        match err.downcast::<Self>() {
            Ok(err) => err,
            Err(sel) => match sel.downcast::<SingleRowError>() {
                Ok(err) => HTTPError {
                    code: 404,
                    message: format!("{:?}", err),
                    data: None,
                },
                Err(sel) => HTTPError {
                    code: 500,
                    message: format!("{:?}", sel),
                    data: None,
                },
            },
        }
    }
}

impl From<ValidationError> for HTTPError {
    fn from(err: ValidationError) -> Self {
        HTTPError {
            code: 400,
            message: format!("{:?}", err),
            data: None,
        }
    }
}

impl From<ValidationErrors> for HTTPError {
    fn from(err: ValidationErrors) -> Self {
        HTTPError {
            code: 400,
            message: format!("{:?}", err),
            data: None,
        }
    }
}
