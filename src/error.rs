use axum::{http::StatusCode, response::IntoResponse, Json};
use serde::Serialize;
use thiserror::Error;

pub type ApiResult<T> = Result<T, ApiError>;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error(transparent)]
    Unexpected(#[from] anyhow::Error),
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        use ApiError::*;

        let (status, message) = match &self {
            BadRequest(message) => (StatusCode::BAD_REQUEST, message.clone()),
            NotFound(message) => (StatusCode::NOT_FOUND, message.clone()),
            Unexpected(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "unexpected error".to_string(),
            ),
        };

        let payload = Json(ErrorResponse { error: message });
        (status, payload).into_response()
    }
}
