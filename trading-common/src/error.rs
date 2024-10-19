use anyhow::Error as AnyhowError;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use thiserror::Error;
#[derive(Error, Debug)]
pub enum AppError {
    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("Postgrest error: {0}")]
    PostgrestError(String),

    #[error("Json parse error: {0}")]
    JsonParseError(String),

    #[error("Request error: {0}")]
    RequestError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Server error: {0}")]
    ServerError(String),

    #[error("Port parse error: {0}")]
    PortParseError(#[from] std::num::ParseIntError),
    // #[error("Not found: {0}")]
    // NotFound(String),

    // #[error("Internal server error")]
    // InternalServerError,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            AppError::DatabaseError(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
            AppError::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            AppError::PostgrestError(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
            AppError::JsonParseError(message) => (StatusCode::BAD_REQUEST, message),
            AppError::RequestError(message) => (StatusCode::BAD_REQUEST, message),
            AppError::ConfigError(message) => (StatusCode::BAD_REQUEST, message),
            AppError::ServerError(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
            AppError::PortParseError(err) => (StatusCode::BAD_REQUEST, err.to_string()),
            // AppError::NotFound(message) => (StatusCode::NOT_FOUND, message),
            // AppError::InternalServerError => (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error".to_string()),
        };

        (status, error_message).into_response()
    }
}

impl From<serde_json::Error> for AppError {
    fn from(err: serde_json::Error) -> Self {
        AppError::BadRequest(err.to_string())
    }
}

impl From<AnyhowError> for AppError {
    fn from(err: AnyhowError) -> Self {
        AppError::ServerError(err.to_string())
    }
}
