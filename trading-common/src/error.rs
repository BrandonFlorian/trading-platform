use anyhow::Error as AnyhowError;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use solana_client::client_error::ClientError;
use solana_sdk::pubkey::ParsePubkeyError;
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

    #[error("Surf error: {0}")]
    SurfError(String),

    #[error("Solana RPC error: {0}")]
    SolanaRpcError(#[from] ClientError),

    #[error("Pubkey parse error: {0}")]
    PubkeyParseError(#[from] ParsePubkeyError),

    #[error("{0}")]
    Generic(String),
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
            AppError::SurfError(err) => (StatusCode::BAD_REQUEST, err.to_string()),
            AppError::SolanaRpcError(err) => (StatusCode::BAD_REQUEST, err.to_string()),
            AppError::PubkeyParseError(err) => (StatusCode::BAD_REQUEST, err.to_string()),
            AppError::Generic(err) => (StatusCode::INTERNAL_SERVER_ERROR, err),
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

impl From<surf::Error> for AppError {
    fn from(err: surf::Error) -> Self {
        AppError::SurfError(err.to_string())
    }
}

impl From<solana_sdk::program_error::ProgramError> for AppError {
    fn from(error: solana_sdk::program_error::ProgramError) -> Self {
        AppError::Generic(error.to_string())
    }
}
