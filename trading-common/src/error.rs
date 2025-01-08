use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use solana_client::client_error::ClientError;
use solana_sdk::{program_error::ProgramError, pubkey::ParsePubkeyError};
use thiserror::Error;
use tokio::sync::mpsc;

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

    #[error("Solana RPC error: {source}")]
    SolanaRpcError {
        #[from]
        source: ClientError,
    },

    #[error("Token account error: {0}")]
    TokenAccountError(String),

    #[error("Insufficient balance: {0}")]
    InsufficientBalanceError(String),

    #[error("Transaction error: {0}")]
    TransactionError(String),

    #[error("Pubkey parse error: {source}")]
    PubkeyParseError {
        #[from]
        source: ParsePubkeyError,
    },

    #[error("Program error: {source}")]
    ProgramError {
        #[from]
        source: ProgramError,
    },

    #[error("WebSocket connection error: {0}")]
    WebSocketConnectionError(String),

    #[error("WebSocket health check failed")]
    WebSocketHealthCheckFailed,

    #[error("WebSocket send error: {0}")]
    WebSocketSendError(String),

    #[error("WebSocket receive error: {0}")]
    WebSocketReceiveError(String),

    #[error("WebSocket timeout error: {0}")]
    WebSocketTimeout(String),

    #[error("WebSocket state error: {0}")]
    WebSocketStateError(String),

    #[error("WebSocket error: {0}")]
    WebSocketError(String),

    #[error("Failed to initialize monitor: {0}")]
    InitializationError(String),

    #[error("Message processing error: {0}")]
    MessageProcessingError(String),

    #[error("Task error: {0}")]
    TaskError(String),

    #[error("Redis error: {0}")]
    RedisError(String),

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
            AppError::ConfigError(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
            AppError::ServerError(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
            AppError::PortParseError(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
            AppError::SurfError(err) => (StatusCode::BAD_GATEWAY, err),
            AppError::SolanaRpcError { source } => (StatusCode::BAD_GATEWAY, source.to_string()),
            AppError::TokenAccountError(message) => (StatusCode::BAD_REQUEST, message),
            AppError::InsufficientBalanceError(message) => (StatusCode::BAD_REQUEST, message),
            AppError::TransactionError(message) => (StatusCode::BAD_REQUEST, message),
            AppError::PubkeyParseError { source } => (StatusCode::BAD_REQUEST, source.to_string()),
            AppError::ProgramError { source } => (StatusCode::BAD_REQUEST, source.to_string()),
            AppError::WebSocketConnectionError(msg) => (StatusCode::BAD_GATEWAY, msg),
            AppError::WebSocketHealthCheckFailed => (
                StatusCode::BAD_GATEWAY,
                "WebSocket health check failed".to_string(),
            ),
            AppError::WebSocketSendError(msg) => (StatusCode::BAD_GATEWAY, msg),
            AppError::WebSocketReceiveError(msg) => (StatusCode::BAD_GATEWAY, msg),
            AppError::WebSocketTimeout(msg) => (StatusCode::GATEWAY_TIMEOUT, msg),
            AppError::WebSocketStateError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            AppError::WebSocketError(msg) => (StatusCode::BAD_GATEWAY, msg),

            AppError::Generic(err) => (StatusCode::INTERNAL_SERVER_ERROR, err),
            AppError::InitializationError(message) => (StatusCode::BAD_REQUEST, message),
            AppError::MessageProcessingError(message) => (StatusCode::BAD_REQUEST, message),
            AppError::TaskError(message) => (StatusCode::BAD_REQUEST, message),
            AppError::RedisError(message) => (StatusCode::BAD_REQUEST, message),
        };

        let body = serde_json::json!({
            "error": error_message,
            "status": status.as_u16()
        });

        (status, axum::Json(body)).into_response()
    }
}

impl From<serde_json::Error> for AppError {
    fn from(err: serde_json::Error) -> Self {
        AppError::JsonParseError(err.to_string())
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for AppError {
    fn from(err: tokio_tungstenite::tungstenite::Error) -> Self {
        AppError::WebSocketError(err.to_string())
    }
}

impl From<tokio::time::error::Elapsed> for AppError {
    fn from(error: tokio::time::error::Elapsed) -> Self {
        AppError::WebSocketTimeout(error.to_string())
    }
}

impl From<surf::Error> for AppError {
    fn from(err: surf::Error) -> Self {
        AppError::SurfError(err.to_string())
    }
}

impl<T> From<mpsc::error::SendError<T>> for AppError {
    fn from(error: mpsc::error::SendError<T>) -> Self {
        AppError::WebSocketSendError(error.to_string())
    }
}

impl From<anyhow::Error> for AppError {
    fn from(err: anyhow::Error) -> Self {
        AppError::Generic(err.to_string())
    }
}
