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
    #[error("{0}")]
    Generic(String),

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

    #[error("gRPC connection error: {0}")]
    GrpcConnectionError(String),

    #[error("gRPC stream error: {0}")]
    GrpcStreamError(String),

    #[error("Event system error: {0}")]
    EventSystemError(String),

    #[error("Notification delivery error: {0}")]
    NotificationError(String),

    #[error("Service connection error: {service} - {details}")]
    ServiceConnectionError { service: String, details: String },

    #[error("Trade execution error: {details}")]
    TradeExecutionError {
        details: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("Invalid pool address: {0}")]
    InvalidPoolAddress(String),

    #[error("Subscription failed: {0}")]
    SubscriptionError(String),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] borsh::io::Error),

    #[error("Pool not found: {0}")]
    PoolNotFound(String),

    #[error("Price not available: {0}")]
    PriceNotAvailable(String),

    #[error("Invalid price: {0}")]
    InvalidPrice(String),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("Timeout error: {0}")]
    TimeoutError(String),

    #[error("Channel send error: {0}")]
    ChannelSendError(String),

    #[error("Channel receive error: {0}")]
    ChannelReceiveError(String),
}

impl AppError {
    pub fn grpc_connection_error(msg: impl Into<String>) -> Self {
        Self::GrpcConnectionError(msg.into())
    }

    pub fn event_system_error(msg: impl Into<String>) -> Self {
        Self::EventSystemError(msg.into())
    }

    pub fn service_connection_error(
        service: impl Into<String>,
        details: impl Into<String>,
    ) -> Self {
        Self::ServiceConnectionError {
            service: service.into(),
            details: details.into(),
        }
    }

    pub fn trade_execution_error(
        details: impl Into<String>,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self::TradeExecutionError {
            details: details.into(),
            source,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            AppError::DatabaseError(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
            AppError::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            AppError::ConfigError(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
            AppError::PostgrestError(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
            AppError::JsonParseError(message) => (StatusCode::BAD_REQUEST, message),
            AppError::RequestError(message) => (StatusCode::BAD_REQUEST, message),
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
            AppError::GrpcConnectionError(message) => (StatusCode::BAD_GATEWAY, message),
            AppError::GrpcStreamError(message) => (StatusCode::BAD_GATEWAY, message),
            AppError::EventSystemError(message) => (StatusCode::BAD_REQUEST, message),
            AppError::NotificationError(message) => (StatusCode::BAD_REQUEST, message),
            AppError::ServiceConnectionError { service, details } => (
                StatusCode::BAD_GATEWAY,
                format!("{} - {}", service, details),
            ),
            AppError::TradeExecutionError { details, source } => (StatusCode::BAD_REQUEST, details),
            AppError::InvalidPoolAddress(err) => (StatusCode::BAD_REQUEST, err.to_string()),
            AppError::SubscriptionError(err) => (StatusCode::BAD_REQUEST, err.to_string()),
            AppError::SerializationError(err) => (StatusCode::BAD_REQUEST, err.to_string()),
            AppError::PoolNotFound(err) => (StatusCode::BAD_REQUEST, err.to_string()),
            AppError::PriceNotAvailable(err) => (StatusCode::BAD_REQUEST, err.to_string()),
            AppError::InvalidPrice(err) => (StatusCode::BAD_REQUEST, err.to_string()),
            AppError::InternalError(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
            AppError::TimeoutError(err) => (StatusCode::GATEWAY_TIMEOUT, err.to_string()),
            AppError::ChannelSendError(err) => (StatusCode::BAD_REQUEST, err.to_string()),
            AppError::ChannelReceiveError(err) => (StatusCode::BAD_REQUEST, err.to_string()),
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

impl From<tonic::transport::Error> for AppError {
    fn from(error: tonic::transport::Error) -> Self {
        Self::GrpcConnectionError(format!("Transport error: {}", error))
    }
}

impl From<tonic::Status> for AppError {
    fn from(status: tonic::Status) -> Self {
        Self::GrpcConnectionError(format!("Status error: {}", status))
    }
}
