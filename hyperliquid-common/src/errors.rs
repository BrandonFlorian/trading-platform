use thiserror::Error;

#[derive(Error, Debug)]
pub enum HyperliquidError {
    #[error("API error: {0}")]
    ApiError(String),
    
    #[error("Authentication error: {0}")]
    AuthError(String),
    
    #[error("Invalid order: {0}")]
    InvalidOrder(String),
    
    #[error("Network error: {0}")]
    NetworkError(#[from] reqwest::Error),
    
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    
    #[error("SDK error: {0}")]
    SdkError(String),
    
    #[error("Unknown error: {0}")]
    Unknown(String),
}

pub type Result<T> = std::result::Result<T, HyperliquidError>;