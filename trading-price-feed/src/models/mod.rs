use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use trading_common::{
    error::AppError,
    models::{PriceUpdate, SolPriceUpdate},
};

/// Messages sent from clients to the server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMessage {
    /// Subscribe to price updates for a token
    Subscribe { token_address: String },

    /// Unsubscribe from price updates for a token
    Unsubscribe { token_address: String },

    /// Unsubscribe from all tokens
    UnsubscribeAll,

    /// Ping to keep connection alive
    Ping,
}

/// Messages sent from the server to clients
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerMessage {
    /// Price update for a token
    PriceUpdate(PriceUpdate),

    /// SOL price update
    SolPriceUpdate(SolPriceUpdate),

    /// Response to a client message
    Response {
        /// The request that this is responding to
        request_type: String,

        /// Whether the request was successful
        success: bool,

        /// Optional message, especially for errors
        message: Option<String>,

        /// Optional data payload
        data: Option<serde_json::Value>,
    },

    /// Error message
    Error { code: u32, message: String },

    /// Pong response to ping
    Pong { timestamp: i64 },
}

/// Commands sent to the Subscription Coordinator
#[derive(Debug)]
pub enum CoordinatorCommand {
    /// Subscribe a client to a token
    Subscribe {
        client_id: String,
        token_address: String,
        response_tx: oneshot::Sender<Result<(), AppError>>,
    },

    /// Unsubscribe a client from a token
    Unsubscribe {
        client_id: String,
        token_address: String,
    },

    /// Client has disconnected
    ClientDisconnected { client_id: String },

    /// Shutdown the coordinator
    Shutdown {
        response_tx: oneshot::Sender<Result<(), AppError>>,
    },
}

/// Commands sent to Pool Actors
#[derive(Debug)]
pub enum PoolCommand {
    /// Initialize the pool
    Initialize {
        response_tx: oneshot::Sender<Result<(), AppError>>,
    },

    /// Get the current price
    GetPrice {
        response_tx: oneshot::Sender<Result<PriceUpdate, AppError>>,
    },

    /// Shutdown the pool actor
    Shutdown {
        response_tx: oneshot::Sender<Result<(), AppError>>,
    },
}

/// Events published by Pool Actors
#[derive(Debug, Clone)]
pub enum PoolEvent {
    /// Price has been updated
    PriceUpdated(PriceUpdate),

    /// Pool state has changed
    StateChanged {
        token_address: String,
        state: PoolState,
    },
}

/// Pool state
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PoolState {
    /// Pool is initializing
    Initializing,

    /// Pool is active
    Active,

    /// Pool is reconnecting
    Reconnecting { attempt: u32 },

    /// Pool has failed
    Failed { error: String },
}
