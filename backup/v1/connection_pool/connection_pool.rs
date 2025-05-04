use parking_lot::RwLock;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Instant,
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use trading_common::error::AppError;

use crate::websocket_message_handler::WebSocketMessage;

use super::ConnectionPoolConfig;

/// Commands that can be sent to the connection pool
#[derive(Debug)]
pub enum PoolCommand {
    GetConnection {
        response: oneshot::Sender<Result<PooledConnection, AppError>>,
    },
    ReturnConnection {
        conn_id: String,
        conn: WebSocketStream<MaybeTlsStream<TcpStream>>,
    },
    CheckHealth {
        response: oneshot::Sender<PoolHealth>,
    },
    Shutdown,
}

/// Connection pool health information
#[derive(Debug, Clone)]
pub struct PoolHealth {
    pub active_connections: usize,
    pub idle_connections: usize,
    pub in_use_connections: usize,
    pub total_connections_created: u64,
    pub failed_connections: u64,
    pub last_error: Option<String>,
}

/// Manages a pool of WebSocket connections
#[derive(Clone)]
pub struct ConnectionPool {
    pub url: String,
    pub config: ConnectionPoolConfig,
    pub command_tx: mpsc::Sender<PoolCommand>,
    pub state: Arc<RwLock<ConnectionPoolState>>,
}

/// Internal state of the connection pool
pub struct ConnectionPoolState {
    pub idle_connections: VecDeque<IdleConnection>,
    pub active_connections: HashMap<String, ActiveConnection>,
    pub metrics: PoolMetrics,
}

/// Metrics for the connection pool
#[derive(Debug, Default)]
pub struct PoolMetrics {
    pub total_connections_created: u64,
    pub failed_connections: u64,
    pub last_error: Option<String>,
}

/// Represents an idle connection in the pool
pub struct IdleConnection {
    pub id: String,
    pub conn: WebSocketStream<MaybeTlsStream<TcpStream>>,
    pub created_at: Instant,
    pub last_used: Instant,
}

/// Represents an active connection being used
pub struct ActiveConnection {
    pub id: String,
    pub created_at: Instant,
    pub checked_out_at: Instant,
}

/// A connection checked out from the pool
#[derive(Debug)]
pub struct PooledConnection {
    pub id: String,
    pub conn: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    pub return_tx: mpsc::Sender<PoolCommand>,
}

use futures_util::SinkExt;
use tokio_tungstenite::tungstenite::Message;

impl PooledConnection {
    pub async fn send(&mut self, message: WebSocketMessage) -> Result<(), AppError> {
        if let Some(ref mut conn) = self.conn {
            conn.send(Message::from(message))
                .await
                .map_err(|e| AppError::WebSocketSendError(e.to_string()))?;
            Ok(())
        } else {
            Err(AppError::WebSocketError(
                "Connection not initialized".into(),
            ))
        }
    }

    pub async fn close(&mut self) -> Result<(), AppError> {
        if let Some(conn) = &mut self.conn {
            conn.close(None)
                .await
                .map_err(|e| AppError::WebSocketError(e.to_string()))?;
        }
        Ok(())
    }
}

impl ConnectionPool {
    pub fn new(url: String, config: ConnectionPoolConfig) -> Self {
        let (command_tx, command_rx) = mpsc::channel(100);
        let state = Arc::new(RwLock::new(ConnectionPoolState {
            idle_connections: VecDeque::new(),
            active_connections: HashMap::new(),
            metrics: PoolMetrics::default(),
        }));

        let pool = Self {
            url,
            config,
            command_tx: command_tx.clone(),
            state: state.clone(),
        };

        // Spawn pool management task
        let pool_manager = ConnectionPool {
            url: pool.url.clone(),
            config: pool.config.clone(),
            command_tx: command_tx.clone(),
            state: state.clone(),
        };

        tokio::spawn(async move {
            pool_manager.run();
        });

        pool
    }

    /// Get a connection from the pool
    pub async fn get_connection(&self) -> Result<PooledConnection, AppError> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(PoolCommand::GetConnection { response: tx })
            .await
            .map_err(|_| AppError::ConnectionPoolError("Failed to request connection".into()))?;

        rx.await
            .map_err(|_| AppError::ConnectionPoolError("Failed to receive connection".into()))?
    }

    /// Get current pool health
    pub async fn get_health(&self) -> Result<PoolHealth, AppError> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(PoolCommand::CheckHealth { response: tx })
            .await
            .map_err(|_| AppError::ConnectionPoolError("Failed to request health check".into()))?;

        Ok(rx
            .await
            .map_err(|_| AppError::ConnectionPoolError("Failed to receive health status".into()))?)
    }

    /// Shutdown the connection pool
    pub async fn shutdown(&self) -> Result<(), AppError> {
        self.command_tx
            .send(PoolCommand::Shutdown)
            .await
            .map_err(|_| AppError::ConnectionPoolError("Failed to send shutdown command".into()))
    }
}
