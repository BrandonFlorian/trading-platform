use crate::connection_pool::ConnectionPoolConfig;
use crate::websocket_message_handler::WebSocketMessage;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use trading_common::error::AppError;

use super::{
    connection_pool::{PoolHealth, PooledConnection},
    ConnectionPool,
};

/// Connection manager with pool support
pub struct PooledConnectionManager {
    pool: ConnectionPool,
    active_connections: Arc<RwLock<HashMap<String, PooledConnection>>>,
}

impl PooledConnectionManager {
    pub fn new(url: String, config: ConnectionPoolConfig) -> Self {
        Self {
            pool: ConnectionPool::new(url, config),
            active_connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get a connection for a specific token
    pub async fn get_connection_for_token(&self, token: &str) -> Result<(), AppError> {
        let mut connections = self.active_connections.write().await;

        // Check if we already have a connection for this token
        if connections.contains_key(token) {
            return Ok(());
        }

        // Get a new connection from the pool
        let conn = self.pool.get_connection().await?;
        connections.insert(token.to_string(), conn);

        Ok(())
    }

    /// Send a message over the token's connection
    pub async fn send_message(
        &self,
        token: &str,
        message: WebSocketMessage,
    ) -> Result<(), AppError> {
        let mut connections = self.active_connections.write().await;

        if let Some(conn) = connections.get_mut(token) {
            conn.send(message).await?;
            Ok(())
        } else {
            Err(AppError::ConnectionNotFound(token.to_string()))
        }
    }

    /// Release a token's connection back to the pool
    pub async fn release_connection(&self, token: &str) -> Result<(), AppError> {
        let mut connections = self.active_connections.write().await;
        connections.remove(token);
        Ok(())
    }

    /// Get current pool health
    pub async fn get_health(&self) -> Result<PoolHealth, AppError> {
        self.pool.get_health().await
    }

    /// Shutdown the connection manager and pool
    pub async fn shutdown(&self) -> Result<(), AppError> {
        // Release all connections
        let mut connections = self.active_connections.write().await;
        connections.clear();

        // Shutdown the pool
        self.pool.shutdown().await
    }
}
