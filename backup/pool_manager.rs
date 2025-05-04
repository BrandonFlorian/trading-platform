use std::{sync::Arc, time::Instant};

use backoff::backoff::Backoff;
use parking_lot::RwLock;
use tokio::{net::TcpStream, sync::mpsc};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use trading_common::error::AppError;

use crate::websocket_message_handler::connect_websocket;

use super::{
    connection_pool::{
        ActiveConnection, ConnectionPoolState, IdleConnection, PoolCommand, PoolHealth,
        PooledConnection,
    },
    ConnectionPoolConfig,
};

/// Manages the lifecycle of the connection pool
pub struct PoolManager {
    pub url: String,
    pub config: ConnectionPoolConfig,
    pub command_rx: mpsc::Receiver<PoolCommand>,
    pub state: Arc<RwLock<ConnectionPoolState>>,
}

impl PoolManager {
    pub async fn run(mut self) {
        let mut health_check_interval = tokio::time::interval(self.config.health_check_interval);

        // Initialize minimum connections
        self.initialize_pool().await;

        loop {
            tokio::select! {
                Some(command) = self.command_rx.recv() => {
                    match command {
                        PoolCommand::GetConnection { response } => {
                            let result = self.handle_get_connection().await;
                            let _ = response.send(result);
                        }
                        PoolCommand::ReturnConnection { conn_id, conn } => {
                            self.handle_return_connection(conn_id, conn).await;
                        }
                        PoolCommand::CheckHealth { response } => {
                            let health = self.get_pool_health();
                            let _ = response.send(health);
                        }
                        PoolCommand::Shutdown => {
                            self.handle_shutdown().await;
                            break;
                        }
                    }
                }
                _ = health_check_interval.tick() => {
                    self.perform_health_check().await;
                }
            }
        }
    }

    pub async fn initialize_pool(&mut self) {
        let mut state = self.state.write();
        while state.idle_connections.len() < self.config.min_connections {
            match self.create_connection().await {
                Ok(conn) => {
                    let idle_conn = IdleConnection {
                        id: uuid::Uuid::new_v4().to_string(),
                        conn,
                        created_at: Instant::now(),
                        last_used: Instant::now(),
                    };
                    state.idle_connections.push_back(idle_conn);
                    state.metrics.total_connections_created += 1;
                }
                Err(e) => {
                    state.metrics.failed_connections += 1;
                    state.metrics.last_error = Some(e.to_string());
                    tracing::error!("Failed to initialize connection: {}", e);
                }
            }
        }
    }

    pub async fn handle_get_connection(&mut self) -> Result<PooledConnection, AppError> {
        let mut state = self.state.write();

        // Try to get an idle connection
        while let Some(idle_conn) = state.idle_connections.pop_front() {
            // Check if the connection is still valid
            if idle_conn.created_at.elapsed() > self.config.max_lifetime
                || idle_conn.last_used.elapsed() > self.config.idle_timeout
            {
                // Connection is too old or idle, discard it
                continue;
            }

            // Create active connection record
            let active_conn = ActiveConnection {
                id: idle_conn.id.clone(),
                created_at: idle_conn.created_at,
                checked_out_at: Instant::now(),
            };
            state
                .active_connections
                .insert(idle_conn.id.clone(), active_conn);

            // Create sender for returning connection
            let (return_tx, _) = mpsc::channel(1);

            return Ok(PooledConnection {
                id: idle_conn.id,
                conn: Some(idle_conn.conn),
                return_tx,
            });
        }

        // No idle connections available, try to create a new one if under max
        if state.active_connections.len() + state.idle_connections.len()
            < self.config.max_connections
        {
            match self.create_connection().await {
                Ok(conn) => {
                    let conn_id = uuid::Uuid::new_v4().to_string();
                    let active_conn = ActiveConnection {
                        id: conn_id.clone(),
                        created_at: Instant::now(),
                        checked_out_at: Instant::now(),
                    };
                    state
                        .active_connections
                        .insert(conn_id.clone(), active_conn);
                    state.metrics.total_connections_created += 1;

                    // Create sender for returning connection
                    let (return_tx, _) = mpsc::channel(1);

                    Ok(PooledConnection {
                        id: conn_id,
                        conn: Some(conn),
                        return_tx,
                    })
                }
                Err(e) => {
                    state.metrics.failed_connections += 1;
                    state.metrics.last_error = Some(e.to_string());
                    Err(e)
                }
            }
        } else {
            Err(AppError::ConnectionPoolError(
                "Connection pool exhausted".into(),
            ))
        }
    }

    pub async fn handle_return_connection(
        &mut self,
        conn_id: String,
        conn: WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) {
        let mut state = self.state.write();

        // Remove from active connections
        state.active_connections.remove(&conn_id);

        // Add to idle pool if we're under min_connections
        if state.idle_connections.len() < self.config.min_connections {
            let idle_conn = IdleConnection {
                id: conn_id,
                conn,
                created_at: Instant::now(),
                last_used: Instant::now(),
            };
            state.idle_connections.push_back(idle_conn);
        }
        // Otherwise, let it drop and close
    }

    pub async fn perform_health_check(&mut self) {
        let mut state = self.state.write();

        // Clean up idle connections
        state.idle_connections.retain(|conn| {
            conn.created_at.elapsed() <= self.config.max_lifetime
                && conn.last_used.elapsed() <= self.config.idle_timeout
        });

        // Ensure minimum connections
        while state.idle_connections.len() < self.config.min_connections {
            match self.create_connection().await {
                Ok(conn) => {
                    let idle_conn = IdleConnection {
                        id: uuid::Uuid::new_v4().to_string(),
                        conn,
                        created_at: Instant::now(),
                        last_used: Instant::now(),
                    };
                    state.idle_connections.push_back(idle_conn);
                    state.metrics.total_connections_created += 1;
                }
                Err(e) => {
                    state.metrics.failed_connections += 1;
                    state.metrics.last_error = Some(e.to_string());
                    tracing::error!("Failed to create connection during health check: {}", e);
                    break;
                }
            }
        }

        // Check active connections for timeouts
        let timed_out: Vec<String> = state
            .active_connections
            .iter()
            .filter(|(_, conn)| conn.checked_out_at.elapsed() > self.config.connection_timeout)
            .map(|(id, _)| id.clone())
            .collect();

        for id in timed_out {
            state.active_connections.remove(&id);
            tracing::warn!("Removed timed out connection: {}", id);
        }
    }

    pub fn get_pool_health(&self) -> PoolHealth {
        let state = self.state.read();
        PoolHealth {
            active_connections: state.active_connections.len(),
            idle_connections: state.idle_connections.len(),
            in_use_connections: state.active_connections.len(),
            total_connections_created: state.metrics.total_connections_created,
            failed_connections: state.metrics.failed_connections,
            last_error: state.metrics.last_error.clone(),
        }
    }

    pub async fn handle_shutdown(&mut self) {
        let mut state = self.state.write();

        // Close all idle connections
        while let Some(mut conn) = state.idle_connections.pop_front() {
            if let Err(e) = conn.conn.close(None).await {
                tracing::error!("Error closing idle connection: {}", e);
            }
        }

        // Log warning about any remaining active connections
        if !state.active_connections.is_empty() {
            tracing::warn!(
                "{} connections still active during shutdown",
                state.active_connections.len()
            );
        }
    }

    pub async fn create_connection(
        &self,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, AppError> {
        let mut backoff = backoff::ExponentialBackoff {
            initial_interval: self.config.retry_initial_interval,
            max_interval: self.config.retry_max_interval,
            max_elapsed_time: Some(self.config.connection_timeout),
            ..Default::default()
        };

        let mut attempt = 0;
        while attempt < self.config.retry_max_attempts {
            match connect_websocket(&self.url).await {
                Ok(conn) => return Ok(conn),
                Err(e) => {
                    attempt += 1;
                    if attempt >= self.config.retry_max_attempts {
                        return Err(e);
                    }

                    if let Some(duration) = backoff.next_backoff() {
                        tracing::warn!(
                            "Failed to create connection (attempt {}/{}), retrying in {:?}: {}",
                            attempt,
                            self.config.retry_max_attempts,
                            duration,
                            e
                        );
                        tokio::time::sleep(duration).await;
                    }
                }
            }
        }

        Err(AppError::ConnectionPoolError(
            "Max connection attempts exceeded".into(),
        ))
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        let id = self.id.clone();
        let return_tx = self.return_tx.clone();

        // Take ownership of the connection
        if let Some(conn) = std::mem::take(&mut self.conn) {
            // Spawn a task to return the connection to the pool
            tokio::spawn(async move {
                if let Err(e) = return_tx
                    .send(PoolCommand::ReturnConnection {
                        conn_id: id.clone(),
                        conn,
                    })
                    .await
                {
                    tracing::error!("Failed to return connection {}: {}", id, e);
                }
            });
        }
    }
}

// Extension trait for mpsc::Receiver to get sender
trait ReceiverExt {
    type Item;
    fn into_tx(self) -> mpsc::Sender<Self::Item>;
}

impl<T> ReceiverExt for mpsc::Receiver<T> {
    type Item = T;
    fn into_tx(self) -> mpsc::Sender<Self::Item> {
        self.into_tx()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::connection_pool::ConnectionPool;

    use super::*;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_pool_initialization() {
        let config = ConnectionPoolConfig {
            min_connections: 2,
            max_connections: 5,
            ..Default::default()
        };

        let pool = ConnectionPool::new("ws://localhost:8080".into(), config);

        // Wait for initialization
        tokio::time::sleep(Duration::from_millis(100)).await;

        let health = pool.get_health().await.unwrap();
        assert_eq!(health.idle_connections, 2);
        assert_eq!(health.active_connections, 0);
    }

    #[tokio::test]
    async fn test_connection_checkout() {
        let config = ConnectionPoolConfig {
            min_connections: 1,
            max_connections: 3,
            ..Default::default()
        };

        let pool = ConnectionPool::new("ws://localhost:8080".into(), config);

        // Get a connection
        let conn = pool.get_connection().await.unwrap();
        let health = pool.get_health().await.unwrap();

        assert_eq!(health.active_connections, 1);
        assert_eq!(health.idle_connections, 0);

        // Return connection by dropping
        drop(conn);

        // Wait for return
        tokio::time::sleep(Duration::from_millis(100)).await;

        let health = pool.get_health().await.unwrap();
        assert_eq!(health.active_connections, 0);
        assert_eq!(health.idle_connections, 1);
    }

    #[tokio::test]
    async fn test_pool_exhaustion() {
        let config = ConnectionPoolConfig {
            min_connections: 1,
            max_connections: 2,
            ..Default::default()
        };

        let pool = ConnectionPool::new("ws://localhost:8080".into(), config);

        // Get max connections
        let conn1 = pool.get_connection().await.unwrap();
        let conn2 = pool.get_connection().await.unwrap();

        // Try to get another connection
        let result = timeout(Duration::from_secs(1), pool.get_connection()).await;

        assert!(result.is_err() || result.unwrap().is_err());

        // Return connections
        drop(conn1);
        drop(conn2);
    }
}
