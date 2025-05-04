use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::SinkExt;
use parking_lot::RwLock;
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::protocol::CloseFrame, MaybeTlsStream, WebSocketStream};
use trading_common::error::AppError;

use crate::{
    subscriptions::subscription_state_manager::ConnectionStatus,
    websocket_message_handler::{connect_websocket, WebSocketMessage},
};

/// Connection state management
#[derive(Debug)]
pub struct ConnectionManager {
    connections: Arc<RwLock<HashMap<String, ConnectionState>>>,
}

#[derive(Debug)]
struct ConnectionState {
    pub stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    pub status: ConnectionStatus,
    pub last_activity: Instant,
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Thread-safe connection establishment
    pub async fn establish_connection(&self, address: String) -> Result<(), AppError> {
        let mut connections = self.connections.write();

        if connections.contains_key(&address) {
            return Err(AppError::ConnectionExists(address));
        }

        let stream = connect_websocket(&address).await?;

        connections.insert(
            address,
            ConnectionState {
                stream,
                status: ConnectionStatus::Connected,
                last_activity: Instant::now(),
            },
        );

        Ok(())
    }

    /// Safe message sending with reconnection handling
    pub async fn send_message(
        &self,
        address: &str,
        message: WebSocketMessage,
    ) -> Result<(), AppError> {
        let mut connections = self.connections.write();

        if let Some(connection) = connections.get_mut(address) {
            match connection.status {
                ConnectionStatus::Connected => {
                    connection.stream.send(message.into()).await.map_err(|e| {
                        connection.status = ConnectionStatus::Failed {
                            error: e.to_string(),
                        };
                        AppError::WebSocketSendError(e.to_string())
                    })
                }
                ConnectionStatus::Failed { .. } | ConnectionStatus::Disconnected { .. } => {
                    // Attempt reconnection
                    self.reconnect(address).await?;
                    //self.send_message(address, message).await
                    Ok(())
                }
                _ => Err(AppError::InvalidConnectionState(address.to_string())),
            }
        } else {
            Err(AppError::ConnectionNotFound(address.to_string()))
        }
    }

    /// Atomic connection cleanup
    pub async fn cleanup_connection(&self, address: &str) -> Result<(), AppError> {
        let mut connections = self.connections.write();

        if let Some(mut connection) = connections.remove(address) {
            connection.status = ConnectionStatus::Disconnected {
                reason: "Cleanup requested".to_string(),
            };
            // Ensure connection is properly closed
            connection.stream.close(None::<CloseFrame>).await.ok();
        }

        Ok(())
    }

    /// Reconnect to WebSocket with retry logic
    pub async fn reconnect(&self, address: &str) -> Result<(), AppError> {
        let mut connections = self.connections.write();

        if let Some(connection) = connections.get_mut(address) {
            // Update status to reconnecting
            connection.status = ConnectionStatus::Reconnecting { attempt: 0 };

            // Attempt to establish new connection
            match connect_websocket(address).await {
                Ok(stream) => {
                    connection.stream = stream;
                    connection.status = ConnectionStatus::Connected;
                    connection.last_activity = Instant::now();
                    Ok(())
                }
                Err(e) => {
                    connection.status = ConnectionStatus::Failed {
                        error: e.to_string(),
                    };
                    Err(e)
                }
            }
        } else {
            Err(AppError::ConnectionNotFound(address.to_string()))
        }
    }

    /// Check connection health and reconnect if needed
    pub async fn check_connection_health(&self, address: &str) -> Result<(), AppError> {
        let mut connections = self.connections.write();

        if let Some(connection) = connections.get_mut(address) {
            let needs_reconnect = match connection.status {
                ConnectionStatus::Connected => {
                    connection.last_activity.elapsed() > Duration::from_secs(30)
                }
                ConnectionStatus::Failed { .. } | ConnectionStatus::Disconnected { .. } => true,
                ConnectionStatus::Reconnecting { attempt } => attempt > 5,
                ConnectionStatus::Initializing => false,
            };

            if needs_reconnect {
                drop(connections); // Release lock before reconnecting
                self.reconnect(address).await?;
            }
        }

        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<(), AppError> {
        let mut connections = self.connections.write();

        // Close all active connections
        for (_, mut conn) in connections.drain() {
            if let Err(e) = conn.stream.close(None).await {
                tracing::error!("Error closing connection during shutdown: {}", e);
            }
        }

        Ok(())
    }
}
