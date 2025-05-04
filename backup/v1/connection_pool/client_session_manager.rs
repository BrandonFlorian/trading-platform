use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use parking_lot::RwLock;
use tokio::sync::mpsc;
use trading_common::error::AppError;

use crate::websocket_message_handler::WebSocketMessage;

/// Client session management
#[derive(Debug)]
pub struct ClientSessionManager {
    sessions: Arc<RwLock<HashMap<String, ClientSession>>>,
}

#[derive(Debug)]
pub struct ClientSession {
    pub sender: mpsc::Sender<WebSocketMessage>,
    pub subscriptions: HashSet<String>,
    pub last_seen: Instant,
}

impl ClientSessionManager {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Atomic client registration
    pub async fn register_client(
        &self,
        client_id: String,
        sender: mpsc::Sender<WebSocketMessage>,
    ) -> Result<(), AppError> {
        let mut sessions = self.sessions.write();

        sessions.insert(
            client_id,
            ClientSession {
                sender,
                subscriptions: HashSet::new(),
                last_seen: Instant::now(),
            },
        );

        Ok(())
    }

    /// Safe message sending with connection check
    pub async fn send_message(
        &self,
        client_id: &str,
        message: WebSocketMessage,
    ) -> Result<(), AppError> {
        let sessions = self.sessions.read();

        if let Some(session) = sessions.get(client_id) {
            session.sender.send(message).await.map_err(|_| {
                AppError::WebSocketSendError(format!("Failed to send to client {}", client_id))
            })
        } else {
            Err(AppError::InvalidClientId(client_id.to_string()))
        }
    }

    /// Atomic client cleanup
    pub async fn cleanup_client(&self, client_id: &str) -> Result<HashSet<String>, AppError> {
        let mut sessions = self.sessions.write();

        if let Some(session) = sessions.remove(client_id) {
            Ok(session.subscriptions)
        } else {
            Ok(HashSet::new())
        }
    }
}
