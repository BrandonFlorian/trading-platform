use axum::extract::ws::Message;
use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use trading_common::error::AppError;

use crate::subscriptions::PartitionedSubscriptionManager;

// WebSocket message types
#[derive(Debug, Clone)]
pub enum WebSocketMessage {
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close,
}

impl From<Message> for WebSocketMessage {
    fn from(msg: Message) -> Self {
        match msg {
            Message::Text(text) => WebSocketMessage::Text(text.to_string()),
            Message::Binary(data) => WebSocketMessage::Binary(data.to_vec()),
            Message::Ping(data) => WebSocketMessage::Ping(data.to_vec()),
            Message::Pong(data) => WebSocketMessage::Pong(data.to_vec()),
            Message::Close(_) => WebSocketMessage::Close,
            _ => WebSocketMessage::Close, // Handle other cases as Close
        }
    }
}

impl From<WebSocketMessage> for Message {
    fn from(msg: WebSocketMessage) -> Self {
        match msg {
            WebSocketMessage::Text(text) => Message::Text(text.into()),
            WebSocketMessage::Binary(data) => Message::Binary(data.into()),
            WebSocketMessage::Ping(data) => Message::Ping(data.into()),
            WebSocketMessage::Pong(data) => Message::Pong(data.into()),
            WebSocketMessage::Close => Message::Close(None),
        }
    }
}

impl From<WebSocketMessage> for tokio_tungstenite::tungstenite::Message {
    fn from(msg: WebSocketMessage) -> Self {
        match msg {
            WebSocketMessage::Text(text) => {
                tokio_tungstenite::tungstenite::Message::Text(text.into())
            }
            WebSocketMessage::Binary(data) => {
                tokio_tungstenite::tungstenite::Message::Binary(data.into())
            }
            WebSocketMessage::Ping(data) => {
                tokio_tungstenite::tungstenite::Message::Ping(data.into())
            }
            WebSocketMessage::Pong(data) => {
                tokio_tungstenite::tungstenite::Message::Pong(data.into())
            }
            WebSocketMessage::Close => tokio_tungstenite::tungstenite::Message::Close(None),
        }
    }
}

// Commands for the message handler
#[derive(Debug)]
pub enum HandlerCommand {
    // Send a message to a specific client
    SendMessage {
        client_id: String,
        message: WebSocketMessage,
        response_tx: oneshot::Sender<Result<(), AppError>>,
    },
    // Broadcast a message to all clients
    BroadcastMessage {
        message: WebSocketMessage,
    },
    // Broadcast a message to clients subscribed to a token
    BroadcastToToken {
        token_address: String,
        message: WebSocketMessage,
    },
    // Register a new client
    RegisterClient {
        client_id: String,
        sender: mpsc::Sender<WebSocketMessage>,
        response_tx: oneshot::Sender<Result<(), AppError>>,
    },
    // Unregister a client
    UnregisterClient {
        client_id: String,
    },
    // Subscribe a client to a token
    SubscribeClientToToken {
        client_id: String,
        token_address: String,
        response_tx: oneshot::Sender<Result<(), AppError>>,
    },
    // Unsubscribe a client from a token
    UnsubscribeClientFromToken {
        client_id: String,
        token_address: String,
    },
    // Get handler status
    GetStatus {
        response_tx: oneshot::Sender<HandlerStatus>,
    },
    // Shutdown the handler
    Shutdown,
}

// Status information for the message handler
#[derive(Debug, Clone, Serialize)]
pub struct HandlerStatus {
    pub active_clients: usize,
    pub active_token_subscriptions: usize,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub last_update: chrono::DateTime<chrono::Utc>,
}

// WebSocket message handler
pub struct WebSocketMessageHandler {
    command_tx: mpsc::Sender<HandlerCommand>,
    client_senders: Arc<RwLock<HashMap<String, mpsc::Sender<WebSocketMessage>>>>,
    token_subscriptions: Arc<RwLock<HashMap<String, Vec<String>>>>, // token_address -> [client_ids]
    stats: Arc<RwLock<HandlerStats>>,
}

// Statistics for the message handler
#[derive(Debug, Clone)]
pub struct HandlerStats {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub last_update: chrono::DateTime<chrono::Utc>,
}

impl Default for HandlerStats {
    fn default() -> Self {
        Self {
            messages_sent: 0,
            messages_received: 0,
            last_update: chrono::Utc::now(),
        }
    }
}

impl WebSocketMessageHandler {
    pub fn new() -> Self {
        let (command_tx, command_rx) = mpsc::channel(100);
        let client_senders = Arc::new(RwLock::new(HashMap::new()));
        let token_subscriptions = Arc::new(RwLock::new(HashMap::new()));
        let stats = Arc::new(RwLock::new(HandlerStats::default()));

        // Clone references for the handler task
        let client_senders_clone = client_senders.clone();
        let token_subscriptions_clone = token_subscriptions.clone();
        let stats_clone = stats.clone();

        // Spawn the handler task
        tokio::spawn(async move {
            Self::run_handler(
                command_rx,
                client_senders_clone,
                token_subscriptions_clone,
                stats_clone,
            )
            .await;
        });

        Self {
            command_tx,
            client_senders,
            token_subscriptions,
            stats,
        }
    }

    // Run the handler task
    async fn run_handler(
        mut command_rx: mpsc::Receiver<HandlerCommand>,
        client_senders: Arc<RwLock<HashMap<String, mpsc::Sender<WebSocketMessage>>>>,
        token_subscriptions: Arc<RwLock<HashMap<String, Vec<String>>>>,
        stats: Arc<RwLock<HandlerStats>>,
    ) {
        while let Some(command) = command_rx.recv().await {
            match command {
                HandlerCommand::SendMessage {
                    client_id,
                    message,
                    response_tx,
                } => {
                    let result =
                        Self::send_message(&client_id, message, &client_senders, &stats).await;
                    let _ = response_tx.send(result);
                }
                HandlerCommand::BroadcastMessage { message } => {
                    Self::broadcast_message(message, &client_senders, &stats).await;
                }
                HandlerCommand::BroadcastToToken {
                    token_address,
                    message,
                } => {
                    Self::broadcast_to_token(
                        &token_address,
                        message,
                        &client_senders,
                        &token_subscriptions,
                        &stats,
                    )
                    .await;
                }
                HandlerCommand::RegisterClient {
                    client_id,
                    sender,
                    response_tx,
                } => {
                    let mut senders = client_senders.write().await;
                    senders.insert(client_id, sender);
                    let _ = response_tx.send(Ok(()));
                }
                HandlerCommand::UnregisterClient { client_id } => {
                    let mut senders = client_senders.write().await;
                    senders.remove(&client_id);

                    // Also remove client from all token subscriptions
                    let mut subscriptions = token_subscriptions.write().await;
                    for clients in subscriptions.values_mut() {
                        clients.retain(|id| id != &client_id);
                    }
                    // Clean up empty token subscriptions
                    subscriptions.retain(|_, clients| !clients.is_empty());
                }
                HandlerCommand::SubscribeClientToToken {
                    client_id,
                    token_address,
                    response_tx,
                } => {
                    let mut subscriptions = token_subscriptions.write().await;

                    // Check if client exists
                    let client_exists = {
                        let senders = client_senders.read().await;
                        senders.contains_key(&client_id)
                    };

                    if client_exists {
                        let clients = subscriptions.entry(token_address).or_insert_with(Vec::new);
                        if !clients.contains(&client_id) {
                            clients.push(client_id);
                        }
                        let _ = response_tx.send(Ok(()));
                    } else {
                        let _ = response_tx.send(Err(AppError::InvalidClientId(client_id)));
                    }
                }
                HandlerCommand::UnsubscribeClientFromToken {
                    client_id,
                    token_address,
                } => {
                    let mut subscriptions = token_subscriptions.write().await;
                    if let Some(clients) = subscriptions.get_mut(&token_address) {
                        clients.retain(|id| id != &client_id);
                        if clients.is_empty() {
                            subscriptions.remove(&token_address);
                        }
                    }
                }
                HandlerCommand::GetStatus { response_tx } => {
                    let status =
                        Self::get_status(&client_senders, &token_subscriptions, &stats).await;
                    let _ = response_tx.send(status);
                }
                HandlerCommand::Shutdown => {
                    // Close all client connections
                    let senders = client_senders.read().await;
                    for sender in senders.values() {
                        let _ = sender.send(WebSocketMessage::Close).await;
                    }
                    break;
                }
            }
        }

        tracing::info!("WebSocket message handler shutdown complete");
    }

    // Send a message to a specific client
    async fn send_message(
        client_id: &str,
        message: WebSocketMessage,
        client_senders: &Arc<RwLock<HashMap<String, mpsc::Sender<WebSocketMessage>>>>,
        stats: &Arc<RwLock<HandlerStats>>,
    ) -> Result<(), AppError> {
        let senders = client_senders.read().await;

        if let Some(sender) = senders.get(client_id) {
            sender.send(message).await.map_err(|_| {
                AppError::WebSocketSendError(format!(
                    "Failed to send message to client {}",
                    client_id
                ))
            })?;

            // Update stats
            let mut stats = stats.write().await;
            stats.messages_sent += 1;
            stats.last_update = chrono::Utc::now();

            Ok(())
        } else {
            Err(AppError::InvalidClientId(client_id.to_string()))
        }
    }

    // Broadcast a message to all clients
    async fn broadcast_message(
        message: WebSocketMessage,
        client_senders: &Arc<RwLock<HashMap<String, mpsc::Sender<WebSocketMessage>>>>,
        stats: &Arc<RwLock<HandlerStats>>,
    ) {
        let senders = client_senders.read().await;
        let mut sent_count = 0;

        for (client_id, sender) in senders.iter() {
            if let Err(e) = sender.send(message.clone()).await {
                tracing::error!("Failed to send message to client {}: {}", client_id, e);
            } else {
                sent_count += 1;
            }
        }

        // Update stats
        if sent_count > 0 {
            let mut stats = stats.write().await;
            stats.messages_sent += sent_count;
            stats.last_update = chrono::Utc::now();
        }
    }

    // Broadcast a message to clients subscribed to a token
    pub async fn broadcast_to_token(
        token_address: &str,
        message: WebSocketMessage,
        client_senders: &Arc<RwLock<HashMap<String, mpsc::Sender<WebSocketMessage>>>>,
        token_subscriptions: &Arc<RwLock<HashMap<String, Vec<String>>>>,
        stats: &Arc<RwLock<HandlerStats>>,
    ) {
        let subscriptions = token_subscriptions.read().await;

        if let Some(client_ids) = subscriptions.get(token_address) {
            let senders = client_senders.read().await;
            let mut sent_count = 0;

            for client_id in client_ids {
                if let Some(sender) = senders.get(client_id) {
                    if let Err(e) = sender.send(message.clone()).await {
                        tracing::error!("Failed to send message to client {}: {}", client_id, e);
                    } else {
                        sent_count += 1;
                    }
                }
            }

            // Update stats
            if sent_count > 0 {
                let mut stats = stats.write().await;
                stats.messages_sent += sent_count;
                stats.last_update = chrono::Utc::now();
            }
        }
    }

    // Get handler status
    async fn get_status(
        client_senders: &Arc<RwLock<HashMap<String, mpsc::Sender<WebSocketMessage>>>>,
        token_subscriptions: &Arc<RwLock<HashMap<String, Vec<String>>>>,
        stats: &Arc<RwLock<HandlerStats>>,
    ) -> HandlerStatus {
        let client_count = client_senders.read().await.len();
        let token_count = token_subscriptions.read().await.len();
        let stats = stats.read().await.clone();

        HandlerStatus {
            active_clients: client_count,
            active_token_subscriptions: token_count,
            messages_sent: stats.messages_sent,
            messages_received: stats.messages_received,
            last_update: stats.last_update,
        }
    }

    // Public API

    // Send a message to a specific client
    pub async fn send_to_client<T: Serialize>(
        &self,
        client_id: &str,
        data: &T,
    ) -> Result<(), AppError> {
        let json = serde_json::to_string(data)
            .map_err(|e| AppError::JsonParseError(format!("Failed to serialize message: {}", e)))?;

        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send(HandlerCommand::SendMessage {
                client_id: client_id.to_string(),
                message: WebSocketMessage::Text(json),
                response_tx: tx,
            })
            .await
            .map_err(|_| {
                AppError::InternalError("Failed to send command to message handler".into())
            })?;

        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| AppError::TimeoutError("Message send timed out".into()))?
            .map_err(|_| AppError::InternalError("Failed to receive send response".into()))?
    }

    // Broadcast a message to all clients
    pub async fn broadcast<T: Serialize>(&self, data: &T) -> Result<(), AppError> {
        let json = serde_json::to_string(data)
            .map_err(|e| AppError::JsonParseError(format!("Failed to serialize message: {}", e)))?;

        self.command_tx
            .send(HandlerCommand::BroadcastMessage {
                message: WebSocketMessage::Text(json),
            })
            .await
            .map_err(|_| AppError::InternalError("Failed to send broadcast command".into()))?;

        Ok(())
    }

    // Broadcast a message to clients subscribed to a token
    pub async fn broadcast_to_token_subscribers<T: Serialize>(
        &self,
        token_address: &str,
        data: &T,
    ) -> Result<(), AppError> {
        let json = serde_json::to_string(data)
            .map_err(|e| AppError::JsonParseError(format!("Failed to serialize message: {}", e)))?;

        self.command_tx
            .send(HandlerCommand::BroadcastToToken {
                token_address: token_address.to_string(),
                message: WebSocketMessage::Text(json),
            })
            .await
            .map_err(|_| {
                AppError::InternalError("Failed to send token broadcast command".into())
            })?;

        Ok(())
    }

    // Register a new client
    pub async fn register_client(
        &self,
        client_id: &str,
        sender: mpsc::Sender<WebSocketMessage>,
    ) -> Result<(), AppError> {
        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send(HandlerCommand::RegisterClient {
                client_id: client_id.to_string(),
                sender,
                response_tx: tx,
            })
            .await
            .map_err(|_| AppError::InternalError("Failed to send register command".into()))?;

        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| AppError::TimeoutError("Registration timed out".into()))?
            .map_err(|_| {
                AppError::InternalError("Failed to receive registration response".into())
            })?
    }

    // Unregister a client
    pub async fn unregister_client(&self, client_id: &str) -> Result<(), AppError> {
        self.command_tx
            .send(HandlerCommand::UnregisterClient {
                client_id: client_id.to_string(),
            })
            .await
            .map_err(|_| AppError::InternalError("Failed to send unregister command".into()))?;

        Ok(())
    }

    // Subscribe a client to a token
    pub async fn subscribe_client_to_token(
        &self,
        client_id: &str,
        token_address: &str,
    ) -> Result<(), AppError> {
        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send(HandlerCommand::SubscribeClientToToken {
                client_id: client_id.to_string(),
                token_address: token_address.to_string(),
                response_tx: tx,
            })
            .await
            .map_err(|_| AppError::InternalError("Failed to send subscribe command".into()))?;

        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| AppError::TimeoutError("Subscription timed out".into()))?
            .map_err(|_| {
                AppError::InternalError("Failed to receive subscription response".into())
            })?
    }

    // Unsubscribe a client from a token
    pub async fn unsubscribe_client_from_token(
        &self,
        client_id: &str,
        token_address: &str,
    ) -> Result<(), AppError> {
        self.command_tx
            .send(HandlerCommand::UnsubscribeClientFromToken {
                client_id: client_id.to_string(),
                token_address: token_address.to_string(),
            })
            .await
            .map_err(|_| AppError::InternalError("Failed to send unsubscribe command".into()))?;

        Ok(())
    }

    // Get handler status
    pub async fn get_handler_status(&self) -> Result<HandlerStatus, AppError> {
        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send(HandlerCommand::GetStatus { response_tx: tx })
            .await
            .map_err(|_| AppError::InternalError("Failed to send status command".into()))?;

        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| AppError::TimeoutError("Status request timed out".into()))?
            .map_err(|_| AppError::InternalError("Failed to receive status response".into()))
    }

    // Shutdown the handler
    pub async fn shutdown(&self) -> Result<(), AppError> {
        self.command_tx
            .send(HandlerCommand::Shutdown)
            .await
            .map_err(|_| AppError::InternalError("Failed to send shutdown command".into()))?;

        Ok(())
    }
}

// Helper function to handle a client WebSocket connection
pub async fn handle_client_websocket(
    websocket: axum::extract::ws::WebSocket,
    client_id: String,
    message_handler: Arc<WebSocketMessageHandler>,
    subscription_manager: Arc<PartitionedSubscriptionManager>,
) {
    let (ws_sink, ws_stream) = websocket.split();

    // Clone client_id for both tasks
    let sink_client_id = client_id.clone();
    let stream_client_id = client_id.clone();

    // Create channel for sending messages to the client
    let (msg_tx, mut msg_rx) = mpsc::channel::<WebSocketMessage>(100);

    // Register client with message handler
    if let Err(e) = message_handler
        .register_client(&client_id, msg_tx.clone())
        .await
    {
        tracing::error!("Failed to register client {}: {}", client_id, e);
        return;
    }

    // Task for sending messages to the client
    let sink_task = tokio::spawn(async move {
        let mut sink = ws_sink;

        while let Some(msg) = msg_rx.recv().await {
            // Check if it's a close message before consuming it
            let is_close = matches!(msg, WebSocketMessage::Close);

            // Convert to WebSocket message (this consumes msg)
            let ws_msg: Message = msg.into();

            if let Err(e) = sink.send(ws_msg).await {
                tracing::error!("Error sending message to client {}: {}", sink_client_id, e);
                break;
            }

            // Use the flag we set earlier instead of checking msg again
            if is_close {
                break;
            }
        }

        // Ensure the connection is closed
        let _ = sink.close().await;
    });

    // Task for receiving messages from the client
    let message_handler_clone = message_handler.clone();
    let subscription_manager_clone = subscription_manager.clone();

    let stream_task = tokio::spawn(async move {
        let mut stream = ws_stream;

        while let Some(msg_result) = stream.next().await {
            match msg_result {
                Ok(msg) => {
                    match msg {
                        Message::Text(text) => {
                            // Handle client commands
                            if let Err(e) = handle_client_message(
                                &text,
                                &stream_client_id,
                                &message_handler_clone,
                                &subscription_manager_clone,
                            )
                            .await
                            {
                                tracing::error!("Error handling client message: {}", e);

                                // Send error response
                                let error_response = serde_json::json!({
                                    "type": "error",
                                    "data": {
                                        "message": format!("Error: {}", e)
                                    }
                                });

                                if let Ok(json) = serde_json::to_string(&error_response) {
                                    let _ = msg_tx.send(WebSocketMessage::Text(json)).await;
                                }
                            }
                        }
                        Message::Ping(data) => {
                            // Respond with pong
                            if let Err(e) = msg_tx.send(WebSocketMessage::Pong(data.to_vec())).await
                            {
                                tracing::error!(
                                    "Failed to send pong to client {}: {}",
                                    stream_client_id,
                                    e
                                );
                            }
                        }
                        Message::Pong(_) => {
                            // Client responded to our ping, update last seen time if needed
                        }
                        Message::Close(_) => {
                            tracing::info!("Client {} requested close", stream_client_id);
                            break;
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    tracing::error!("Error from client {}: {}", stream_client_id, e);
                    break;
                }
            }
        }
    });

    // Wait for either task to complete
    tokio::select! {
        _ = sink_task => tracing::info!("Sink task for client {} completed", client_id),
        _ = stream_task => tracing::info!("Stream task for client {} completed", client_id),
    }

    // Clean up
    message_handler.unregister_client(&client_id).await.ok();
    subscription_manager
        .client_disconnected(&client_id)
        .await
        .ok();

    tracing::info!("Client {} disconnected", client_id);
}

// Parse and handle client messages
pub async fn handle_client_message(
    message: &str,
    client_id: &str,
    message_handler: &Arc<WebSocketMessageHandler>,
    subscription_manager: &Arc<PartitionedSubscriptionManager>,
) -> Result<(), AppError> {
    // Parse message as JSON
    let json: serde_json::Value = serde_json::from_str(message)?;

    // Extract message type
    let msg_type = json
        .get("type")
        .and_then(|t| t.as_str())
        .ok_or_else(|| AppError::InvalidMessageFormat("Missing 'type' field".into()))?;

    match msg_type {
        "subscribe" => {
            // Check if it's a batch subscription
            if let Some(tokens) = json.get("tokens").and_then(|t| t.as_array()) {
                let token_strings: Vec<String> = tokens
                    .iter()
                    .filter_map(|t| t.as_str().map(|s| s.to_string()))
                    .collect();

                if !token_strings.is_empty() {
                    let count = token_strings.len();
                    // Use batch subscribe for multiple tokens
                    subscription_manager
                        .batch_subscribe(token_strings, client_id)
                        .await?;

                    // Send confirmation
                    let response = serde_json::json!({
                        "type": "subscribed_batch",
                        "count": count
                    });
                    message_handler.send_to_client(client_id, &response).await?;
                    return Ok(());
                }
            }

            // Fall back to single token subscription
            let token_address = json
                .get("token")
                .and_then(|t| t.as_str())
                .ok_or_else(|| AppError::InvalidMessageFormat("Missing 'token' field".into()))?;

            subscription_manager
                .subscribe_to_token(client_id, token_address)
                .await?;
            message_handler
                .subscribe_client_to_token(client_id, token_address)
                .await?;

            // Send confirmation
            let response = serde_json::json!({
                "type": "subscribed",
                "data": { "token": token_address }
            });
            message_handler.send_to_client(client_id, &response).await?;
        }
        "unsubscribe" => {
            // Check if it's a request to unsubscribe from all
            if let Some(all) = json.get("all").and_then(|a| a.as_bool()) {
                if all {
                    subscription_manager.unsubscribe_all(client_id).await?;

                    // Send confirmation
                    let response = serde_json::json!({
                        "type": "unsubscribed_all"
                    });
                    message_handler.send_to_client(client_id, &response).await?;
                    return Ok(());
                }
            }

            // Handle regular single token unsubscribe
            let token_address = json
                .get("token")
                .and_then(|t| t.as_str())
                .ok_or_else(|| AppError::InvalidMessageFormat("Missing 'token' field".into()))?;

            subscription_manager
                .unsubscribe_from_token(client_id, token_address)
                .await?;
            message_handler
                .unsubscribe_client_from_token(client_id, token_address)
                .await?;

            // Send confirmation
            let response = serde_json::json!({
                "type": "unsubscribed",
                "data": { "token": token_address }
            });
            message_handler.send_to_client(client_id, &response).await?;
        }
        "ping" => {
            // Simple ping/pong mechanism
            let response = serde_json::json!({
                "type": "pong",
                "timestamp": chrono::Utc::now().timestamp()
            });
            message_handler.send_to_client(client_id, &response).await?;
        }
        _ => {
            // Unknown message type
            return Err(AppError::InvalidMessageFormat(format!(
                "Unknown message type: {}",
                msg_type
            )));
        }
    }

    Ok(())
}

pub async fn connect_websocket(
    url: &str,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, AppError> {
    let mut backoff = ExponentialBackoff {
        initial_interval: Duration::from_millis(100),
        max_interval: Duration::from_secs(10),
        max_elapsed_time: Some(Duration::from_secs(60)),
        ..Default::default()
    };

    loop {
        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                return Ok(ws_stream);
            }
            Err(err) => {
                if let Some(duration) = backoff.next_backoff() {
                    tracing::warn!(
                        "Failed to connect to WebSocket at {}, retrying in {:?}: {}",
                        url,
                        duration,
                        err
                    );
                    tokio::time::sleep(duration).await;
                } else {
                    return Err(AppError::WebSocketError(format!(
                        "Failed to establish WebSocket connection after retries: {}",
                        err
                    )));
                }
            }
        }
    }
}
