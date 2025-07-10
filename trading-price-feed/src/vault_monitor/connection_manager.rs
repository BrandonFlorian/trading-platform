use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use trading_common::error::AppError;
use uuid::Uuid;

use super::SubscriptionRoute;

/// Maximum subscriptions per WebSocket connection (Solana's limit)
const MAX_SUBSCRIPTIONS_PER_CONNECTION: usize = 100;

/// WebSocket connection wrapper
pub struct WebSocketConnection {
    pub id: String,
    pub stream: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    pub subscription_count: usize,
    pub is_healthy: bool,
    pub last_ping: std::time::Instant,
}

impl WebSocketConnection {
    pub fn new(id: String, stream: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>) -> Self {
        Self {
            id,
            stream,
            subscription_count: 0,
            is_healthy: true,
            last_ping: std::time::Instant::now(),
        }
    }

    pub fn can_accept_subscription(&self) -> bool {
        self.is_healthy && self.subscription_count < MAX_SUBSCRIPTIONS_PER_CONNECTION
    }

    pub fn add_subscription(&mut self) {
        self.subscription_count += 1;
    }

    pub fn remove_subscription(&mut self) {
        if self.subscription_count > 0 {
            self.subscription_count -= 1;
        }
    }
}

/// Manages multiple WebSocket connections for load balancing
pub struct WebSocketManager {
    connections: Arc<RwLock<HashMap<String, WebSocketConnection>>>,
    available_connections: Arc<RwLock<VecDeque<String>>>,
    subscription_routes: Arc<RwLock<HashMap<String, SubscriptionRoute>>>,
    request_id_counter: Arc<AtomicU64>,
    ws_url: String,
    message_sender: mpsc::Sender<(String, Value)>, // (subscription_id, message)
    pending_confirmations: Arc<RwLock<HashMap<u64, tokio::sync::oneshot::Sender<String>>>>, // request_id -> subscription_id
}

impl WebSocketManager {
    pub fn new(ws_url: String) -> (Self, mpsc::Receiver<(String, Value)>) {
        let (message_sender, message_receiver) = mpsc::channel(1000);

        let manager = Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            available_connections: Arc::new(RwLock::new(VecDeque::new())),
            subscription_routes: Arc::new(RwLock::new(HashMap::new())),
            request_id_counter: Arc::new(AtomicU64::new(1)),
            ws_url,
            message_sender,
            pending_confirmations: Arc::new(RwLock::new(HashMap::new())),
        };

        (manager, message_receiver)
    }

    /// Get or create a connection that can accept new subscriptions
    pub async fn get_available_connection(&self) -> Result<String, AppError> {
        // Try to find an existing connection with capacity
        {
            let connections = self.connections.read().await;
            let mut available = self.available_connections.write().await;

            for connection_id in available.iter() {
                if let Some(conn) = connections.get(connection_id) {
                    if conn.can_accept_subscription() {
                        return Ok(connection_id.clone());
                    }
                }
            }
        }

        // Create a new connection if none available
        let connection_id = self.create_new_connection().await?;
        Ok(connection_id)
    }

    /// Create a new WebSocket connection
    async fn create_new_connection(&self) -> Result<String, AppError> {
        let connection_id = Uuid::new_v4().to_string();

        tracing::info!("Creating new WebSocket connection: {}", connection_id);

        let (ws_stream, _) = connect_async(&self.ws_url).await.map_err(|e| {
            AppError::WebSocketError(format!("Failed to connect to WebSocket: {}", e))
        })?;

        let connection = WebSocketConnection::new(connection_id.clone(), ws_stream);

        // Start message processing task for this connection
        self.start_connection_task(&connection_id).await;

        // Store the connection
        {
            let mut connections = self.connections.write().await;
            let mut available = self.available_connections.write().await;

            connections.insert(connection_id.clone(), connection);
            available.push_back(connection_id.clone());
        }

        tracing::info!(
            "WebSocket connection {} created successfully",
            connection_id
        );
        Ok(connection_id)
    }

    /// Start a task to handle messages for a specific connection
    async fn start_connection_task(&self, connection_id: &str) {
        let connection_id = connection_id.to_string();
        let connections = Arc::clone(&self.connections);
        let subscription_routes = Arc::clone(&self.subscription_routes);
        let message_sender = self.message_sender.clone();
        let pending_confirmations = Arc::clone(&self.pending_confirmations);

        tokio::spawn(async move {
            tracing::info!("Started WebSocket message processing task for connection: {}", connection_id);
            
            tracing::info!("WebSocket stream ready for connection: {}", connection_id);
            
            loop {
                // Use a timeout to avoid holding the lock indefinitely
                let message = {
                    let connections_guard = connections.read().await;
                    if let Some(conn) = connections_guard.get(&connection_id) {
                        // Check if connection is still healthy
                        if !conn.is_healthy {
                            break;
                        }
                        drop(connections_guard);
                        
                        // Now get a write lock and read the message
                        let mut connections_guard = connections.write().await;
                        if let Some(conn) = connections_guard.get_mut(&connection_id) {
                            tracing::debug!("Waiting for next WebSocket message on connection: {}", connection_id);
                            
                            // Use timeout to prevent indefinite blocking
                            match tokio::time::timeout(
                                std::time::Duration::from_millis(100),
                                conn.stream.next()
                            ).await {
                                Ok(Some(Ok(msg))) => {
                                    tracing::info!("✅ Received WebSocket message on connection: {}", connection_id);
                                    Some(msg)
                                },
                                Ok(Some(Err(e))) => {
                                    tracing::error!(
                                        "WebSocket error on connection {}: {}",
                                        connection_id,
                                        e
                                    );
                                    conn.is_healthy = false;
                                    break;
                                }
                                Ok(None) => {
                                    tracing::warn!("WebSocket connection {} closed", connection_id);
                                    conn.is_healthy = false;
                                    break;
                                }
                                Err(_) => {
                                    // Timeout - continue loop to check for new messages
                                    None
                                }
                            }
                        } else {
                            tracing::error!("Connection {} not found in connections map", connection_id);
                            break;
                        }
                    } else {
                        tracing::error!("Connection {} not found in connections map", connection_id);
                        break;
                    }
                };

                if let Some(msg) = message {
                    if let Err(e) = Self::process_message(
                        &connection_id,
                        msg,
                        &subscription_routes,
                        &message_sender,
                        &pending_confirmations,
                    )
                    .await
                    {
                        tracing::error!(
                            "Error processing message on connection {}: {}",
                            connection_id,
                            e
                        );
                    }
                }
            }

            // Clean up dead connection
            let mut connections_guard = connections.write().await;
            connections_guard.remove(&connection_id);
            tracing::info!("Connection {} cleaned up", connection_id);
        });
    }

    /// Process incoming WebSocket messages
    async fn process_message(
        connection_id: &str,
        message: Message,
        subscription_routes: &Arc<RwLock<HashMap<String, SubscriptionRoute>>>,
        message_sender: &mpsc::Sender<(String, Value)>,
        pending_confirmations: &Arc<RwLock<HashMap<u64, tokio::sync::oneshot::Sender<String>>>>,
    ) -> Result<(), AppError> {
        match message {
            Message::Text(text) => {
                tracing::debug!("Received WebSocket message: {}", text);
                let json: Value = serde_json::from_str(&text).map_err(|e| {
                    AppError::JsonParseError(format!("Failed to parse WebSocket message: {}", e))
                })?;

                // Handle subscription confirmation responses
                if let Some(result) = json.get("result") {
                    if let Some(id) = json.get("id").and_then(|id| id.as_u64()) {
                        // Subscription ID can be either a string or a number
                        let subscription_id = if let Some(s) = result.as_str() {
                            s.to_string()
                        } else if let Some(n) = result.as_u64() {
                            n.to_string()
                        } else {
                            tracing::warn!("Received subscription response but result is neither string nor number: {:?}", result);
                            return Ok(());
                        };

                        tracing::info!(
                            "Received subscription confirmation: request_id={}, subscription_id={}",
                            id,
                            subscription_id
                        );
                        let mut confirmations = pending_confirmations.write().await;
                        if let Some(tx) = confirmations.remove(&id) {
                            let _ = tx.send(subscription_id);
                        }
                    }
                }
                // Handle subscription notifications
                else if let Some(params) = json.get("params") {
                    if let Some(subscription) = params.get("subscription").and_then(|s| s.as_str())
                    {
                        tracing::debug!(
                            "Received subscription notification: subscription_id={}",
                            subscription
                        );
                        let routes = subscription_routes.read().await;
                        if routes.contains_key(subscription) {
                            tracing::info!(
                                "Forwarding vault update for subscription: {}",
                                subscription
                            );
                            if let Err(e) =
                                message_sender.send((subscription.to_string(), json)).await
                            {
                                tracing::error!("Failed to forward message: {}", e);
                            }
                        } else {
                            tracing::warn!(
                                "Received notification for unknown subscription: {}",
                                subscription
                            );
                        }
                    }
                }
                // Handle error messages
                else if let Some(error) = json.get("error") {
                    if let Some(id) = json.get("id").and_then(|id| id.as_u64()) {
                        let mut confirmations = pending_confirmations.write().await;
                        if let Some(tx) = confirmations.remove(&id) {
                            tracing::error!("Subscription error for request_id {}: {}", id, error);
                            // Send error to the confirmation waiter
                            let _ = tx.send("error".to_string());
                        }
                    } else {
                        tracing::error!("Received error message: {}", error);
                    }
                }
                // Handle any other message types
                else {
                    tracing::debug!("Received unknown message type: {}", json);
                }
            }
            Message::Ping(data) => {
                let mut connections = subscription_routes.write().await;
                // Send pong response - we'll handle this in the connection loop
                drop(connections);
            }
            Message::Close(_) => {
                tracing::info!(
                    "WebSocket connection {} received close message",
                    connection_id
                );
            }
            _ => {}
        }

        Ok(())
    }

    /// Subscribe to an account on the best available connection
    pub async fn subscribe_to_account(
        &self,
        account: &solana_sdk::pubkey::Pubkey,
        route: SubscriptionRoute,
    ) -> Result<String, AppError> {
        self.subscribe_to_account_with_commitment(account, route, "processed").await
    }

    /// Subscribe to an account with specific commitment level
    pub async fn subscribe_to_account_with_commitment(
        &self,
        account: &solana_sdk::pubkey::Pubkey,
        route: SubscriptionRoute,
        commitment: &str,
    ) -> Result<String, AppError> {
        let connection_id = self.get_available_connection().await?;

        let request_id = self.request_id_counter.fetch_add(1, Ordering::SeqCst);

        let subscription_msg = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "accountSubscribe",
            "params": [
                account.to_string(),
                {
                    "encoding": "base64",
                    "commitment": commitment
                }
            ]
        });

        // Send subscription request
        tracing::info!(
            "Sending accountSubscribe request for {}: {}",
            account,
            subscription_msg
        );
        {
            let mut connections = self.connections.write().await;
            if let Some(conn) = connections.get_mut(&connection_id) {
                conn.stream
                    .send(Message::Text(subscription_msg.to_string().into()))
                    .await
                    .map_err(|e| {
                        AppError::WebSocketError(format!(
                            "Failed to send subscription request: {}",
                            e
                        ))
                    })?;

                conn.add_subscription();
            } else {
                return Err(AppError::WebSocketError("Connection not found".to_string()));
            }
        }

        // Wait for subscription confirmation
        let subscription_id = self
            .wait_for_subscription_confirmation(&connection_id, request_id)
            .await?;

        if subscription_id == "error" {
            return Err(AppError::WebSocketError("Subscription failed".to_string()));
        }

        // Store the route
        {
            let mut routes = self.subscription_routes.write().await;
            routes.insert(subscription_id.clone(), route);
        }

        tracing::info!(
            "Successfully subscribed to account {} with subscription ID: {}",
            account,
            subscription_id
        );
        Ok(subscription_id)
    }

    /// Subscribe to Raydium program to monitor all pool account changes
    pub async fn subscribe_to_raydium_program(
        &self,
        route: SubscriptionRoute,
    ) -> Result<String, AppError> {
        let connection_id = self.get_available_connection().await?;
        let request_id = self.request_id_counter.fetch_add(1, Ordering::SeqCst);
        
        // Raydium V4 program ID
        let raydium_program = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
        
        let subscription_msg = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "programSubscribe",
            "params": [
                raydium_program,
                {
                    "encoding": "base64",
                    "commitment": "processed",
                    "filters": [
                        {
                            "dataSize": 752  // Size of Raydium pool account
                        }
                    ]
                }
            ]
        });

        tracing::info!(
            "Sending programSubscribe request for Raydium: {}",
            subscription_msg
        );
        
        {
            let mut connections = self.connections.write().await;
            if let Some(conn) = connections.get_mut(&connection_id) {
                conn.stream
                    .send(Message::Text(subscription_msg.to_string().into()))
                    .await
                    .map_err(|e| {
                        AppError::WebSocketError(format!(
                            "Failed to send subscription request: {}",
                            e
                        ))
                    })?;
                conn.add_subscription();
            } else {
                return Err(AppError::WebSocketError("Connection not found".to_string()));
            }
        }

        let subscription_id = self
            .wait_for_subscription_confirmation(&connection_id, request_id)
            .await?;
            
        if subscription_id == "error" {
            return Err(AppError::WebSocketError("Program subscription failed".to_string()));
        }

        {
            let mut routes = self.subscription_routes.write().await;
            routes.insert(subscription_id.clone(), route);
        }

        tracing::info!(
            "Successfully subscribed to Raydium program with subscription ID: {}",
            subscription_id
        );
        Ok(subscription_id)
    }

    /// Wait for subscription confirmation from Solana
    async fn wait_for_subscription_confirmation(
        &self,
        connection_id: &str,
        request_id: u64,
    ) -> Result<String, AppError> {
        // Create a channel to wait for the subscription confirmation
        let (tx, rx) = tokio::sync::oneshot::channel();

        // Store the confirmation waiter
        {
            let mut confirmations = self.pending_confirmations.write().await;
            confirmations.insert(request_id, tx);
        }

        // Wait for confirmation with timeout
        tokio::time::timeout(std::time::Duration::from_secs(10), rx)
            .await
            .map_err(|_| AppError::WebSocketError("Subscription confirmation timeout".to_string()))?
            .map_err(|_| {
                AppError::WebSocketError("Subscription confirmation cancelled".to_string())
            })
    }

    /// Unsubscribe from an account
    pub async fn unsubscribe(&self, subscription_id: &str) -> Result<(), AppError> {
        let route = {
            let mut routes = self.subscription_routes.write().await;
            routes.remove(subscription_id)
        };

        if let Some(_route) = route {
            // Find the connection and send unsubscribe message
            // Implementation depends on tracking which connection has which subscription
            tracing::info!("Unsubscribed from subscription: {}", subscription_id);
        }

        Ok(())
    }

    /// Get health status of all connections
    pub async fn get_connection_health(&self) -> HashMap<String, bool> {
        let connections = self.connections.read().await;
        connections
            .iter()
            .map(|(id, conn)| (id.clone(), conn.is_healthy))
            .collect()
    }
}
