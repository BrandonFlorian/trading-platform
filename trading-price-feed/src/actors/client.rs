use axum::extract::ws::{Message, WebSocket};
use futures_util::{stream::SplitSink, stream::SplitStream, SinkExt, StreamExt};
use std::{collections::HashSet, sync::Arc};
use tokio::sync::{broadcast, mpsc, oneshot};
use trading_common::error::AppError;
use trading_common::models::PriceUpdate;

use crate::{
    messages::WebSocketMessage,
    models::{ClientMessage, CoordinatorCommand, ServerMessage},
};

type WsSink = SplitSink<WebSocket, Message>;
type WsStream = SplitStream<WebSocket>;

/// Client actor - manages a single client connection
pub struct ClientActor {
    /// Unique client ID
    client_id: String,

    /// WebSocket sink for sending messages to the client
    ws_sink: WsSink,

    /// WebSocket stream for receiving messages from the client
    ws_stream: WsStream,

    /// Sender for coordinator commands
    coordinator_tx: mpsc::Sender<CoordinatorCommand>,

    /// Receiver for price updates
    price_rx: broadcast::Receiver<PriceUpdate>,

    /// Set of token addresses this client is subscribed to
    subscribed_tokens: HashSet<String>,
}

impl ClientActor {
    /// Create a new client actor
    pub fn new(
        client_id: String,
        socket: WebSocket,
        coordinator_tx: mpsc::Sender<CoordinatorCommand>,
        price_rx: broadcast::Receiver<PriceUpdate>,
    ) -> Self {
        let (ws_sink, ws_stream) = socket.split();

        Self {
            client_id,
            ws_sink,
            ws_stream,
            coordinator_tx,
            price_rx,
            subscribed_tokens: HashSet::new(),
        }
    }

    /// Run the client actor
    pub async fn run(self) {
        tracing::info!("Starting client actor for {}", self.client_id);

        // Create a channel for outgoing messages
        let (msg_tx, mut msg_rx) = mpsc::channel::<WebSocketMessage>(100);

        // Split the client_id and subscribed tokens for the tasks
        let client_id = self.client_id.clone();
        let client_id_for_send = client_id.clone();
        let client_id_for_price = client_id.clone();

        // Store subscribed tokens in an Arc<RwLock> for thread-safe access
        let subscribed_tokens = Arc::new(tokio::sync::RwLock::new(HashSet::new()));
        let subscribed_tokens_for_price = subscribed_tokens.clone();

        // Get what we need from self before spawning tasks
        let coordinator_tx = self.coordinator_tx.clone();
        let mut price_rx = self.price_rx.resubscribe();

        // Take ownership of the sink and stream
        let (mut ws_sink, mut ws_stream) = (self.ws_sink, self.ws_stream);

        // Spawn a task to process outgoing messages
        let send_task = tokio::spawn(async move {
            while let Some(msg) = msg_rx.recv().await {
                match msg {
                    WebSocketMessage::Text(text) => {
                        if let Err(e) = ws_sink.send(Message::Text(text.into())).await {
                            tracing::error!(
                                "Failed to send text message to client {}: {}",
                                client_id_for_send,
                                e
                            );
                            break;
                        }
                    }
                    WebSocketMessage::Binary(data) => {
                        if let Err(e) = ws_sink.send(Message::Binary(data.into())).await {
                            tracing::error!(
                                "Failed to send binary message to client {}: {}",
                                client_id_for_send,
                                e
                            );
                            break;
                        }
                    }
                    WebSocketMessage::Ping(data) => {
                        if let Err(e) = ws_sink.send(Message::Ping(data.into())).await {
                            tracing::error!(
                                "Failed to send ping to client {}: {}",
                                client_id_for_send,
                                e
                            );
                            break;
                        }
                    }
                    WebSocketMessage::Pong(data) => {
                        if let Err(e) = ws_sink.send(Message::Pong(data.into())).await {
                            tracing::error!(
                                "Failed to send pong to client {}: {}",
                                client_id_for_send,
                                e
                            );
                            break;
                        }
                    }
                    WebSocketMessage::Close => {
                        let _ = ws_sink.send(Message::Close(None)).await;
                        break;
                    }
                }
            }
        });

        // Spawn a task to process price updates
        let msg_tx_for_price = msg_tx.clone();

        let price_task = tokio::spawn(async move {
            while let Ok(update) = price_rx.recv().await {
                let tokens = subscribed_tokens_for_price.read().await;
                if tokens.contains(&update.token_address) {
                    // Create server message
                    let server_msg = ServerMessage::PriceUpdate(update);

                    // Serialize and send
                    if let Ok(json) = serde_json::to_string(&server_msg) {
                        if let Err(e) = msg_tx_for_price.send(WebSocketMessage::Text(json)).await {
                            tracing::error!(
                                "Failed to send price update to client {}: {}",
                                client_id_for_price,
                                e
                            );
                            break;
                        }
                    }
                }
            }
        });

        // Process incoming messages from client
        while let Some(result) = ws_stream.next().await {
            match result {
                Ok(msg) => {
                    match msg {
                        Message::Text(text) => {
                            if let Err(e) = handle_client_message(
                                &client_id,
                                &text,
                                msg_tx.clone(),
                                coordinator_tx.clone(),
                                subscribed_tokens.clone(),
                            )
                            .await
                            {
                                tracing::error!(
                                    "Error handling message from client {}: {}",
                                    client_id,
                                    e
                                );

                                // Send error to client
                                let error_msg = ServerMessage::Error {
                                    code: 400,
                                    message: format!("Error: {}", e),
                                };

                                if let Ok(json) = serde_json::to_string(&error_msg) {
                                    let _ = msg_tx.send(WebSocketMessage::Text(json)).await;
                                }
                            }
                        }
                        Message::Binary(_) => {
                            // We don't expect binary messages
                        }
                        Message::Ping(data) => {
                            // Respond with pong through the channel
                            if let Err(e) = msg_tx.send(WebSocketMessage::Pong(data.to_vec())).await
                            {
                                tracing::error!("Failed to send pong message to channel: {}", e);
                                break;
                            }
                        }
                        Message::Pong(_) => {
                            // We don't expect pong messages as we don't send pings
                        }
                        Message::Close(_) => {
                            // Client closed connection
                            break;
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("WebSocket error for client {}: {}", client_id, e);
                    break;
                }
            }
        }

        // Client disconnected, clean up
        handle_disconnect(&client_id, coordinator_tx).await;

        // Abort tasks
        price_task.abort();
        send_task.abort();

        tracing::info!("Client actor for {} shut down", client_id);
    }
}

/// Handle a message from the client - now a standalone function instead of a method
async fn handle_client_message(
    client_id: &str,
    text: &str,
    msg_tx: mpsc::Sender<WebSocketMessage>,
    coordinator_tx: mpsc::Sender<CoordinatorCommand>,
    subscribed_tokens: Arc<tokio::sync::RwLock<HashSet<String>>>,
) -> Result<(), AppError> {
    // Parse client message
    let client_msg: ClientMessage = serde_json::from_str(text)
        .map_err(|e| AppError::JsonParseError(format!("Failed to parse client message: {}", e)))?;

    match client_msg {
        ClientMessage::Subscribe { token_address } => {
            // Subscribe to token
            let (tx, rx) = oneshot::channel();
            coordinator_tx
                .send(CoordinatorCommand::Subscribe {
                    client_id: client_id.to_string(),
                    token_address: token_address.clone(),
                    response_tx: tx,
                })
                .await
                .map_err(|_| {
                    AppError::ChannelSendError("Failed to send subscribe command".to_string())
                })?;

            // Wait for response
            rx.await.map_err(|_| {
                AppError::ChannelReceiveError("Failed to receive subscribe response".to_string())
            })??;

            // Add to subscribed tokens
            {
                let mut tokens = subscribed_tokens.write().await;
                tokens.insert(token_address.clone());
            }

            // Send confirmation to client
            let response = ServerMessage::Response {
                request_type: "subscribe".to_string(),
                success: true,
                message: None,
                data: Some(serde_json::json!({ "token_address": token_address })),
            };

            if let Ok(json) = serde_json::to_string(&response) {
                msg_tx
                    .send(WebSocketMessage::Text(json))
                    .await
                    .map_err(|e| {
                        AppError::WebSocketError(format!("Failed to send response: {}", e))
                    })?;
            }
        }
        ClientMessage::Unsubscribe { token_address } => {
            // Unsubscribe from token
            coordinator_tx
                .send(CoordinatorCommand::Unsubscribe {
                    client_id: client_id.to_string(),
                    token_address: token_address.clone(),
                })
                .await
                .map_err(|_| {
                    AppError::ChannelSendError("Failed to send unsubscribe command".to_string())
                })?;

            // Remove from subscribed tokens
            {
                let mut tokens = subscribed_tokens.write().await;
                tokens.remove(&token_address);
            }

            // Send confirmation to client
            let response = ServerMessage::Response {
                request_type: "unsubscribe".to_string(),
                success: true,
                message: None,
                data: Some(serde_json::json!({ "token_address": token_address })),
            };

            if let Ok(json) = serde_json::to_string(&response) {
                msg_tx
                    .send(WebSocketMessage::Text(json))
                    .await
                    .map_err(|e| {
                        AppError::WebSocketError(format!("Failed to send response: {}", e))
                    })?;
            }
        }
        ClientMessage::UnsubscribeAll => {
            // Get all tokens
            let tokens = {
                let tokens_guard = subscribed_tokens.read().await;
                tokens_guard.clone()
            };

            // Unsubscribe from all tokens
            for token_address in &tokens {
                coordinator_tx
                    .send(CoordinatorCommand::Unsubscribe {
                        client_id: client_id.to_string(),
                        token_address: token_address.clone(),
                    })
                    .await
                    .map_err(|_| {
                        AppError::ChannelSendError("Failed to send unsubscribe command".to_string())
                    })?;
            }

            // Clear subscribed tokens
            {
                let mut tokens_guard = subscribed_tokens.write().await;
                tokens_guard.clear();
            }

            // Send confirmation to client
            let response = ServerMessage::Response {
                request_type: "unsubscribe_all".to_string(),
                success: true,
                message: None,
                data: None,
            };

            if let Ok(json) = serde_json::to_string(&response) {
                msg_tx
                    .send(WebSocketMessage::Text(json))
                    .await
                    .map_err(|e| {
                        AppError::WebSocketError(format!("Failed to send response: {}", e))
                    })?;
            }
        }
        ClientMessage::Ping => {
            // Respond with pong
            let response = ServerMessage::Pong {
                timestamp: chrono::Utc::now().timestamp(),
            };

            if let Ok(json) = serde_json::to_string(&response) {
                msg_tx
                    .send(WebSocketMessage::Text(json))
                    .await
                    .map_err(|e| AppError::WebSocketError(format!("Failed to send pong: {}", e)))?;
            }
        }
    }

    Ok(())
}

/// Handle client disconnection - now a standalone function
async fn handle_disconnect(client_id: &str, coordinator_tx: mpsc::Sender<CoordinatorCommand>) {
    tracing::info!("Client {} disconnected", client_id);

    // Notify coordinator
    if let Err(e) = coordinator_tx
        .send(CoordinatorCommand::ClientDisconnected {
            client_id: client_id.to_string(),
        })
        .await
    {
        tracing::error!(
            "Failed to notify coordinator of client disconnection: {}",
            e
        );
    }
}
