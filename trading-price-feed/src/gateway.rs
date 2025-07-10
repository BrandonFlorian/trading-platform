// src/gateway.rs - Simplified gateway for new vault monitoring system

use axum::{
    extract::{State, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router, Json,
};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use trading_common::models::PriceUpdate;

/// Application state
pub struct AppState {
    /// Sender for price updates (for compatibility)
    price_tx: broadcast::Sender<PriceUpdate>,
    
    /// Mock coordinator for compatibility (not used)
    _coordinator_tx: mpsc::Sender<String>, // Placeholder
}

/// Create a new router
pub fn create_router(
    coordinator_tx: mpsc::Sender<String>, // Simplified placeholder
    price_tx: broadcast::Sender<PriceUpdate>,
) -> Router {
    let app_state = Arc::new(AppState {
        price_tx,
        _coordinator_tx: coordinator_tx,
    });

    Router::new()
        .route("/health", get(health_check))
        .route("/status", get(status))
        .route("/ws", get(handle_ws_connection))
        .with_state(app_state)
}

/// Health check endpoint
pub async fn health_check() -> impl IntoResponse {
    Json(json!({
        "status": "healthy",
        "service": "trading-price-feed",
        "version": "2.0.0-vault-monitoring"
    }))
}

/// Status endpoint
pub async fn status() -> impl IntoResponse {
    Json(json!({
        "status": "running",
        "implementation": "vault-based-monitoring",
        "features": [
            "real-time-vault-subscriptions",
            "zero-rpc-price-updates", 
            "connection-pooling",
            "auto-recovery"
        ]
    }))
}

/// Handler for WebSocket connections (simplified for new implementation)
pub async fn handle_ws_connection(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    tracing::info!("WebSocket connection request received");
    
    ws.on_upgrade(move |socket| async move {
        // For now, just provide a simple WebSocket that can receive price updates
        let mut price_rx = state.price_tx.subscribe();
        let (mut sender, mut receiver) = socket.split();
        
        use axum::extract::ws::Message;
        use futures_util::{SinkExt, StreamExt};
        
        // Send welcome message
        let welcome = json!({
            "type": "welcome",
            "message": "Connected to vault-based price feed",
            "version": "2.0.0"
        });
        
        if let Err(e) = sender.send(Message::Text(welcome.to_string().into())).await {
            tracing::error!("Failed to send welcome message: {}", e);
            return;
        }
        
        loop {
            tokio::select! {
                // Handle incoming messages from client
                msg = receiver.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            tracing::info!("Received: {}", text);
                            // Echo for now
                            let response = json!({
                                "type": "echo",
                                "data": text.to_string()
                            });
                            if let Err(e) = sender.send(Message::Text(response.to_string().into())).await {
                                tracing::error!("Failed to send echo: {}", e);
                                break;
                            }
                        }
                        Some(Ok(Message::Close(_))) => {
                            tracing::info!("Client disconnected");
                            break;
                        }
                        Some(Err(e)) => {
                            tracing::error!("WebSocket error: {}", e);
                            break;
                        }
                        None => {
                            tracing::info!("WebSocket stream ended");
                            break;
                        }
                        _ => {}
                    }
                }
                // Handle price updates
                price_update = price_rx.recv() => {
                    match price_update {
                        Ok(update) => {
                            let message = json!({
                                "type": "price_update",
                                "data": update
                            });
                            if let Err(e) = sender.send(Message::Text(message.to_string().into())).await {
                                tracing::error!("Failed to send price update: {}", e);
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::error!("Price receiver error: {}", e);
                        }
                    }
                }
            }
        }
        
        tracing::info!("WebSocket connection closed");
    })
}