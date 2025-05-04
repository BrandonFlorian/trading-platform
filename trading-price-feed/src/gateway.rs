// src/gateway.rs

use axum::{
    extract::{Path, State, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router,
};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot};
use trading_common::models::PriceUpdate;
use uuid::Uuid;

use crate::{actors::ClientActor, models::CoordinatorCommand};

/// Application state
pub struct AppState {
    /// Sender for coordinator commands
    coordinator_tx: mpsc::Sender<CoordinatorCommand>,

    /// Sender for price updates
    price_tx: broadcast::Sender<PriceUpdate>,
}

/// Create a new router
pub fn create_router(
    coordinator_tx: mpsc::Sender<CoordinatorCommand>,
    price_tx: broadcast::Sender<PriceUpdate>,
) -> Router {
    let app_state = Arc::new(AppState {
        coordinator_tx,
        price_tx,
    });

    Router::new()
        .route("/ws", get(handle_ws_connection))
        .route("/ws/token_address", get(handle_ws_connection))
        .with_state(app_state)
}

/// Handler for WebSocket connections
pub async fn handle_ws_connection(
    ws: WebSocketUpgrade,
    token_address: Option<Path<String>>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    tracing::info!(
        "WebSocket connection request with token_address: {:?}",
        token_address
    );
    ws.on_upgrade(move |socket| async move {
        // Generate a unique client ID
        let client_id = Uuid::new_v4().to_string();
        tracing::info!("Client ID: {}", client_id);
        // Get price subscription
        let price_rx = state.price_tx.subscribe();
        tracing::info!("Price subscription: {:?}", price_rx);

        // Create client actor
        let client = ClientActor::new(
            client_id.clone(),
            socket,
            state.coordinator_tx.clone(),
            price_rx,
        );

        tracing::info!("Client actor created");

        // If token_address was provided in the path, auto-subscribe
        if let Some(Path(token)) = token_address {
            if !token.is_empty() {
                tracing::info!("Attempting auto-subscription to token: {}", token);
                let (tx, rx) = oneshot::channel();
                if let Err(e) = state
                    .coordinator_tx
                    .send(CoordinatorCommand::Subscribe {
                        client_id: client_id.clone(),
                        token_address: token.clone(),
                        response_tx: tx,
                    })
                    .await
                {
                    tracing::error!("Failed to send auto-subscribe command: {}", e);
                    return;
                }

                tracing::info!("Auto-subscribe command sent");

                // Wait for response
                match rx.await {
                    Ok(Ok(_)) => {
                        tracing::info!("Auto-subscribed client {} to token {}", client_id, token);
                    }
                    Ok(Err(e)) => {
                        tracing::error!("Auto-subscribe failed: {}", e);
                        return;
                    }
                    Err(e) => {
                        tracing::error!("Failed to receive auto-subscribe response: {}", e);
                        return;
                    }
                }
            }
        }

        // Run the client actor
        client.run().await;
        tracing::info!("Client actor run");
    })
}
