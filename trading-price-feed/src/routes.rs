use crate::service::PriceFeedService;
use axum::{
    extract::{ws::Message, Path, Query, State, WebSocketUpgrade},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{delete, get, post},
    Router,
};
use futures_util::SinkExt;
use futures_util::StreamExt;
use serde::Deserialize;
use std::{collections::HashMap, sync::Arc};
use trading_common::error::AppError;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct SubscribeRequest {
    pub token_address: String,
    pub client_id: String,
}

pub fn create_router(service: Arc<PriceFeedService>) -> Router {
    Router::new()
        .route("/ws", get(subscribe_price_feed))
        .route("/price/{token_address}", get(get_price))
        //.route("/prices", get(get_all_prices))
        .route("/subscribe", post(subscribe_token))
        .route(
            "/unsubscribe/{token_address}/{client_id}",
            delete(unsubscribe_token),
        )
        .with_state(service)
}

async fn get_price(
    State(service): State<Arc<PriceFeedService>>,
    Path(token_address): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let price = service.get_price(&token_address).await?;
    Ok(Json(price))
}

#[axum::debug_handler]
async fn subscribe_token(
    State(service): State<Arc<PriceFeedService>>,
    Json(request): Json<SubscribeRequest>,
) -> Result<Json<()>, AppError> {
    let pool_monitor = service.pool_monitor.clone();
    pool_monitor
        .subscribe_token(&request.token_address, &request.client_id)
        .await?;

    Ok(Json(()))
}

async fn unsubscribe_token(
    State(service): State<Arc<PriceFeedService>>,
    Path((token_address, client_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    service
        .pool_monitor
        .unsubscribe_token(&token_address, &client_id)
        .await;
    Ok(StatusCode::OK)
}

async fn subscribe_price_feed(
    State(service): State<Arc<PriceFeedService>>,
    ws: WebSocketUpgrade,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    // Immediately log what we received
    tracing::warn!(
        "WebSocket connection request received with params: {:?}",
        params
    );

    // Extract token before upgrade
    let token = params.get("token").cloned();
    tracing::warn!("Token extracted from params: {:?}", token);

    ws.on_upgrade(move |socket| handle_socket(socket, service, token))
}

pub async fn handle_socket(
    socket: axum::extract::ws::WebSocket,
    service: Arc<PriceFeedService>,
    token: Option<String>,
) {
    let (mut sender, mut receiver) = socket.split();
    let client_id = Uuid::new_v4().to_string();

    tracing::info!("WebSocket upgraded for client {}", client_id);

    if let Some(token_addr) = token {
        let pool_monitor = service.pool_monitor.clone();
        // Move price sender initialization before subscription
        let mut rx = pool_monitor.subscribe_to_updates();

        match pool_monitor.subscribe_token(&token_addr, &client_id).await {
            Ok(()) => {
                tracing::info!("Successfully subscribed to token {}", token_addr);

                // Create a task for handling WebSocket messages
                let message_task = tokio::spawn({
                    let mut receiver = receiver;
                    let client_id = client_id.clone();
                    let token_addr = token_addr.clone();
                    let pool_monitor = pool_monitor.clone();

                    async move {
                        while let Some(Ok(_msg)) = receiver.next().await {
                            tracing::debug!("Received message from client {}", client_id);
                        }
                        pool_monitor
                            .unsubscribe_token(&token_addr, &client_id)
                            .await;
                        tracing::info!("Client {} disconnected", client_id);
                    }
                });

                // Create a task for handling price updates
                let price_task = tokio::spawn({
                    let mut sender = sender;
                    let client_id = client_id.clone();
                    let token_addr = token_addr.clone();

                    async move {
                        while let Ok(update) = rx.recv().await {
                            if update.token_address == token_addr {
                                if let Ok(msg) = serde_json::to_string(&update) {
                                    if let Err(e) = sender.send(Message::Text(msg.into())).await {
                                        tracing::error!(
                                            "Failed to send price update to client {}: {}",
                                            client_id,
                                            e
                                        );
                                        break;
                                    }
                                }
                            }
                        }
                    }
                });

                // Wait for either task to complete
                tokio::select! {
                    _ = message_task => tracing::info!("Message task completed"),
                    _ = price_task => tracing::info!("Price task completed"),
                }

                // Cleanup
                pool_monitor
                    .unsubscribe_token(&token_addr, &client_id)
                    .await;
            }
            Err(e) => {
                tracing::error!("Failed to subscribe to token {}: {}", token_addr, e);
                let error_msg = serde_json::json!({
                    "error": format!("Failed to subscribe to token: {}", e)
                });
                if let Ok(msg) = serde_json::to_string(&error_msg) {
                    let _ = sender.send(Message::Text(msg.into())).await;
                }
            }
        }
    } else {
        tracing::error!("No token provided for client {}", client_id);
        let error_msg = serde_json::json!({
            "error": "No token provided"
        });
        if let Ok(msg) = serde_json::to_string(&error_msg) {
            let _ = sender.send(Message::Text(msg.into())).await;
        }
    }
}
