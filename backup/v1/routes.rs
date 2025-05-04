use crate::{
    service::PriceFeedService,
    websocket_message_handler::{handle_client_message, handle_client_websocket, WebSocketMessage},
};
use axum::{
    extract::{Path, Query, State, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router,
};
use std::{collections::HashMap, sync::Arc};

use trading_common::error::AppError;
use uuid::Uuid;

pub fn create_router(service: Arc<PriceFeedService>) -> Router {
    Router::new()
        .route("/ws", get(subscribe_price_feed))
        .route("/status", get(get_status))
        .route("/price/{token_address}", get(get_price))
        .with_state(service)
}

async fn get_status(
    State(service): State<Arc<PriceFeedService>>,
) -> Result<impl IntoResponse, AppError> {
    let status = service.subscription_manager.get_worker_status().await?;
    Ok(axum::Json(status))
}

async fn get_price(
    State(service): State<Arc<PriceFeedService>>,
    Path(token_address): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let price = service.get_price(&token_address).await?;
    Ok(axum::Json(price))
}
async fn subscribe_price_feed(
    State(service): State<Arc<PriceFeedService>>,
    ws: WebSocketUpgrade,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    tracing::info!("WebSocket connection request with params: {:?}", params);

    // Generate a unique client ID
    let client_id = Uuid::new_v4().to_string();

    // Clone parameters and service references needed after upgrade
    let service_clone = service.clone();
    let tokens = params.get("token").map(|t| {
        t.split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<String>>()
    });
    let client_id_clone = client_id.clone();

    ws.on_upgrade(move |socket| async move {
        // Process initial subscriptions if any tokens were provided
        if let Some(tokens) = tokens {
            if !tokens.is_empty() {
                if let Err(e) = service_clone
                    .subscription_manager
                    .batch_subscribe(tokens, &client_id_clone)
                    .await
                {
                    tracing::error!("Failed to process initial subscriptions: {}", e);
                }
            }
        }

        // Hand off to the WebSocket handler
        handle_client_websocket(
            socket,
            client_id_clone,
            service_clone.message_handler.clone(),
            service_clone.subscription_manager.clone(),
        )
        .await;
    })
}
