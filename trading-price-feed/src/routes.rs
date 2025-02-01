use crate::service::PriceFeedService;
use axum::{
    extract::{Path, Query, State, WebSocketUpgrade},
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
        .route("/prices", get(get_all_prices))
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

async fn get_all_prices(
    State(service): State<Arc<PriceFeedService>>,
) -> Result<impl IntoResponse, AppError> {
    let prices = service.get_all_prices().await;
    Ok(Json(prices))
}

#[axum::debug_handler]
async fn subscribe_token(
    State(service): State<Arc<PriceFeedService>>,
    Json(request): Json<SubscribeRequest>,
) -> Result<Json<()>, AppError> {
    let pool_monitor = service.pool_monitor.write().await;
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
        .write()
        .await
        .unsubscribe_token(&token_address, &client_id)
        .await;
    Ok(StatusCode::OK)
}

pub async fn subscribe_price_feed(
    State(service): State<Arc<PriceFeedService>>,
    ws: WebSocketUpgrade,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, service, params))
}

async fn handle_socket(
    socket: axum::extract::ws::WebSocket,
    service: Arc<PriceFeedService>,
    params: HashMap<String, String>,
) {
    let (mut sender, _) = socket.split();
    let client_id = Uuid::new_v4().to_string();

    if let Some(token_addr) = params.get("token") {
        let pool_monitor = service.pool_monitor.write().await;
        if let Err(e) = pool_monitor.subscribe_token(token_addr, &client_id).await {
            eprintln!("Failed to subscribe to token {}: {}", token_addr, e);
            return;
        }

        let mut rx = pool_monitor.subscribe_to_updates();
        drop(pool_monitor); // Release lock early

        while let Ok(update) = rx.recv().await {
            let msg = serde_json::to_string(&update)
                .map(|text| axum::extract::ws::Message::Text(text.into()))
                .unwrap_or_else(|e| {
                    eprintln!("Failed to serialize price update: {}", e);
                    axum::extract::ws::Message::Close(None)
                });

            if sender.send(msg).await.is_err() {
                break;
            }
        }

        // Cleanup subscription
        let pool_monitor = service.pool_monitor.write().await;
        pool_monitor.unsubscribe_token(token_addr, &client_id).await;
    }
}
