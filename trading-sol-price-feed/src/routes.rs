use axum::{
    extract::{State, WebSocketUpgrade},
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use futures_util::SinkExt;
use futures_util::StreamExt;
use std::sync::Arc;
use trading_common::error::AppError;

use crate::service::SolPriceFeedService;

pub fn create_router(service: Arc<SolPriceFeedService>) -> Router {
    Router::new()
        .route("/price", get(get_price))
        .route("/ws", get(subscribe_price_feed))
        .with_state(service)
}

async fn get_price(
    State(service): State<Arc<SolPriceFeedService>>,
) -> Result<Json<Option<trading_common::models::SolPriceUpdate>>, AppError> {
    Ok(Json(service.get_current_price()))
}

async fn subscribe_price_feed(
    State(service): State<Arc<SolPriceFeedService>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, service))
}

async fn handle_socket(socket: axum::extract::ws::WebSocket, service: Arc<SolPriceFeedService>) {
    let (mut sender, _) = socket.split();
    let mut rx = service.subscribe_to_updates();

    while let Ok(price) = rx.recv().await {
        if let Ok(msg) = serde_json::to_string(&price) {
            if sender
                .send(axum::extract::ws::Message::Text(msg.into()))
                .await
                .is_err()
            {
                break;
            }
        }
    }
}
