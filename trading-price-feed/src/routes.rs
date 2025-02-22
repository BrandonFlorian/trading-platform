use crate::service::PriceFeedService;
use axum::{
    extract::{ws::Message, Path, Query, State, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{broadcast, mpsc},
    time::Instant,
};
use trading_common::error::AppError;
use uuid::Uuid;

const PING_INTERVAL: Duration = Duration::from_secs(30);
// const PING_TIMEOUT: Duration = Duration::from_secs(60);

pub fn create_router(service: Arc<PriceFeedService>) -> Router {
    Router::new()
        .route("/ws", get(subscribe_price_feed))
        .route("/price/{token_address}", get(get_price))
        .with_state(service)
}

async fn get_price(
    State(service): State<Arc<PriceFeedService>>,
    Path(token_address): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let price = service.get_price(&token_address).await?;
    Ok(axum::Json(price))
}

#[derive(Debug)]
struct WebSocketConnection {
    id: String,
    tokens: HashSet<String>,
    last_ping_pong: Instant,
}

impl WebSocketConnection {
    fn new(tokens: Vec<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            tokens: tokens.into_iter().collect(),
            last_ping_pong: Instant::now(),
        }
    }

    // fn is_alive(&self) -> bool {
    //     self.last_ping_pong.elapsed() < PING_TIMEOUT
    // }

    fn update_ping(&mut self) {
        self.last_ping_pong = Instant::now();
    }
}

async fn subscribe_price_feed(
    State(service): State<Arc<PriceFeedService>>,
    ws: WebSocketUpgrade,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    tracing::info!("WebSocket connection request with params: {:?}", params);

    let tokens = params.get("token").map(|t| {
        t.split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<String>>()
    });

    ws.on_upgrade(move |socket| handle_socket(socket, service, tokens))
}

pub async fn handle_socket(
    socket: axum::extract::ws::WebSocket,
    service: Arc<PriceFeedService>,
    tokens: Option<Vec<String>>,
) {
    let (mut sender, mut receiver) = socket.split();

    // Early return if no tokens provided
    let tokens = match tokens {
        Some(t) => t,
        None => {
            let error_msg = serde_json::json!({
                "error": "No tokens provided"
            });
            if let Ok(msg) = serde_json::to_string(&error_msg) {
                let _ = sender.send(Message::Text(msg.into())).await;
            }
            return;
        }
    };

    let connection = Arc::new(tokio::sync::Mutex::new(WebSocketConnection::new(
        tokens.clone(),
    )));
    let client_id = connection.lock().await.id.clone();
    let tokens_vec = tokens.clone();
    let token_set: HashSet<String> = tokens.into_iter().collect();

    // Set up message channel for sending WebSocket messages
    let (msg_tx, mut msg_rx) = mpsc::channel(100);
    let ping_msg_tx = msg_tx.clone();

    // First get initial prices
    for token_address in &tokens_vec {
        match service.get_price(token_address).await {
            Ok(Some(price_data)) => {
                tracing::info!("Got initial price for {}: {:?}", token_address, price_data);
                // Send directly through the websocket sender
                if let Ok(msg) = serde_json::to_string(&price_data) {
                    if let Err(e) = sender.send(Message::Text(msg.into())).await {
                        tracing::error!("Failed to send initial price: {}", e);
                    }
                }
            }
            Ok(None) => {
                tracing::error!("No price data found for token {}", token_address);
                let error_msg = serde_json::json!({
                    "error": format!("No price data found for token {}", token_address)
                });
                if let Ok(msg) = serde_json::to_string(&error_msg) {
                    let _ = msg_tx.send(Message::Text(msg.into())).await;
                }
            }
            Err(e) => {
                tracing::error!("Error getting initial price for {}: {}", token_address, e);
                let error_msg = serde_json::json!({
                    "error": format!("Failed to get price for token {}: {}", token_address, e)
                });
                if let Ok(msg) = serde_json::to_string(&error_msg) {
                    let _ = msg_tx.send(Message::Text(msg.into())).await;
                }
            }
        }
    }

    // Set up price updates subscription
    let pool_monitor = service.pool_monitor.clone();
    let mut price_rx = pool_monitor.subscribe_to_updates();

    // Set up shutdown signal
    let (shutdown_tx, _) = broadcast::channel(16);
    let shutdown_tx = Arc::new(shutdown_tx);

    // Now subscribe to future updates
    if let Err(e) = pool_monitor
        .batch_subscribe(tokens_vec.clone(), &client_id)
        .await
    {
        tracing::error!("Failed to subscribe to tokens: {}", e);
        let error_msg = serde_json::json!({
            "error": format!("Failed to subscribe to tokens: {}", e)
        });
        if let Ok(msg) = serde_json::to_string(&error_msg) {
            let _ = msg_tx.send(Message::Text(msg.into())).await;
        }
        return;
    }

    // Ping task
    let ping_task = {
        let mut shutdown_rx = shutdown_tx.subscribe();
        let client_id = client_id.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(PING_INTERVAL);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = ping_msg_tx.send(Message::Ping(vec![].into())).await {
                            tracing::error!("Failed to send ping to client {}: {}", client_id, e);
                            break;
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        tracing::info!("Ping task shutting down for client {}", client_id);
                        break;
                    }
                }
            }
        })
    };

    // Message sending task
    let send_task = {
        let client_id = client_id.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(msg) = msg_rx.recv() => {
                        if let Err(e) = sender.send(msg).await {
                            tracing::error!("Failed to send message to client {}: {}", client_id, e);
                            break;
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        tracing::info!("Send task shutting down for client {}", client_id);
                        break;
                    }
                    else => break
                }
            }
        })
    };

    // Message handling task
    let message_task = {
        let mut shutdown_rx = shutdown_tx.subscribe();
        let client_id = client_id.clone();
        let pool_monitor = pool_monitor.clone();
        let connection = connection.clone();
        let tokens = tokens_vec.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = receiver.next() => {
                        match msg {
                            Some(Ok(Message::Pong(_))) => {
                                // Lock connection only when updating ping
                                let mut conn = connection.lock().await;
                                conn.update_ping();
                            }
                            Some(Ok(Message::Close(_))) => {
                                tracing::info!("Client {} requested close", client_id);
                                break;
                            }
                            Some(Ok(msg)) => {
                                tracing::debug!("Received message from client {}: {:?}", client_id, msg);
                            }
                            None => {
                                tracing::info!("Client {} connection closed", client_id);
                                break;
                            }
                            Some(Err(e)) => {
                                tracing::error!("Error from client {}: {}", client_id, e);
                                break;
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        tracing::info!("Message task shutting down for client {}", client_id);
                        break;
                    }
                }
            }

            // Cleanup subscriptions
            let tokens = tokens.clone();
            for token in tokens {
                pool_monitor.unsubscribe_token(&token, &client_id).await;
            }
        })
    };

    // Price updates task
    let price_task = {
        let msg_tx = msg_tx.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let client_id = client_id.clone();
        let token_set = token_set.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = price_rx.recv() => {
                        match result {
                            Ok(update) => {
                                if token_set.contains(&update.token_address) {
                                    if let Ok(msg) = serde_json::to_string(&update) {
                                        if let Err(e) = msg_tx.send(Message::Text(msg.into())).await {
                                            tracing::error!("Failed to queue price update for client {}: {}", client_id, e);
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("Price subscription error for client {}: {}", client_id, e);
                                break;
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        tracing::info!("Price task shutting down for client {}", client_id);
                        break;
                    }
                }
            }
        })
    };

    // Wait for any task to complete
    tokio::select! {
        _ = message_task => tracing::info!("Message task completed for client {}", client_id),
        _ = price_task => tracing::info!("Price task completed for client {}", client_id),
        _ = ping_task => tracing::info!("Ping task completed for client {}", client_id),
        _ = send_task => tracing::info!("Send task completed for client {}", client_id),
    }

    // Signal all tasks to shut down
    let _ = shutdown_tx.send(());

    // Give tasks time to cleanup
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send close frame to client
    let _ = msg_tx.send(Message::Close(None)).await;

    // Clean up
    let tokens = {
        let conn = connection.lock().await;
        conn.tokens.clone()
    };

    for token in tokens {
        pool_monitor.unsubscribe_token(&token, &client_id).await;
    }

    // Final cleanup pause
    tokio::time::sleep(Duration::from_millis(100)).await;

    tracing::info!("Connection closed for client {}", client_id);
}
