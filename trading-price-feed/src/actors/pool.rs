// src/actors/pool.rs

use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use trading_common::dex::DexType;
use trading_common::error::AppError;
use trading_common::models::PriceUpdate;
use trading_common::redis::RedisPool;

use crate::models::{PoolCommand, PoolEvent, PoolState};
use crate::raydium::{RaydiumPool, RaydiumPoolFinder};

/// Responsible for managing a single token's price feed
pub struct PoolActor {
    /// Token address (mint)
    token_address: String,

    /// Pool data
    pool: Option<Arc<RaydiumPool>>,

    /// Receiver for commands
    command_rx: mpsc::Receiver<PoolCommand>,

    /// Sender for events
    event_tx: broadcast::Sender<PoolEvent>,

    /// Solana RPC client
    rpc_client: Arc<RpcClient>,

    /// Redis client for caching and SOL price
    redis_client: Arc<RedisPool>,

    /// Last update time
    last_update: Instant,

    /// WebSocket connection to Solana RPC
    ws_connection: Option<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>,

    /// Subscription ID for the WebSocket
    subscription_id: Option<String>,

    /// Current state of the pool
    state: PoolState,

    /// Last received price update
    last_price: Option<PriceUpdate>,

    /// Retry attempts counter
    retry_count: u32,
}

impl PoolActor {
    /// Create a new pool actor
    pub fn new(
        token_address: String,
        command_rx: mpsc::Receiver<PoolCommand>,
        event_tx: broadcast::Sender<PoolEvent>,
        rpc_client: Arc<RpcClient>,
        redis_client: Arc<RedisPool>,
    ) -> Self {
        Self {
            token_address,
            pool: None,
            command_rx,
            event_tx,
            rpc_client,
            redis_client,
            last_update: Instant::now(),
            ws_connection: None,
            subscription_id: None,
            state: PoolState::Initializing,
            last_price: None,
            retry_count: 0,
        }
    }

    /// Start the pool actor
    pub async fn run(mut self) {
        tracing::info!("Starting pool actor for token {}", self.token_address);

        // Initialize pool when started
        if let Err(e) = self.initialize_pool().await {
            tracing::error!(
                "Failed to initialize pool for {}: {}",
                self.token_address,
                e
            );
            self.update_state(PoolState::Failed {
                error: e.to_string(),
            })
            .await;
            return;
        }

        let mut health_check_interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                Some(cmd) = self.command_rx.recv() => {
                    match cmd {
                        PoolCommand::Initialize { response_tx } => {
                            let result = self.initialize_pool().await;
                            let _ = response_tx.send(result);
                        }
                        PoolCommand::GetPrice { response_tx } => {
                            let result = self.get_current_price().await;
                            let _ = response_tx.send(result);
                        }
                        PoolCommand::Shutdown { response_tx } => {
                            let result = self.handle_shutdown().await;
                            let _ = response_tx.send(result);
                            break;
                        }
                    }
                }
                _ = health_check_interval.tick() => {
                    self.check_health().await;
                }
                ws_msg = async {
                    if let Some(ws) = &mut self.ws_connection {
                        ws.next().await
                    } else {
                        futures_util::future::pending().await
                    }
                }, if self.ws_connection.is_some() => {
                    match ws_msg {
                        Some(Ok(msg)) => {
                            if let Err(e) = self.handle_ws_message(msg).await {
                                tracing::error!("Error handling WS message: {}", e);
                                self.handle_ws_error(e).await;
                            }
                        }
                        Some(Err(e)) => {
                            tracing::error!("WebSocket error: {}", e);
                            self.handle_ws_error(AppError::WebSocketError(e.to_string())).await;
                        }
                        None => {
                            tracing::warn!("WebSocket connection closed");
                            self.handle_ws_disconnect().await;
                        }
                    }
                }
                else => {
                    // If none of the above match and we're in a failed state, break the loop
                    if matches!(self.state, PoolState::Failed { .. }) {
                        tracing::error!("Pool actor in failed state, exiting");
                        break;
                    }
                }
            }
        }

        tracing::info!("Pool actor for {} shutting down", self.token_address);
    }

    /// Find and initialize the pool
    async fn initialize_pool(&mut self) -> Result<(), AppError> {
        tracing::info!("Initializing pool for token {}", self.token_address);
        self.update_state(PoolState::Initializing).await;

        // 1. Find the Raydium pool for this token using PoolFinder
        let pool_finder =
            RaydiumPoolFinder::new(self.rpc_client.clone(), self.redis_client.clone());
        let pool = pool_finder.find_pool(&self.token_address).await?;

        tracing::info!(
            "Found Raydium pool {} for token {}",
            pool.address.to_string(),
            self.token_address
        );

        // 2. Get the initial price
        let sol_price = self.get_sol_price().await?;
        let price_data = pool.fetch_price_data(&self.rpc_client, sol_price).await?;
        tracing::info!("Price data: {:?}", price_data);
        // 3. Create price update
        let price_update = PriceUpdate {
            token_address: self.token_address.clone(),
            price_sol: price_data.price_sol,
            price_usd: price_data
                .price_usd
                .or(Some(price_data.price_sol * sol_price)),
            market_cap: price_data.market_cap,
            timestamp: chrono::Utc::now().timestamp(),
            dex_type: DexType::Raydium,
            liquidity: price_data.liquidity,
            liquidity_usd: price_data.liquidity_usd,
            pool_address: Some(pool.address.to_string()),
            volume_24h: price_data.volume_24h,
            volume_6h: price_data.volume_6h,
            volume_1h: price_data.volume_1h,
            volume_5m: price_data.volume_5m,
        };

        self.last_price = Some(price_update.clone());

        tracing::info!("Price update: {:?}", price_update);
        // 4. Store pool
        self.pool = Some(Arc::new(pool));
        tracing::info!("Pool stored");
        // 5. Set up WebSocket subscription
        self.setup_websocket_subscription().await?;
        tracing::info!("WebSocket subscription set up");
        // 6. Update state to active
        self.update_state(PoolState::Active).await;
        tracing::info!("State updated to active");
        // 7. Publish initial price update
        self.publish_price_update(price_update).await?;
        tracing::info!("Initial price update published");

        tracing::info!(
            "Pool for token {} initialized successfully",
            self.token_address
        );
        Ok(())
    }

    /// Get current SOL price from Redis
    async fn get_sol_price(&self) -> Result<f64, AppError> {
        tracing::info!("Getting SOL price from Redis");

        let sol_price = self
            .redis_client
            .get_sol_price()
            .await?
            .ok_or_else(|| AppError::RedisError("SOL price not found in Redis".to_string()))?;

        tracing::info!("Current SOL price: ${:.2}", sol_price);
        Ok(sol_price)
    }

    /// Set up WebSocket subscription to the pool
    async fn setup_websocket_subscription(&mut self) -> Result<(), AppError> {
        if self.pool.is_none() {
            return Err(AppError::InternalError("Pool not initialized".to_string()));
        }

        // Connect to Solana RPC WebSocket
        let rpc_ws_url = std::env::var("SOLANA_RPC_WS_URL")
            .map_err(|_| AppError::InternalError("SOLANA_RPC_WS_URL not set".to_string()))?;

        let (ws_stream, _) = connect_async(&rpc_ws_url).await.map_err(|e| {
            AppError::WebSocketError(format!("Failed to connect to WebSocket: {}", e))
        })?;

        self.ws_connection = Some(ws_stream);

        // Subscribe to account updates
        if let Some(ws) = &mut self.ws_connection {
            let pool_address = self.pool.as_ref().unwrap().address.to_string();

            let subscribe_msg = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "accountSubscribe",
                "params": [
                    pool_address,
                    {
                        "encoding": "base64",
                        "commitment": "confirmed"
                    }
                ]
            });

            tracing::info!("Subscribing to pool account: {}", pool_address);

            ws.send(Message::Text(subscribe_msg.to_string().into()))
                .await
                .map_err(|e| {
                    AppError::WebSocketError(format!("Failed to send subscription request: {}", e))
                })?;

            // Wait for subscription confirmation
            if let Some(msg) = ws.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        let response: serde_json::Value =
                            serde_json::from_str(&text).map_err(|e| {
                                AppError::JsonParseError(format!("Failed to parse response: {}", e))
                            })?;

                        if let Some(result) = response.get("result") {
                            if let Some(subscription_id) = result.as_str() {
                                self.subscription_id = Some(subscription_id.to_string());
                                tracing::info!(
                                    "Successfully subscribed to pool {}, subscription ID: {}",
                                    pool_address,
                                    subscription_id
                                );
                                return Ok(());
                            }
                        }

                        return Err(AppError::WebSocketError(format!(
                            "Failed to parse subscription ID: {}",
                            text
                        )));
                    }
                    Ok(_) => {
                        return Err(AppError::WebSocketError(
                            "Received non-text response for subscription".to_string(),
                        ));
                    }
                    Err(e) => {
                        return Err(AppError::WebSocketError(format!(
                            "WebSocket error while waiting for subscription confirmation: {}",
                            e
                        )));
                    }
                }
            } else {
                return Err(AppError::WebSocketError(
                    "WebSocket closed while waiting for subscription confirmation".to_string(),
                ));
            }
        }

        Err(AppError::WebSocketError(
            "Failed to set up WebSocket connection".to_string(),
        ))
    }

    /// Handle a WebSocket message
    async fn handle_ws_message(&mut self, msg: Message) -> Result<(), AppError> {
        match msg {
            Message::Text(text) => {
                let json: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
                    AppError::JsonParseError(format!("Failed to parse WebSocket message: {}", e))
                })?;

                // Check if this is a notification for our subscription
                if let Some(params) = json.get("params") {
                    if let Some(subscription) = params.get("subscription").and_then(|s| s.as_str())
                    {
                        if let Some(our_subscription) = &self.subscription_id {
                            if subscription == our_subscription {
                                // This is our notification, process it
                                return self.process_account_notification(params).await;
                            }
                        }
                    }
                }
            }
            Message::Binary(_) => {
                // We don't expect binary messages
            }
            Message::Ping(data) => {
                // Respond with pong
                if let Some(ws) = &mut self.ws_connection {
                    ws.send(Message::Pong(data)).await.map_err(|e| {
                        AppError::WebSocketError(format!("Failed to send pong: {}", e))
                    })?;
                }
            }
            Message::Pong(_) => {
                // We don't expect pong messages as we don't send pings
            }
            Message::Close(_) => {
                // WebSocket was closed
                self.handle_ws_disconnect().await;
            }
            _ => {}
        }

        Ok(())
    }

    /// Process an account notification from the WebSocket
    async fn process_account_notification(
        &mut self,
        params: &serde_json::Value,
    ) -> Result<(), AppError> {
        // Extract account data
        let account_data = params
            .get("result")
            .and_then(|r| r.get("value"))
            .and_then(|v| v.get("data"))
            .and_then(|d| d.as_array())
            .and_then(|a| a.first())
            .and_then(|s| s.as_str())
            .ok_or_else(|| AppError::JsonParseError("Invalid account data format".to_string()))?;

        // Decode base64 data
        let decoded_data = base64::engine::general_purpose::STANDARD
            .decode(account_data)
            .map_err(|e| {
                AppError::SerializationError(borsh::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to decode base64 data: {}", e),
                ))
            })?;

        if let Some(pool) = self.pool.as_ref() {
            // Update pool with the new data
            let mut pool_ref = pool.account_data.write();

            // Call update_from_websocket_data, first cloning the Arc to avoid deadlock
            let pool_clone: Arc<RaydiumPool> = Arc::clone(pool);
            drop(pool_ref); // Release the lock before the async call

            pool_clone.update_from_websocket_data(&decoded_data).await?;

            // Get SOL price
            let sol_price = self.get_sol_price().await?;

            // Calculate new price
            let price_data = pool_clone
                .fetch_price_data(&self.rpc_client, sol_price)
                .await?;

            // Create price update
            let price_update = PriceUpdate {
                token_address: self.token_address.clone(),
                price_sol: price_data.price_sol,
                price_usd: price_data
                    .price_usd
                    .or_else(|| Some(price_data.price_sol * sol_price)),
                market_cap: price_data.market_cap,
                timestamp: chrono::Utc::now().timestamp(),
                dex_type: DexType::Raydium,
                liquidity: price_data.liquidity,
                liquidity_usd: price_data.liquidity_usd,
                pool_address: Some(pool_clone.address.to_string()),
                volume_24h: price_data.volume_24h,
                volume_6h: price_data.volume_6h,
                volume_1h: price_data.volume_1h,
                volume_5m: price_data.volume_5m,
            };

            // Update last price and publish
            self.last_price = Some(price_update.clone());
            self.publish_price_update(price_update).await?;

            // Update last update time
            self.last_update = Instant::now();

            return Ok(());
        }

        Err(AppError::InternalError("Pool not initialized".to_string()))
    }

    /// Handle a WebSocket error
    async fn handle_ws_error(&mut self, error: AppError) {
        tracing::error!("WebSocket error for {}: {}", self.token_address, error);

        // Update state to reconnecting
        self.retry_count += 1;
        self.update_state(PoolState::Reconnecting {
            attempt: self.retry_count,
        })
        .await;

        // Close current connection
        self.ws_connection = None;
        self.subscription_id = None;

        // Try to reconnect if not too many retries
        if self.retry_count <= 5 {
            match self.initialize_pool().await {
                Ok(_) => {
                    tracing::info!("Successfully reconnected to pool {}", self.token_address);
                    self.retry_count = 0;
                    self.update_state(PoolState::Active).await;
                }
                Err(e) => {
                    tracing::error!("Failed to reconnect to pool {}: {}", self.token_address, e);
                    if self.retry_count >= 5 {
                        self.update_state(PoolState::Failed {
                            error: e.to_string(),
                        })
                        .await;
                    }
                }
            }
        } else {
            self.update_state(PoolState::Failed {
                error: format!("Too many reconnection attempts: {}", error),
            })
            .await;
        }
    }

    /// Handle WebSocket disconnection
    async fn handle_ws_disconnect(&mut self) {
        tracing::warn!("WebSocket disconnected for {}", self.token_address);
        self.handle_ws_error(AppError::WebSocketError(
            "WebSocket disconnected".to_string(),
        ))
        .await;
    }

    /// Check health of the pool
    async fn check_health(&mut self) {
        // Check if we've received updates recently
        let stale_threshold = Duration::from_secs(60);

        if self.last_update.elapsed() > stale_threshold && matches!(self.state, PoolState::Active) {
            tracing::warn!(
                "Pool {} has not received updates for {} seconds",
                self.token_address,
                self.last_update.elapsed().as_secs()
            );

            self.handle_ws_error(AppError::TimeoutError(
                "No updates received recently".to_string(),
            ))
            .await;
        }
    }

    /// Get the current price
    async fn get_current_price(&self) -> Result<PriceUpdate, AppError> {
        match &self.last_price {
            Some(price) => Ok(price.clone()),
            None => {
                // If we don't have a cached price, calculate it fresh
                if let Some(pool) = &self.pool {
                    let sol_price = self.get_sol_price().await?;
                    let price_data = pool.fetch_price_data(&self.rpc_client, sol_price).await?;

                    let price_update = PriceUpdate {
                        token_address: self.token_address.clone(),
                        price_sol: price_data.price_sol,
                        price_usd: price_data
                            .price_usd
                            .or_else(|| Some(price_data.price_sol * sol_price)),
                        market_cap: price_data.market_cap,
                        timestamp: chrono::Utc::now().timestamp(),
                        dex_type: DexType::Raydium,
                        liquidity: price_data.liquidity,
                        liquidity_usd: price_data.liquidity_usd,
                        pool_address: Some(pool.address.to_string()),
                        volume_24h: price_data.volume_24h,
                        volume_6h: price_data.volume_6h,
                        volume_1h: price_data.volume_1h,
                        volume_5m: price_data.volume_5m,
                    };

                    return Ok(price_update);
                }

                Err(AppError::InternalError(
                    "No price data available".to_string(),
                ))
            }
        }
    }

    /// Update the pool state and notify subscribers
    async fn update_state(&mut self, new_state: PoolState) {
        // Only update if state has changed
        if self.state != new_state {
            tracing::info!(
                "Pool {} state changed from {:?} to {:?}",
                self.token_address,
                self.state,
                new_state
            );

            self.state = new_state.clone();

            // Publish state change event
            let event = PoolEvent::StateChanged {
                token_address: self.token_address.clone(),
                state: new_state,
            };

            if let Err(e) = self.event_tx.send(event) {
                tracing::error!("Failed to publish state change: {}", e);
            }
        }
    }

    /// Publish a price update
    async fn publish_price_update(&self, update: PriceUpdate) -> Result<(), AppError> {
        // Publish price update event
        let event = PoolEvent::PriceUpdated(update);

        self.event_tx.send(event).map(|_| ()).map_err(|e| {
            AppError::ChannelSendError(format!("Failed to publish price update: {}", e))
        })
    }

    /// Shutdown the pool actor
    async fn handle_shutdown(&mut self) -> Result<(), AppError> {
        tracing::info!("Shutting down pool actor for {}", self.token_address);

        // Unsubscribe from WebSocket if connected
        if let Some(subscription_id) = &self.subscription_id {
            if let Some(ws) = &mut self.ws_connection {
                let unsubscribe_msg = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "accountUnsubscribe",
                    "params": [subscription_id]
                });

                let _ = ws
                    .send(Message::Text(unsubscribe_msg.to_string().into()))
                    .await;
                let _ = ws.close(None).await;
            }
        }

        self.ws_connection = None;
        self.subscription_id = None;

        Ok(())
    }
}

/// Pool factory - responsible for creating new pool actors
pub struct PoolFactory {
    rpc_client: Arc<RpcClient>,
    redis_client: Arc<RedisPool>,
    event_tx: broadcast::Sender<PoolEvent>,
}

impl PoolFactory {
    /// Create a new pool factory
    pub fn new(
        rpc_client: Arc<RpcClient>,
        redis_client: Arc<RedisPool>,
    ) -> (Self, broadcast::Receiver<PoolEvent>) {
        let (event_tx, event_rx) = broadcast::channel(1000);

        let factory = Self {
            rpc_client,
            redis_client,
            event_tx,
        };

        (factory, event_rx)
    }

    /// Create a new pool actor
    pub fn create_pool(
        &self,
        token_address: String,
    ) -> (mpsc::Sender<PoolCommand>, tokio::task::JoinHandle<()>) {
        let (command_tx, command_rx) = mpsc::channel(100);

        let pool = PoolActor::new(
            token_address,
            command_rx,
            self.event_tx.clone(),
            self.rpc_client.clone(),
            self.redis_client.clone(),
        );

        let handle = tokio::spawn(async move {
            pool.run().await;
        });

        (command_tx, handle)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct CachedPoolData {
    address: String,
    base_mint: String,
    quote_mint: String,
    base_vault: String,
    quote_vault: String,
    base_decimals: u8,
    quote_decimals: u8,
}

impl From<&RaydiumPool> for CachedPoolData {
    fn from(pool: &RaydiumPool) -> Self {
        Self {
            address: pool.address.to_string(),
            base_mint: pool.base_mint.to_string(),
            quote_mint: pool.quote_mint.to_string(),
            base_vault: pool.base_vault.to_string(),
            quote_vault: pool.quote_vault.to_string(),
            base_decimals: pool.base_decimals,
            quote_decimals: pool.quote_decimals,
        }
    }
}

impl CachedPoolData {
    fn to_pool(&self) -> Result<RaydiumPool, AppError> {
        Ok(RaydiumPool {
            address: Pubkey::from_str(&self.address)?,
            base_mint: Pubkey::from_str(&self.base_mint)?,
            quote_mint: Pubkey::from_str(&self.quote_mint)?,
            base_vault: Pubkey::from_str(&self.base_vault)?,
            quote_vault: Pubkey::from_str(&self.quote_vault)?,
            base_decimals: self.base_decimals,
            quote_decimals: self.quote_decimals,
            account_data: Arc::new(RwLock::new(None)),
        })
    }
}
