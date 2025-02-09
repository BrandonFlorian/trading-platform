use crate::raydium::RaydiumPool;
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use solana_account_decoder::UiAccountEncoding;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;
use std::time::Duration;
use std::{collections::HashMap, str::FromStr, sync::Arc};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, RwLock};
use tokio_tungstenite::{tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream};
use trading_common::redis::RedisConnection;
use trading_common::{error::AppError, models::PriceUpdate};
use trading_common::{WebSocketConfig, WebSocketConnectionManager, RAYDIUM_V4};

pub struct PoolWebSocketMonitor {
    connection_manager: Arc<RwLock<WebSocketConnectionManager>>,
    subscribed_pools: Arc<RwLock<HashMap<Pubkey, RaydiumPool>>>,
    price_sender: broadcast::Sender<PriceUpdate>,
    rpc_client: Arc<RpcClient>,
    redis_connection: Arc<RedisConnection>,
    client_subscriptions: Arc<RwLock<HashMap<String, HashSet<String>>>>,
}

impl PoolWebSocketMonitor {
    pub fn new(
        rpc_ws_url: String,
        price_sender: broadcast::Sender<PriceUpdate>,
        rpc_client: Arc<RpcClient>,
        redis_connection: Arc<RedisConnection>,
        client_subscriptions: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    ) -> Self {
        let ws_config = WebSocketConfig {
            health_check_interval: Duration::from_secs(30),
            connection_timeout: Duration::from_secs(5),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            max_retries: 3,
        };

        let connection_manager = WebSocketConnectionManager::new(rpc_ws_url, Some(ws_config));

        Self {
            connection_manager: Arc::new(RwLock::new(connection_manager)),
            subscribed_pools: Arc::new(RwLock::new(HashMap::new())),
            price_sender,
            rpc_client,
            redis_connection,
            client_subscriptions,
        }
    }

    pub async fn start(&self) -> Result<(), AppError> {
        // Get connection from manager
        let mut manager = self.connection_manager.write().await;
        let connection = manager.ensure_connection().await?;

        // Subscribe to all pools
        let pools = self.subscribed_pools.read().await;
        tracing::info!("Starting monitor with {} subscribed pools", pools.len());

        for (pubkey, _) in pools.iter() {
            tracing::info!("Setting up subscription for pool: {}", pubkey);
            self.subscribe_to_pool_updates(connection, pubkey).await?;
        }

        // Handle messages
        tracing::info!("Starting message processing loop");
        while let Some(msg) = connection.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    tracing::debug!("Received WebSocket message: {}", text);
                    if let Err(e) = self.process_pool_update(&text).await {
                        tracing::error!("Error processing pool update: {}", e);
                    }
                }
                Ok(Message::Close(_)) => {
                    tracing::info!("WebSocket connection closed");
                    break;
                }
                Err(e) => {
                    tracing::error!("WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }

    async fn subscribe_to_pool_updates(
        &self,
        connection: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        pool_pubkey: &Pubkey,
    ) -> Result<(), AppError> {
        let subscription = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "accountSubscribe",
            "params": [
                pool_pubkey.to_string(),
                {"encoding": "base64", "commitment": "processed"}
            ]
        });

        connection
            .send(Message::Text(subscription.to_string().into()))
            .await?;

        Ok(())
    }

    pub fn subscribe_to_updates(&self) -> broadcast::Receiver<PriceUpdate> {
        self.price_sender.subscribe()
    }

    pub async fn process_pool_update(&self, msg: &str) -> Result<(), AppError> {
        tracing::debug!("Processing WebSocket message: {}", msg);

        let parsed: Value = serde_json::from_str(msg)?;

        // Check if this is a subscription notification
        if let Some(params) = parsed.get("params") {
            let notification = params
                .get("result")
                .ok_or_else(|| AppError::WebSocketError("Missing result in notification".into()))?;

            // Extract account data and pubkey
            let data = notification
                .get("value")
                .and_then(|v| v.get("data"))
                .and_then(|d| d.as_array())
                .and_then(|arr| arr.first())
                .and_then(|v| v.as_str())
                .ok_or_else(|| AppError::WebSocketError("Invalid data format".into()))?;

            let pubkey = notification
                .get("value")
                .and_then(|v| v.get("pubkey"))
                .and_then(|p| p.as_str())
                .ok_or_else(|| AppError::WebSocketError("Missing pubkey".into()))?;

            let pool_pubkey = Pubkey::from_str(pubkey)?;

            // Process pool update
            tracing::debug!("Processing update for pool: {}", pool_pubkey);

            let pools = self.subscribed_pools.read().await;
            if let Some(pool) = pools.get(&pool_pubkey) {
                // Decode account data
                let decoded_data = base64::engine::general_purpose::STANDARD
                    .decode(data)
                    .map_err(|e| AppError::WebSocketError(format!("Invalid base64: {}", e)))?;

                // Update pool data
                pool.update_from_websocket_data(&decoded_data).await?;

                let current_sol_price =
                    self.redis_connection
                        .get_sol_price()
                        .await?
                        .ok_or_else(|| {
                            AppError::RedisError("SOL price not found in cache".to_string())
                        })?;

                let price_data = pool
                    .fetch_price_data(&self.rpc_client, current_sol_price)
                    .await?;

                tracing::info!(
                    "Calculated updated price for token {}: SOL {}, USD {}",
                    pool.base_mint,
                    price_data.price_sol,
                    price_data.price_usd.unwrap_or_default()
                );

                // Create and send price update
                let update = PriceUpdate {
                    token_address: pool.base_mint.to_string(),
                    price_sol: price_data.price_sol,
                    price_usd: price_data.price_usd,
                    market_cap: price_data.market_cap,
                    timestamp: chrono::Utc::now().timestamp(),
                    dex_type: trading_common::dex::DexType::Raydium,
                    liquidity: price_data.liquidity,
                    liquidity_usd: price_data.liquidity_usd,
                    pool_address: Some(pool_pubkey.to_string()),
                    volume_24h: price_data.volume_24h,
                    volume_6h: price_data.volume_6h,
                    volume_1h: price_data.volume_1h,
                    volume_5m: price_data.volume_5m,
                };

                let receiver_count = self.price_sender.receiver_count();
                tracing::info!("Broadcasting price update to {} receivers", receiver_count);

                if let Err(e) = self.price_sender.send(update) {
                    tracing::error!("Failed to send price update: {}", e);
                } else {
                    tracing::debug!("Successfully sent price update");
                }
            }
        }

        Ok(())
    }

    pub async fn find_pool(&self, token_address: &str) -> Result<RaydiumPool, AppError> {
        let token_pubkey = Pubkey::from_str(token_address)
            .map_err(|_| AppError::InvalidPoolAddress(token_address.to_string()))?;

        // Search for pool with token as base mint
        let memcmp = Memcmp::new(432, MemcmpEncodedBytes::Base58(token_pubkey.to_string()));
        let filters = vec![RpcFilterType::DataSize(752), RpcFilterType::Memcmp(memcmp)];

        let config = RpcProgramAccountsConfig {
            filters: Some(filters),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                commitment: Some(CommitmentConfig::confirmed()),
                ..Default::default()
            },
            ..Default::default()
        };

        let raydium_program = Pubkey::from_str(RAYDIUM_V4)?;

        // Get all matching pool accounts
        let accounts = self
            .rpc_client
            .get_program_accounts_with_config(&raydium_program, config)
            .map_err(|e| AppError::SolanaRpcError { source: e })?;

        // Find first valid pool
        for (pubkey, account) in accounts {
            if let Ok(mut pool) = RaydiumPool::from_account_data(&pubkey, &account.data) {
                pool.load_metadata(&self.rpc_client, &self.redis_connection)
                    .await?;
                if pool.base_mint == token_pubkey {
                    return Ok(pool);
                }
            }
        }

        Err(AppError::PoolNotFound(token_address.to_string()))
    }

    pub async fn stop(&self) -> Result<(), AppError> {
        self.connection_manager.write().await.shutdown().await
    }

    pub async fn subscribe_token(
        &self,
        token_address: &str,
        client_id: &str,
    ) -> Result<(), AppError> {
        tracing::info!(
            "Adding subscription for token {} from client {}",
            token_address,
            client_id
        );

        let mut subscribed_pools = self.subscribed_pools.write().await;
        let mut client_subs = self.client_subscriptions.write().await;

        // Check if we already have this pool
        let pubkey = Pubkey::from_str(token_address)?;

        let pool = if let std::collections::hash_map::Entry::Vacant(e) =
            subscribed_pools.entry(pubkey)
        {
            tracing::info!("Finding pool for token {}", token_address);
            let pool = self.find_pool(token_address).await?;

            // Get initial price
            let current_sol_price = self
                .redis_connection
                .get_sol_price()
                .await?
                .ok_or_else(|| AppError::RedisError("SOL price not found in cache".to_string()))?;

            let initial_price = pool
                .fetch_price_data(&self.rpc_client, current_sol_price)
                .await?;

            tracing::info!(
                "Initial price for token {}: SOL {}, USD {}",
                token_address,
                initial_price.price_sol,
                initial_price.price_usd.unwrap_or_default()
            );

            // Emit initial price update
            let update = PriceUpdate {
                token_address: token_address.to_string(),
                price_sol: initial_price.price_sol,
                price_usd: initial_price.price_usd,
                market_cap: initial_price.market_cap,
                timestamp: chrono::Utc::now().timestamp(),
                dex_type: trading_common::dex::DexType::Raydium,
                liquidity: initial_price.liquidity,
                liquidity_usd: initial_price.liquidity_usd,
                pool_address: Some(pool.address.to_string()),
                volume_24h: initial_price.volume_24h,
                volume_6h: initial_price.volume_6h,
                volume_1h: initial_price.volume_1h,
                volume_5m: initial_price.volume_5m,
            };

            if let Err(e) = self.price_sender.send(update) {
                tracing::error!("Failed to send initial price update: {}", e);
            }

            // Subscribe via WebSocket
            let mut manager = self.connection_manager.write().await;
            let connection = manager.ensure_connection().await?;

            tracing::info!("Subscribing to pool updates for {}", pool.address);
            self.subscribe_to_pool_updates(connection, &pool.address)
                .await?;

            e.insert(pool.clone());
            pool
        } else {
            subscribed_pools.get(&pubkey).unwrap().clone()
        };

        // Update client subscriptions
        client_subs
            .entry(token_address.to_string())
            .or_insert_with(HashSet::new)
            .insert(client_id.to_string());

        tracing::info!(
            "Successfully subscribed client {} to token {} with pool {}",
            client_id,
            token_address,
            pool.address
        );

        Ok(())
    }

    pub async fn unsubscribe_token(&self, token_address: &str, client_id: &str) {
        // Remove client subscription
        let mut client_subs = self.client_subscriptions.write().await;
        if let Some(subs) = client_subs.get_mut(token_address) {
            subs.remove(client_id);

            // If no more clients subscribed to this token
            if subs.is_empty() {
                client_subs.remove(token_address);

                // Remove pool tracking
                let mut pools = self.subscribed_pools.write().await;
                if let Ok(pubkey) = Pubkey::from_str(token_address) {
                    pools.remove(&pubkey);
                }
            }
        }
    }
}
