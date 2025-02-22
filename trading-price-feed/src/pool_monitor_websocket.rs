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
use std::time::{Duration, Instant};
use std::{collections::HashMap, str::FromStr, sync::Arc};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, RwLock};
use tokio_tungstenite::{tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream};
use trading_common::redis::RedisPool;
use trading_common::{error::AppError, models::PriceUpdate};
use trading_common::{WebSocketConfig, WebSocketConnectionManager, RAYDIUM_V4, WSOL};

pub struct PoolWebSocketMonitor {
    connection_manager: Arc<RwLock<WebSocketConnectionManager>>,
    subscribed_pools: Arc<RwLock<HashMap<Pubkey, RaydiumPool>>>,
    price_sender: broadcast::Sender<PriceUpdate>,
    rpc_client: Arc<RpcClient>,
    redis_connection: Arc<RedisPool>,
    client_subscriptions: Arc<RwLock<HashMap<String, HashSet<String>>>>,
}

impl PoolWebSocketMonitor {
    pub fn new(
        rpc_ws_url: String,
        price_sender: broadcast::Sender<PriceUpdate>,
        rpc_client: Arc<RpcClient>,
        redis_connection: Arc<RedisPool>,
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

    pub async fn subscribe_to_pool_updates(
        &self,
        connection: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        pool_pubkey: &Pubkey,
    ) -> Result<(), AppError> {
        tracing::info!("Subscribing to pool updates for {}", pool_pubkey);

        let subscription = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "accountSubscribe",
            "params": [
                pool_pubkey.to_string(),
                {
                    "encoding": "base64",
                    "commitment": "confirmed",
                    "filters": [
                        {
                            "memcmp": {
                                "offset": 8,
                                "bytes": RAYDIUM_V4
                            }
                        }
                    ]
                }
            ]
        });

        // Send subscription with timeout
        let timeout = Duration::from_secs(5);
        match tokio::time::timeout(timeout, async {
            connection
                .send(Message::Text(subscription.to_string().into()))
                .await?;

            // Wait for confirmation
            while let Some(msg) = connection.next().await {
                match msg {
                    Ok(Message::Text(resp)) => {
                        let v: Value = serde_json::from_str(&resp)?;
                        if let Some(result) = v.get("result") {
                            tracing::info!("Subscription confirmed with ID: {}", result);
                            return Ok(());
                        }
                    }
                    _ => continue,
                }
            }
            Err(AppError::WebSocketError(
                "No subscription confirmation".into(),
            ))
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(AppError::WebSocketError("Subscription timeout".into())),
        }
    }

    pub fn subscribe_to_updates(&self) -> broadcast::Receiver<PriceUpdate> {
        self.price_sender.subscribe()
    }

    pub async fn process_pool_update(&self, msg: &str) -> Result<(), AppError> {
        tracing::info!("Processing pool update: {}", msg);
        let parsed: Value = serde_json::from_str(msg)?;

        // Extract data from notification
        let (pool_pubkey, data) = {
            let params = parsed
                .get("params")
                .ok_or_else(|| AppError::WebSocketError("Missing params".into()))?;

            let value = params
                .get("value")
                .ok_or_else(|| AppError::WebSocketError("Missing value".into()))?;

            let pubkey = value
                .get("pubkey")
                .and_then(|p| p.as_str())
                .ok_or_else(|| AppError::WebSocketError("Missing pubkey".into()))?;

            let data = value
                .get("data")
                .and_then(|d| d.as_array())
                .and_then(|arr| arr.first())
                .and_then(|v| v.as_str())
                .ok_or_else(|| AppError::WebSocketError("Invalid data format".into()))?;

            (Pubkey::from_str(pubkey)?, data)
        };

        // Get the pool
        let pools = self.subscribed_pools.read().await;
        if let Some(pool) = pools.get(&pool_pubkey) {
            // Decode and process pool data
            let decoded_data = base64::engine::general_purpose::STANDARD
                .decode(data)
                .map_err(|e| {
                    AppError::SerializationError(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Failed to decode base64 data: {}", e),
                    ))
                })?;

            // Update pool data
            pool.update_from_websocket_data(&decoded_data).await?;

            // Get current SOL price
            let current_sol_price = self
                .redis_connection
                .get_sol_price()
                .await?
                .ok_or_else(|| AppError::RedisError("SOL price not found".to_string()))?;

            // Calculate new price data
            let price_data = pool
                .fetch_price_data(&self.rpc_client, current_sol_price)
                .await?;

            // Create and broadcast price update
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

            // Broadcast update
            if let Err(e) = self.price_sender.send(update.clone()) {
                tracing::error!("Failed to send price update through channel: {}", e);
            }

            tracing::info!("Successfully processed and broadcast pool update");
        }

        Ok(())
    }

    pub async fn find_pool(&self, token_address: &str) -> Result<RaydiumPool, AppError> {
        tracing::info!("Finding pool for token {}", token_address);
        let token_pubkey = Pubkey::from_str(token_address)
            .map_err(|_| AppError::InvalidPoolAddress(token_address.to_string()))?;

        // SOL token pubkey - we'll use this to validate quote mint
        let sol_pubkey = Pubkey::from_str(WSOL)?;

        // Search for pool with token as base mint
        let memcmp = Memcmp::new(432, MemcmpEncodedBytes::Base58(token_pubkey.to_string()));
        let filters = vec![RpcFilterType::DataSize(752), RpcFilterType::Memcmp(memcmp)];

        tracing::info!("Searching for pools with base mint {}", token_pubkey);

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

        tracing::info!("Found {} potential pool accounts", accounts.len());

        // Track the best pool based on liquidity
        let mut best_pool: Option<(RaydiumPool, u64)> = None;

        // Find pool with highest liquidity
        for (pubkey, account) in accounts {
            if let Ok(mut pool) = RaydiumPool::from_account_data(&pubkey, &account.data) {
                // Verify this is a SOL pool
                if pool.quote_mint != sol_pubkey {
                    continue;
                }

                tracing::info!("Loading metadata for pool candidate: {}", pubkey);
                pool.load_metadata(&self.rpc_client, &self.redis_connection)
                    .await?;

                // Get pool liquidity
                let quote_balance = self
                    .rpc_client
                    .get_token_account_balance(&pool.quote_vault)
                    .map_err(|e| AppError::SolanaRpcError { source: e })?;

                let liquidity = quote_balance.amount.parse::<u64>().unwrap_or(0);

                // Update best pool if this has higher liquidity
                match &best_pool {
                    None => best_pool = Some((pool, liquidity)),
                    Some((_, current_liquidity)) if liquidity > *current_liquidity => {
                        best_pool = Some((pool, liquidity));
                    }
                    _ => {}
                }
            }
        }

        // Return the pool with highest liquidity
        best_pool
            .map(|(pool, _)| pool)
            .ok_or_else(|| AppError::PoolNotFound(token_address.to_string()))
    }

    pub async fn stop(&self) -> Result<(), AppError> {
        tracing::info!("Initiating pool monitor shutdown");

        // First get all pool keys without holding the lock
        let pool_keys: Vec<Pubkey> = {
            let pools = self.subscribed_pools.read().await;
            pools.keys().cloned().collect()
        };

        // Then unsubscribe each pool
        for pubkey in pool_keys {
            let unsubscribe = json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "accountUnsubscribe",
                "params": [pubkey.to_string()]
            });

            // Get a fresh connection for each unsubscribe
            let mut manager = self.connection_manager.write().await;
            if let Ok(connection) = manager.ensure_connection().await {
                let _ = connection
                    .send(Message::Text(unsubscribe.to_string().into()))
                    .await;
            }
        }

        // Clear state
        {
            let mut pools = self.subscribed_pools.write().await;
            pools.clear();
        }
        {
            let mut subs = self.client_subscriptions.write().await;
            subs.clear();
        }

        // Final shutdown
        self.connection_manager.write().await.shutdown().await?;

        tracing::info!("Pool monitor shutdown complete");
        Ok(())
    }

    pub async fn unsubscribe_token(&self, token_address: &str, client_id: &str) {
        tracing::info!(
            "Unsubscribing client {} from token {}",
            client_id,
            token_address
        );
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

    pub async fn batch_subscribe(
        &self,
        token_addresses: Vec<String>,
        client_id: &str,
    ) -> Result<(), AppError> {
        tracing::info!("Starting batch_subscribe");

        let mut pools_to_subscribe = Vec::new();
        for token_address in token_addresses {
            match self.find_pool(&token_address).await {
                Ok(pool) => {
                    let pubkey = Pubkey::from_str(&token_address)?;
                    pools_to_subscribe.push((pubkey, pool, token_address));
                }
                Err(e) => {
                    tracing::error!("Failed to find pool for token {}: {}", token_address, e);
                }
            }
        }

        for (pubkey, pool, token_address) in pools_to_subscribe {
            let mut attempts = 0;
            const MAX_ATTEMPTS: u32 = 3;
            const TIMEOUT: Duration = Duration::from_secs(5);

            'retry: while attempts < MAX_ATTEMPTS {
                attempts += 1;
                tracing::info!("Attempt {} to subscribe to pool {}", attempts, pool.address);

                let subscription = json!({
                    "jsonrpc": "2.0",
                    "id": attempts,
                    "method": "accountSubscribe",
                    "params": [
                        pool.address.to_string(),
                        {
                            "encoding": "base64",
                            "commitment": "confirmed"
                        }
                    ]
                });

                // Take a short-lived lock to send the subscription
                let response_id = {
                    let mut manager = self.connection_manager.write().await;
                    match manager.ensure_connection().await {
                        Ok(connection) => {
                            if let Err(e) = connection
                                .send(Message::Text(subscription.to_string().into()))
                                .await
                            {
                                tracing::error!("Failed to send subscription: {}", e);
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                continue 'retry;
                            }
                            attempts
                        }
                        Err(e) => {
                            tracing::error!("Failed to get connection: {}", e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue 'retry;
                        }
                    }
                };

                // Clone values for the async block
                let token_address_for_async = token_address.clone();
                let pool_for_async = pool.clone();
                let client_id_for_async = client_id.to_string();

                // Wait for confirmation with timeout, without holding the lock
                match tokio::time::timeout(TIMEOUT, async move {
                    let start = Instant::now();
                    while start.elapsed() < TIMEOUT {
                        let msg = {
                            let mut manager = self.connection_manager.write().await;
                            match manager.ensure_connection().await {
                                Ok(connection) => match connection.next().await {
                                    Some(Ok(Message::Text(resp))) => Some(resp),
                                    Some(Err(e)) => {
                                        tracing::error!("Error receiving message: {}", e);
                                        return Err(AppError::WebSocketError(e.to_string()));
                                    }
                                    _ => None,
                                },
                                Err(e) => return Err(AppError::WebSocketError(e.to_string())),
                            }
                        };

                        if let Some(resp) = msg {
                            if let Ok(v) = serde_json::from_str::<Value>(&resp) {
                                if v.get("id").and_then(|id| id.as_u64())
                                    == Some(response_id as u64)
                                    && v.get("result").is_some()
                                {
                                    // Update state
                                    {
                                        let mut pools = self.subscribed_pools.write().await;
                                        pools.insert(pubkey, pool_for_async.clone());
                                    }
                                    {
                                        let mut client_subs =
                                            self.client_subscriptions.write().await;
                                        client_subs
                                            .entry(token_address_for_async.clone())
                                            .or_insert_with(HashSet::new)
                                            .insert(client_id_for_async.clone());
                                    }
                                    tracing::info!(
                                        "Successfully completed subscription setup for {}",
                                        token_address_for_async
                                    );
                                    return Ok(());
                                }
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(AppError::WebSocketError(
                        "Timeout waiting for confirmation".to_string(),
                    ))
                })
                .await
                {
                    Ok(Ok(())) => break 'retry, // Success!
                    Ok(Err(e)) => {
                        tracing::error!("Failed to get subscription confirmation: {}", e);
                    }
                    Err(_) => {
                        tracing::error!("Timeout waiting for subscription confirmation");
                    }
                }

                if attempts < MAX_ATTEMPTS {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        tracing::info!("Completed batch_subscribe");
        Ok(())
    }

    pub async fn recover_subscriptions(&self) -> Result<(), AppError> {
        let pools = self.subscribed_pools.read().await;
        let mut manager = self.connection_manager.write().await;
        let connection = manager.ensure_connection().await?;

        for (_, pool) in pools.iter() {
            tracing::info!("Recovering subscription for pool: {}", pool.address);
            self.subscribe_to_pool_updates(connection, &pool.address)
                .await?;
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), AppError> {
        // Clear subscriptions first
        {
            let mut pools = self.subscribed_pools.write().await;
            pools.clear();

            let mut client_subs = self.client_subscriptions.write().await;
            client_subs.clear();
        }

        // Close WebSocket connection
        self.connection_manager.write().await.shutdown().await?;

        Ok(())
    }
}
