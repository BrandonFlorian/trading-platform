use crate::{config::PriceFeedConfig, raydium::RaydiumPool};
use chrono::{DateTime, Utc};
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
};
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
};
use tokio::sync::{broadcast, mpsc, RwLock};
use trading_common::{
    dex::DexType, error::AppError, event_system::EventSystem, models::PriceUpdate, RedisConnection,
    RAYDIUM_V4,
};

struct ActiveTokenFeed {
    pool_address: Pubkey,
    last_update: DateTime<Utc>,
    subscribers: HashSet<String>,
}

pub struct PoolMonitor {
    rpc_client: Arc<RpcClient>,
    event_system: Arc<EventSystem>,
    redis_connection: RedisConnection,
    price_sender: broadcast::Sender<PriceUpdate>,
    stop_sender: Option<mpsc::Sender<()>>,
    config: PriceFeedConfig,
    active_tokens: Arc<RwLock<HashMap<String, ActiveTokenFeed>>>,
    current_sol_price: Arc<RwLock<f64>>,
}

impl PoolMonitor {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        event_system: Arc<EventSystem>,
        redis_connection: RedisConnection,
        config: PriceFeedConfig,
    ) -> Self {
        let (price_sender, _) = broadcast::channel(100);
        Self {
            rpc_client,
            event_system,
            redis_connection,
            price_sender,
            stop_sender: None,
            config,
            active_tokens: Arc::new(RwLock::new(HashMap::new())),
            current_sol_price: Arc::new(RwLock::new(0.0)),
        }
    }

    pub fn subscribe_to_updates(&self) -> broadcast::Receiver<PriceUpdate> {
        self.price_sender.subscribe()
    }

    pub async fn start_monitoring(&mut self) -> Result<(), AppError> {
        // Start SOL price subscription first
        self.subscribe_to_sol_price().await?;

        let (price_tx, _) = broadcast::channel(100);
        let (stop_tx, stop_rx) = mpsc::channel(1);

        self.price_sender = price_tx.clone();
        self.stop_sender = Some(stop_tx);

        // Clone what we need for the monitoring task
        let rpc_client = Arc::clone(&self.rpc_client);
        let price_tx = price_tx.clone();
        let active_tokens = Arc::clone(&self.active_tokens);
        let current_sol_price = Arc::clone(&self.current_sol_price);

        tokio::spawn({
            async move {
                let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
                let mut stop_rx = stop_rx;

                loop {
                    tokio::select! {
                        _ = stop_rx.recv() => {
                            break;
                        }
                        _ = interval.tick() => {
                            if let Err(e) = Self::update_pool_prices(
                                &rpc_client,
                                &price_tx,
                                &active_tokens,
                                &current_sol_price,
                            ).await {
                                tracing::error!("Failed to update pool prices: {}", e);
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn stop_monitoring(&mut self) -> Result<(), AppError> {
        if let Some(stop_sender) = self.stop_sender.take() {
            let _ = stop_sender.send(()).await;
        }
        Ok(())
    }

    async fn subscribe_to_sol_price(&mut self) -> Result<(), AppError> {
        let mut redis = self.redis_connection.clone();
        let current_sol_price = Arc::clone(&self.current_sol_price);

        let mut rx = redis.subscribe_to_sol_price().await?;

        tokio::spawn(async move {
            while let Ok(update) = rx.recv().await {
                let mut current_sol_price = current_sol_price.write().await;
                *current_sol_price = update.price_usd;
                tracing::debug!("Updated SOL price: ${:.2}", update.price_usd);
            }
        });

        Ok(())
    }

    async fn update_pool_prices(
        rpc_client: &Arc<RpcClient>,
        price_tx: &broadcast::Sender<PriceUpdate>,
        active_tokens: &Arc<RwLock<HashMap<String, ActiveTokenFeed>>>,
        current_sol_price: &Arc<RwLock<f64>>,
    ) -> Result<(), AppError> {
        let tokens: Vec<(String, Pubkey)> = {
            let active_tokens = active_tokens.read().await;
            active_tokens
                .iter()
                .map(|(addr, feed)| (addr.clone(), feed.pool_address))
                .collect()
        };

        // Get current SOL price for USD conversions
        let sol_price = *current_sol_price.read().await;
        if sol_price == 0.0 {
            tracing::warn!("No SOL price available, skipping USD calculations");
        }

        for (token_address, pool_address) in tokens {
            match rpc_client.get_account(&pool_address) {
                Ok(account) => {
                    if let Ok(pool) = RaydiumPool::from_account_data(&pool_address, &account.data) {
                        if let Ok(price_data) = pool
                            .fetch_price_data(rpc_client, *current_sol_price.read().await)
                            .await
                        {
                            // Calculate USD values using SOL price
                            let price_usd = if sol_price > 0.0 {
                                Some(price_data.price_sol * sol_price)
                            } else {
                                None
                            };

                            let update = PriceUpdate {
                                token_address: token_address.clone(),
                                price_sol: price_data.price_sol,
                                price_usd,
                                market_cap: price_data.market_cap,
                                timestamp: Utc::now().timestamp(),
                                dex_type: DexType::Raydium,
                                liquidity: price_data.liquidity,
                                liquidity_usd: price_data.liquidity.map(|l| l * sol_price),
                                pool_address: Some(pool_address.to_string()),
                                volume_24h: price_data.volume_24h.map(|v| v * sol_price),
                                volume_6h: price_data.volume_6h.map(|v| v * sol_price),
                                volume_1h: price_data.volume_1h.map(|v| v * sol_price),
                                volume_5m: price_data.volume_5m.map(|v| v * sol_price),
                            };

                            if let Err(e) = price_tx.send(update) {
                                tracing::error!("Failed to send price update: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to fetch account data for pool {}: {}",
                        pool_address,
                        e
                    );
                }
            }
        }

        Ok(())
    }

    pub async fn subscribe_token(
        &self,
        token_address: &str,
        client_id: &str,
    ) -> Result<(), AppError> {
        let mut active_tokens = self.active_tokens.write().await;

        if let Some(feed) = active_tokens.get_mut(token_address) {
            feed.subscribers.insert(client_id.to_string());
            return Ok(());
        }

        // Find Raydium pool for token
        let pool_address = self.find_raydium_pool(token_address).await?;

        active_tokens.insert(
            token_address.to_string(),
            ActiveTokenFeed {
                pool_address,
                last_update: Utc::now(),
                subscribers: HashSet::from([client_id.to_string()]),
            },
        );

        Ok(())
    }

    pub async fn unsubscribe_token(&self, token_address: &str, client_id: &str) {
        let mut active_tokens = self.active_tokens.write().await;

        if let Some(feed) = active_tokens.get_mut(token_address) {
            feed.subscribers.remove(client_id);

            // Remove feed if no subscribers
            if feed.subscribers.is_empty() {
                active_tokens.remove(token_address);
            }
        }
    }

    async fn find_raydium_pool(&self, token_address: &str) -> Result<Pubkey, AppError> {
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

        let accounts = self
            .rpc_client
            .get_program_accounts_with_config(&Pubkey::from_str(RAYDIUM_V4)?, config)
            .map_err(|e| AppError::SolanaRpcError { source: e })?;

        // Find first valid pool
        for (pubkey, account) in accounts {
            if let Ok(pool) = RaydiumPool::from_account_data(&pubkey, &account.data) {
                if pool.base_mint == token_pubkey {
                    return Ok(pubkey);
                }
            }
        }

        Err(AppError::PoolNotFound(token_address.to_string()))
    }
}
