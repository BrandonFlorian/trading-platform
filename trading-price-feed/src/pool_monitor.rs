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
use tokio::sync::RwLock;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use trading_common::{
    dex::DexType,
    error::AppError,
    event_system::EventSystem,
    models::{ConnectionStatus, ConnectionType, PriceUpdate},
    ConnectionMonitor, RAYDIUM_V4,
};

struct ActiveTokenFeed {
    pool_address: Pubkey,
    last_update: DateTime<Utc>,
    subscribers: HashSet<String>, // Client IDs
}

pub struct PoolMonitor {
    rpc_client: Arc<RpcClient>,
    event_system: Arc<EventSystem>,
    connection_monitor: Arc<ConnectionMonitor>,
    price_sender: Arc<RwLock<Option<broadcast::Sender<PriceUpdate>>>>,
    stop_sender: Option<mpsc::Sender<()>>,
    config: PriceFeedConfig,
    active_tokens: Arc<RwLock<HashMap<String, ActiveTokenFeed>>>,
}

impl PoolMonitor {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        event_system: Arc<EventSystem>,
        connection_monitor: Arc<ConnectionMonitor>,
        config: PriceFeedConfig,
    ) -> Self {
        Self {
            rpc_client,
            event_system,
            connection_monitor,
            price_sender: Arc::new(RwLock::new(None)),
            stop_sender: None,
            config,
            active_tokens: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn start_monitoring(&mut self) -> Result<(), AppError> {
        let (price_tx, _) = broadcast::channel(100);
        let (stop_tx, stop_rx) = mpsc::channel(1);

        let mut price_sender = self.price_sender.write().await;
        *price_sender = Some(price_tx.clone());
        self.stop_sender = Some(stop_tx);

        // Clone what we need for the monitoring task
        let rpc_client = Arc::clone(&self.rpc_client);
        let connection_monitor = Arc::clone(&self.connection_monitor);
        let price_tx = price_tx.clone();
        let active_tokens = Arc::clone(&self.active_tokens);

        tokio::spawn({
            let connection_monitor = connection_monitor.clone();
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
                            ).await {
                                tracing::error!("Failed to update pool prices: {}", e);
                                connection_monitor
                                    .update_status(
                                        ConnectionType::WebSocket,
                                        ConnectionStatus::Error,
                                        Some(e.to_string()),
                                    )
                                    .await;
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

    pub async fn subscribe_to_updates(&self) -> ReceiverStream<PriceUpdate> {
        let (tx, rx) = mpsc::channel(100);

        // Get a read lock and clone the sender if it exists
        if let Some(price_sender) = self.price_sender.read().await.as_ref() {
            let price_sender = price_sender.clone();

            tokio::spawn(async move {
                let mut price_rx = price_sender.subscribe();
                while let Ok(price) = price_rx.recv().await {
                    if tx.send(price).await.is_err() {
                        break;
                    }
                }
            });
        }

        ReceiverStream::new(rx)
    }

    async fn update_pool_prices(
        rpc_client: &Arc<RpcClient>,
        price_tx: &broadcast::Sender<PriceUpdate>,
        active_tokens: &Arc<RwLock<HashMap<String, ActiveTokenFeed>>>,
    ) -> Result<(), AppError> {
        // Take a snapshot of the current active tokens
        let tokens: Vec<(String, Pubkey)> = {
            let active_tokens = active_tokens.read().await;
            tracing::debug!("Active tokens: {}", active_tokens.len());
            active_tokens
                .iter()
                .map(|(addr, feed)| (addr.clone(), feed.pool_address))
                .collect()
        };

        for (token_address, pool_address) in tokens {
            tracing::debug!("Fetching price for token: {}", token_address);
            match rpc_client.get_account(&pool_address) {
                Ok(account) => {
                    tracing::debug!("Got account data for pool: {}", pool_address);
                    if let Ok(pool) = RaydiumPool::from_account_data(&pool_address, &account.data) {
                        if let Ok(price_data) = pool.fetch_price_data(rpc_client).await {
                            tracing::debug!("Price data: {:?}", price_data);
                            let update = PriceUpdate {
                                token_address: token_address.clone(),
                                price_sol: price_data.price_sol,
                                timestamp: Utc::now().timestamp(),
                                dex_type: DexType::Raydium,
                                liquidity: price_data.liquidity,
                                market_cap: 0.0,
                                pool_address: Some(pool_address.to_string()),
                                volume_24h: None,
                                volume_6h: None,
                                volume_1h: None,
                                volume_5m: None,
                            };

                            if let Err(e) = price_tx.send(update) {
                                tracing::error!("Failed to send price update: {}", e);
                            } else {
                                tracing::debug!("Successfully sent price update");
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
