use crate::{
    config::PriceFeedConfig,
    raydium::{RaydiumPool, UsdcSolPool},
};
use chrono::{DateTime, Utc};
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
};
use solana_sdk::{commitment_config::CommitmentConfig, program_pack::Pack, pubkey::Pubkey};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
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
    //usdc_sol_pool: Arc<RwLock<UsdcSolPool>>,
    //usdc_sol_initialized: AtomicBool,
    usdc_sol_state: UsdcSolState,
}

#[derive(Clone)]
struct UsdcSolState {
    pool: Arc<RwLock<UsdcSolPool>>,
    initialized: Arc<AtomicBool>,
}

impl PoolMonitor {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        event_system: Arc<EventSystem>,
        connection_monitor: Arc<ConnectionMonitor>,
        config: PriceFeedConfig,
    ) -> Self {
        let usdc_sol_pool = UsdcSolPool::new(
            "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2",
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            "So11111111111111111111111111111111111111112",
        )
        .expect("Failed to create USDC/SOL pool");

        let usdc_sol_state = UsdcSolState {
            pool: Arc::new(RwLock::new(usdc_sol_pool)),
            initialized: Arc::new(AtomicBool::new(false)),
        };
        Self {
            rpc_client,
            event_system,
            connection_monitor,
            price_sender: Arc::new(RwLock::new(None)),
            stop_sender: None,
            config,
            active_tokens: Arc::new(RwLock::new(HashMap::new())),
            // usdc_sol_pool: Arc::new(RwLock::new(usdc_sol_pool)),
            // usdc_sol_initialized: AtomicBool::new(false),
            usdc_sol_state,
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
        let usdc_sol_state = self.usdc_sol_state.clone();

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
                                &usdc_sol_state,
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
        usdc_sol_state: &UsdcSolState,
    ) -> Result<(), AppError> {
        // First get USDC/SOL price
        let sol_price = {
            let pool_address = usdc_sol_state.pool.read().await.address;
            tracing::debug!("Fetching USDC/SOL pool data from address: {}", pool_address);

            let account = rpc_client.get_account(&pool_address)?;
            tracing::debug!("Got account data of length: {}", account.data.len());

            match RaydiumPool::from_account_data(&pool_address, &account.data) {
                Ok(pool) => {
                    tracing::debug!("Successfully parsed USDC/SOL pool");
                    match pool.fetch_price_data(rpc_client).await {
                        Ok(price_data) => {
                            tracing::debug!("Got USDC/SOL price data: {:?}", price_data);
                            let mut usdc_pool = usdc_sol_state.pool.write().await;
                            usdc_pool.last_sol_price = price_data.price_sol;
                            usdc_pool.last_update = chrono::Utc::now();
                            usdc_sol_state.initialized.store(true, Ordering::SeqCst);
                            tracing::info!(
                                "Updated USDC/SOL price to: {}",
                                usdc_pool.last_sol_price
                            );
                            usdc_pool.last_sol_price
                        }
                        Err(e) => {
                            tracing::error!("Failed to get USDC/SOL price data: {}", e);
                            return Err(AppError::PriceNotAvailable(format!(
                                "USDC/SOL price data: {}",
                                e
                            )));
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to parse USDC/SOL pool data: {}", e);
                    return Err(AppError::PriceNotAvailable(format!(
                        "USDC/SOL pool parse failed: {}",
                        e
                    )));
                }
            }
        };

        // Take a snapshot of the current active tokens
        let tokens: Vec<(String, Pubkey)> = {
            let active_tokens = active_tokens.read().await;
            active_tokens
                .iter()
                .map(|(addr, feed)| (addr.clone(), feed.pool_address))
                .collect()
        };

        for (token_address, pool_address) in tokens {
            match rpc_client.get_account(&pool_address) {
                Ok(account) => {
                    if let Ok(pool) = RaydiumPool::from_account_data(&pool_address, &account.data) {
                        if let Ok(mut price_data) = pool.fetch_price_data(rpc_client).await {
                            // Calculate USD values using SOL price
                            if !pool.is_usdc_pool() {
                                price_data.price_usd = Some(price_data.price_sol * sol_price);
                                price_data.market_cap *= sol_price;
                                price_data.liquidity_usd = Some(price_data.liquidity * sol_price); // Convert liquidity to USD

                                tracing::info!(
                                    "SOL price: {}, Token price in SOL: {}, Token price in USD: {}",
                                    sol_price,
                                    price_data.price_sol,
                                    price_data.price_usd.unwrap_or(0.0)
                                );
                                tracing::info!(
                                    "Market cap in SOL: {}, Market cap in USD: {}",
                                    price_data.market_cap / sol_price,
                                    price_data.market_cap
                                );
                                tracing::info!(
                                    "Liquidity in SOL: {}, Liquidity in USD: {}",
                                    price_data.liquidity,
                                    price_data.liquidity_usd.unwrap_or(0.0)
                                );
                            }

                            let update = PriceUpdate {
                                token_address: token_address.clone(),
                                price_sol: price_data.price_sol,
                                price_usd: price_data.price_usd,
                                market_cap: price_data.market_cap,
                                timestamp: Utc::now().timestamp(),
                                dex_type: DexType::Raydium,
                                liquidity: price_data.liquidity,
                                liquidity_usd: price_data.liquidity_usd,
                                pool_address: Some(pool_address.to_string()),
                                volume_24h: price_data.volume_24h,
                                volume_6h: price_data.volume_6h,
                                volume_1h: price_data.volume_1h,
                                volume_5m: price_data.volume_5m,
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
