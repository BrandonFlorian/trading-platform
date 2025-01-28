use crate::{
    config::PriceFeedConfig,
    raydium::{self, RaydiumPool},
};
use parking_lot::RwLock;
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    client_error::reqwest,
    rpc_client::RpcClient,
    rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
};
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use std::{collections::HashMap, error::Error, str::FromStr, sync::Arc};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use trading_common::{
    error::AppError,
    event_system::EventSystem,
    models::{ConnectionStatus, ConnectionType, PriceUpdate},
    ConnectionMonitor, RaydiumPoolInfo, RAYDIUM_V4,
};

pub struct PoolMonitor {
    rpc_client: Arc<RpcClient>,
    event_system: Arc<EventSystem>,
    connection_monitor: Arc<ConnectionMonitor>,
    price_sender: Option<broadcast::Sender<PriceUpdate>>,
    pools: Vec<RaydiumPool>,
    stop_sender: Option<mpsc::Sender<()>>,
    config: PriceFeedConfig,
    pool_cache: Arc<RwLock<HashMap<String, RaydiumPoolInfo>>>,
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
            price_sender: None,
            pools: Vec::new(),
            stop_sender: None,
            config,
            pool_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn start_monitoring(&mut self) -> Result<(), AppError> {
        let (price_tx, _) = broadcast::channel(100);
        let (stop_tx, stop_rx) = mpsc::channel(1);

        self.price_sender = Some(price_tx.clone());
        self.stop_sender = Some(stop_tx);

        // Start monitoring task
        let rpc_client = Arc::clone(&self.rpc_client);
        let connection_monitor = Arc::clone(&self.connection_monitor);
        let price_tx = price_tx.clone();
        let token_addresses = self.config.token_addresses.clone();
        tokio::spawn(async move {
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
                            &connection_monitor,
                            &token_addresses,
                        )
                        .await
                        {
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
        });

        Ok(())
    }

    pub async fn stop_monitoring(&mut self) -> Result<(), AppError> {
        if let Some(stop_sender) = self.stop_sender.take() {
            let _ = stop_sender.send(()).await;
        }
        Ok(())
    }

    pub fn subscribe_to_updates(&self) -> ReceiverStream<PriceUpdate> {
        let (tx, rx) = mpsc::channel(100);

        if let Some(price_sender) = &self.price_sender {
            let mut price_rx = price_sender.subscribe();

            tokio::spawn(async move {
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
        connection_monitor: &Arc<ConnectionMonitor>,
        token_addresses: &[String],
    ) -> Result<(), AppError> {
        let program_id = Pubkey::from_str(RAYDIUM_V4)
            .map_err(|_| AppError::InvalidPoolAddress(RAYDIUM_V4.to_string()))?;

        // Known pool address for debugging
        let known_pool = Pubkey::from_str("CpsMssqi3P9VMvNqxrdWVbSBCwyUHbGgNcrw7MorBq3g")?;

        // First verify we can read the known pool correctly
        match rpc_client.get_account_data(&known_pool) {
            Ok(data) => {
                tracing::info!("Successfully fetched known pool data, size: {}", data.len());

                // Dump data in chunks
                for i in (0..data.len()).step_by(32) {
                    let end = std::cmp::min(i + 32, data.len());
                    tracing::info!("Bytes {}-{}: {:?}", i, end, &data[i..end]);
                }

                // Let's also try to get the account info
                if let Ok(account) = rpc_client.get_account(&known_pool) {
                    tracing::info!("Account owner: {}", account.owner);
                }

                // Try alternate offsets for mint
                let potential_offsets = [232, 264, 368, 400, 464, 496, 576, 608];
                for offset in potential_offsets {
                    if offset + 32 <= data.len() {
                        let mut bytes = [0u8; 32];
                        bytes.copy_from_slice(&data[offset..offset + 32]);
                        let pubkey = Pubkey::new_from_array(bytes);
                        tracing::info!("Potential pubkey at offset {}: {}", offset, pubkey);
                    }
                }

                match RaydiumPool::from_account_data(&known_pool, &data) {
                    Ok(pool) => {
                        tracing::info!("Pool parsed successfully");
                        tracing::info!("Debug string: {}", pool.to_debug_string());
                    }
                    Err(e) => {
                        tracing::error!("Failed to parse pool data: {}", e);
                    }
                }
            }
            Err(e) => {
                tracing::error!("Failed to fetch pool data: {}", e);
            }
        }

        for token_mint in token_addresses {
            let token_pubkey = Pubkey::from_str(token_mint)?;

            tracing::info!("Searching for pool with token: {}", token_mint);

            // Log the actual bytes we're searching for
            tracing::info!("Token bytes (base58): {}", token_pubkey.to_string());
            tracing::info!("Token raw bytes: {:?}", token_pubkey.to_bytes());

            let token_bytes = token_pubkey.to_bytes();
            if let Ok(data) = rpc_client.get_account_data(&known_pool) {
                let positions = Self::find_token_in_data(&data, &token_bytes).await;
                tracing::info!("Found token bytes at offsets: {:?}", positions);
            }

            // Try both base and quote mint positions
            for (position, offset) in [("base", 432), ("quote", 400)] {
                let memcmp = Memcmp::new(432, MemcmpEncodedBytes::Base58(token_pubkey.to_string()));

                let filters = vec![RpcFilterType::DataSize(752), RpcFilterType::Memcmp(memcmp)];

                let config = solana_client::rpc_config::RpcProgramAccountsConfig {
                    filters: Some(filters),
                    sort_results: Some(true),
                    account_config: solana_client::rpc_config::RpcAccountInfoConfig {
                        encoding: Some(UiAccountEncoding::Base64),
                        commitment: Some(CommitmentConfig::confirmed()),
                        ..Default::default()
                    },
                    with_context: Some(true),
                };

                tracing::info!("Querying for {} mint at offset {}", position, offset);
                match rpc_client.get_program_accounts_with_config(&program_id, config) {
                    Ok(accounts) => {
                        tracing::info!(
                            "Found {} accounts for {} position",
                            accounts.len(),
                            position
                        );
                        for (pubkey, account) in accounts {
                            tracing::info!("Examining account: {}", pubkey);
                            if let Ok(pool) = RaydiumPool::from_account_data(&pubkey, &account.data)
                            {
                                tracing::info!(
                                    "Pool parsed successfully. Base mint: {}, Quote mint: {}",
                                    pool.base_mint,
                                    pool.quote_mint
                                );
                                // Rest of the price processing code...
                                if let Ok(price_data) = pool.fetch_price_data(rpc_client).await {
                                    let update = PriceUpdate {
                                        token_address: token_mint.to_string(),
                                        price_sol: price_data.price_sol,
                                        timestamp: chrono::Utc::now().timestamp(),
                                        dex_type: trading_common::dex::DexType::Raydium,
                                        liquidity: price_data.liquidity,
                                        market_cap: price_data.market_cap,
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
                    }
                    Err(e) => {
                        tracing::error!("Failed to query {} mint accounts: {}", position, e);
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn add_pool(&mut self, pool_address: &str) -> Result<(), AppError> {
        let pubkey = Pubkey::try_from(pool_address)
            .map_err(|_| AppError::InvalidPoolAddress(pool_address.to_string()))?;

        let pool = raydium::load_pool(&self.rpc_client, &pubkey).await?;
        self.pools.push(pool);
        Ok(())
    }

    pub async fn remove_pool(&mut self, pool_address: &str) {
        self.pools
            .retain(|pool| pool.address.to_string() != pool_address);
    }

    async fn get_pool_info(
        &self,
        token_address: &str,
    ) -> Result<Option<RaydiumPoolInfo>, AppError> {
        // Check cache first
        if let Some(info) = self.pool_cache.read().get(token_address) {
            return Ok(Some(info.clone()));
        }

        // Query Raydium API
        let url = format!("https://api.raydium.io/v2/main/pool/{}", token_address);

        let response = reqwest::Client::new()
            .get(&url)
            .header("User-Agent", "Mozilla/5.0")
            .send()
            .await
            .map_err(|e| AppError::RequestError(format!("Failed to query Raydium API: {}", e)))?;

        if !response.status().is_success() {
            return Ok(None);
        }

        let pool_info: RaydiumPoolInfo = response
            .json()
            .await
            .map_err(|e| AppError::JsonParseError(format!("Failed to parse pool info: {}", e)))?;

        // Cache the result
        self.pool_cache
            .write()
            .insert(token_address.to_string(), pool_info.clone());

        Ok(Some(pool_info))
    }

    async fn fetch_pool_prices(&self) -> Result<Vec<PriceUpdate>, AppError> {
        let mut updates = Vec::new();

        for pool in &self.pools {
            match pool.fetch_price_data(&self.rpc_client).await {
                Ok(price_data) => {
                    updates.push(PriceUpdate {
                        token_address: pool.base_mint.to_string(),
                        price_sol: price_data.price_sol,
                        timestamp: chrono::Utc::now().timestamp(),
                        dex_type: trading_common::dex::DexType::Raydium,
                        liquidity: price_data.liquidity,
                        market_cap: price_data.market_cap,
                        volume_24h: price_data.volume_24h,
                        volume_6h: price_data.volume_6h,
                        volume_1h: price_data.volume_1h,
                        volume_5m: price_data.volume_5m,
                    });
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to fetch price data for pool {}: {}",
                        pool.address,
                        e
                    );
                    continue;
                }
            }
        }

        Ok(updates)
    }

    async fn find_token_in_data(data: &[u8], token_bytes: &[u8]) -> Vec<usize> {
        let mut positions = Vec::new();
        for i in 0..data.len() - 32 {
            if &data[i..i + 32] == token_bytes {
                positions.push(i);
            }
        }
        positions
    }
}
