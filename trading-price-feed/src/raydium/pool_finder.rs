use crate::raydium::RaydiumPool;
use serde::{Deserialize, Serialize};
use solana_account_decoder::UiAccountEncoding;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use trading_common::{error::AppError, redis::RedisPool, RAYDIUM_V4, WSOL};

#[derive(Clone)]
pub struct RaydiumPoolFinder {
    rpc_client: Arc<RpcClient>,
    redis_connection: Arc<RedisPool>,
    active_pools: Arc<RwLock<HashMap<String, CachedPool>>>,
    metrics: Arc<RwLock<PoolFinderMetrics>>,
}

struct CachedPool {
    pool: RaydiumPool,
    last_updated: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
struct PoolFinderMetrics {
    cache_hits: u64,
    cache_misses: u64,
    rpc_calls: u64,
    failed_lookups: u64,
}

impl RaydiumPoolFinder {
    const POOL_CACHE_TTL: Duration = Duration::from_secs(30);
    const METADATA_CACHE_TTL: Duration = Duration::from_secs(3600);

    pub fn new(rpc_client: Arc<RpcClient>, redis_connection: Arc<RedisPool>) -> Self {
        Self {
            rpc_client,
            redis_connection,
            active_pools: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(RwLock::new(PoolFinderMetrics::default())),
        }
    }

    pub async fn find_pool(&self, token_address: &str) -> Result<RaydiumPool, AppError> {
        // Try cache first
        if let Some(pool) = self.get_from_cache(token_address).await? {
            return Ok(pool);
        }

        // Cache miss - do RPC lookup
        let pool = self.find_pool_from_rpc(token_address).await?;

        // Cache the result
        self.cache_pool(token_address, &pool).await?;

        Ok(pool)
    }

    pub async fn find_pool_from_rpc(&self, token_address: &str) -> Result<RaydiumPool, AppError> {
        tracing::info!("Finding pool for token {}", token_address);
        let token_pubkey = Pubkey::from_str(token_address)
            .map_err(|_| AppError::InvalidPoolAddress(token_address.to_string()))?;

        // SOL token pubkey - we'll use this to validate quote mint
        let sol_pubkey = Pubkey::from_str(WSOL)?;

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

        self.metrics.write().await.rpc_calls += 1;
        let accounts = self
            .rpc_client
            .get_program_accounts_with_config(&raydium_program, config)
            .map_err(|e| AppError::SolanaRpcError { source: e })?;

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

        match best_pool {
            Some((pool, _)) => Ok(pool),
            None => {
                self.metrics.write().await.failed_lookups += 1;
                Err(AppError::PoolNotFound(token_address.to_string()))
            }
        }
    }

    async fn get_from_cache(&self, token_address: &str) -> Result<Option<RaydiumPool>, AppError> {
        // Check in-memory cache first
        let active_pools = self.active_pools.read().await;
        if let Some(cached) = active_pools.get(token_address) {
            if cached.last_updated.elapsed() < Self::POOL_CACHE_TTL {
                self.metrics.write().await.cache_hits += 1;
                return Ok(Some(cached.pool.clone()));
            }
        }
        drop(active_pools);

        // Try Redis cache
        if let Some(pool_data) = self
            .redis_connection
            .get_pool_data(&Pubkey::from_str(token_address)?)
            .await?
        {
            self.metrics.write().await.cache_hits += 1;
            let cached_data: CachedPoolData = serde_json::from_str(&pool_data)?;
            return Ok(Some(cached_data.to_pool()?));
        }

        self.metrics.write().await.cache_misses += 1;
        Ok(None)
    }

    async fn cache_pool(&self, token_address: &str, pool: &RaydiumPool) -> Result<(), AppError> {
        // Create serializable version for Redis
        let cached_data = CachedPoolData::from(pool);

        // Update Redis cache
        self.redis_connection
            .set_pool_data(
                &Pubkey::from_str(token_address)?,
                &serde_json::to_string(&cached_data)?,
                Self::POOL_CACHE_TTL,
            )
            .await?;

        // Update in-memory cache
        let mut active_pools = self.active_pools.write().await;
        active_pools.insert(
            token_address.to_string(),
            CachedPool {
                pool: pool.clone(),
                last_updated: Instant::now(),
            },
        );

        Ok(())
    }

    pub fn start_background_refresh(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(15));
            loop {
                interval.tick().await;
                let finder = self.clone();
                let _ = finder.refresh_active_pools().await;
            }
        });
    }

    async fn refresh_active_pools(&self) -> Result<(), AppError> {
        let tokens: Vec<String> = {
            let active_pools = self.active_pools.read().await;
            active_pools.keys().cloned().collect()
        };

        for token_address in tokens {
            if let Err(e) = self.find_pool(&token_address).await {
                tracing::error!("Failed to refresh pool {}: {}", token_address, e);
            }
        }

        Ok(())
    }

    pub async fn get_metrics(&self) -> PoolFinderMetricsSnapshot {
        let metrics = self.metrics.read().await;
        PoolFinderMetricsSnapshot {
            cache_hits: metrics.cache_hits,
            cache_misses: metrics.cache_misses,
            rpc_calls: metrics.rpc_calls,
            failed_lookups: metrics.failed_lookups,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolFinderMetricsSnapshot {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub rpc_calls: u64,
    pub failed_lookups: u64,
}
