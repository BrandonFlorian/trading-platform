use parking_lot::RwLock;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tokio::sync::broadcast;
use trading_common::{
    error::AppError,
    models::{PriceUpdate, SolPriceUpdate},
    redis::RedisPool,
};

use crate::raydium::RaydiumPool;

pub struct PriceCalculator {
    pub redis_connection: Arc<RedisPool>,
    pub rpc_client: Arc<RpcClient>,
}

impl PriceCalculator {
    pub fn new(redis_connection: Arc<RedisPool>, rpc_client: Arc<RpcClient>) -> Self {
        Self {
            redis_connection,
            rpc_client,
        }
    }

    /// Calculate price information from pool data
    pub async fn calculate_token_price(
        &self,
        pool: &RaydiumPool,
        token_mint: &Pubkey,
    ) -> Result<PriceUpdate, AppError> {
        tracing::info!("Calculating token price for {}", token_mint);
        let sol_price = self
            .redis_connection
            .get_sol_price()
            .await?
            .ok_or_else(|| AppError::RedisError("SOL price not found in cache".to_string()))?;

        let price_data = pool.fetch_price_data(&self.rpc_client, sol_price).await?;

        // Calculate USD values
        let price_usd = Some(price_data.price_sol * sol_price);
        let liquidity_usd = price_data.liquidity.map(|l| l * sol_price);

        Ok(PriceUpdate {
            token_address: token_mint.to_string(),
            price_sol: price_data.price_sol,
            price_usd,
            market_cap: price_data.market_cap,
            timestamp: chrono::Utc::now().timestamp(),
            dex_type: trading_common::dex::DexType::Raydium,
            liquidity: price_data.liquidity,
            liquidity_usd,
            pool_address: Some(pool.address.to_string()),
            volume_24h: price_data.volume_24h,
            volume_6h: price_data.volume_6h,
            volume_1h: price_data.volume_1h,
            volume_5m: price_data.volume_5m,
        })
    }
}
