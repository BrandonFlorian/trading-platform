use parking_lot::RwLock;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tokio::sync::broadcast;
use trading_common::{
    error::AppError,
    models::{PriceUpdate, SolPriceUpdate},
    redis::RedisConnection,
};

use crate::raydium::RaydiumPool;

pub struct PriceCalculator {
    pub current_sol_price: Arc<RwLock<f64>>,
    pub price_sender: broadcast::Sender<PriceUpdate>,
    pub redis_connection: Arc<RedisConnection>,
    pub rpc_client: Arc<RpcClient>,
}

impl PriceCalculator {
    pub fn new(
        price_sender: broadcast::Sender<PriceUpdate>,
        redis_connection: Arc<RedisConnection>,
        rpc_client: Arc<RpcClient>,
    ) -> Self {
        Self {
            current_sol_price: Arc::new(RwLock::new(0.0)),
            price_sender,
            redis_connection,
            rpc_client,
        }
    }

    /// Update SOL price from external feed
    pub async fn update_sol_price(&self, update: SolPriceUpdate) {
        let mut price = self.current_sol_price.write();
        *price = update.price_usd;
    }

    /// Calculate price information from pool data
    pub async fn calculate_token_price(
        &self,
        pool: &RaydiumPool,
        token_mint: &Pubkey,
    ) -> Result<PriceUpdate, AppError> {
        let sol_price = *self.current_sol_price.read();
        let price_data = pool.fetch_price_data(&self.rpc_client, sol_price).await?;

        Ok(PriceUpdate {
            token_address: token_mint.to_string(),
            price_sol: price_data.price_sol,
            price_usd: price_data.price_usd,
            market_cap: price_data.market_cap,
            timestamp: chrono::Utc::now().timestamp(),
            dex_type: trading_common::dex::DexType::Raydium,
            liquidity: price_data.liquidity,
            liquidity_usd: price_data.liquidity_usd,
            pool_address: Some(pool.address.to_string()),
            volume_24h: price_data.volume_24h,
            volume_6h: price_data.volume_6h,
            volume_1h: price_data.volume_1h,
            volume_5m: price_data.volume_5m,
        })
    }

    /// Broadcast a price update to subscribers
    pub async fn broadcast_price_update(&self, update: PriceUpdate) -> Result<(), AppError> {
        self.price_sender.send(update).map_err(|e| {
            AppError::NotificationError(format!("Failed to send price update: {}", e))
        })?;
        Ok(())
    }
}
