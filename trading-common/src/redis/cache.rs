use super::RedisConnection;
use crate::error::AppError;
use redis::AsyncCommands;
use solana_sdk::pubkey::Pubkey;
use std::time::Duration;

pub struct CacheConfig {
    pub sol_price_ttl: Duration,
    pub token_metadata_ttl: Duration,
    pub pool_data_ttl: Duration,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            sol_price_ttl: Duration::from_secs(2),
            token_metadata_ttl: Duration::from_secs(3600),
            pool_data_ttl: Duration::from_secs(30),
        }
    }
}

impl RedisConnection {
    pub async fn get_token_decimals(&self, mint: &Pubkey) -> Result<Option<u8>, AppError> {
        let key = format!("token:{}:decimals", mint);
        let mut conn = self.connection.lock().await;
        conn.get(&key)
            .await
            .map_err(|e| AppError::RedisError(e.to_string()))
    }

    pub async fn set_token_decimals(
        &self,
        mint: &Pubkey,
        decimals: u8,
        ttl: Duration,
    ) -> Result<(), AppError> {
        let key = format!("token:{}:decimals", mint);
        let mut conn = self.connection.lock().await;
        conn.set_ex(&key, decimals, ttl.as_secs())
            .await
            .map_err(|e| AppError::RedisError(e.to_string()))
    }

    pub async fn get_sol_price(&self) -> Result<Option<f64>, AppError> {
        let mut conn = self.connection.lock().await;
        conn.get("sol:price")
            .await
            .map_err(|e| AppError::RedisError(e.to_string()))
    }

    pub async fn set_sol_price(&mut self, price: f64, ttl: Duration) -> Result<(), AppError> {
        let mut conn = self.connection.lock().await;
        conn.set_ex("sol:price", price, ttl.as_secs())
            .await
            .map_err(|e| AppError::RedisError(e.to_string()))
    }

    pub async fn get_pool_data(
        &mut self,
        pool_address: &Pubkey,
    ) -> Result<Option<String>, AppError> {
        let key = format!("pool:{}:data", pool_address);
        let mut conn = self.connection.lock().await;
        conn.get(&key)
            .await
            .map_err(|e| AppError::RedisError(e.to_string()))
    }

    pub async fn set_pool_data(
        &mut self,
        pool_address: &Pubkey,
        data: &str,
        ttl: Duration,
    ) -> Result<(), AppError> {
        let key = format!("pool:{}:data", pool_address);
        let mut conn = self.connection.lock().await;
        conn.set_ex(&key, data, ttl.as_secs())
            .await
            .map_err(|e| AppError::RedisError(e.to_string()))
    }
}
