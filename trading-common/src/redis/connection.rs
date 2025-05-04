use bb8_redis::{bb8, redis::AsyncCommands, RedisConnectionManager};
use redis::Client;
use std::sync::Arc;
use std::time::Duration;

use crate::{
    error::AppError,
    models::{ConnectionStatus, ConnectionType},
    ConnectionMonitor,
};

const DEFAULT_POOL_SIZE: u32 = 20;
const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct RedisPool {
    pool: bb8::Pool<RedisConnectionManager>,
    connection_monitor: Arc<ConnectionMonitor>,
    connection_url: String,
}

impl RedisPool {
    pub async fn new(
        redis_url: &str,
        connection_monitor: Arc<ConnectionMonitor>,
    ) -> Result<Self, AppError> {
        let redis_url = Self::ensure_resp3_protocol(redis_url);
        tracing::info!("Initializing Redis connection pool at {}", redis_url);

        let manager = RedisConnectionManager::new(redis_url.clone())
            .map_err(|e| AppError::RedisError(format!("Failed to create Redis manager: {}", e)))?;

        let pool = bb8::Pool::builder()
            .max_size(DEFAULT_POOL_SIZE)
            .connection_timeout(DEFAULT_CONNECTION_TIMEOUT)
            .build(manager)
            .await
            .map_err(|e| {
                AppError::RedisError(format!("Failed to create connection pool: {}", e))
            })?;

        // Test the connection
        let mut conn = pool
            .get()
            .await
            .map_err(|e| AppError::RedisError(format!("Failed to get connection: {}", e)))?;

        let ping: String = conn
            .ping()
            .await
            .map_err(|e| AppError::RedisError(format!("Failed to ping Redis: {}", e)))?;

        if ping != "PONG" {
            return Err(AppError::RedisError(
                "Failed to connect to Redis".to_string(),
            ));
        }

        connection_monitor
            .update_status(ConnectionType::Redis, ConnectionStatus::Connected, None)
            .await;

        Ok(Self {
            pool: pool.clone(),
            connection_monitor,
            connection_url: redis_url,
        })
    }

    fn ensure_resp3_protocol(url: &str) -> String {
        if !url.contains("protocol=resp3") {
            if url.contains('?') {
                format!("{}&protocol=resp3", url)
            } else {
                format!("{}?protocol=resp3", url)
            }
        } else {
            url.to_string()
        }
    }

    pub async fn get_connection(
        &self,
    ) -> Result<bb8::PooledConnection<'_, RedisConnectionManager>, AppError> {
        self.pool
            .get()
            .await
            .map_err(|e| AppError::RedisError(format!("Failed to get connection from pool: {}", e)))
    }

    // Create a dedicated PubSub connection using the modern multiplexed approach
    pub async fn subscribe(&self, channel: &str) -> Result<redis::aio::PubSub, AppError> {
        let client = Client::open(self.connection_url.clone())
            .map_err(|e| AppError::RedisError(format!("Failed to create Redis client: {}", e)))?;

        // Get a dedicated PubSub connection
        let mut pubsub = client
            .get_async_pubsub()
            .await
            .map_err(|e| AppError::RedisError(format!("Failed to get PubSub connection: {}", e)))?;

        // Subscribe to the channel
        pubsub
            .subscribe(channel)
            .await
            .map_err(|e| AppError::RedisError(format!("Failed to subscribe to channel: {}", e)))?;

        Ok(pubsub)
    }

    // Normal pool operations
    // pub async fn get_token_decimals(&self, mint: &str) -> Result<Option<u8>, AppError> {
    //     let mut conn = self.get_connection().await?;
    //     let key = format!("token:{}:decimals", mint);

    //     conn.get(&key)
    //         .await
    //         .map_err(|e| AppError::RedisError(format!("Failed to get token decimals: {}", e)))
    // }

    // pub async fn set_token_decimals(
    //     &self,
    //     mint: &str,
    //     decimals: u8,
    //     ttl: Duration,
    // ) -> Result<(), AppError> {
    //     let mut conn = self.get_connection().await?;
    //     let key = format!("token:{}:decimals", mint);

    //     conn.set_ex(&key, decimals, ttl.as_secs())
    //         .await
    //         .map_err(|e| AppError::RedisError(format!("Failed to set token decimals: {}", e)))
    // }

    pub async fn publish<T: serde::Serialize>(
        &self,
        channel: &str,
        message: &T,
    ) -> Result<(), AppError> {
        let mut conn = self.get_connection().await?;
        let payload = serde_json::to_string(message)
            .map_err(|e| AppError::JsonParseError(format!("Failed to serialize message: {}", e)))?;

        conn.publish(channel, payload)
            .await
            .map_err(|e| AppError::RedisError(format!("Failed to publish message: {}", e)))
    }

    pub async fn is_healthy(&self) -> bool {
        if let Ok(mut conn) = self.get_connection().await {
            match conn.ping::<String>().await {
                Ok(response) => response == "PONG",
                Err(_) => false,
            }
        } else {
            false
        }
    }
}
