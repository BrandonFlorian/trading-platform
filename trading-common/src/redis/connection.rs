use crate::error::AppError;
use crate::models::{ConnectionStatus, ConnectionType};
use crate::ConnectionMonitor;
use redis::aio::ConnectionManager;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct RedisConnection {
    pub connection: Arc<Mutex<redis::aio::ConnectionManager>>,
    pub connection_monitor: Arc<ConnectionMonitor>,
}

impl RedisConnection {
    pub async fn new(
        redis_url: &str,
        connection_monitor: Arc<ConnectionMonitor>,
    ) -> Result<Self, AppError> {
        let redis_url = Self::ensure_resp3_protocol(redis_url);
        let client =
            redis::Client::open(redis_url).map_err(|e| AppError::RedisError(e.to_string()))?;
        let connection = ConnectionManager::new(client)
            .await
            .map_err(|e| AppError::RedisError(e.to_string()))?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            connection_monitor,
        })
    }

    pub async fn is_healthy(&mut self) -> Result<bool, AppError> {
        match redis::cmd("PING")
            .query_async::<String>(&mut *self.connection.lock().await)
            .await
        {
            Ok(response) => Ok(response == "PONG"),
            Err(e) => {
                self.connection_monitor
                    .update_status(
                        ConnectionType::Redis,
                        ConnectionStatus::Error,
                        Some(e.to_string()),
                    )
                    .await;
                Err(AppError::RedisError(e.to_string()))
            }
        }
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

    pub fn create_redis_client(redis_url: &str) -> Result<redis::Client, AppError> {
        let redis_url = Self::ensure_resp3_protocol(redis_url);
        redis::Client::open(redis_url)
            .map_err(|e| AppError::RedisError(format!("Failed to create Redis client: {}", e)))
    }

    pub async fn create_connection() -> Result<redis::aio::ConnectionManager, AppError> {
        let redis_url = std::env::var("REDIS_URL")
            .map_err(|e| AppError::RedisError(format!("REDIS_URL not set: {}", e)))?;

        let redis_url = Self::ensure_resp3_protocol(&redis_url);

        let client = redis::Client::open(redis_url)
            .map_err(|e| AppError::RedisError(format!("Failed to create Redis client: {}", e)))?;

        let manager_config =
            redis::aio::ConnectionManagerConfig::new().set_automatic_resubscription();

        ConnectionManager::new_with_config(client, manager_config)
            .await
            .map_err(|e| {
                AppError::RedisError(format!("Failed to create connection manager: {}", e))
            })
    }

    pub fn spawn_keepalive(mut con: redis::aio::ConnectionManager) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                if let Err(e) = redis::cmd("PING").query_async::<String>(&mut con).await {
                    tracing::error!("Redis keep-alive failed: {}", e);
                    break;
                }
            }
        });
    }
}
