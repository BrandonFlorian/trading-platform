use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{env, time::Duration};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceFeedConfig {
    pub rpc_ws_url: String,    // Solana RPC WebSocket URL
    pub server_ws_url: String, // Our WebSocket server URL for clients
    pub ws_port: u16,          // Add this
    pub http_port: u16,        // Add this
    pub update_interval: Duration,
    pub token_addresses: Vec<String>,
    pub cache_duration: Duration,
    pub retry_interval: Duration,
    pub max_retries: u32,
}

impl PriceFeedConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let rpc_ws_url = env::var("SOLANA_RPC_WS_URL").context("SOLANA_RPC_WS_URL must be set")?;

        let host = env::var("PRICE_FEED_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());

        let ws_port = env::var("PRICE_FEED_WS_PORT")
            .context("PRICE_FEED_WS_PORT must be set")?
            .parse()?;

        let http_port = env::var("PRICE_FEED_PORT")
            .context("PRICE_FEED_PORT must be set")?
            .parse()?;

        let update_interval = env::var("PRICE_FEED_UPDATE_INTERVAL")
            .map(|v| v.parse::<u64>().unwrap_or(1))
            .unwrap_or(1);

        let cache_duration = env::var("PRICE_FEED_CACHE_DURATION")
            .map(|v| v.parse::<u64>().unwrap_or(60))
            .unwrap_or(60);

        let retry_interval = env::var("PRICE_FEED_RETRY_INTERVAL")
            .map(|v| v.parse::<u64>().unwrap_or(1))
            .unwrap_or(1);

        let max_retries = env::var("PRICE_FEED_MAX_RETRIES")
            .map(|v| v.parse::<u32>().unwrap_or(3))
            .unwrap_or(3);

        Ok(Self {
            rpc_ws_url,
            server_ws_url: format!("ws://{}:{}", host, ws_port),
            ws_port,
            http_port,
            update_interval: Duration::from_secs(update_interval),
            token_addresses: Vec::new(),
            cache_duration: Duration::from_secs(cache_duration),
            retry_interval: Duration::from_secs(retry_interval),
            max_retries,
        })
    }

    pub fn with_update_interval(mut self, interval: Duration) -> Self {
        self.update_interval = interval;
        self
    }

    pub fn with_cache_duration(mut self, duration: Duration) -> Self {
        self.cache_duration = duration;
        self
    }
}
