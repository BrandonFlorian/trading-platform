use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceFeedConfig {
    pub update_interval: Duration,
    pub token_addresses: Vec<String>,
    pub cache_duration: Duration,
    pub retry_interval: Duration,
    pub max_retries: u32,
}

impl Default for PriceFeedConfig {
    fn default() -> Self {
        Self {
            update_interval: Duration::from_secs(1),
            token_addresses: Vec::new(),
            cache_duration: Duration::from_secs(60),
            retry_interval: Duration::from_secs(1),
            max_retries: 3,
        }
    }
}

impl PriceFeedConfig {
    pub fn new() -> Self {
        Self::default()
    }

    fn default() -> Self {
        Self {
            update_interval: Duration::from_secs(1),
            token_addresses: Vec::new(),
            cache_duration: Duration::from_secs(60),
            retry_interval: Duration::from_secs(1),
            max_retries: 3,
        }
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
