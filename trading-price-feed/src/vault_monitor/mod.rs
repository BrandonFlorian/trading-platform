pub mod connection_manager;
pub mod subscription_manager;
pub mod vault_subscriber;
pub mod price_calculator;

pub use connection_manager::WebSocketManager;
pub use subscription_manager::SubscriptionManager;
pub use vault_subscriber::VaultSubscriber;
pub use price_calculator::PriceCalculator;

use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Represents a vault (token account) subscription
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultSubscription {
    pub subscription_id: String,
    pub vault_address: Pubkey,
    pub vault_type: VaultType,
    pub token_address: String,
    pub last_balance: Option<u64>,
    pub decimals: u8,
}

/// Type of vault being monitored
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum VaultType {
    Base,   // Token being traded
    Quote,  // SOL (quote currency)
}

/// Cached balance data from vault account
#[derive(Debug, Clone)]
pub struct VaultBalance {
    pub vault_address: Pubkey,
    pub balance: u64,
    pub owner: Pubkey,
    pub mint: Pubkey,
    pub last_updated: std::time::Instant,
}

/// Pool monitoring state
#[derive(Debug, Clone)]
pub struct PoolMonitorState {
    pub token_address: String,
    pub pool_address: Pubkey,
    pub base_vault: Pubkey,
    pub quote_vault: Pubkey,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    pub base_balance: Option<u64>,
    pub quote_balance: Option<u64>,
    pub last_price_update: Option<std::time::Instant>,
}

/// WebSocket message routing information
#[derive(Debug, Clone)]
pub struct SubscriptionRoute {
    pub subscription_id: String,
    pub token_address: String,
    pub vault_type: VaultType,
    pub vault_address: Pubkey,
}

/// Price update event from vault balance changes
#[derive(Debug, Clone)]
pub struct VaultPriceUpdate {
    pub token_address: String,
    pub price_sol: f64,
    pub price_usd: Option<f64>,
    pub base_balance: u64,
    pub quote_balance: u64,
    pub liquidity_sol: f64,
    pub timestamp: i64,
}