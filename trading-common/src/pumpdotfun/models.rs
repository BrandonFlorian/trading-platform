use crate::error::AppError;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpFunCoinData {
    pub mint: String,
    pub name: String,
    pub symbol: String,
    pub description: String,
    pub image_uri: String,
    pub metadata_uri: String,
    pub twitter: String,
    pub telegram: String,
    pub bonding_curve: String,
    pub associated_bonding_curve: String,
    pub creator: String,
    pub created_timestamp: i64,
    pub raydium_pool: Option<String>,
    pub complete: bool,
    pub virtual_sol_reserves: i64,
    pub virtual_token_reserves: i64,
    pub total_supply: i64,
    pub website: String,
    pub show_name: bool,
    pub king_of_the_hill_timestamp: Option<i64>,
    pub market_cap: f64,
    pub reply_count: i32,
    pub last_reply: i64,
    pub nsfw: bool,
    pub market_id: Option<String>,
    pub inverted: Option<String>,
    pub profile_image: String,
    pub usd_market_cap: f64,
}

#[derive(Debug, Clone)]
pub struct PumpFunTokenContainer {
    pub mint_address: Pubkey,
    pub pump_fun_coin_data: Option<PumpFunCoinData>,
    pub program_account_info: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct TokenAccountOwnerContainer {
    pub owner_address: Pubkey,
    pub mint_address: Pubkey,
    pub token_account_address: Option<Pubkey>,
}

#[derive(Error, Debug)]
pub enum PumpFunError {
    #[error("Surf error: {0}")]
    SurfError(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[error("Anyhow error: {0}")]
    AnyhowError(#[from] anyhow::Error),
}
