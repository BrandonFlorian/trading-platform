use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use thiserror::Error;

use super::LAMPORTS_PER_SOL;

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

#[derive(Debug)]
pub struct PumpFunCalcResult {
    pub token_out: u64,
    pub max_sol_cost: u64,
    pub price_per_token: f64,
    pub adjusted_max_token_output: f64,
    pub adjusted_min_token_output: f64,
}

impl PumpFunCalcResult {
    pub fn new(
        virtual_token_reserves: i64,
        virtual_sol_reserves: i64,
        sol_quantity: f64,
        slippage: f64,
        decimals: u8,
    ) -> Self {
        let vtokenr = virtual_token_reserves as f64;
        let vsolr = virtual_sol_reserves as f64;

        // Calculate expected output
        let sol_lamports = sol_quantity * LAMPORTS_PER_SOL as f64;
        let token_out = ((sol_lamports * vtokenr) / vsolr) as u64;

        // Calculate price per token in SOL (not lamports)
        let price_per_token = vsolr / (vtokenr * LAMPORTS_PER_SOL as f64);

        // Calculate max output with proper decimal handling
        let max_token_output = (token_out as f64) / 10f64.powi(decimals as i32);
        let min_token_output = max_token_output * (1.0 - slippage);

        // Max SOL cost with slippage
        let max_sol_cost = (sol_lamports * (1.0 + slippage)) as u64;

        Self {
            token_out,
            max_sol_cost,
            price_per_token,
            adjusted_max_token_output: max_token_output,
            adjusted_min_token_output: min_token_output,
        }
    }
}
