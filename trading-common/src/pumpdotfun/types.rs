use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use thiserror::Error;

use super::LAMPORTS_PER_SOL;

#[derive(Debug, Clone)]
pub struct TokenMetadata {
    pub mint: Pubkey,
    pub decimals: u8,
    pub balance: u64,
    pub name: String,
    pub symbol: String,
    pub uri: Option<String>,
    pub update_authority: Option<String>,
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub struct BondingCurveData {
    pub padding: [u8; 8],
    pub virtual_token_reserves: i64,
    pub virtual_sol_reserves: i64,
    pub real_token_reserves: i64,
    pub real_sol_reserves: i64,
    pub token_total_supply: i64,
    pub complete: bool,
}

impl BondingCurveData {
    pub fn get_price(&self) -> f64 {
        self.virtual_sol_reserves as f64
            / (self.virtual_token_reserves as f64 * LAMPORTS_PER_SOL as f64)
    }

    pub fn calculate_buy_amount(&self, sol_quantity: f64) -> (u64, u64) {
        let sol_in_lamports = (sol_quantity * LAMPORTS_PER_SOL as f64) as u64;
        let token_out = ((sol_in_lamports as f64 * self.virtual_token_reserves as f64)
            / self.virtual_sol_reserves as f64) as u64;
        (token_out, sol_in_lamports)
    }

    pub fn calculate_sell_amount(&self, token_quantity: f64, decimals: u8) -> (u64, u64) {
        let token_amount = (token_quantity * 10f64.powi(decimals as i32)) as u64;
        let expected_sol_output = ((token_amount as f64 * self.virtual_sol_reserves as f64)
            / self.virtual_token_reserves as f64) as u64;
        (token_amount, expected_sol_output)
    }
}

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
        _decimals: u8,
    ) -> Self {
        let vtokenr = virtual_token_reserves as f64;
        let vsolr = virtual_sol_reserves as f64;

        let sol_in_lamports = sol_quantity * LAMPORTS_PER_SOL as f64;
        let token_out = ((sol_in_lamports * vtokenr) / vsolr) as u64;

        // Convert token_out to human readable (divide by 10^6)
        let max_token_output = token_out as f64 / 1_000_000.0; // 6 decimals
        let min_token_output = max_token_output * (1.0 - slippage);

        Self {
            token_out,
            max_sol_cost: (sol_in_lamports * (1.0 + slippage)) as u64,
            price_per_token: vsolr / (vtokenr * LAMPORTS_PER_SOL as f64),
            adjusted_max_token_output: max_token_output,
            adjusted_min_token_output: min_token_output,
        }
    }
}
