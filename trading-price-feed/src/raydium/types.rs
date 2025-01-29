use std::str::FromStr;

use borsh::{BorshDeserialize, BorshSerialize};
use solana_sdk::pubkey::Pubkey;

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct PoolState {
    pub status: u64,
    pub nonce: u64,
    pub order_num: u64,
    pub depth: u64,
    pub base_decimals: u64,
    pub quote_decimals: u64,
    pub state: u64,
    pub reset_flag: u64,
    pub min_size: u64,
    pub vol_max_cut_ratio: u64,
    pub amount_wave_ratio: u64,
    pub base_lot_size: u64,
    pub quote_lot_size: u64,
    pub min_price_multiplier: u64,
    pub max_price_multiplier: u64,
    pub system_decimal_value: u64,
    pub min_separate_numerator: u64,
    pub min_separate_denominator: u64,
    pub trade_fee_numerator: u64,
    pub trade_fee_denominator: u64,
    pub pnl_numerator: u64,
    pub pnl_denominator: u64,
    pub swap_fee_numerator: u64,
    pub swap_fee_denominator: u64,
    pub base_need_take_pnl: u64,
    pub quote_need_take_pnl: u64,
    pub quote_total_pnl: u64,
    pub base_total_pnl: u64,
    pub pool_open_time: u64,
    pub punish_pc_amount: u64,
    pub punish_coin_amount: u64,
    pub ordere_book_to_init_time: u64,
    pub base_vault_key: Pubkey,
    pub quote_vault_key: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub lp_mint: Pubkey,
    pub open_orders: Pubkey,
    pub market_id: Pubkey,
    pub market_program_id: Pubkey,
    pub target_orders: Pubkey,
    pub withdraw_queue: Pubkey,
    pub lp_vault: Pubkey,
    pub owner: Pubkey,
    pub pnl_owner: Pubkey,
}

#[derive(Debug, Clone)]
pub struct PriceData {
    pub price_sol: f64,
    pub price_usd: Option<f64>,
    pub liquidity: f64,
    pub liquidity_usd: Option<f64>,
    pub market_cap: f64,
    pub volume_24h: Option<f64>,
    pub volume_6h: Option<f64>,
    pub volume_1h: Option<f64>,
    pub volume_5m: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct UsdcSolPool {
    pub address: Pubkey,
    pub usdc_mint: Pubkey,
    pub sol_mint: Pubkey,
    pub last_sol_price: f64,
    pub last_update: chrono::DateTime<chrono::Utc>,
}

impl UsdcSolPool {
    pub fn new(
        address: &str,
        usdc_mint: &str,
        sol_mint: &str,
    ) -> Result<Self, trading_common::error::AppError> {
        Ok(Self {
            address: Pubkey::from_str(address)?,
            usdc_mint: Pubkey::from_str(usdc_mint)?,
            sol_mint: Pubkey::from_str(sol_mint)?,
            last_sol_price: 0.0,
            last_update: chrono::Utc::now(),
        })
    }
}
