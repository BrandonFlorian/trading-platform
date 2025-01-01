use std::str::FromStr;

use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

#[derive(Debug, Serialize, Deserialize)]
pub struct RaydiumApiResponse {
    pub id: String,
    pub success: bool,
    pub data: RaydiumApiData,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RaydiumApiData {
    pub count: i32,
    pub data: Vec<RaydiumPoolInfo>,
    #[serde(rename = "hasNextPage")]
    pub has_next_page: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaydiumPoolInfo {
    #[serde(rename = "type")]
    pub pool_type: String,
    #[serde(rename = "programId")]
    pub program_id: String,
    pub id: String,
    #[serde(rename = "mintA")]
    pub mint_a: TokenInfo,
    #[serde(rename = "mintB")]
    pub mint_b: TokenInfo,
    pub price: f64,
    #[serde(rename = "mintAmountA")]
    pub mint_amount_a: f64,
    #[serde(rename = "mintAmountB")]
    pub mint_amount_b: f64,
    #[serde(rename = "feeRate")]
    pub fee_rate: f64,
    #[serde(rename = "openTime")]
    pub open_time: String,
    pub tvl: f64,
    #[serde(rename = "marketId")]
    pub market_id: String,
    #[serde(rename = "lpMint")]
    pub lp_mint: TokenInfo,
    #[serde(rename = "lpPrice")]
    pub lp_price: f64,
    #[serde(rename = "lpAmount")]
    pub lp_amount: f64,
    #[serde(rename = "burnPercent")]
    pub burn_percent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenInfo {
    #[serde(rename = "chainId")]
    pub chain_id: i32,
    pub address: String,
    #[serde(rename = "programId")]
    pub program_id: String,
    #[serde(rename = "logoURI")]
    pub logo_uri: String,
    pub symbol: String,
    pub name: String,
    pub decimals: i32,
    pub tags: Vec<String>,
    pub extensions: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PoolStats {
    pub volume: f64,
    #[serde(rename = "volumeQuote")]
    pub volume_quote: f64,
    #[serde(rename = "volumeFee")]
    pub volume_fee: f64,
    pub apr: f64,
    #[serde(rename = "feeApr")]
    pub fee_apr: f64,
    #[serde(rename = "priceMin")]
    pub price_min: f64,
    #[serde(rename = "priceMax")]
    pub price_max: f64,
    #[serde(rename = "rewardApr")]
    pub reward_apr: Vec<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RaydiumPool {
    pub id: String,
    #[serde(rename = "baseMint")]
    pub base_mint: String,
    #[serde(rename = "quoteMint")]
    pub quote_mint: String,
    #[serde(rename = "lpMint")]
    pub lp_mint: String,
    #[serde(rename = "baseDecimals")]
    pub base_decimals: u8,
    #[serde(rename = "quoteDecimals")]
    pub quote_decimals: u8,
    pub version: u8,
    #[serde(rename = "programId")]
    pub program_id: String,
    #[serde(rename = "authority")]
    pub authority: String,
    #[serde(rename = "openOrders")]
    pub open_orders: String,
    #[serde(rename = "targetOrders")]
    pub target_orders: String,
    #[serde(rename = "baseVault")]
    pub base_vault: String,
    #[serde(rename = "quoteVault")]
    pub quote_vault: String,
    #[serde(rename = "marketId")]
    pub market_id: String,
    #[serde(rename = "marketProgramId")]
    pub market_program_id: String,
    #[serde(rename = "marketAuthority")]
    pub market_authority: String,
    #[serde(rename = "marketBaseVault")]
    pub market_base_vault: String,
    #[serde(rename = "marketQuoteVault")]
    pub market_quote_vault: String,
    #[serde(rename = "marketBids")]
    pub market_bids: String,
    #[serde(rename = "marketAsks")]
    pub market_asks: String,
    #[serde(rename = "marketEventQueue")]
    pub market_event_queue: String,
}

#[derive(Debug)]
pub struct PoolKeys {
    pub id: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub base_vault: Pubkey,
    pub quote_vault: Pubkey,
    pub open_orders: Pubkey,
    pub target_orders: Pubkey,
    pub market_id: Pubkey,
    pub market_base_vault: Pubkey,
    pub market_quote_vault: Pubkey,
    pub market_authority: Pubkey,
    pub bids: Pubkey,
    pub asks: Pubkey,
    pub event_queue: Pubkey,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RaydiumPoolKeyResponse {
    pub success: bool,
    pub data: Vec<RaydiumPoolKeyInfo>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RaydiumPoolKeyInfo {
    #[serde(rename = "programId")]
    pub program_id: String,
    pub id: String,
    #[serde(rename = "mintA")]
    pub mint_a: TokenInfo,
    #[serde(rename = "mintB")]
    pub mint_b: TokenInfo,
    #[serde(rename = "openTime")]
    pub open_time: String,
    pub vault: VaultInfo,
    pub authority: String,
    #[serde(rename = "openOrders")]
    pub open_orders: String,
    #[serde(rename = "targetOrders")]
    pub target_orders: String,
    #[serde(rename = "mintLp")]
    pub mint_lp: TokenInfo,
    #[serde(rename = "marketId")]
    pub market_id: String,
    #[serde(rename = "marketProgramId")]
    pub market_program_id: String,
    #[serde(rename = "marketAuthority")]
    pub market_authority: String,
    #[serde(rename = "marketBaseVault")]
    pub market_base_vault: String,
    #[serde(rename = "marketQuoteVault")]
    pub market_quote_vault: String,
    #[serde(rename = "marketBids")]
    pub market_bids: String,
    #[serde(rename = "marketAsks")]
    pub market_asks: String,
    #[serde(rename = "marketEventQueue")]
    pub market_event_queue: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct VaultInfo {
    pub A: String,
    pub B: String,
}

impl From<RaydiumPool> for PoolKeys {
    fn from(pool: RaydiumPool) -> Self {
        Self {
            id: Pubkey::from_str(&pool.id).unwrap(),
            base_mint: Pubkey::from_str(&pool.base_mint).unwrap(),
            quote_mint: Pubkey::from_str(&pool.quote_mint).unwrap(),
            base_vault: Pubkey::from_str(&pool.base_vault).unwrap(),
            quote_vault: Pubkey::from_str(&pool.quote_vault).unwrap(),
            open_orders: Pubkey::from_str(&pool.open_orders).unwrap(),
            target_orders: Pubkey::from_str(&pool.target_orders).unwrap(),
            market_id: Pubkey::from_str(&pool.market_id).unwrap(),
            market_base_vault: Pubkey::from_str(&pool.market_base_vault).unwrap(),
            market_quote_vault: Pubkey::from_str(&pool.market_quote_vault).unwrap(),
            market_authority: Pubkey::from_str(&pool.market_authority).unwrap(),
            bids: Pubkey::from_str(&pool.market_bids).unwrap(),
            asks: Pubkey::from_str(&pool.market_asks).unwrap(),
            event_queue: Pubkey::from_str(&pool.market_event_queue).unwrap(),
        }
    }
}

impl From<RaydiumPoolKeyInfo> for PoolKeys {
    fn from(pool: RaydiumPoolKeyInfo) -> Self {
        Self {
            id: Pubkey::from_str(&pool.id).unwrap(),
            base_mint: Pubkey::from_str(&pool.mint_a.address).unwrap(),
            quote_mint: Pubkey::from_str(&pool.mint_b.address).unwrap(),
            base_vault: Pubkey::from_str(&pool.vault.A).unwrap(),
            quote_vault: Pubkey::from_str(&pool.vault.B).unwrap(),
            open_orders: Pubkey::from_str(&pool.open_orders).unwrap(),
            target_orders: Pubkey::from_str(&pool.target_orders).unwrap(),
            market_id: Pubkey::from_str(&pool.market_id).unwrap(),
            market_base_vault: Pubkey::from_str(&pool.market_base_vault).unwrap(),
            market_quote_vault: Pubkey::from_str(&pool.market_quote_vault).unwrap(),
            market_authority: Pubkey::from_str(&pool.market_authority).unwrap(),
            bids: Pubkey::from_str(&pool.market_bids).unwrap(),
            asks: Pubkey::from_str(&pool.market_asks).unwrap(),
            event_queue: Pubkey::from_str(&pool.market_event_queue).unwrap(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C, align(8))]
#[derive(bytemuck::Pod, bytemuck::Zeroable)]
pub struct AmmV4 {
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
    pub base_vault_balance: u64,
    pub quote_vault_balance: u64,
    pub lp_mint_supply: u64,
}

impl AmmV4 {
    pub fn get_price(&self) -> f64 {
        let base_amount = self.base_vault_balance as f64 / (10f64.powi(self.base_decimals as i32));
        let quote_amount =
            self.quote_vault_balance as f64 / (10f64.powi(self.quote_decimals as i32));

        if base_amount > 0.0 {
            quote_amount / base_amount
        } else {
            0.0
        }
    }

    pub fn get_tvl(&self) -> f64 {
        let quote_amount =
            self.quote_vault_balance as f64 / (10f64.powi(self.quote_decimals as i32));
        quote_amount * 2.0 // Approximate TVL as twice the quote amount since it's balanced
    }

    pub fn get_liquidity(&self) -> f64 {
        let quote_amount =
            self.quote_vault_balance as f64 / (10f64.powi(self.quote_decimals as i32));
        quote_amount
    }
}
