use borsh::{BorshDeserialize, BorshSerialize};
use solana_sdk::pubkey::Pubkey;

#[derive(BorshDeserialize, BorshSerialize, Debug)]
pub struct LiquidityStateV4 {
    pub status: u64,
    pub nonce: u64,
    pub order_num: u64,
    pub depth: u64,
    pub coin_decimals: u64,
    pub pc_decimals: u64,
    pub state: u64,
    pub reset_flag: u64,
    pub min_size: u64,
    pub vol_max_cut_ratio: u64,
    pub amount_wave_ratio: u64,
    pub coin_lot_size: u64,
    pub pc_lot_size: u64,
    pub min_price_multiplier: u64,
    pub max_price_multiplier: u64,
    pub system_decimals_value: u64,
    pub min_separate_numerator: u64,
    pub min_separate_denominator: u64,
    pub trade_fee_numerator: u64,
    pub trade_fee_denominator: u64,
    pub pnl_numerator: u64,
    pub pnl_denominator: u64,
    pub swap_fee_numerator: u64,
    pub swap_fee_denominator: u64,
    pub need_take_pnl_coin: u64,
    pub need_take_pnl_pc: u64,
    pub total_pnl_pc: u64,
    pub total_pnl_coin: u64,
    pub pool_open_time: u64,
    pub punish_pc_amount: u64,
    pub punish_coin_amount: u64,
    pub orderbook_to_init_time: u64,
    pub swap_coin_in_amount: u128,
    pub swap_pc_out_amount: u128,
    pub swap_coin2_pc_fee: u64,
    pub swap_pc_in_amount: u128,
    pub swap_coin_out_amount: u128,
    pub swap_pc2_coin_fee: u64,
    pub pool_coin_token_account: Pubkey,
    pub pool_pc_token_account: Pubkey,
    pub coin_mint_address: Pubkey,
    pub pc_mint_address: Pubkey,
    pub lp_mint_address: Pubkey,
    pub amm_open_orders: Pubkey,
    pub serum_market: Pubkey,
    pub serum_program_id: Pubkey,
    pub amm_target_orders: Pubkey,
    pub pool_withdraw_queue: Pubkey,
    pub pool_temp_lp_token_account: Pubkey,
    pub amm_owner: Pubkey,
    pub pnl_owner: Pubkey,
}

#[derive(Debug)]
pub struct AccountFlags {
    pub initialized: bool,
    pub market: bool,
    pub open_orders: bool,
    pub request_queue: bool,
    pub event_queue: bool,
    pub bids: bool,
    pub asks: bool,
}

#[derive(BorshDeserialize, BorshSerialize, Debug)]
pub struct MarketStateV3 {
    pub account_flags: u64,
    pub own_address: Pubkey,
    pub vault_signer_nonce: u64,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub base_vault: Pubkey,
    pub base_deposits_total: u64,
    pub base_fees_accrued: u64,
    pub quote_vault: Pubkey,
    pub quote_deposits_total: u64,
    pub quote_fees_accrued: u64,
    pub quote_dust_threshold: u64,
    pub request_queue: Pubkey,
    pub event_queue: Pubkey,
    pub bids: Pubkey,
    pub asks: Pubkey,
    pub base_lot_size: u64,
    pub quote_lot_size: u64,
    pub fee_rate_bps: u64,
    pub referrer_rebate_accrued: u64,
}

#[derive(BorshDeserialize, BorshSerialize, Debug)]
pub struct OpenOrders {
    pub account_flags: u64,
    pub market: Pubkey,
    pub owner: Pubkey,
    pub base_token_free: u64,
    pub base_token_total: u64,
    pub quote_token_free: u64,
    pub quote_token_total: u64,
    pub free_slot_bits: [u8; 16],
    pub is_bid_bits: [u8; 16],
    pub orders: [[u8; 16]; 128],
    pub client_ids: [u64; 128],
    pub referrer_rebate_accrued: u64,
}

impl AccountFlags {
    pub fn from_u64(value: u64) -> Self {
        Self {
            initialized: (value & (1 << 0)) != 0,
            market: (value & (1 << 1)) != 0,
            open_orders: (value & (1 << 2)) != 0,
            request_queue: (value & (1 << 3)) != 0,
            event_queue: (value & (1 << 4)) != 0,
            bids: (value & (1 << 5)) != 0,
            asks: (value & (1 << 6)) != 0,
        }
    }
}
