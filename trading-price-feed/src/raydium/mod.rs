mod pool;
mod types;

pub use pool::*;
pub use types::*;

use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use trading_common::error::AppError;

pub async fn load_pool(
    rpc_client: &Arc<RpcClient>,
    address: &Pubkey,
) -> Result<RaydiumPool, AppError> {
    let account = rpc_client
        .get_account(address)
        .map_err(|e| AppError::SolanaRpcError { source: e })?;

    RaydiumPool::from_account_data(address, &account.data)
}

pub fn calculate_price(
    token_a_amount: u64,
    token_a_decimals: u8,
    token_b_amount: u64,
    token_b_decimals: u8,
) -> f64 {
    let token_a_float = token_a_amount as f64 / 10f64.powi(token_a_decimals as i32);
    let token_b_float = token_b_amount as f64 / 10f64.powi(token_b_decimals as i32);

    if token_a_float == 0.0 {
        0.0
    } else {
        token_b_float / token_a_float
    }
}

pub fn calculate_liquidity(
    token_a_amount: u64,
    token_a_decimals: u8,
    token_b_amount: u64,
    token_b_decimals: u8,
) -> f64 {
    let token_b_float = token_b_amount as f64 / 10f64.powi(token_b_decimals as i32);
    token_b_float * 2.0 // Approximate TVL as twice the quote amount since it's balanced
}
