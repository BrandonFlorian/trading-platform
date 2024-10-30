use std::str::FromStr;

use borsh::{BorshDeserialize, BorshSerialize};
use solana_client::rpc_client::RpcClient;
use solana_sdk::program_pack::Pack;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::Signer;
use solana_sdk::{account::Account, instruction::Instruction};
use spl_associated_token_account::{
    get_associated_token_address, instruction::create_associated_token_account,
};

use crate::{error::AppError, raydium::RAYDIUM_V4_PROGRAM_ID};

use super::{layouts::*, RaydiumPoolKeys, OPENBOOK_PROGRAM_ID, RAYDIUM_V4_AUTHORITY, WSOL};

pub async fn get_pool_reserves(
    rpc_client: &RpcClient,
    pool_keys: &RaydiumPoolKeys,
) -> Result<(i64, i64), AppError> {
    // Get AMM data and parse
    let amm_data = rpc_client.get_account_data(&pool_keys.amm_id)?;
    let liquidity_state = LiquidityStateV4::try_from_slice(&amm_data)
        .map_err(|e| AppError::ServerError(format!("Failed to parse liquidity state: {}", e)))?;

    // Get open orders data
    let open_orders_data = rpc_client.get_account_data(&pool_keys.open_orders)?;
    let open_orders = OpenOrders::try_from_slice(&open_orders_data)
        .map_err(|e| AppError::ServerError(format!("Failed to parse open orders: {}", e)))?;

    // Get base token balance
    let base_balance = rpc_client
        .get_token_account_balance(&pool_keys.base_vault)?
        .ui_amount
        .unwrap_or(0.0);

    // Get quote token balance
    let quote_balance = rpc_client
        .get_token_account_balance(&pool_keys.quote_vault)?
        .ui_amount
        .unwrap_or(0.0);

    // Calculate base decimal factor
    let base_decimal_factor = 10_u64.pow(pool_keys.base_decimals as u32);
    let quote_decimal_factor = 10_u64.pow(pool_keys.quote_decimals as u32);

    // Calculate PnL adjustments
    let base_pnl = liquidity_state.need_take_pnl_coin as f64 / base_decimal_factor as f64;
    let quote_pnl = liquidity_state.need_take_pnl_pc as f64 / quote_decimal_factor as f64;

    // Calculate open orders adjustments
    let open_orders_base = open_orders.base_token_total as f64 / base_decimal_factor as f64;
    let open_orders_quote = open_orders.quote_token_total as f64 / quote_decimal_factor as f64;

    // Calculate total token amounts
    let base_total = (base_balance + open_orders_base - base_pnl) * base_decimal_factor as f64;
    let quote_total = (quote_balance + open_orders_quote - quote_pnl) * quote_decimal_factor as f64;

    Ok((base_total as i64, quote_total as i64))
}

pub async fn get_token_price(rpc_client: &RpcClient, pair_address: &str) -> Result<f64, AppError> {
    let pool_keys = fetch_pool_keys(rpc_client, pair_address).await?;
    let (base_reserves, quote_reserves) = get_pool_reserves(rpc_client, &pool_keys).await?;

    // Price depends on whether base token is WSOL or not
    let price = if pool_keys.base_mint == WSOL {
        base_reserves as f64 / quote_reserves as f64
    } else {
        quote_reserves as f64 / base_reserves as f64
    };

    Ok(price)
}

pub async fn get_pair_address_from_rpc(
    rpc_client: &RpcClient,
    token_address: &str,
) -> Result<String, AppError> {
    const BASE_OFFSET: usize = 400;
    const QUOTE_OFFSET: usize = 432;
    const DATA_LENGTH: u64 = 752;
    const WSOL_ADDRESS: &str = "So11111111111111111111111111111111111111112";

    // Helper function to fetch AMM ID
    async fn fetch_amm_id(
        rpc_client: &RpcClient,
        base_mint: &str,
        quote_mint: &str,
    ) -> Result<Option<String>, AppError> {
        let config = solana_client::rpc_config::RpcProgramAccountsConfig {
            filters: Some(vec![
                solana_client::rpc_filter::RpcFilterType::DataSize(DATA_LENGTH),
                solana_client::rpc_filter::RpcFilterType::Memcmp(
                    solana_client::rpc_filter::Memcmp::new_raw_bytes(
                        BASE_OFFSET,
                        bs58::decode(base_mint).into_vec().unwrap(),
                    ),
                ),
                solana_client::rpc_filter::RpcFilterType::Memcmp(
                    solana_client::rpc_filter::Memcmp::new_raw_bytes(
                        QUOTE_OFFSET,
                        bs58::decode(quote_mint).into_vec().unwrap(),
                    ),
                ),
            ]),
            with_context: Some(true),
            account_config: solana_client::rpc_config::RpcAccountInfoConfig {
                encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
                data_slice: None,
                commitment: Some(solana_sdk::commitment_config::CommitmentConfig::confirmed()),
                min_context_slot: None,
            },
            sort_results: Some(true),
        };

        let accounts =
            rpc_client.get_program_accounts_with_config(&RAYDIUM_V4_PROGRAM_ID, config)?;

        Ok(accounts.first().map(|(pubkey, _)| pubkey.to_string()))
    }

    // First attempt: token as base, WSOL as quote
    if let Some(pair_address) = fetch_amm_id(rpc_client, token_address, WSOL_ADDRESS).await? {
        return Ok(pair_address);
    }

    // Second attempt: WSOL as base, token as quote
    if let Some(pair_address) = fetch_amm_id(rpc_client, WSOL_ADDRESS, token_address).await? {
        return Ok(pair_address);
    }

    Err(AppError::BadRequest(
        "No Raydium pool found for token".to_string(),
    ))
}

// Add helper function to deserialize account data
pub fn decode_pool_account_data(
    rpc_client: &RpcClient,
    data: &[u8],
) -> Result<(LiquidityStateV4, MarketStateV3, OpenOrders), AppError> {
    let liquidity_state = LiquidityStateV4::try_from_slice(data)
        .map_err(|e| AppError::ServerError(format!("Failed to decode liquidity state: {}", e)))?;

    let market_data = rpc_client.get_account_data(&liquidity_state.serum_market)?;
    let market_state = MarketStateV3::try_from_slice(&market_data)
        .map_err(|e| AppError::ServerError(format!("Failed to decode market state: {}", e)))?;

    let open_orders_data = rpc_client.get_account_data(&liquidity_state.amm_open_orders)?;
    let open_orders = OpenOrders::try_from_slice(&open_orders_data)
        .map_err(|e| AppError::ServerError(format!("Failed to decode open orders: {}", e)))?;

    Ok((liquidity_state, market_state, open_orders))
}

// Update fetch_pool_keys to use the new decoding functions
pub async fn fetch_pool_keys(
    rpc_client: &RpcClient,
    pair_address: &str,
) -> Result<RaydiumPoolKeys, AppError> {
    let amm_id = Pubkey::from_str(pair_address)
        .map_err(|_| AppError::BadRequest("Invalid pair address".to_string()))?;

    let amm_data = rpc_client.get_account_data(&amm_id)?;
    let (liquidity_state, market_state, _) = decode_pool_account_data(&rpc_client, &amm_data)?;

    // Create program address for market authority
    let seeds = &[
        market_state.own_address.as_ref(),
        &[market_state.vault_signer_nonce.try_into().unwrap()],
    ];
    let (market_authority, _) = Pubkey::find_program_address(seeds, &OPENBOOK_PROGRAM_ID);

    Ok(RaydiumPoolKeys {
        amm_id,
        base_mint: market_state.base_mint,
        quote_mint: market_state.quote_mint,
        lp_mint: liquidity_state.lp_mint_address,
        base_decimals: liquidity_state.coin_decimals,
        quote_decimals: liquidity_state.pc_decimals,
        lp_decimals: liquidity_state.coin_decimals,
        authority: RAYDIUM_V4_AUTHORITY,
        open_orders: liquidity_state.amm_open_orders,
        target_orders: liquidity_state.amm_target_orders,
        base_vault: liquidity_state.pool_coin_token_account,
        quote_vault: liquidity_state.pool_pc_token_account,
        market_id: liquidity_state.serum_market,
        market_base_vault: market_state.base_vault,
        market_quote_vault: market_state.quote_vault,
        market_authority,
        bids: market_state.bids,
        asks: market_state.asks,
        event_queue: market_state.event_queue,
    })
}

pub async fn get_token_account(
    rpc_client: &RpcClient,
    owner: &Pubkey,
    mint: &Pubkey,
) -> Result<(Pubkey, Option<Instruction>), AppError> {
    // First try to get existing ATA
    let ata = get_associated_token_address(owner, mint);

    match rpc_client.get_account(&ata) {
        Ok(_) => Ok((ata, None)),
        Err(_) => {
            // Account doesn't exist, create instruction to make it
            let create_ata_ix = create_associated_token_account(
                owner, // Payer
                owner, // Owner
                mint,
                &spl_token::id(),
            );
            Ok((ata, Some(create_ata_ix)))
        }
    }
}

pub async fn get_token_balance(
    rpc_client: &RpcClient,
    token_account: &Pubkey,
) -> Result<f64, AppError> {
    let account = rpc_client.get_account(token_account)?;
    let token_account = spl_token::state::Account::unpack(&account.data)?;
    Ok(token_account.amount as f64)
}

pub fn make_swap_instruction(
    amount_in: u64,
    token_account_in: Pubkey,
    token_account_out: Pubkey,
    pool_keys: &RaydiumPoolKeys,
    owner: &impl Signer,
) -> Result<Instruction, AppError> {
    use solana_sdk::instruction::AccountMeta;

    let accounts = vec![
        AccountMeta::new_readonly(spl_token::id(), false),
        AccountMeta::new(pool_keys.amm_id, false),
        AccountMeta::new_readonly(pool_keys.authority, false),
        AccountMeta::new(pool_keys.open_orders, false),
        AccountMeta::new(pool_keys.target_orders, false),
        AccountMeta::new(pool_keys.base_vault, false),
        AccountMeta::new(pool_keys.quote_vault, false),
        AccountMeta::new_readonly(OPENBOOK_PROGRAM_ID, false),
        AccountMeta::new(pool_keys.market_id, false),
        AccountMeta::new(pool_keys.bids, false),
        AccountMeta::new(pool_keys.asks, false),
        AccountMeta::new(pool_keys.event_queue, false),
        AccountMeta::new(pool_keys.market_base_vault, false),
        AccountMeta::new(pool_keys.market_quote_vault, false),
        AccountMeta::new_readonly(pool_keys.market_authority, false),
        AccountMeta::new(token_account_in, false),
        AccountMeta::new(token_account_out, false),
        AccountMeta::new_readonly(owner.pubkey(), true),
    ];

    let data = SwapInstruction {
        instruction: 9, // Swap instruction discriminator
        amount_in,
        min_amount_out: 0, // No slippage protection for now
    }
    .try_to_vec()?;

    Ok(Instruction::new_with_bytes(
        RAYDIUM_V4_PROGRAM_ID,
        &data,
        accounts,
    ))
}

// Add new instruction data structure
#[derive(BorshSerialize, BorshDeserialize)]
pub struct SwapInstruction {
    pub instruction: u8,
    pub amount_in: u64,
    pub min_amount_out: u64,
}

impl SwapInstruction {
    pub fn try_to_vec(&self) -> Result<Vec<u8>, AppError> {
        borsh::to_vec(self).map_err(|e| AppError::ServerError(e.to_string()))
    }
}
