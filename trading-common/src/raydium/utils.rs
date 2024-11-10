use anyhow::Result;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    message::Message,
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    transaction::Transaction,
};
use std::str::FromStr;

use crate::error::AppError;

use super::{
    types::{
        PoolKeys, RaydiumApiResponse, RaydiumPoolInfo, RaydiumPoolKeyInfo, RaydiumPoolKeyResponse,
    },
    COMPUTE_BUDGET_PRICE, COMPUTE_BUDGET_UNITS, OPEN_BOOK_PROGRAM, RAY_AUTHORITY_V4, RAY_V4,
    TOKEN_PROGRAM_ID, WSOL,
};

// Core functionality
pub async fn get_pool_info(token_mint: &str) -> Result<RaydiumPoolInfo, AppError> {
    let url = format!(
        "https://api-v3.raydium.io/pools/info/mint?\
         mint1={}&\
         poolType=standard&\
         poolSortField=liquidity&\
         sortType=desc&\
         pageSize=1&\
         page=1",
        token_mint
    );

    let mut response = surf::get(&url)
        .header("User-Agent", "Mozilla/5.0")
        .await
        .map_err(|e| AppError::RequestError(e.to_string()))?;

    let response_text = response
        .body_string()
        .await
        .map_err(|e| AppError::RequestError(e.to_string()))?;

    let api_response: RaydiumApiResponse = serde_json::from_str(&response_text).map_err(|e| {
        AppError::JsonParseError(format!(
            "Failed to parse pool info: {}. Response: {}",
            e, response_text
        ))
    })?;

    if !api_response.success || api_response.data.data.is_empty() {
        return Err(AppError::BadRequest(format!(
            "No pool found for token {}",
            token_mint
        )));
    }

    Ok(api_response.data.data[0].clone())
}

pub async fn get_pool_keys(pool_id: &str) -> Result<RaydiumPoolKeyInfo, AppError> {
    let url = format!("https://api-v3.raydium.io/pools/key/ids?ids={}", pool_id);

    let mut response = surf::get(&url)
        .header("User-Agent", "Mozilla/5.0")
        .await
        .map_err(|e| AppError::RequestError(e.to_string()))?;

    let response_text = response
        .body_string()
        .await
        .map_err(|e| AppError::RequestError(e.to_string()))?;

    let api_response: RaydiumPoolKeyResponse =
        serde_json::from_str(&response_text).map_err(|e| {
            AppError::JsonParseError(format!(
                "Failed to parse pool keys: {}. Response: {}",
                e, response_text
            ))
        })?;

    if !api_response.success || api_response.data.is_empty() {
        return Err(AppError::BadRequest(format!(
            "No pool keys found for pool {}",
            pool_id
        )));
    }

    // Add validation for required fields
    let pool_info = &api_response.data[0];

    // Validate all addresses can be parsed as Pubkeys
    let _ = Pubkey::from_str(&pool_info.id)?;
    let _ = Pubkey::from_str(&pool_info.mint_a.address)?;
    let _ = Pubkey::from_str(&pool_info.mint_b.address)?;
    let _ = Pubkey::from_str(&pool_info.market_id)?;
    let _ = Pubkey::from_str(&pool_info.market_authority)?;
    let _ = Pubkey::from_str(&pool_info.open_orders)?;
    let _ = Pubkey::from_str(&pool_info.target_orders)?;
    let _ = Pubkey::from_str(&pool_info.vault.A)?;
    let _ = Pubkey::from_str(&pool_info.vault.B)?;

    println!("Pool keys validated for pool {}", pool_id);
    println!("Market ID: {}", pool_info.market_id);
    println!("Market Authority: {}", pool_info.market_authority);
    println!("OpenOrders: {}", pool_info.open_orders);
    println!("Base Vault: {}", pool_info.vault.A);
    println!("Quote Vault: {}", pool_info.vault.B);

    Ok(api_response.data[0].clone())
}

pub fn create_swap_instruction(
    pool_keys: &PoolKeys,
    amount_in: u64,
    minimum_out: u64,
    token_account_in: Pubkey,
    token_account_out: Pubkey,
    owner: &Keypair,
) -> Result<Instruction, AppError> {
    let accounts = vec![
        // Token Program
        AccountMeta::new_readonly(Pubkey::from_str(TOKEN_PROGRAM_ID)?, false),
        // AMM accounts
        AccountMeta::new(pool_keys.id, false),
        AccountMeta::new_readonly(RAY_AUTHORITY_V4, false),
        AccountMeta::new(pool_keys.open_orders, false),
        AccountMeta::new(pool_keys.target_orders, false),
        AccountMeta::new(pool_keys.base_vault, false),
        AccountMeta::new(pool_keys.quote_vault, false),
        // Serum/OpenBook accounts
        AccountMeta::new_readonly(OPEN_BOOK_PROGRAM, false),
        AccountMeta::new(pool_keys.market_id, false),
        AccountMeta::new(pool_keys.bids, false),
        AccountMeta::new(pool_keys.asks, false),
        AccountMeta::new(pool_keys.event_queue, false),
        AccountMeta::new(pool_keys.market_base_vault, false),
        AccountMeta::new(pool_keys.market_quote_vault, false),
        AccountMeta::new_readonly(pool_keys.market_authority, false),
        // User accounts
        AccountMeta::new(token_account_in, false),
        AccountMeta::new(token_account_out, false),
        AccountMeta::new_readonly(owner.pubkey(), true),
    ];

    // Create instruction data
    let mut data = Vec::with_capacity(17);
    data.push(9u8); // Swap instruction discriminator
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&minimum_out.to_le_bytes());

    Ok(Instruction::new_with_bytes(RAY_V4, &data, accounts))
}

// Utility functions for WSOL handling
pub async fn create_wsol_account_instructions(
    rpc_client: &RpcClient,
    owner: &Keypair,
    amount: u64,
) -> Result<(Keypair, Vec<Instruction>)> {
    let wsol_account = Keypair::new();
    let rent = rpc_client.get_minimum_balance_for_rent_exemption(165)?;
    let total_amount = rent + amount;

    let create_account_ix = solana_sdk::system_instruction::create_account(
        &owner.pubkey(),
        &wsol_account.pubkey(),
        total_amount,
        165,
        &Pubkey::from_str(TOKEN_PROGRAM_ID)?,
    );

    let init_account_ix = spl_token::instruction::initialize_account(
        &Pubkey::from_str(TOKEN_PROGRAM_ID)?,
        &wsol_account.pubkey(),
        &Pubkey::from_str(WSOL)?,
        &owner.pubkey(),
    )?;

    Ok((wsol_account, vec![create_account_ix, init_account_ix]))
}

// Helper function for price calculation
pub async fn calculate_price_impact(
    rpc_client: &RpcClient,
    pool_keys: &PoolKeys,
    amount_in: u64,
    is_base_to_quote: bool,
) -> Result<f64> {
    let base_balance = rpc_client.get_token_account_balance(&pool_keys.base_vault)?;
    let quote_balance = rpc_client.get_token_account_balance(&pool_keys.quote_vault)?;

    let base_amount = base_balance.ui_amount.unwrap_or(0.0);
    let quote_amount = quote_balance.ui_amount.unwrap_or(0.0);

    let current_price = quote_amount / base_amount;
    let k = base_amount * quote_amount;

    let new_base_amount = if is_base_to_quote {
        base_amount + (amount_in as f64 / 10f64.powi(base_balance.decimals as i32))
    } else {
        base_amount
    };

    let new_quote_amount = k / new_base_amount;
    let new_price = new_quote_amount / new_base_amount;

    Ok(((new_price - current_price) / current_price).abs())
}

// Main swap function that builds and sends the transaction
pub async fn execute_swap(
    rpc_client: &RpcClient,
    owner: &Keypair,
    pool_keys: &PoolKeys,
    amount_in: u64,
    minimum_out: u64,
    source_token: Pubkey,
    destination_token: Pubkey,
) -> Result<String> {
    // Set compute budget
    let compute_budget_ix =
        solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(
            COMPUTE_BUDGET_UNITS,
        );
    let priority_fee_ix =
        solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_price(
            COMPUTE_BUDGET_PRICE,
        );

    // Create swap instruction
    let swap_ix = create_swap_instruction(
        pool_keys,
        amount_in,
        minimum_out,
        source_token,
        destination_token,
        owner,
    )?;

    // Build transaction
    let recent_blockhash = rpc_client.get_latest_blockhash()?;
    let message = Message::new(
        &[compute_budget_ix, priority_fee_ix, swap_ix],
        Some(&owner.pubkey()),
    );
    let mut transaction = Transaction::new_unsigned(message);
    transaction.sign(&[owner], recent_blockhash);

    // Send and confirm transaction
    let signature = rpc_client.send_and_confirm_transaction_with_spinner(&transaction)?;

    Ok(signature.to_string())
}

pub async fn get_token_balance(
    rpc_client: &RpcClient,
    token_account: Pubkey,
) -> Result<f64, AppError> {
    let account_balance = rpc_client
        .get_token_account_balance(&token_account)
        .map_err(|_| AppError::BadRequest("Failed to get token balance".to_string()))?;

    Ok(account_balance.ui_amount.unwrap_or(0.0))
}
