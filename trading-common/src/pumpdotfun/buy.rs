use crate::error::AppError;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use solana_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::Keypair;
use solana_sdk::{
    instruction::AccountMeta, instruction::Instruction, message::Message, pubkey::Pubkey,
    signer::Signer, transaction::Transaction,
};
use solana_transaction_status::UiTransactionEncoding;
use std::str::FromStr;
use thiserror::Error;

use crate::models::{BuyRequest, BuyResponse, SellRequest, SellResponse};
use crate::utils::{confirm_transaction, get_token_balance};
use solana_client::rpc_config::RpcSendTransactionConfig;

const UNIT_PRICE: u64 = 1_000;
const UNIT_BUDGET: u32 = 200_000;
const LAMPORTS_PER_SOL: u64 = 1_000_000_000;

pub async fn process_buy_request(
    rpc_client: &RpcClient,
    server_keypair: &Keypair,
    request: BuyRequest,
) -> Result<BuyResponse, AppError> {
    println!("Processing buy request");
    // Convert request into needed container formats
    let token_address = Pubkey::from_str(&request.token_address)
        .map_err(|e| AppError::BadRequest(format!("Invalid token address: {}", e)))?;
    println!("Token address: {:?}", token_address);
    // Get coin data and create container
    let coin_data = get_coin_data(&token_address).await?;
    println!("Coin data: {:?}", coin_data);
    let pump_fun_token_container = PumpFunTokenContainer {
        mint_address: token_address,
        pump_fun_coin_data: Some(coin_data),
        program_account_info: None,
    };
    println!("Pump fun token container: {:?}", pump_fun_token_container);
    // Ensure token account exists
    let token_account = ensure_token_account(
        rpc_client,
        server_keypair,
        &token_address,
        &server_keypair.pubkey(),
    )
    .await?;
    println!("Token account: {:?}", token_account);
    let token_account_container = TokenAccountOwnerContainer {
        owner_address: server_keypair.pubkey(),
        mint_address: token_address,
        token_account_address: Some(token_account),
    };
    println!("Token account container: {:?}", token_account_container);
    // Execute the buy
    let signature = buy(
        rpc_client,
        server_keypair,
        &token_account_container,
        &pump_fun_token_container,
        request.sol_quantity,
        request.slippage_tolerance,
    )
    .await?;
    println!("Signature: {:?}", signature);
    // Calculate final amounts for response
    let calcs = PumpFunCalcResult::new(
        pump_fun_token_container
            .pump_fun_coin_data
            .as_ref()
            .unwrap()
            .virtual_token_reserves,
        pump_fun_token_container
            .pump_fun_coin_data
            .as_ref()
            .unwrap()
            .virtual_sol_reserves,
        request.sol_quantity,
        request.slippage_tolerance,
        9, // decimals
    );
    println!("Calcs: {:?}", calcs);
    Ok(BuyResponse {
        success: true,
        signature: signature.to_string(),
        solscan_tx_url: format!("https://solscan.io/tx/{}", signature),
        token_quantity: calcs.adjusted_max_token_output,
        sol_spent: request.sol_quantity,
        error: None,
    })
}

pub async fn buy(
    rpc_client: &RpcClient,
    secret_keypair: &impl Signer,
    token_account_container: &TokenAccountOwnerContainer,
    pump_fun_token_container: &PumpFunTokenContainer,
    sol_quantity: f64,
    slippage: f64,
) -> Result<String, AppError> {
    let user_address = secret_keypair.pubkey();

    // Calculate amounts (matching Python implementation)
    let sol_in_lamports = (sol_quantity * LAMPORTS_PER_SOL as f64) as u64;

    let virtual_token_reserves = pump_fun_token_container
        .pump_fun_coin_data
        .as_ref()
        .unwrap()
        .virtual_token_reserves as f64;
    let virtual_sol_reserves = pump_fun_token_container
        .pump_fun_coin_data
        .as_ref()
        .unwrap()
        .virtual_sol_reserves as f64;

    let amount = ((sol_in_lamports as f64 * virtual_token_reserves) / virtual_sol_reserves) as u64;
    let max_sol_cost = (sol_in_lamports as f64 * (1.0 + slippage)) as u64;

    println!(
        "Sol in (lamports): {}, Amount out: {}, Max cost: {}",
        sol_in_lamports, amount, max_sol_cost
    );

    let mut data = Vec::with_capacity(24);
    data.extend_from_slice(&[102, 6, 61, 18, 1, 218, 235, 234]); // "66063d1201daebea"
    data.extend_from_slice(&amount.to_le_bytes());
    data.extend_from_slice(&max_sol_cost.to_le_bytes());

    let accounts = build_pump_fun_accounts(
        &user_address,
        pump_fun_token_container,
        &token_account_container.token_account_address.unwrap(),
    );

    let instruction = Instruction::new_with_bytes(PUMP_FUN_PROGRAM_ID, &data, accounts);

    // Add compute budget instructions
    let compute_unit_price_ix = ComputeBudgetInstruction::set_compute_unit_price(1_000);
    let compute_unit_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(100_000);

    let recent_blockhash = rpc_client.get_latest_blockhash()?;

    let message = Message::new_with_blockhash(
        &[compute_unit_price_ix, compute_unit_limit_ix, instruction],
        Some(&user_address),
        &recent_blockhash,
    );

    let transaction = Transaction::new(&[secret_keypair], message, recent_blockhash);

    let signature = rpc_client.send_and_confirm_transaction(&transaction)?;
    println!("Signature: {}", signature);

    Ok(signature.to_string())
}
