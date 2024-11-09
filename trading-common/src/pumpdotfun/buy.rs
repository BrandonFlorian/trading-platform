use crate::error::AppError;
use solana_client::{rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    message::Message,
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    transaction::Transaction,
};
use solana_transaction_status::UiTransactionEncoding;
use std::str::FromStr;

use crate::models::{BuyRequest, BuyResponse};
use crate::utils::confirm_transaction;

use super::{
    constants::*,
    types::{
        BondingCurveData, PumpFunCalcResult, PumpFunTokenContainer, TokenAccountOwnerContainer,
    },
    utils::{derive_trading_accounts, ensure_token_account, get_bonding_curve_data},
};

pub async fn buy(
    rpc_client: &RpcClient,
    secret_keypair: &impl Signer,
    token_account_container: &TokenAccountOwnerContainer,
    pump_fun_token_container: &PumpFunTokenContainer,
    sol_quantity: f64,
    slippage: f64,
) -> Result<String, AppError> {
    let user_address = secret_keypair.pubkey();

    // Validate slippage
    if slippage >= 1.0 {
        return Err(AppError::BadRequest(
            "Slippage must be less than 100%".to_string(),
        ));
    }

    println!(
        "Initiator {} >> Buy: Token: {} Slippage %: {}",
        user_address,
        token_account_container.mint_address,
        slippage * 100.0
    );

    // Get bonding curve data directly from chain
    let bonding_curve_data =
        get_bonding_curve_data(rpc_client, &pump_fun_token_container.mint_address).await?;

    // Calculate amounts using bonding curve data
    let (token_out, sol_in_lamports) = bonding_curve_data.calculate_buy_amount(sol_quantity);
    let max_sol_cost = (sol_in_lamports as f64 * (1.0 + slippage)) as u64;

    // Calculate min/max outputs for logging
    let decimals = 9; // Most Solana tokens use 9 decimals
    let max_token_output = token_out as f64 / 10f64.powi(decimals);
    let min_token_output = max_token_output * (1.0 - slippage);

    println!(
        "Token Output >> Min: {:.8}, Max: {:.8}",
        min_token_output, max_token_output
    );

    println!(
        "Sol in (lamports): {}, Token out: {}, Max cost: {}",
        sol_in_lamports, token_out, max_sol_cost
    );

    // Build and send transaction
    let (instruction, compute_budget_instructions) = build_buy_instructions(
        user_address,
        pump_fun_token_container,
        token_account_container,
        token_out,
        max_sol_cost,
    )?;

    let signature = send_buy_transaction(
        rpc_client,
        secret_keypair,
        &instruction,
        &compute_budget_instructions,
        user_address,
    )
    .await?;

    println!("Transaction signature: {}", signature);

    // Confirm transaction with retries
    match confirm_transaction(rpc_client, &signature, 20, 3).await {
        Ok(true) => {
            println!("Buy transaction confirmed successfully!");
            Ok(signature.to_string())
        }
        Ok(false) => Err(AppError::ServerError(
            "Transaction failed during confirmation".to_string(),
        )),
        Err(e) => {
            println!("Error during confirmation: {:?}", e);
            Err(e)
        }
    }
}

fn build_buy_instructions(
    user_address: Pubkey,
    pump_fun_token_container: &PumpFunTokenContainer,
    token_account_container: &TokenAccountOwnerContainer,
    token_out: u64,
    max_sol_cost: u64,
) -> Result<(Instruction, Vec<Instruction>), AppError> {
    // Build instruction data
    let mut data = Vec::with_capacity(24);
    data.extend_from_slice(&BUY_DISCRIMINATOR);
    data.extend_from_slice(&token_out.to_le_bytes());
    data.extend_from_slice(&max_sol_cost.to_le_bytes());

    // Get trading accounts
    let (bonding_curve, associated_bonding_curve) =
        derive_trading_accounts(&pump_fun_token_container.mint_address)?;

    let accounts = vec![
        AccountMeta::new_readonly(GLOBAL, false),
        AccountMeta::new(FEE_RECIPIENT, false),
        AccountMeta::new_readonly(pump_fun_token_container.mint_address, false),
        AccountMeta::new(bonding_curve, false),
        AccountMeta::new(associated_bonding_curve, false),
        AccountMeta::new(
            token_account_container.token_account_address.unwrap(),
            false,
        ),
        AccountMeta::new(user_address, true),
        AccountMeta::new_readonly(SYSTEM_PROGRAM, false),
        AccountMeta::new_readonly(TOKEN_KEG_PROGRAM_ID, false),
        AccountMeta::new_readonly(solana_program::sysvar::rent::ID, false),
        AccountMeta::new_readonly(EVENT_AUTHORITY, false),
        AccountMeta::new_readonly(PUMP_FUN_PROGRAM_ID, false),
    ];

    // Create instructions
    let instruction = Instruction::new_with_bytes(PUMP_FUN_PROGRAM_ID, &data, accounts);
    let compute_budget_instructions = vec![
        ComputeBudgetInstruction::set_compute_unit_price(UNIT_PRICE),
        ComputeBudgetInstruction::set_compute_unit_limit(UNIT_BUDGET),
    ];

    Ok((instruction, compute_budget_instructions))
}

async fn send_buy_transaction(
    rpc_client: &RpcClient,
    secret_keypair: &impl Signer,
    instruction: &Instruction,
    compute_budget_instructions: &[Instruction],
    user_address: Pubkey,
) -> Result<solana_sdk::signature::Signature, AppError> {
    let recent_blockhash = rpc_client.get_latest_blockhash()?;

    let mut instructions = Vec::with_capacity(compute_budget_instructions.len() + 1);
    instructions.extend_from_slice(compute_budget_instructions);
    instructions.push(instruction.clone());

    let message =
        Message::new_with_blockhash(&instructions, Some(&user_address), &recent_blockhash);

    let transaction = Transaction::new(&[secret_keypair], message, recent_blockhash);

    const CONFIG: RpcSendTransactionConfig = RpcSendTransactionConfig {
        skip_preflight: false,
        preflight_commitment: Some(CommitmentConfig::confirmed().commitment),
        encoding: Some(UiTransactionEncoding::Base64),
        max_retries: Some(20),
        min_context_slot: None,
    };

    rpc_client
        .send_transaction_with_config(&transaction, CONFIG)
        .map_err(|e| AppError::RequestError(format!("Failed to send transaction: {}", e)))
}

pub async fn process_buy_request(
    rpc_client: &RpcClient,
    server_keypair: &Keypair,
    request: BuyRequest,
) -> Result<BuyResponse, AppError> {
    println!("Processing buy request");
    let token_address = Pubkey::from_str(&request.token_address)
        .map_err(|e| AppError::BadRequest(format!("Invalid token address: {}", e)))?;

    println!("Token address: {:?}", token_address);

    // Create containers
    let pump_fun_token_container = PumpFunTokenContainer {
        mint_address: token_address,
        pump_fun_coin_data: None, // We don't need the API data anymore
        program_account_info: None,
    };

    let token_account = ensure_token_account(
        rpc_client,
        server_keypair,
        &token_address,
        &server_keypair.pubkey(),
    )
    .await?;

    let token_account_container = TokenAccountOwnerContainer {
        owner_address: server_keypair.pubkey(),
        mint_address: token_address,
        token_account_address: Some(token_account),
    };

    // Execute buy
    let signature = buy(
        rpc_client,
        server_keypair,
        &token_account_container,
        &pump_fun_token_container,
        request.sol_quantity,
        request.slippage_tolerance,
    )
    .await?;

    // Get bonding curve data for calculations
    let bonding_curve_data = get_bonding_curve_data(rpc_client, &token_address).await?;

    let (token_out, _) = bonding_curve_data.calculate_buy_amount(request.sol_quantity);
    let adjusted_token_output = token_out as f64 / 1e9; // Convert to human readable

    Ok(BuyResponse {
        success: true,
        signature: signature.to_string(),
        solscan_tx_url: format!("https://solscan.io/tx/{}", signature),
        token_quantity: adjusted_token_output,
        sol_spent: request.sol_quantity,
        error: None,
    })
}
