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

use crate::{
    data::{confirm_transaction, get_token_balance},
    models::{SellRequest, SellResponse},
};

use super::{
    constants::*,
    types::{PumpFunTokenContainer, TokenAccountOwnerContainer},
    utils::{derive_trading_accounts, ensure_token_account, get_bonding_curve_data},
};

pub async fn sell(
    rpc_client: &RpcClient,
    secret_keypair: &impl Signer,
    token_account_container: &TokenAccountOwnerContainer,
    pump_fun_token_container: &PumpFunTokenContainer,
    token_quantity: f64,
    slippage: f64,
) -> Result<String, AppError> {
    let user_address = secret_keypair.pubkey();

    println!(
        "Selling token with Pump.fun DEX with keypair: {}",
        secret_keypair.pubkey()
    );

    // Get token holdings
    let token_holdings = rpc_client
        .get_token_account_balance(&token_account_container.token_account_address.unwrap())?;

    println!("Token account balance retrieved: {}", token_holdings.amount);

    let token_balance = token_holdings.amount.parse::<u64>().unwrap();
    let token_decimals = token_holdings.decimals;

    println!("Token balance: {} (smallest unit)", token_balance);
    println!("Token decimals: {}", token_decimals);

    // Get bonding curve data from chain
    let bonding_curve_data =
        get_bonding_curve_data(rpc_client, &pump_fun_token_container.mint_address).await?;

    println!(
        "Token Reserves for {}",
        token_account_container.mint_address
    );
    println!(
        "Virtual token reserves: {}",
        bonding_curve_data.virtual_token_reserves
    );
    println!(
        "Virtual sol reserves: {}",
        bonding_curve_data.virtual_sol_reserves
    );

    // Calculate sell amounts
    let (token_amount, expected_sol_output) =
        bonding_curve_data.calculate_sell_amount(token_quantity, token_decimals);
    let min_sol_output = (expected_sol_output as f64 * (1.0 - slippage)) as u64;

    println!(
        "Expected SOL output: {} SOL",
        expected_sol_output as f64 / LAMPORTS_PER_SOL as f64
    );
    println!(
        "Minimum SOL output with slippage: {} SOL",
        min_sol_output as f64 / LAMPORTS_PER_SOL as f64
    );

    let (instruction, compute_budget_instructions) = build_sell_instructions(
        user_address,
        pump_fun_token_container,
        token_account_container,
        token_amount,
        min_sol_output,
    )?;

    let signature = send_sell_transaction(
        rpc_client,
        secret_keypair,
        &instruction,
        &compute_budget_instructions,
        user_address,
    )
    .await?;

    match confirm_transaction(rpc_client, &signature, 20, 3).await {
        Ok(true) => {
            println!("Transaction confirmed successfully!");
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

fn build_sell_instructions(
    user_address: Pubkey,
    pump_fun_token_container: &PumpFunTokenContainer,
    token_account_container: &TokenAccountOwnerContainer,
    token_amount: u64,
    min_sol_output: u64,
) -> Result<(Instruction, Vec<Instruction>), AppError> {
    let mut data = Vec::with_capacity(24);
    data.extend_from_slice(&SELL_DISCRIMINATOR);
    data.extend_from_slice(&token_amount.to_le_bytes());
    data.extend_from_slice(&min_sol_output.to_le_bytes());

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
        AccountMeta::new_readonly(ASSOCIATED_TOKEN_PROGRAM_ID, false),
        AccountMeta::new_readonly(TOKEN_KEG_PROGRAM_ID, false),
        AccountMeta::new_readonly(EVENT_AUTHORITY, false),
        AccountMeta::new_readonly(PUMP_FUN_PROGRAM_ID, false),
    ];

    let instruction = Instruction::new_with_bytes(PUMP_FUN_PROGRAM_ID, &data, accounts);
    let compute_budget_instructions = vec![
        ComputeBudgetInstruction::set_compute_unit_price(UNIT_PRICE),
        ComputeBudgetInstruction::set_compute_unit_limit(UNIT_BUDGET),
    ];

    Ok((instruction, compute_budget_instructions))
}

async fn send_sell_transaction(
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

pub async fn process_sell_request(
    rpc_client: &RpcClient,
    server_keypair: &Keypair,
    request: SellRequest,
) -> Result<SellResponse, AppError> {
    println!("Processing sell request: {:?}", request);

    let token_address = Pubkey::from_str(&request.token_address)
        .map_err(|e| AppError::BadRequest(format!("Invalid token address: {}", e)))?;

    // Create containers
    let pump_fun_token_container = PumpFunTokenContainer {
        mint_address: token_address,
        pump_fun_coin_data: None, // No longer need API data
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

    // Verify balance
    let token_balance = get_token_balance(rpc_client, &token_account).await?;
    if token_balance < request.token_quantity {
        return Err(AppError::BadRequest(
            "Insufficient token balance".to_string(),
        ));
    }

    // Get bonding curve data for price calculation
    let bonding_curve_data = get_bonding_curve_data(rpc_client, &token_address).await?;
    let (_, expected_sol_output) = bonding_curve_data.calculate_sell_amount(
        request.token_quantity,
        9, // Most Solana tokens use 9 decimals
    );

    let signature = sell(
        rpc_client,
        server_keypair,
        &token_account_container,
        &pump_fun_token_container,
        request.token_quantity,
        request.slippage_tolerance,
    )
    .await?;

    Ok(SellResponse {
        success: true,
        signature: signature.clone(),
        token_quantity: request.token_quantity,
        sol_received: expected_sol_output as f64 / LAMPORTS_PER_SOL as f64,
        solscan_tx_url: format!("https://solscan.io/tx/{}", signature),
        error: None,
    })
}
