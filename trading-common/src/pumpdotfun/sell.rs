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

pub struct SellResult {
    pub signature: String,
    pub token_amount: u64,
    pub expected_sol_output: u64,
    pub token_decimals: u8,
}

pub async fn sell(
    rpc_client: &RpcClient,
    secret_keypair: &impl Signer,
    token_account_container: &TokenAccountOwnerContainer,
    pump_fun_token_container: &PumpFunTokenContainer,
    token_quantity: f64,
    slippage: f64,
) -> Result<SellResult, AppError> {
    let user_address = secret_keypair.pubkey();


    // Get token holdings and decimals once
    let token_holdings = rpc_client
        .get_token_account_balance(&token_account_container.token_account_address.unwrap())?;

    let token_decimals = token_holdings.decimals;

    // Get bonding curve data and calculate amounts
    let bonding_curve_data =
        get_bonding_curve_data(rpc_client, &pump_fun_token_container.mint_address).await?;

    let (token_amount, expected_sol_output) =
        bonding_curve_data.calculate_sell_amount(token_quantity, token_decimals);
    let min_sol_output = (expected_sol_output as f64 * (1.0 - slippage)) as u64;


    let (instruction, compute_budget_instructions) = build_sell_instructions(
        rpc_client,
        user_address,
        pump_fun_token_container,
        token_account_container,
        token_amount,
        min_sol_output,
    ).await?;

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
            Ok(SellResult {
                signature: signature.to_string(),
                token_amount,
                expected_sol_output,
                token_decimals,
            })
        }
        Ok(false) => Err(AppError::ServerError(
            "Transaction failed during confirmation".to_string(),
        )),
        Err(e) => {
            Err(e)
        }
    }
}

async fn build_sell_instructions(
    rpc_client: &RpcClient,
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

    // Derive the creator vault address
    let creator_vault = super::utils::derive_creator_vault(rpc_client, &pump_fun_token_container.mint_address).await?;

    // Build accounts with creator vault at position 8 (where constraint expects it)
    let accounts = vec![
        AccountMeta::new_readonly(GLOBAL, false),          // 0: global
        AccountMeta::new(FEE_RECIPIENT, false),           // 1: feeRecipient
        AccountMeta::new_readonly(pump_fun_token_container.mint_address, false), // 2: mint
        AccountMeta::new(bonding_curve, false),           // 3: bonding_curve
        AccountMeta::new(associated_bonding_curve, false), // 4: associatedBondingCurve
        AccountMeta::new(                                 // 5: associatedUser
            token_account_container.token_account_address.unwrap(),
            false,
        ),
        AccountMeta::new(user_address, true),             // 6: user
        AccountMeta::new_readonly(SYSTEM_PROGRAM, false), // 7: system_program
        AccountMeta::new(creator_vault, false),           // 8: creator_vault (where constraint expects it)
        AccountMeta::new_readonly(TOKEN_KEG_PROGRAM_ID, false), // 9: token_program
        AccountMeta::new_readonly(EVENT_AUTHORITY, false), // 10: event_authority
        AccountMeta::new_readonly(PUMP_FUN_PROGRAM_ID, false), // 11: program
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

    let token_address = Pubkey::from_str(&request.token_address)
        .map_err(|e| AppError::BadRequest(format!("Invalid token address: {}", e)))?;

    // Create containers
    let pump_fun_token_container = PumpFunTokenContainer {
        mint_address: token_address,
        pump_fun_coin_data: None,
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

    // Execute sell and get results
    let sell_result = sell(
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
        signature: sell_result.signature.clone(),
        token_quantity: request.token_quantity,
        sol_received: sell_result.expected_sol_output as f64 / LAMPORTS_PER_SOL as f64,
        solscan_tx_url: format!("https://solscan.io/tx/{}", sell_result.signature),
        error: None,
    })
}
