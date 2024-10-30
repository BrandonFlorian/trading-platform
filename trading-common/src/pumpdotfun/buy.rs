use std::str::FromStr;

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

use crate::models::{BuyRequest, BuyResponse};
use crate::utils::confirm_transaction;

use super::{
    constants::*,
    types::{PumpFunCalcResult, PumpFunTokenContainer, TokenAccountOwnerContainer},
    utils::{ensure_token_account, get_bonding_curve_info, get_coin_data},
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

    // Get virtual reserves from bonding curve
    let (virtual_token_reserves, virtual_sol_reserves) =
        get_bonding_curve_info(rpc_client, pump_fun_token_container).await?;

    let virtual_token_reserves = virtual_token_reserves as f64;
    let virtual_sol_reserves = virtual_sol_reserves as f64;

    // Calculate expected token output
    let sol_in_lamports = sol_quantity * LAMPORTS_PER_SOL as f64;
    let token_out = ((sol_in_lamports * virtual_token_reserves) / virtual_sol_reserves) as u64;

    if token_out == 0 {
        return Err(AppError::BadRequest(
            "Calculated token output is zero".to_string(),
        ));
    }

    // Calculate min/max outputs
    let decimals = 9; // Default decimals for now

    let max_token_output = (token_out as f64) / 10f64.powi(decimals);
    let min_token_output = max_token_output * (1.0 - slippage);

    println!(
        "Token Output >> Min: {:.8}, Max: {:.8}",
        min_token_output, max_token_output
    );

    // Calculate max SOL cost with slippage
    let max_sol_cost = (sol_in_lamports * (1.0 + slippage)) as u64;

    println!(
        "Sol in (lamports): {}, Token out: {}, Max cost: {}",
        sol_in_lamports, token_out, max_sol_cost
    );

    // Build instruction
    let (instruction, compute_budget_instructions) = build_buy_instructions(
        user_address,
        pump_fun_token_container,
        token_account_container,
        token_out,
        max_sol_cost,
    )?;

    // Send transaction
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
    let mut data = Vec::with_capacity(24);
    data.extend_from_slice(&BUY_DISCRIMINATOR);
    data.extend_from_slice(&token_out.to_le_bytes());
    data.extend_from_slice(&max_sol_cost.to_le_bytes());

    let bonding_curve_pubkey = Pubkey::from_str(
        &pump_fun_token_container
            .pump_fun_coin_data
            .as_ref()
            .unwrap()
            .bonding_curve,
    )?;
    let associated_bonding_curve_pubkey = Pubkey::from_str(
        &pump_fun_token_container
            .pump_fun_coin_data
            .as_ref()
            .unwrap()
            .associated_bonding_curve,
    )?;

    let accounts = vec![
        AccountMeta::new_readonly(GLOBAL, false),
        AccountMeta::new(FEE_RECIPIENT, false),
        AccountMeta::new_readonly(pump_fun_token_container.mint_address, false),
        AccountMeta::new(bonding_curve_pubkey, false),
        AccountMeta::new(associated_bonding_curve_pubkey, false),
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

    let coin_data = get_coin_data(&token_address).await?;
    println!("Coin data: {:?}", coin_data);

    let pump_fun_token_container = PumpFunTokenContainer {
        mint_address: token_address,
        pump_fun_coin_data: Some(coin_data),
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

    let signature = buy(
        rpc_client,
        server_keypair,
        &token_account_container,
        &pump_fun_token_container,
        request.sol_quantity,
        request.slippage_tolerance,
    )
    .await?;

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
        9,
    );

    Ok(BuyResponse {
        success: true,
        signature: signature.to_string(),
        solscan_tx_url: format!("https://solscan.io/tx/{}", signature),
        token_quantity: calcs.adjusted_max_token_output,
        sol_spent: request.sol_quantity,
        error: None,
    })
}
