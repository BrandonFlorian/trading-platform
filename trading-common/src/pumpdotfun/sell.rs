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
    models::{SellRequest, SellResponse},
    utils::{confirm_transaction, get_token_balance},
};

use super::{
    constants::*,
    types::{PumpFunTokenContainer, TokenAccountOwnerContainer},
    utils::{ensure_token_account, get_bonding_curve_info, get_coin_data},
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

    let token_holdings = rpc_client
        .get_token_account_balance(&token_account_container.token_account_address.unwrap())?;

    println!("Token account balance retrieved: {}", token_holdings.amount);

    let token_balance = token_holdings.amount.parse::<u64>().unwrap();
    let token_decimals = token_holdings.decimals;

    println!("Token balance: {} (smallest unit)", token_balance);
    println!("Token decimals: {}", token_decimals);

    let (virtual_token_reserves, virtual_sol_reserves) =
        get_bonding_curve_info(rpc_client, pump_fun_token_container).await?;

    println!(
        "Token Reserves for {}",
        token_account_container.mint_address
    );
    println!("Virtual token reserves: {}", virtual_token_reserves);
    println!("Virtual sol reserves: {}", virtual_sol_reserves);

    let token_amount = token_quantity * 10f64.powi(token_decimals as i32);
    let price_per_token = virtual_sol_reserves as f64 / virtual_token_reserves as f64;
    let expected_sol_output = token_amount * price_per_token;
    let min_sol_output = (expected_sol_output * (1.0 - slippage)) as u64;

    println!(
        "Expected SOL output: {} SOL",
        expected_sol_output / LAMPORTS_PER_SOL as f64
    );
    println!(
        "Minimum SOL output with slippage: {} SOL",
        min_sol_output as f64 / LAMPORTS_PER_SOL as f64
    );

    let (instruction, compute_budget_instructions) = build_sell_instructions(
        user_address,
        pump_fun_token_container,
        token_account_container,
        token_amount as u64,
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

    let accounts = vec![
        AccountMeta::new_readonly(GLOBAL, false),
        AccountMeta::new(FEE_RECIPIENT, false),
        AccountMeta::new_readonly(pump_fun_token_container.mint_address, false),
        AccountMeta::new(
            Pubkey::from_str(
                &pump_fun_token_container
                    .pump_fun_coin_data
                    .as_ref()
                    .unwrap()
                    .bonding_curve,
            )?,
            false,
        ),
        AccountMeta::new(
            Pubkey::from_str(
                &pump_fun_token_container
                    .pump_fun_coin_data
                    .as_ref()
                    .unwrap()
                    .associated_bonding_curve,
            )?,
            false,
        ),
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

    let coin_data = get_coin_data(&token_address).await?;
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

    let token_balance = get_token_balance(rpc_client, &token_account).await?;
    if token_balance < request.token_quantity {
        return Err(AppError::BadRequest(
            "Insufficient token balance".to_string(),
        ));
    }

    let signature = sell(
        rpc_client,
        server_keypair,
        &token_account_container,
        &pump_fun_token_container,
        request.token_quantity,
        request.slippage_tolerance,
    )
    .await?;

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
    let price_per_token = virtual_sol_reserves / (virtual_token_reserves * LAMPORTS_PER_SOL as f64);
    let sol_received = request.token_quantity * price_per_token;

    Ok(SellResponse {
        success: true,
        signature: signature.clone(),
        token_quantity: request.token_quantity,
        sol_received,
        solscan_tx_url: format!("https://solscan.io/tx/{}", signature),
        error: None,
    })
}
