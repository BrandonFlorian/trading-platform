use std::str::FromStr;

use crate::{
    create_wsol_account_instructions,
    error::AppError,
    models::{SellRequest, SellResponse},
    utils::confirm_transaction,
};

use super::constants::*;
use super::layouts::PoolKeys;
use super::utils::*;
use solana_client::{rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig};
use solana_sdk::{
    commitment_config::CommitmentConfig, compute_budget::ComputeBudgetInstruction,
    message::Message, pubkey::Pubkey, signature::Keypair, signer::Signer, transaction::Transaction,
};
use solana_transaction_status::UiTransactionEncoding;
use spl_token::instruction as token_instruction;

pub async fn process_sell_request(
    rpc_client: &RpcClient,
    server_keypair: &Keypair,
    request: &SellRequest,
) -> Result<SellResponse, AppError> {
    println!("Processing Raydium sell request: {:?}", request);

    // Get pool info and market data
    let pool_info = get_pool_info(&request.token_address).await?;
    let pool_keys = get_pool_keys(&pool_info.id).await?;
    let pool_keys = PoolKeys::from(pool_keys);

    // Get token decimals from the pool info
    let token_decimals = if pool_info.mint_a.address == request.token_address {
        pool_info.mint_a.decimals
    } else {
        pool_info.mint_b.decimals
    };
    println!("Token decimals: {}", token_decimals);

    // Calculate amounts using correct decimals
    let amount_in = (request.token_quantity * 10f64.powi(token_decimals)) as u64;
    let expected_sol_output = pool_info.price * request.token_quantity;
    let minimum_out = ((expected_sol_output * (1.0 - request.slippage_tolerance))
        * LAMPORTS_PER_SOL as f64) as u64;

    println!(
        "Sell calculation:\n\
         Amount in (raw): {}\n\
         Expected SOL out: {} SOL\n\
         Minimum SOL out: {} SOL",
        amount_in,
        expected_sol_output,
        minimum_out as f64 / LAMPORTS_PER_SOL as f64
    );

    // Create WSOL account
    let (wsol_keypair, wsol_instructions) = create_wsol_account_instructions(
        rpc_client,
        server_keypair,
        0, // No initial SOL for selling
    )
    .await?;

    // Build transaction
    let mut instructions = vec![
        ComputeBudgetInstruction::set_compute_unit_price(COMPUTE_BUDGET_PRICE),
        ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_BUDGET_UNITS),
    ];
    instructions.extend(wsol_instructions);

    // Get token account
    let token_mint = Pubkey::from_str(&request.token_address)?;
    let token_account = spl_associated_token_account::get_associated_token_address(
        &server_keypair.pubkey(),
        &token_mint,
    );

    // Check balance
    let balance = get_token_balance(rpc_client, token_account).await?;
    if balance < request.token_quantity {
        return Err(AppError::BadRequest(format!(
            "Insufficient token balance. Have {} but tried to sell {}",
            balance, request.token_quantity
        )));
    }

    // Create swap instruction
    let swap_ix = create_swap_instruction(
        &pool_keys,
        amount_in,
        minimum_out,
        token_account,
        wsol_keypair.pubkey(),
        server_keypair,
    )?;
    instructions.push(swap_ix);

    // Add close WSOL account instruction
    let close_wsol_ix = token_instruction::close_account(
        &Pubkey::from_str(TOKEN_PROGRAM_ID)?,
        &wsol_keypair.pubkey(),
        &server_keypair.pubkey(),
        &server_keypair.pubkey(),
        &[],
    )?;
    instructions.push(close_wsol_ix);

    // Execute transaction
    let recent_blockhash = rpc_client.get_latest_blockhash()?;
    let message = Message::new_with_blockhash(
        &instructions,
        Some(&server_keypair.pubkey()),
        &recent_blockhash,
    );
    let transaction = Transaction::new(&[server_keypair, &wsol_keypair], message, recent_blockhash);

    // Send and confirm
    let signature = rpc_client.send_transaction_with_config(
        &transaction,
        RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentConfig::confirmed().commitment),
            encoding: Some(UiTransactionEncoding::Base64),
            max_retries: Some(3),
            min_context_slot: None,
        },
    )?;

    println!("Transaction sent: {}", signature);

    match confirm_transaction(rpc_client, &signature, 20, 3).await {
        Ok(true) => Ok(SellResponse {
            success: true,
            signature: signature.to_string(),
            token_quantity: request.token_quantity,
            sol_received: expected_sol_output,
            solscan_tx_url: format!("https://solscan.io/tx/{}", signature),
            error: None,
        }),
        _ => Err(AppError::ServerError(
            "Transaction failed during confirmation".to_string(),
        )),
    }
}
