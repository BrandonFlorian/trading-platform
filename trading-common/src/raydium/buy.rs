use super::constants::*;
use super::types::PoolKeys;
use crate::{
    data::{confirm_transaction, get_token_balance},
    error::AppError,
    extract_transaction_details,
    models::{BuyRequest, BuyResponse},
    raydium::{
        constants::{COMPUTE_BUDGET_PRICE, COMPUTE_BUDGET_UNITS, LAMPORTS_PER_SOL},
        utils::{
            create_swap_instruction, create_wsol_account_instructions, get_pool_info, get_pool_keys,
        },
    },
};
use solana_client::{rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig};
use solana_sdk::{
    commitment_config::CommitmentConfig, compute_budget::ComputeBudgetInstruction,
    message::Message, pubkey::Pubkey, signature::Keypair, signer::Signer, transaction::Transaction,
};
use solana_transaction_status::UiTransactionEncoding;
use spl_token::instruction as token_instruction;
use std::str::FromStr;

pub async fn process_buy_request(
    rpc_client: &RpcClient,
    server_keypair: &Keypair,
    request: &BuyRequest,
) -> Result<BuyResponse, AppError> {
    println!("Processing Raydium buy request: {:?}", request);

    // Get pool info first to verify pool exists
    let pool_info = get_pool_info(&request.token_address).await?;
    println!("Found pool info: {}", pool_info.id);

    // Get complete pool keys
    let pool_keys = get_pool_keys(&pool_info.id).await?;
    let pool_keys = PoolKeys::from(pool_keys);
    println!("Pool keys fetched successfully for pool {}", pool_info.id);

    // Validate token accounts and amounts
    let token_mint = Pubkey::from_str(&request.token_address)
        .map_err(|_| AppError::BadRequest("Invalid token address".to_string()))?;

    let amount_in = (request.sol_quantity * LAMPORTS_PER_SOL as f64) as u64;
    let minimum_out = ((amount_in as f64) * (1.0 - request.slippage_tolerance)) as u64;

    println!(
        "Swap parameters: amount_in={}, minimum_out={}, slippage={}",
        amount_in, minimum_out, request.slippage_tolerance
    );

    // Create temporary WSOL account
    let (wsol_keypair, wsol_instructions) =
        create_wsol_account_instructions(rpc_client, server_keypair, amount_in).await?;

    // Get or create token account
    let token_account = spl_associated_token_account::get_associated_token_address(
        &server_keypair.pubkey(),
        &token_mint,
    );

    // Check if token account exists, if not add creation instruction
    let mut instructions = vec![
        ComputeBudgetInstruction::set_compute_unit_price(COMPUTE_BUDGET_PRICE),
        ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_BUDGET_UNITS),
    ];

    // Add WSOL account instructions
    instructions.extend(wsol_instructions);

    // Add token account creation if needed
    if rpc_client.get_account(&token_account).is_err() {
        println!("Creating new associated token account");
        instructions.push(
            spl_associated_token_account::instruction::create_associated_token_account(
                &server_keypair.pubkey(),
                &server_keypair.pubkey(),
                &token_mint,
                &spl_token::id(),
            ),
        );
    }

    // Create swap instruction
    let swap_ix = create_swap_instruction(
        &pool_keys,
        amount_in,
        minimum_out,
        wsol_keypair.pubkey(),
        token_account,
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

    println!("Sending transaction...");
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

    println!("Transaction sent, signature: {}", signature);

    // Wait for confirmation
    match confirm_transaction(rpc_client, &signature, 20, 3).await {
        Ok(true) => {
            println!("Transaction confirmed successfully");

            // Get the transaction data to extract exact token amount received
            let tx_data = rpc_client.get_transaction_with_config(
                &signature,
                solana_client::rpc_config::RpcTransactionConfig {
                    encoding: Some(solana_transaction_status::UiTransactionEncoding::Json),
                    commitment: Some(solana_sdk::commitment_config::CommitmentConfig::confirmed()),
                    max_supported_transaction_version: Some(0),
                },
            )?;

            println!("Transaction data: {:?}", tx_data);

            // Extract token amount from transaction data
            let (_, _, amount_token, _, _) = extract_transaction_details(&tx_data)?;
            println!("Tokens received from swap: {}", amount_token);

            Ok(BuyResponse {
                success: true,
                signature: signature.to_string(),
                token_quantity: amount_token,
                sol_spent: request.sol_quantity,
                solscan_tx_url: format!("https://solscan.io/tx/{}", signature),
                error: None,
            })
        }
        _ => Err(AppError::ServerError(
            "Transaction failed during confirmation".to_string(),
        )),
    }
}
