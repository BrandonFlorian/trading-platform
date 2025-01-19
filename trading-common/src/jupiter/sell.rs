use solana_client::{rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig};
use solana_sdk::{
    commitment_config::CommitmentLevel,
    compute_budget::ComputeBudgetInstruction,
    signature::Keypair,
    signer::Signer,
    transaction::{Transaction, VersionedTransaction},
};
use solana_transaction_status::UiTransactionEncoding;

use crate::{
    data::confirm_transaction,
    error::AppError,
    models::{SellRequest, SellResponse},
    COMPUTE_BUDGET_PRICE,
};

use super::{JupiterApi, QuoteRequest, SwapRequest, WSOL_ADDRESS};

pub async fn process_sell_request(
    rpc_client: &RpcClient,
    server_keypair: &Keypair,
    request: &SellRequest,
) -> Result<SellResponse, AppError> {
    let jupiter = JupiterApi::new();
    let amount_lamports = (request.token_quantity * 1_000_000_000.0) as u64;
    let slippage_bps = (request.slippage_tolerance * 10_000.0) as u32;

    // Get quote
    let quote_request = QuoteRequest {
        input_mint: request.token_address.clone(),
        output_mint: WSOL_ADDRESS.to_string(),
        amount: amount_lamports,
        slippage_bps,
        only_direct_routes: true,
    };

    let quote_response = jupiter.get_quote(&quote_request).await?;

    // Get swap transaction
    let swap_request = SwapRequest {
        user_public_key: server_keypair.pubkey().to_string(),
        wrap_unwrap_sol: true,
        use_shared_accounts: true,
        fee_account: None,
        compute_unit_price_micro_lamports: Some(COMPUTE_BUDGET_PRICE),
        quote_response: quote_response.clone(),
    };

    let swap_response = jupiter.get_swap_transaction(&swap_request).await?;

    // Decode and sign transaction
    let swap_transaction = base64::decode(&swap_response.swap_transaction)
        .map_err(|e| AppError::TransactionError(format!("Failed to decode transaction: {}", e)))?;

    let versioned_transaction = bincode::deserialize::<VersionedTransaction>(&swap_transaction)
        .map_err(|e| {
            AppError::TransactionError(format!("Failed to deserialize transaction: {}", e))
        })?;

    // Add compute budget instruction
    let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_price(COMPUTE_BUDGET_PRICE);

    // Sign and send transaction
    let recent_blockhash = rpc_client.get_latest_blockhash()?;

    let mut final_tx =
        Transaction::new_with_payer(&[compute_budget_ix], Some(&server_keypair.pubkey()));
    final_tx
        .message
        .account_keys
        .extend(versioned_transaction.message.static_account_keys());
    final_tx.sign(&[server_keypair], recent_blockhash);

    // Send transaction
    let signature = rpc_client.send_transaction_with_config(
        &final_tx,
        RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Processed),
            encoding: Some(UiTransactionEncoding::Base64),
            max_retries: Some(3),
            ..RpcSendTransactionConfig::default()
        },
    )?;

    // Wait for confirmation and extract amounts
    match confirm_transaction(rpc_client, &signature, 20, 3).await {
        Ok(true) => {
            // Parse the successful quote to get amounts
            let route = quote_response
                .routes
                .first()
                .ok_or_else(|| AppError::ServerError("No route found in quote".to_string()))?;

            let sol_received = route.out_amount.parse::<f64>().unwrap_or(0.0) / 1e9;

            Ok(SellResponse {
                success: true,
                signature: signature.to_string(),
                token_quantity: request.token_quantity,
                sol_received,
                solscan_tx_url: format!("https://solscan.io/tx/{}", signature),
                error: None,
            })
        }
        _ => Err(AppError::ServerError(
            "Transaction failed during confirmation".to_string(),
        )),
    }
}
