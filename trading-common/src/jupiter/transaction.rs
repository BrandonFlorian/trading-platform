use std::str::FromStr;

use super::EncodedInstruction;
use crate::{error::AppError, jupiter::constants::JUPITER_PROGRAM_ID, TransactionType, WSOL};
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};
use solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta;

pub fn extract_transaction_details(
    transaction: &EncodedConfirmedTransactionWithStatusMeta,
) -> Result<(TransactionType, String, f64, f64, f64)> {
    let meta = transaction
        .transaction
        .meta
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No transaction metadata"))?;

    let empty_logs = Vec::new();
    let logs = meta.log_messages.as_ref().unwrap_or(&empty_logs);

    println!("Analyzing Jupiter transaction logs...");

    // Check if this is a Jupiter transaction
    if !logs.iter().any(|log| log.contains(JUPITER_PROGRAM_ID)) {
        return Err(anyhow::anyhow!("Not a Jupiter transaction"));
    }

    let empty_token_balances = Vec::new();
    let pre_balances = meta
        .pre_token_balances
        .as_ref()
        .unwrap_or(&empty_token_balances);
    let post_balances = meta
        .post_token_balances
        .as_ref()
        .unwrap_or(&empty_token_balances);

    // Find the non-WSOL token
    let token_balance = pre_balances
        .iter()
        .find(|balance| balance.mint != WSOL)
        .ok_or_else(|| anyhow::anyhow!("Could not find token balance"))?;

    let token_address = token_balance.mint.clone();

    // Calculate token amount change
    let pre_amount = pre_balances
        .iter()
        .find(|b| b.mint == token_address)
        .and_then(|b| b.ui_token_amount.ui_amount)
        .unwrap_or(0.0);

    let post_amount = post_balances
        .iter()
        .find(|b| b.mint == token_address)
        .and_then(|b| b.ui_token_amount.ui_amount)
        .unwrap_or(0.0);

    let token_amount_change = post_amount - pre_amount;
    println!("Token amount change: {}", token_amount_change);

    // Determine transaction type based on the actual operation
    // Jupiter uses SharedAccountsRoute for both buy and sell
    let transaction_type = if token_amount_change > 0.0 {
        println!("Detected Jupiter BUY (token balance increased)");
        TransactionType::Buy
    } else {
        println!("Detected Jupiter SELL (token balance decreased)");
        TransactionType::Sell
    };

    let amount_token = token_amount_change.abs();

    // Calculate SOL amount change
    let pre_sol = meta.pre_balances.first().copied().unwrap_or(0);
    let post_sol = meta.post_balances.first().copied().unwrap_or(0);
    let amount_sol = ((post_sol as i64 - pre_sol as i64).abs() as f64) / 1e9;

    let price_per_token = if amount_token > 0.0 {
        amount_sol / amount_token
    } else {
        0.0
    };

    println!("Transaction summary:");
    println!("  Token: {}", token_address);
    println!("  Type: {:?}", transaction_type);
    println!("  Amount token: {}", amount_token);
    println!("  Amount SOL: {}", amount_sol);
    println!("  Price per token: {}", price_per_token);

    Ok((
        transaction_type,
        token_address,
        amount_token,
        amount_sol,
        price_per_token,
    ))
}

pub fn extract_accounts(
    transaction: &EncodedConfirmedTransactionWithStatusMeta,
    transaction_type: &TransactionType,
) -> Result<(String, String)> {
    match &transaction.transaction.transaction {
        solana_transaction_status::EncodedTransaction::Json(tx) => {
            let account_keys = crate::data::get_account_keys_from_message(&tx.message);

            // Jupiter's account order is similar to Raydium's:
            // First account is usually the authority/user
            let (seller, buyer) = match transaction_type {
                TransactionType::Sell => (
                    account_keys.first().cloned().unwrap_or_default(),
                    account_keys.get(1).cloned().unwrap_or_default(),
                ),
                TransactionType::Buy => (
                    account_keys.get(1).cloned().unwrap_or_default(),
                    account_keys.first().cloned().unwrap_or_default(),
                ),
                _ => (String::new(), String::new()),
            };

            Ok((seller, buyer))
        }
        _ => Ok((String::new(), String::new())),
    }
}

pub fn convert_encoded(encoded: EncodedInstruction) -> Result<Instruction, AppError> {
    let program_id = Pubkey::from_str(&encoded.program_id)
        .map_err(|e| AppError::TransactionError(format!("Invalid program id: {}", e)))?;

    let accounts = encoded
        .accounts
        .into_iter()
        .map(|acc| {
            let pubkey = Pubkey::from_str(&acc.pubkey).map_err(|e| {
                AppError::TransactionError(format!("Invalid account pubkey: {}", e))
            })?;
            Ok(AccountMeta {
                pubkey,
                is_signer: acc.is_signer,
                is_writable: acc.is_writable,
            })
        })
        .collect::<Result<Vec<_>, AppError>>()?;

    let data = STANDARD
        .decode(&encoded.data)
        .map_err(|e| AppError::TransactionError(format!("Invalid instruction data: {}", e)))?;

    Ok(Instruction {
        program_id,
        accounts,
        data,
    })
}
