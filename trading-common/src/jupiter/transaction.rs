use std::str::FromStr;

use super::EncodedInstruction;
use crate::{error::AppError, jupiter::constants::JUPITER_PROGRAM_ID, TransactionType, WSOL};
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiMessage};

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
    for log in logs {
        println!("Log: {}", log);
    }

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

    println!("Pre balances: {:?}", pre_balances);
    println!("Post balances: {:?}", post_balances);
    println!(
        "Pre SOL balance: {}",
        meta.pre_balances.first().copied().unwrap_or(0) as f64 / 1e9
    );
    println!(
        "Post SOL balance: {}",
        meta.post_balances.first().copied().unwrap_or(0) as f64 / 1e9
    );

    // Find the token with significant balance change for the tracked wallet
    let tracked_wallet_pubkey = match &transaction.transaction.transaction {
        solana_transaction_status::EncodedTransaction::Json(tx) => match &tx.message {
            UiMessage::Parsed(msg) => msg
                .account_keys
                .first()
                .ok_or_else(|| anyhow::anyhow!("No account keys found"))?
                .pubkey
                .clone(),
            UiMessage::Raw(msg) => msg
                .account_keys
                .first()
                .ok_or_else(|| anyhow::anyhow!("No account keys found"))?
                .clone(),
        },
        _ => return Err(anyhow::anyhow!("Unsupported transaction format")),
    };

    // Find token balances for the tracked wallet
    let tracked_pre_balances: Vec<_> = pre_balances
        .iter()
        .filter(|b| {
            b.owner
                .as_ref()
                .map_or(false, |owner| owner == &tracked_wallet_pubkey)
        })
        .collect();

    let tracked_post_balances: Vec<_> = post_balances
        .iter()
        .filter(|b| {
            b.owner
                .as_ref()
                .map_or(false, |owner| owner == &tracked_wallet_pubkey)
        })
        .collect();

    println!("Tracked wallet pre balances: {:?}", tracked_pre_balances);
    println!("Tracked wallet post balances: {:?}", tracked_post_balances);

    // Find the token with the largest change
    let mut max_change = 0.0;
    let mut target_token = None;

    for balance in tracked_pre_balances
        .iter()
        .chain(tracked_post_balances.iter())
    {
        if balance.mint == WSOL {
            continue;
        }

        let pre_amount: f64 = tracked_pre_balances
            .iter()
            .filter(|b| b.mint == balance.mint)
            .filter_map(|b| b.ui_token_amount.ui_amount)
            .sum();

        let post_amount: f64 = tracked_post_balances
            .iter()
            .filter(|b| b.mint == balance.mint)
            .filter_map(|b| b.ui_token_amount.ui_amount)
            .sum();

        let change = (post_amount - pre_amount).abs();
        println!("Token {} change: {}", balance.mint, change);

        if change > max_change {
            max_change = change;
            target_token = Some(balance);
        }
    }

    let token_balance = target_token
        .ok_or_else(|| anyhow::anyhow!("Could not find token with significant balance change"))?;

    let token_address = token_balance.mint.clone();
    println!(
        "Found token with largest balance change: {} (change: {})",
        token_address, max_change
    );

    // Calculate token amount change for the tracked wallet
    let pre_amount: f64 = tracked_pre_balances
        .iter()
        .filter(|b| b.mint == token_address)
        .filter_map(|b| b.ui_token_amount.ui_amount)
        .sum();

    let post_amount: f64 = tracked_post_balances
        .iter()
        .filter(|b| b.mint == token_address)
        .filter_map(|b| b.ui_token_amount.ui_amount)
        .sum();

    let token_amount_change = post_amount - pre_amount;
    println!("Pre amount for token {}: {}", token_address, pre_amount);
    println!("Post amount for token {}: {}", token_address, post_amount);
    println!("Token amount change: {}", token_amount_change);

    // Calculate SOL amount change (including wrapped SOL)
    let pre_sol = pre_balances
        .iter()
        .find(|b| b.mint == WSOL)
        .and_then(|b| b.ui_token_amount.ui_amount)
        .unwrap_or(0.0)
        + (meta.pre_balances.first().copied().unwrap_or(0) as f64 / 1e9);

    let post_sol = post_balances
        .iter()
        .find(|b| b.mint == WSOL)
        .and_then(|b| b.ui_token_amount.ui_amount)
        .unwrap_or(0.0)
        + (meta.post_balances.first().copied().unwrap_or(0) as f64 / 1e9);

    let amount_sol = (post_sol - pre_sol).abs();

    // Determine transaction type based on token amount change
    let transaction_type = if token_amount_change > 0.0 {
        println!(
            "Detected Jupiter BUY (token balance increased by {})",
            token_amount_change
        );
        TransactionType::Buy
    } else if token_amount_change < 0.0 {
        println!(
            "Detected Jupiter SELL (token balance decreased by {})",
            token_amount_change.abs()
        );
        TransactionType::Sell
    } else {
        // If there's no change, look at the direction of SOL movement
        let sol_change = (post_sol - pre_sol) as f64;
        if sol_change < 0.0 {
            println!("Detected Jupiter BUY (based on SOL decrease)");
            TransactionType::Buy
        } else {
            println!("Detected Jupiter SELL (based on SOL increase)");
            TransactionType::Sell
        }
    };

    let amount_token = token_amount_change.abs();
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
