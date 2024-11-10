use anyhow::Result;
use serde_json::Value;
use solana_client::{rpc_client::RpcClient, rpc_config::RpcTransactionConfig};
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding};
use std::str::FromStr;
use std::{sync::Arc, time::Duration};

use crate::{
    data::{get_account_keys_from_message, get_metadata},
    ClientTxInfo, TransactionType,
};

pub async fn process_websocket_message(
    text: &str,
    rpc_client: &Arc<RpcClient>,
) -> Result<Option<ClientTxInfo>> {
    println!("Processing websocket message");
    let value: Value = serde_json::from_str(text)?;
    println!("Raw message: {}", text);

    // Check for error in json_data
    if value.get("error").is_some() {
        println!("Received error message from RPC");
        return Ok(None);
    }

    let result = match value.get("params").and_then(|p| p.get("result")) {
        Some(r) => r,
        None => {
            println!("No result in message (subscription confirmation)");
            return Ok(None);
        }
    };

    let signature = match result
        .get("value")
        .and_then(|v| v.get("signature"))
        .and_then(|s| s.as_str())
    {
        Some(s) => s.to_string(),
        None => {
            println!("No signature found");
            return Ok(None);
        }
    };

    // Configure transaction fetch
    let config = RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::JsonParsed),
        commitment: Some(solana_sdk::commitment_config::CommitmentConfig::confirmed()),
        max_supported_transaction_version: Some(0),
    };

    // Fetch full transaction with retries
    let signature_obj = Signature::from_str(&signature)?;
    let mut retries = 20;
    let mut transaction_data = None;

    while retries > 0 {
        match rpc_client.get_transaction_with_config(&signature_obj, config) {
            Ok(data) => {
                transaction_data = Some(data);
                break;
            }
            Err(e) => {
                println!(
                    "Error fetching transaction {} (retry {}): {}",
                    signature,
                    21 - retries,
                    e
                );
                retries -= 1;
                if retries > 0 {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    let transaction_data = match transaction_data {
        Some(data) => data,
        None => {
            println!("Failed to fetch transaction data after retries");
            return Ok(None);
        }
    };

    // Process the transaction data to create ClientTxInfo
    create_client_tx_info(&transaction_data, &signature, rpc_client).await
}

pub async fn create_client_tx_info(
    transaction_data: &EncodedConfirmedTransactionWithStatusMeta,
    signature: &str,
    rpc_client: &Arc<RpcClient>,
) -> Result<Option<ClientTxInfo>> {
    // Get the meta data
    let meta = match &transaction_data.transaction.meta {
        Some(meta) => meta,
        None => {
            println!("No meta data in transaction");
            return Ok(None);
        }
    };

    // Create longer-lived empty vectors
    let empty_token_balances = Vec::new();
    let empty_logs = Vec::new();

    // Extract token balances with proper lifetimes
    let pre_balances = meta
        .pre_token_balances
        .as_ref()
        .unwrap_or(&empty_token_balances);
    let post_balances = meta
        .post_token_balances
        .as_ref()
        .unwrap_or(&empty_token_balances);

    // Get token info
    let token_address = match post_balances.first() {
        Some(balance) => balance.mint.clone(),
        None => {
            println!("No token balance information");
            return Ok(None);
        }
    };

    // Calculate token amount change
    let pre_amount = pre_balances
        .first()
        .and_then(|b| b.ui_token_amount.ui_amount)
        .unwrap_or(0.0);
    let post_amount = post_balances
        .first()
        .and_then(|b| b.ui_token_amount.ui_amount)
        .unwrap_or(0.0);
    let amount_token = (post_amount - pre_amount).abs();

    // Calculate SOL amount change
    let pre_sol = meta.pre_balances.first().copied().unwrap_or(0);
    let post_sol = meta.post_balances.first().copied().unwrap_or(0);
    let amount_sol = ((post_sol as i64 - pre_sol as i64).abs() as f64) / 1e9;

    // Get transaction logs and determine type using the longer-lived empty vector
    let logs = meta.log_messages.as_ref().unwrap_or(&empty_logs);
    let transaction_type = if logs.iter().any(|log| log.contains("Instruction: Buy")) {
        TransactionType::Buy
    } else if logs.iter().any(|log| log.contains("Instruction: Sell")) {
        TransactionType::Sell
    } else {
        TransactionType::Unknown
    };

    // Get token metadata
    let token_pubkey = Pubkey::from_str(&token_address)?;
    let token_metadata = get_metadata(rpc_client, &token_pubkey).await?;

    // Get account keys
    let (seller, buyer) = match &transaction_data.transaction.transaction {
        solana_transaction_status::EncodedTransaction::Json(tx) => {
            let account_keys = get_account_keys_from_message(&tx.message);

            match transaction_type {
                TransactionType::Sell => (
                    account_keys.first().cloned().unwrap_or_default(),
                    account_keys.get(1).cloned().unwrap_or_default(),
                ),
                TransactionType::Buy => (
                    account_keys.get(1).cloned().unwrap_or_default(),
                    account_keys.first().cloned().unwrap_or_default(),
                ),
                _ => (String::new(), String::new()),
            }
        }
        _ => (String::new(), String::new()),
    };

    Ok(Some(ClientTxInfo {
        signature: signature.to_string(),
        token_address,
        token_name: token_metadata.name,
        token_symbol: token_metadata.symbol,
        transaction_type,
        amount_token,
        amount_sol,
        price_per_token: if amount_token > 0.0 {
            amount_sol / amount_token
        } else {
            0.0
        },
        token_image_uri: token_metadata.uri,
        market_cap: 0.0,
        usd_market_cap: 0.0,
        timestamp: transaction_data.block_time.unwrap_or(0),
        seller,
        buyer,
    }))
}
