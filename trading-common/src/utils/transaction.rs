use anyhow::Result;
use serde_json::Value;
use solana_client::{rpc_client::RpcClient, rpc_config::RpcTransactionConfig};
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding};
use std::str::FromStr;
use std::{sync::Arc, time::Duration};

use crate::pumpdotfun;
use crate::raydium;
use crate::{data::get_metadata, ClientTxInfo, TransactionType};

use super::dex::{DexTransaction, DexType};

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
    // Detect DEX and extract transaction details
    let dex_type = DexTransaction::detect_dex_type(transaction_data);

    let (transaction_type, token_address, amount_token, amount_sol, price_per_token) =
        match dex_type {
            DexType::PumpFun => crate::pumpdotfun::extract_transaction_details(transaction_data)?,
            DexType::Raydium => crate::raydium::extract_transaction_details(transaction_data)?,
            DexType::Unknown => return Ok(None),
        };

    if transaction_type == TransactionType::Unknown {
        return Ok(None);
    }

    // Get token metadata
    let token_pubkey = Pubkey::from_str(&token_address)?;
    let token_metadata = get_metadata(rpc_client, &token_pubkey).await?;

    // Get buyer/seller based on DEX type
    let (seller, buyer) = match dex_type {
        DexType::PumpFun => {
            pumpdotfun::transaction::extract_accounts(transaction_data, &transaction_type)?
        }
        DexType::Raydium => {
            raydium::transaction::extract_accounts(transaction_data, &transaction_type)?
        }
        DexType::Unknown => (String::new(), String::new()),
    };

    Ok(Some(ClientTxInfo {
        signature: signature.to_string(),
        token_address,
        token_name: token_metadata.name,
        token_symbol: token_metadata.symbol,
        transaction_type,
        amount_token,
        amount_sol,
        price_per_token,
        token_image_uri: token_metadata.uri,
        market_cap: 0.0,
        usd_market_cap: 0.0,
        timestamp: transaction_data.block_time.unwrap_or(0),
        seller,
        buyer,
        dex_type,
    }))
}
