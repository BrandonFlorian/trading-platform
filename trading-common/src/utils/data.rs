use crate::error::AppError;
use anyhow::{anyhow, Context, Result};
use once_cell::sync::Lazy;
use serde_json::Value;
use solana_account_decoder::UiAccountData;
use solana_client::{rpc_client::RpcClient, rpc_config::RpcTransactionConfig};
use solana_program::program_pack::Pack;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signature::Signature;
use solana_transaction_status::UiTransactionEncoding;
use solana_transaction_status::{UiMessage, UiParsedMessage};
use spl_token::state::Mint;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use surf::{Client, Url};
use tokio::time::sleep;

static HTTP_CLIENT: Lazy<Client> = Lazy::new(Client::new);

pub const METADATA_PROGRAM_ID: Pubkey =
    solana_program::pubkey!("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s");
const IPFS_GATEWAYS: &[&str] = &[
    "https://ipfs.io/ipfs/",
    "https://cloudflare-ipfs.com/ipfs/",
    "https://gateway.pinata.cloud/ipfs/",
    "https://gateway.ipfs.io/ipfs/",
];

#[derive(Debug, Clone)]
pub struct TokenMetadata {
    pub update_authority: String,
    pub mint: String,
    pub name: String,
    pub symbol: String,
    pub uri: String,
}

pub fn get_server_keypair() -> Keypair {
    let secret_key =
        std::env::var("SERVER_WALLET_SECRET_KEY").expect("SERVER_WALLET_SECRET_KEY must be set");
    Keypair::from_base58_string(&secret_key)
}

pub fn decode_mint_account(account_data: &[u8]) -> Result<Mint> {
    Mint::unpack(account_data).context("Failed to unpack Mint account data")
}

pub fn get_metadata_account(mint: &Pubkey) -> Pubkey {
    let seeds = &[
        b"metadata".as_ref(),
        METADATA_PROGRAM_ID.as_ref(),
        mint.as_ref(),
    ];
    Pubkey::find_program_address(seeds, &METADATA_PROGRAM_ID).0
}

pub fn unpack_metadata_account(data: &[u8]) -> Result<TokenMetadata> {
    if data.is_empty() || data[0] != 4 {
        return Err(anyhow!("Invalid metadata account data"));
    }

    let mut position = 1;

    // Read authority
    let update_authority = bs58::encode(&data[position..position + 32]).into_string();
    position += 32;

    // Read mint
    let mint = bs58::encode(&data[position..position + 32]).into_string();
    position += 32;

    // Read name
    let name_length = u32::from_le_bytes(data[position..position + 4].try_into()?);
    position += 4;
    let name = String::from_utf8(data[position..position + name_length as usize].to_vec())?
        .trim_matches('\0')
        .to_string();
    position += name_length as usize;

    // Read symbol
    let symbol_length = u32::from_le_bytes(data[position..position + 4].try_into()?);
    position += 4;
    let symbol = String::from_utf8(data[position..position + symbol_length as usize].to_vec())?
        .trim_matches('\0')
        .to_string();
    position += symbol_length as usize;

    // Read uri
    let uri_length = u32::from_le_bytes(data[position..position + 4].try_into()?);
    position += 4;
    let uri = String::from_utf8(data[position..position + uri_length as usize].to_vec())?
        .trim_matches('\0')
        .to_string();

    Ok(TokenMetadata {
        update_authority,
        mint,
        name,
        symbol,
        uri,
    })
}

pub fn format_token_amount(amount: u64, decimals: u8) -> f64 {
    (amount as f64) / 10f64.powi(decimals as i32)
}

pub fn format_balance(balance: f64, _decimals: u8) -> String {
    if balance == 0.0 {
        "0".to_string()
    } else if balance < 0.000001 {
        format!("{:.8}", balance)
    } else {
        format!("{:.6}", balance)
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    }
}

pub fn extract_token_account_info(account_data: &UiAccountData) -> Option<(String, u64, u8)> {
    match account_data {
        UiAccountData::Json(parsed_account) => {
            let info = parsed_account
                .parsed
                .as_object()?
                .get("info")?
                .as_object()?;

            let mint = info.get("mint")?.as_str()?.to_string();
            let balance = info
                .get("tokenAmount")?
                .as_object()?
                .get("amount")?
                .as_str()?
                .parse::<u64>()
                .ok()?;
            let decimals = info
                .get("tokenAmount")?
                .as_object()?
                .get("decimals")?
                .as_u64()? as u8;

            Some((mint, balance, decimals))
        }
        _ => None,
    }
}

pub async fn get_token_balance(rpc_client: &RpcClient, token_account: &Pubkey) -> Result<f64> {
    let account = rpc_client.get_token_account_balance(token_account)?;
    account
        .ui_amount
        .ok_or_else(|| anyhow!("Failed to get token balance"))
}

pub async fn get_metadata(rpc_client: &Arc<RpcClient>, mint: &Pubkey) -> Result<TokenMetadata> {
    let metadata_account = get_metadata_account(mint);
    let account_info = rpc_client
        .get_account_data(&metadata_account)
        .context("Failed to fetch metadata account data")?;

    unpack_metadata_account(&account_info).context("Failed to unpack metadata account data")
}

pub async fn fetch_extended_metadata(uri: &str) -> Result<Value> {
    println!("Fetching extended metadata from {}", uri);
    if uri.starts_with("ipfs://") || uri.contains("/ipfs/") {
        println!("Fetching IPFS metadata from {}", uri);
        fetch_ipfs_metadata(uri).await
    } else {
        println!("Fetching HTTP metadata from {}", uri);
        fetch_http_metadata(uri).await
    }
}

async fn fetch_ipfs_metadata(uri: &str) -> Result<Value> {
    let cid = uri
        .trim_start_matches("ipfs://")
        .trim_start_matches("https://")
        .split("/ipfs/")
        .nth(1)
        .unwrap_or(uri);

    for gateway in IPFS_GATEWAYS {
        let full_uri = format!("{}{}", gateway, cid);
        println!("Trying IPFS gateway: {}", full_uri);
    }

    Err(anyhow!("Failed to fetch IPFS metadata from all gateways"))
}

async fn fetch_http_metadata(initial_uri: &str) -> Result<Value> {
    let mut uri = initial_uri.to_string();
    let mut redirect_count = 0;
    const MAX_REDIRECTS: u8 = 5;

    loop {
        println!("Fetching HTTP metadata from {}", uri);
        let mut response = HTTP_CLIENT
            .get(&uri)
            .await
            .map_err(|e| anyhow!("Failed to fetch metadata from {}: {}", uri, e))?;

        println!("Response: {:?}", response);

        if response.status().is_redirection() {
            if redirect_count >= MAX_REDIRECTS {
                return Err(anyhow!("Too many redirects"));
            }

            let new_location = response
                .header("Location")
                .and_then(|values| values.get(0))
                .map(|value| value.to_string())
                .ok_or_else(|| anyhow!("Redirect without valid Location header"))?;

            // Resolve the new location against the current URI
            let current_url = Url::parse(&uri)?;
            let new_url = current_url.join(&new_location)?;

            println!("Following redirect to: {}", new_url);
            uri = new_url.to_string();
            redirect_count += 1;
            continue;
        }

        let json: Value = response
            .body_json()
            .await
            .map_err(|e| anyhow!("Failed to parse metadata JSON from {}: {}", uri, e))?;

        println!("JSON: {:?}", json);
        return Ok(json);
    }
}

pub async fn confirm_transaction(
    rpc_client: &RpcClient,
    signature: &Signature,
    max_retries: u32,
    retry_interval: u64,
) -> Result<bool, AppError> {
    let mut retries = 0;

    while retries < max_retries {
        match rpc_client.get_transaction_with_config(
            signature,
            RpcTransactionConfig {
                encoding: UiTransactionEncoding::Json.into(),
                commitment: Some(solana_sdk::commitment_config::CommitmentConfig::confirmed()),
                max_supported_transaction_version: Some(0),
            },
        ) {
            Ok(confirmed_tx) => {
                if let Some(meta) = confirmed_tx.transaction.meta {
                    if meta.err.is_none() {
                        println!("Transaction confirmed... try count: {}", retries + 1);
                        return Ok(true);
                    }
                    println!("Transaction failed.");
                    return Ok(false);
                }
            }
            Err(e) => {
                println!(
                    "Awaiting confirmation... try count: {}/{}",
                    retries + 1,
                    max_retries
                );
                if e.to_string().contains("Transaction version") {
                    println!("Transaction failed.");
                    return Ok(false);
                }
            }
        }

        retries += 1;
        tokio::time::sleep(Duration::from_secs(retry_interval)).await;
    }

    println!("Max retries reached. Transaction confirmation failed.");
    Ok(false)
}

pub async fn sleeper(
    signature: &Signature,
    retry_count: &mut u32,
    max_retries: u32,
    retry_interval: u64,
) -> u32 {
    *retry_count += 1;

    println!("Retry {} of {} for {}", retry_count, max_retries, signature);

    sleep(Duration::from_secs(retry_interval)).await;

    *retry_count
}

pub fn validate_token_address(address: &str) -> Result<(), AppError> {
    match Pubkey::from_str(address) {
        Ok(_) => Ok(()),
        Err(_) => Err(AppError::BadRequest(format!(
            "Invalid token address: {}",
            address
        ))),
    }
}

pub fn get_account_keys_from_message(message: &UiMessage) -> Vec<String> {
    match message {
        UiMessage::Parsed(UiParsedMessage { account_keys, .. }) => {
            account_keys.iter().map(|key| key.pubkey.clone()).collect()
        }
        UiMessage::Raw(raw_message) => raw_message.account_keys.clone(),
    }
}
