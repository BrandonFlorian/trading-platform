use crate::error::AppError;
use anyhow::{Context, Result};
use thiserror::Error;

use serde::{Deserialize, Serialize};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    instruction::AccountMeta, instruction::Instruction, message::Message, pubkey::Pubkey,
    signer::Signer, transaction::Transaction,
};
use std::str::FromStr;

use crate::models::{BuyRequest, BuyResponse, SellRequest, SellResponse};
use solana_sdk::signature::Keypair;
// use crate::constants::{
//     EVENT_AUTHORITY, FEE_RECIPIENT, GLOBAL, PUMP_FUN_PROGRAM_ID, SYSTEM_PROGRAM,
//     TOKEN_KEG_PROGRAM_ID,
// };

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpFunCoinData {
    pub mint: String,
    pub name: String,
    pub symbol: String,
    pub description: String,
    pub image_uri: String,
    pub metadata_uri: String,
    pub twitter: String,
    pub telegram: String,
    pub bonding_curve: String,
    pub associated_bonding_curve: String,
    pub creator: String,
    pub created_timestamp: i64,
    pub raydium_pool: String,
    pub complete: bool,
    pub virtual_sol_reserves: i64,
    pub virtual_token_reserves: i64,
    pub total_supply: i64,
    pub website: String,
    pub show_name: bool,
    pub king_of_the_hill_timestamp: i64,
    pub market_cap: f64,
    pub reply_count: i32,
    pub last_reply: i64,
    pub nsfw: bool,
    pub market_id: String,
    pub inverted: String,
    pub username: String,
    pub profile_image: String,
    pub usd_market_cap: f64,
}

#[derive(Debug, Clone)]
pub struct PumpFunTokenContainer {
    pub mint_address: Pubkey,
    pub pump_fun_coin_data: Option<PumpFunCoinData>,
    pub program_account_info: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct TokenAccountOwnerContainer {
    pub owner_address: Pubkey,
    pub mint_address: Pubkey,
    pub token_account_address: Option<Pubkey>,
}

#[derive(Error, Debug)]
pub enum PumpFunError {
    #[error("Surf error: {0}")]
    SurfError(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[error("Anyhow error: {0}")]
    AnyhowError(#[from] anyhow::Error),
}

const BONDING_CURVE_MARGIN_OF_ERROR: f64 = 0.01; // 1%
const BUY: u8 = 0;
const SELL: u8 = 1;
pub const PUMP_FUN_PROGRAM_ID: Pubkey =
    solana_sdk::pubkey!("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");
pub const GLOBAL: Pubkey = solana_sdk::pubkey!("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf");
pub const FEE_RECIPIENT: Pubkey =
    solana_sdk::pubkey!("CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM");
pub const SYSTEM_PROGRAM: Pubkey = solana_sdk::pubkey!("11111111111111111111111111111111");
pub const TOKEN_KEG_PROGRAM_ID: Pubkey =
    solana_sdk::pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
pub const EVENT_AUTHORITY: Pubkey =
    solana_sdk::pubkey!("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1");

pub async fn get_coin_data(token_address: &Pubkey) -> Result<PumpFunCoinData, AppError> {
    let url = format!("https://frontend-api.pump.fun/coins/{}", token_address);
    let mut response = surf::get(url)
        .header(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
        )
        .header("Accept", "*/*")
        .header("Accept-Language", "en-US,en;q=0.5")
        .await?;

    if response.status() != 200 {
        return Err(AppError::RequestError(format!(
            "Error getting coin data: {}",
            response.status()
        )));
    }

    let pump_fun_coin_data: PumpFunCoinData = response.body_json().await?;
    Ok(pump_fun_coin_data)
}

fn decode_bonding_curve_data(data: &[u8]) -> Result<(f64, f64)> {
    if data.len() < 24 {
        return Err(anyhow::anyhow!(
            "Insufficient data to decode bonding curve info"
        ));
    }

    let virtual_token_reserves = f64::from_le_bytes(data[8..16].try_into()?);
    let virtual_sol_reserves = f64::from_le_bytes(data[16..24].try_into()?);

    Ok((virtual_token_reserves, virtual_sol_reserves))
}

pub async fn get_bonding_curve_info(
    rpc_client: &RpcClient,
    pump_fun_token_container: &PumpFunTokenContainer,
) -> Result<(f64, f64), AppError> {
    let bonding_curve_pubkey = Pubkey::from_str(
        &pump_fun_token_container
            .pump_fun_coin_data
            .as_ref()
            .unwrap()
            .bonding_curve,
    )
    .context("Failed to parse bonding curve pubkey")?;

    let account_info = rpc_client
        .get_account_data(&bonding_curve_pubkey)
        .context("Failed to get account info")?;

    if account_info.is_empty() {
        return Err(AppError::BadRequest(
            "Account not found or no data available".to_string(),
        ));
    }

    let (virtual_token_reserves, virtual_sol_reserves) = decode_bonding_curve_data(&account_info)?;

    let pump_fun_virtual_token_reserves = pump_fun_token_container
        .pump_fun_coin_data
        .as_ref()
        .unwrap()
        .virtual_token_reserves as f64;
    let pump_fun_virtual_sol_reserves = pump_fun_token_container
        .pump_fun_coin_data
        .as_ref()
        .unwrap()
        .virtual_sol_reserves as f64;

    // Check if the decoded values are within the expected threshold
    let within_threshold = (virtual_sol_reserves * (1.0 - BONDING_CURVE_MARGIN_OF_ERROR)
        < pump_fun_virtual_sol_reserves)
        && (virtual_token_reserves * (1.0 - BONDING_CURVE_MARGIN_OF_ERROR)
            < pump_fun_virtual_token_reserves);

    if !within_threshold {
        println!(
            "Bonding curve reserves are not within threshold. pump.fun data likely out of sync."
        );
    }

    Ok((virtual_token_reserves, virtual_sol_reserves))
}

pub async fn buy(
    rpc_client: &RpcClient,
    secret_keypair: &impl Signer,
    token_account_container: &TokenAccountOwnerContainer,
    pump_fun_token_container: &PumpFunTokenContainer,
    sol_quantity: f64,
    slippage: f64,
) -> Result<String, AppError> {
    let user_address = secret_keypair.pubkey();

    let (virtual_token_reserves, virtual_sol_reserves) =
        get_bonding_curve_info(rpc_client, pump_fun_token_container).await?;

    let price_per_token = virtual_sol_reserves / virtual_token_reserves;
    let max_token_output = sol_quantity / price_per_token;
    let min_token_output = max_token_output * (1.0 - slippage);

    let token_out = (sol_quantity * virtual_token_reserves / virtual_sol_reserves) as u64;
    let max_sol_cost = (min_token_output * virtual_sol_reserves / virtual_token_reserves) as u64;

    let instruction = build_buy_instruction(
        &user_address,
        pump_fun_token_container,
        &token_account_container.token_account_address.unwrap(),
        token_out,
        max_sol_cost,
    );

    let recent_blockhash = rpc_client.get_latest_blockhash()?;
    let message =
        Message::new_with_blockhash(&[instruction], Some(&user_address), &recent_blockhash);
    let transaction = Transaction::new(&[secret_keypair], message, recent_blockhash);

    let signature = rpc_client.send_and_confirm_transaction(&transaction)?;

    Ok(signature.to_string())
}

pub async fn sell(
    rpc_client: &RpcClient,
    secret_keypair: &impl Signer,
    token_account_container: &TokenAccountOwnerContainer,
    pump_fun_token_container: &PumpFunTokenContainer,
    token_quantity: u64,
    slippage: f64,
) -> Result<String, AppError> {
    let user_address = secret_keypair.pubkey();

    let (virtual_token_reserves, virtual_sol_reserves) =
        get_bonding_curve_info(rpc_client, pump_fun_token_container).await?;

    let price_per_token = virtual_sol_reserves / virtual_token_reserves;
    let expected_sol_output = token_quantity as f64 * price_per_token;
    let min_sol_output = (expected_sol_output * (1.0 - slippage)) as u64;

    let instruction = build_sell_instruction(
        &user_address,
        pump_fun_token_container,
        &token_account_container.token_account_address.unwrap(),
        token_quantity,
        min_sol_output,
    );

    let recent_blockhash = rpc_client.get_latest_blockhash()?;
    let message =
        Message::new_with_blockhash(&[instruction], Some(&user_address), &recent_blockhash);
    let transaction = Transaction::new(&[secret_keypair], message, recent_blockhash);

    let signature = rpc_client.send_and_confirm_transaction(&transaction)?;

    Ok(signature.to_string())
}

fn build_buy_instruction(
    user_address: &Pubkey,
    pump_fun_token_container: &PumpFunTokenContainer,
    token_account_address: &Pubkey,
    token_out: u64,
    max_sol_cost: u64,
) -> Instruction {
    let data = [
        &[BUY],
        token_out.to_le_bytes().as_slice(),
        max_sol_cost.to_le_bytes().as_slice(),
    ]
    .concat();

    Instruction::new_with_bytes(
        PUMP_FUN_PROGRAM_ID,
        &data,
        build_keys(
            user_address,
            pump_fun_token_container,
            token_account_address,
        ),
    )
}

fn build_sell_instruction(
    user_address: &Pubkey,
    pump_fun_token_container: &PumpFunTokenContainer,
    token_account_address: &Pubkey,
    token_quantity: u64,
    min_sol_output: u64,
) -> Instruction {
    let data = [
        &[SELL],
        token_quantity.to_le_bytes().as_slice(),
        min_sol_output.to_le_bytes().as_slice(),
    ]
    .concat();

    Instruction::new_with_bytes(
        PUMP_FUN_PROGRAM_ID,
        &data,
        build_keys(
            user_address,
            pump_fun_token_container,
            token_account_address,
        ),
    )
}

fn build_keys(
    user_address: &Pubkey,
    pump_fun_token_container: &PumpFunTokenContainer,
    token_account_address: &Pubkey,
) -> Vec<AccountMeta> {
    let bonding_curve_pubkey = Pubkey::from_str(
        &pump_fun_token_container
            .pump_fun_coin_data
            .as_ref()
            .unwrap()
            .bonding_curve,
    )
    .expect("Failed to parse bonding curve pubkey");
    let associated_bonding_curve_pubkey = Pubkey::from_str(
        &pump_fun_token_container
            .pump_fun_coin_data
            .as_ref()
            .unwrap()
            .associated_bonding_curve,
    )
    .expect("Failed to parse associated bonding curve pubkey");

    vec![
        AccountMeta::new_readonly(GLOBAL, false),
        AccountMeta::new(FEE_RECIPIENT, false),
        AccountMeta::new_readonly(pump_fun_token_container.mint_address, false),
        AccountMeta::new(bonding_curve_pubkey, false),
        AccountMeta::new(associated_bonding_curve_pubkey, false),
        AccountMeta::new(*token_account_address, false),
        AccountMeta::new(*user_address, true),
        AccountMeta::new_readonly(SYSTEM_PROGRAM, false),
        AccountMeta::new_readonly(TOKEN_KEG_PROGRAM_ID, false),
        AccountMeta::new_readonly(solana_sdk::sysvar::rent::id(), false),
        AccountMeta::new_readonly(EVENT_AUTHORITY, false),
        AccountMeta::new_readonly(PUMP_FUN_PROGRAM_ID, false),
    ]
}

pub async fn execute_pump_fun_buy(
    rpc_client: &RpcClient,
    server_keypair: &Keypair,
    request: BuyRequest,
) -> Result<BuyResponse, AppError> {
    let token_address = Pubkey::from_str(&request.token_address)
        .map_err(|e| AppError::BadRequest(format!("Invalid token address: {}", e)))?;

    let coin_data = get_coin_data(&token_address).await?;

    let pump_fun_token_container = PumpFunTokenContainer {
        mint_address: token_address,
        pump_fun_coin_data: Some(coin_data),
        program_account_info: None,
    };

    let token_account_address = spl_associated_token_account::get_associated_token_address(
        &server_keypair.pubkey(),
        &token_address,
    );

    let token_account_container = TokenAccountOwnerContainer {
        owner_address: server_keypair.pubkey(),
        mint_address: token_address,
        token_account_address: Some(token_account_address),
    };

    let signature = buy(
        rpc_client,
        server_keypair,
        &token_account_container,
        &pump_fun_token_container,
        request.sol_quantity,
        0.01, // Default slippage of 1%
    )
    .await?;

    Ok(BuyResponse {
        success: true,
        signature: signature.clone(),
        solscan_tx_url: format!("https://solscan.io/tx/{}", signature),
        token_quantity: 0.0, // fix this later
        sol_spent: request.sol_quantity,
        error: None,
    })
}

pub async fn execute_pump_fun_sell(
    rpc_client: &RpcClient,
    server_keypair: &Keypair,
    request: SellRequest,
) -> Result<SellResponse, AppError> {
    let token_address = Pubkey::from_str(&request.token_address)
        .map_err(|e| AppError::BadRequest(format!("Invalid token address: {}", e)))?;

    let coin_data = get_coin_data(&token_address).await?;

    let pump_fun_token_container = PumpFunTokenContainer {
        mint_address: token_address,
        pump_fun_coin_data: Some(coin_data),
        program_account_info: None,
    };

    let token_account_address = spl_associated_token_account::get_associated_token_address(
        &server_keypair.pubkey(),
        &token_address,
    );

    let token_account_container = TokenAccountOwnerContainer {
        owner_address: server_keypair.pubkey(),
        mint_address: token_address,
        token_account_address: Some(token_account_address),
    };

    let signature = sell(
        rpc_client,
        server_keypair,
        &token_account_container,
        &pump_fun_token_container,
        (request.token_quantity * 1e9) as u64, // Convert to lamports
        0.01,                                  // Default slippage of 1%
    )
    .await?;

    Ok(SellResponse {
        success: true,
        signature: signature.clone(),
        token_quantity: request.token_quantity,
        sol_received: 0.0, // fix me
        solscan_tx_url: format!("https://solscan.io/tx/{}", signature),
        error: None,
    })
}
