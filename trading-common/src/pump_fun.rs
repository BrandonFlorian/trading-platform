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
use crate::utils::{confirm_transaction, get_token_balance};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::Keypair;
const UNIT_PRICE: u64 = 1_000;
const UNIT_BUDGET: u32 = 200_000;
const LAMPORTS_PER_SOL: u64 = 1_000_000_000;
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
    pub raydium_pool: Option<String>,
    pub complete: bool,
    pub virtual_sol_reserves: i64,
    pub virtual_token_reserves: i64,
    pub total_supply: i64,
    pub website: String,
    pub show_name: bool,
    pub king_of_the_hill_timestamp: Option<i64>,
    pub market_cap: f64,
    pub reply_count: i32,
    pub last_reply: i64,
    pub nsfw: bool,
    pub market_id: Option<String>,
    pub inverted: Option<String>,
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
pub const ASSOCIATED_TOKEN_PROGRAM_ID: Pubkey =
    solana_sdk::pubkey!("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");

#[derive(Debug)]
pub struct PumpFunCalcResult {
    pub token_out: u64,
    pub max_sol_cost: u64,
    pub price_per_token: f64,
    pub adjusted_max_token_output: f64,
    pub adjusted_min_token_output: f64,
}

impl PumpFunCalcResult {
    pub fn new(
        virtual_token_reserves: i64,
        virtual_sol_reserves: i64,
        sol_quantity: f64,
        slippage: f64,
        decimals: u8,
    ) -> Self {
        let vtokenr = virtual_token_reserves as f64;
        let vsolr = virtual_sol_reserves as f64;

        // Calculate expected output
        let sol_lamports = sol_quantity * LAMPORTS_PER_SOL as f64;
        let token_out = ((sol_lamports * vtokenr) / vsolr) as u64;

        // Calculate price per token in SOL (not lamports)
        let price_per_token = vsolr / (vtokenr * LAMPORTS_PER_SOL as f64);

        // Calculate max output with proper decimal handling
        let max_token_output = (token_out as f64) / 10f64.powi(decimals as i32);
        let min_token_output = max_token_output * (1.0 - slippage);

        // Max SOL cost with slippage
        let max_sol_cost = (sol_lamports * (1.0 + slippage)) as u64;

        println!("Price calculations:");
        println!("SOL input (lamports): {}", sol_lamports);
        println!("Token output (raw): {}", token_out);
        println!("Token output (decimal adjusted): {}", max_token_output);
        println!("Min token output (decimal adjusted): {}", min_token_output);
        println!("Max SOL cost (lamports): {}", max_sol_cost);

        Self {
            token_out,
            max_sol_cost,
            price_per_token,
            adjusted_max_token_output: max_token_output,
            adjusted_min_token_output: min_token_output,
        }
    }
}

pub async fn get_coin_data(token_address: &Pubkey) -> Result<PumpFunCoinData, AppError> {
    let url = format!("https://frontend-api.pump.fun/coins/{}", token_address);
    println!("url: {:?}", url);
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

    // Let's see the raw response body first
    let body_str = response.body_string().await?;

    // Now try to parse it
    let pump_fun_coin_data: PumpFunCoinData = serde_json::from_str(&body_str).map_err(|e| {
        AppError::JsonParseError(format!(
            "Failed to parse pump.fun response: {}. Raw response: {}",
            e, body_str
        ))
    })?;

    Ok(pump_fun_coin_data)
}

fn decode_bonding_curve_data(data: &[u8]) -> Result<(i64, i64)> {
    if data.len() < 24 {
        return Err(anyhow::anyhow!(
            "Insufficient data to decode bonding curve info"
        ));
    }

    // The values are stored as i64/u64, not f64
    let virtual_token_reserves = i64::from_le_bytes(data[8..16].try_into()?);
    let virtual_sol_reserves = i64::from_le_bytes(data[16..24].try_into()?);

    println!(
        "Raw decoded values: token_reserves={}, sol_reserves={}",
        virtual_token_reserves, virtual_sol_reserves
    );

    Ok((virtual_token_reserves, virtual_sol_reserves))
}

pub async fn get_bonding_curve_info(
    rpc_client: &RpcClient,
    pump_fun_token_container: &PumpFunTokenContainer,
) -> Result<(i64, i64), AppError> {
    // Changed return type to i64
    let bonding_curve_pubkey = Pubkey::from_str(
        &pump_fun_token_container
            .pump_fun_coin_data
            .as_ref()
            .unwrap()
            .bonding_curve,
    )
    .context("Failed to parse bonding curve pubkey")?;

    println!("Bonding curve pubkey: {}", bonding_curve_pubkey);
    let account_info = rpc_client
        .get_account_data(&bonding_curve_pubkey)
        .context("Failed to get account info")?;

    if account_info.is_empty() {
        return Err(AppError::BadRequest(
            "Account not found or no data available".to_string(),
        ));
    }

    println!("Account info: {:?}", account_info);
    let (virtual_token_reserves, virtual_sol_reserves) = decode_bonding_curve_data(&account_info)?;

    // Get the values from pump.fun for comparison
    let pump_fun_virtual_token_reserves = pump_fun_token_container
        .pump_fun_coin_data
        .as_ref()
        .unwrap()
        .virtual_token_reserves;
    let pump_fun_virtual_sol_reserves = pump_fun_token_container
        .pump_fun_coin_data
        .as_ref()
        .unwrap()
        .virtual_sol_reserves;

    println!(
        "Decoded values from chain: {} {}",
        virtual_token_reserves, virtual_sol_reserves
    );
    println!(
        "Values from API: {} {}",
        pump_fun_virtual_token_reserves, pump_fun_virtual_sol_reserves
    );

    // Check if values are within expected threshold
    let within_threshold = (virtual_sol_reserves as f64 * (1.0 - BONDING_CURVE_MARGIN_OF_ERROR)
        < pump_fun_virtual_sol_reserves as f64)
        && (virtual_token_reserves as f64 * (1.0 - BONDING_CURVE_MARGIN_OF_ERROR)
            < pump_fun_virtual_token_reserves as f64);

    if !within_threshold {
        println!("Warning: Chain values differ significantly from API values");
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

    // Calculate amounts (matching Python implementation)
    let sol_in_lamports = (sol_quantity * LAMPORTS_PER_SOL as f64) as u64;

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

    let amount = ((sol_in_lamports as f64 * virtual_token_reserves) / virtual_sol_reserves) as u64;
    let max_sol_cost = (sol_in_lamports as f64 * (1.0 + slippage)) as u64;

    println!(
        "Sol in (lamports): {}, Amount out: {}, Max cost: {}",
        sol_in_lamports, amount, max_sol_cost
    );

    let mut data = Vec::with_capacity(24);
    data.extend_from_slice(&[102, 6, 61, 18, 1, 218, 235, 234]); // "66063d1201daebea"
    data.extend_from_slice(&amount.to_le_bytes());
    data.extend_from_slice(&max_sol_cost.to_le_bytes());

    let accounts = build_pump_fun_accounts(
        &user_address,
        pump_fun_token_container,
        &token_account_container.token_account_address.unwrap(),
    );

    let instruction = Instruction::new_with_bytes(PUMP_FUN_PROGRAM_ID, &data, accounts);

    // Add compute budget instructions
    let compute_unit_price_ix = ComputeBudgetInstruction::set_compute_unit_price(1_000);
    let compute_unit_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(100_000);

    let recent_blockhash = rpc_client.get_latest_blockhash()?;

    let message = Message::new_with_blockhash(
        &[compute_unit_price_ix, compute_unit_limit_ix, instruction],
        Some(&user_address),
        &recent_blockhash,
    );

    let transaction = Transaction::new(&[secret_keypair], message, recent_blockhash);

    let signature = rpc_client.send_and_confirm_transaction(&transaction)?;
    println!("Signature: {}", signature);

    Ok(signature.to_string())
}

pub async fn sell(
    rpc_client: &RpcClient,
    secret_keypair: &impl Signer,
    token_account_container: &TokenAccountOwnerContainer,
    pump_fun_token_container: &PumpFunTokenContainer,
    token_quantity: f64,
    slippage: f64,
) -> Result<String, AppError> {
    let user_address = secret_keypair.pubkey();

    // Python implementation uses these exact decimal values
    let sol_decimal = 1_000_000_000f64; // 1e9
    let token_decimal = 1_000_000f64; // 1e6 for calculation

    let virtual_sol_reserves = pump_fun_token_container
        .pump_fun_coin_data
        .as_ref()
        .unwrap()
        .virtual_sol_reserves as f64
        / sol_decimal;

    let virtual_token_reserves = pump_fun_token_container
        .pump_fun_coin_data
        .as_ref()
        .unwrap()
        .virtual_token_reserves as f64
        / token_decimal;

    // Calculate price per token (matching Python exactly)
    let token_price = virtual_sol_reserves / virtual_token_reserves;
    println!("Token Price: {:.20} SOL", token_price);

    // Calculate raw amount (needs to be in token decimals)
    let amount = (token_quantity * token_decimal) as u64;

    // Calculate SOL output (matching Python)
    let sol_out = token_quantity * token_price;
    let sol_out_with_slippage = sol_out * (1.0 - slippage);
    let min_sol_output = (sol_out_with_slippage * sol_decimal) as u64;

    println!("Sell calculation:");
    println!("Token quantity: {}", token_quantity);
    println!("Amount (raw): {}", amount);
    println!("Expected SOL output: {}", sol_out);
    println!("Min SOL output (with slippage): {}", sol_out_with_slippage);
    println!("Min SOL output (lamports): {}", min_sol_output);

    // Build instruction data
    let mut data = Vec::with_capacity(24);
    data.extend_from_slice(&[51, 230, 133, 164, 1, 127, 131, 173]); // "33e685a4017f83ad"
    data.extend_from_slice(&amount.to_le_bytes());
    data.extend_from_slice(&min_sol_output.to_le_bytes());

    let accounts = build_pump_fun_accounts(
        &user_address,
        pump_fun_token_container,
        &token_account_container.token_account_address.unwrap(),
    );

    let instruction = Instruction::new_with_bytes(PUMP_FUN_PROGRAM_ID, &data, accounts);

    // Add compute budget instructions
    let compute_unit_price_ix = ComputeBudgetInstruction::set_compute_unit_price(1_000);
    let compute_unit_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(100_000);

    let recent_blockhash = rpc_client.get_latest_blockhash()?;

    let message = Message::new_with_blockhash(
        &[compute_unit_price_ix, compute_unit_limit_ix, instruction],
        Some(&user_address),
        &recent_blockhash,
    );

    let transaction = Transaction::new(&[secret_keypair], message, recent_blockhash);
    println!("Sending sell transaction...");

    let signature = rpc_client.send_transaction(&transaction).map_err(|e| {
        println!("Failed to send transaction: {}", e);
        AppError::SolanaRpcError(e)
    })?;

    println!("Transaction sent successfully!");
    println!("Signature: {}", signature);
    println!("Beginning confirmation process...");
    println!("Signature: {}", signature);

    // Confirm transaction with retries
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

pub async fn process_buy_request(
    rpc_client: &RpcClient,
    server_keypair: &Keypair,
    request: BuyRequest,
) -> Result<BuyResponse, AppError> {
    println!("Processing buy request");
    // Convert request into needed container formats
    let token_address = Pubkey::from_str(&request.token_address)
        .map_err(|e| AppError::BadRequest(format!("Invalid token address: {}", e)))?;
    println!("Token address: {:?}", token_address);
    // Get coin data and create container
    let coin_data = get_coin_data(&token_address).await?;
    println!("Coin data: {:?}", coin_data);
    let pump_fun_token_container = PumpFunTokenContainer {
        mint_address: token_address,
        pump_fun_coin_data: Some(coin_data),
        program_account_info: None,
    };
    println!("Pump fun token container: {:?}", pump_fun_token_container);
    // Ensure token account exists
    let token_account = ensure_token_account(
        rpc_client,
        server_keypair,
        &token_address,
        &server_keypair.pubkey(),
    )
    .await?;
    println!("Token account: {:?}", token_account);
    let token_account_container = TokenAccountOwnerContainer {
        owner_address: server_keypair.pubkey(),
        mint_address: token_address,
        token_account_address: Some(token_account),
    };
    println!("Token account container: {:?}", token_account_container);
    // Execute the buy
    let signature = buy(
        rpc_client,
        server_keypair,
        &token_account_container,
        &pump_fun_token_container,
        request.sol_quantity,
        request.slippage_tolerance,
    )
    .await?;
    println!("Signature: {:?}", signature);
    // Calculate final amounts for response
    let calcs = PumpFunCalcResult::new(
        pump_fun_token_container
            .pump_fun_coin_data
            .as_ref()
            .unwrap()
            .virtual_token_reserves,
        pump_fun_token_container
            .pump_fun_coin_data
            .as_ref()
            .unwrap()
            .virtual_sol_reserves,
        request.sol_quantity,
        request.slippage_tolerance,
        9, // decimals
    );
    println!("Calcs: {:?}", calcs);
    Ok(BuyResponse {
        success: true,
        signature: signature.to_string(),
        solscan_tx_url: format!("https://solscan.io/tx/{}", signature),
        token_quantity: calcs.adjusted_max_token_output,
        sol_spent: request.sol_quantity,
        error: None,
    })
}

pub async fn process_sell_request(
    rpc_client: &RpcClient,
    server_keypair: &Keypair,
    request: SellRequest,
) -> Result<SellResponse, AppError> {
    println!("Processing sell request: {:?}", request);

    let token_address = Pubkey::from_str(&request.token_address)
        .map_err(|e| AppError::BadRequest(format!("Invalid token address: {}", e)))?;

    // Get coin data and create container
    let coin_data = get_coin_data(&token_address).await?;
    let pump_fun_token_container = PumpFunTokenContainer {
        mint_address: token_address,
        pump_fun_coin_data: Some(coin_data),
        program_account_info: None,
    };

    // Ensure token account exists
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

    // Check token balance before selling
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

    // Calculate SOL received for response
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

pub const PUMP_FUN_ACCOUNTS_ORDER: [&str; 12] = [
    "global",
    "feeRecipient",
    "mint",
    "bondingCurve",
    "associatedBondingCurve",
    "tokenAccount",
    "user",
    "systemProgram",
    "tokenProgram",
    "rent",
    "eventAuthority",
    "programId",
];
fn build_pump_fun_accounts(
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

    // Note: Token program order changes for sell
    vec![
        AccountMeta::new_readonly(GLOBAL, false),
        AccountMeta::new(FEE_RECIPIENT, false),
        AccountMeta::new_readonly(pump_fun_token_container.mint_address, false),
        AccountMeta::new(bonding_curve_pubkey, false),
        AccountMeta::new(associated_bonding_curve_pubkey, false),
        AccountMeta::new(*token_account_address, false),
        AccountMeta::new(*user_address, true),
        AccountMeta::new_readonly(SYSTEM_PROGRAM, false),
        AccountMeta::new_readonly(ASSOCIATED_TOKEN_PROGRAM_ID, false), // Changed for sell
        AccountMeta::new_readonly(TOKEN_KEG_PROGRAM_ID, false),
        AccountMeta::new_readonly(EVENT_AUTHORITY, false),
        AccountMeta::new_readonly(PUMP_FUN_PROGRAM_ID, false),
    ]
}

const BUY_DISCRIMINATOR: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234]; // "66063d1201daebea"
const SELL_DISCRIMINATOR: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173]; // "33e685a4017f83ad"

fn create_instruction_data(instruction_type: u8, amount1: u64, amount2: u64) -> Vec<u8> {
    let mut data = Vec::with_capacity(24);

    let discriminator = if instruction_type == BUY {
        BUY_DISCRIMINATOR
    } else {
        SELL_DISCRIMINATOR
    };
    data.extend_from_slice(&discriminator);

    // Add the amounts
    data.extend_from_slice(&amount1.to_le_bytes());
    data.extend_from_slice(&amount2.to_le_bytes());

    println!("Created instruction data (hex): {:02x?}", data);
    data
}

async fn ensure_token_account(
    rpc_client: &RpcClient,
    payer: &Keypair,
    mint: &Pubkey,
    owner: &Pubkey,
) -> Result<Pubkey, AppError> {
    let token_account = spl_associated_token_account::get_associated_token_address(owner, mint);

    // Check if account exists
    match rpc_client.get_account(&token_account) {
        Ok(_) => Ok(token_account),
        Err(_) => {
            // Create ATA instruction
            let create_ata_ix =
                spl_associated_token_account::instruction::create_associated_token_account(
                    &payer.pubkey(),
                    owner,
                    mint,
                    &spl_token::id(),
                );

            let recent_blockhash = rpc_client.get_latest_blockhash()?;
            let create_ata_tx = Transaction::new_signed_with_payer(
                &[create_ata_ix],
                Some(&payer.pubkey()),
                &[payer],
                recent_blockhash,
            );

            rpc_client.send_and_confirm_transaction(&create_ata_tx)?;
            Ok(token_account)
        }
    }
}
