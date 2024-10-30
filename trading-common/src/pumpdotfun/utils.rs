use anyhow::{Context, Result};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{pubkey::Pubkey, signature::Keypair, signer::Signer, transaction::Transaction};
use std::str::FromStr;

use super::types::{PumpFunCoinData, PumpFunTokenContainer};
use crate::error::AppError;

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

    let body_str = response.body_string().await?;
    let pump_fun_coin_data: PumpFunCoinData = serde_json::from_str(&body_str).map_err(|e| {
        AppError::JsonParseError(format!(
            "Failed to parse pump.fun response: {}. Raw response: {}",
            e, body_str
        ))
    })?;

    Ok(pump_fun_coin_data)
}

pub fn decode_bonding_curve_data(data: &[u8]) -> Result<(i64, i64)> {
    if data.len() < 24 {
        return Err(anyhow::anyhow!(
            "Insufficient data to decode bonding curve info"
        ));
    }

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

    let (virtual_token_reserves, virtual_sol_reserves) = decode_bonding_curve_data(&account_info)?;

    // Compare with pump.fun values
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
        "Chain values: {} {}, API values: {} {}",
        virtual_token_reserves,
        virtual_sol_reserves,
        pump_fun_virtual_token_reserves,
        pump_fun_virtual_sol_reserves
    );

    // Threshold check
    let within_threshold = (virtual_sol_reserves as f64
        * (1.0 - super::constants::BONDING_CURVE_MARGIN_OF_ERROR)
        < pump_fun_virtual_sol_reserves as f64)
        && (virtual_token_reserves as f64
            * (1.0 - super::constants::BONDING_CURVE_MARGIN_OF_ERROR)
            < pump_fun_virtual_token_reserves as f64);

    if !within_threshold {
        println!("Warning: Chain values differ significantly from API values");
    }

    Ok((virtual_token_reserves, virtual_sol_reserves))
}

pub async fn ensure_token_account(
    rpc_client: &RpcClient,
    payer: &Keypair,
    mint: &Pubkey,
    owner: &Pubkey,
) -> Result<Pubkey, AppError> {
    let token_account = spl_associated_token_account::get_associated_token_address(owner, mint);

    match rpc_client.get_account(&token_account) {
        Ok(_) => Ok(token_account),
        Err(_) => {
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
