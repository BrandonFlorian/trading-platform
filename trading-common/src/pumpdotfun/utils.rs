use anyhow::{Context, Result};
use borsh::BorshDeserialize;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{pubkey::Pubkey, signature::Keypair, signer::Signer, transaction::Transaction};
use std::{str::FromStr, sync::Arc};

use super::{
    types::{PumpFunCoinData, PumpFunTokenContainer},
    BondingCurveData, BONDING_CURVE_MARGIN_OF_ERROR, PUMP_FUN_PROGRAM_ID,
};
use crate::error::AppError;

pub async fn get_bonding_curve_data(
    rpc_client: &RpcClient,
    mint: &Pubkey,
) -> Result<BondingCurveData> {
    let (bonding_curve, _) = derive_bonding_curve_address(mint);
    let account_data = rpc_client.get_account_data(&bonding_curve)?;
    BondingCurveData::try_from_slice(&account_data).map_err(|e| e.into())
}

pub fn derive_bonding_curve_address(mint: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], &PUMP_FUN_PROGRAM_ID)
}

pub fn derive_trading_accounts(mint: &Pubkey) -> Result<(Pubkey, Pubkey)> {
    let (bonding_curve, _) = derive_bonding_curve_address(mint);
    let associated_bonding_curve =
        spl_associated_token_account::get_associated_token_address(&bonding_curve, mint);

    Ok((bonding_curve, associated_bonding_curve))
}

pub async fn get_bonding_curve_info(
    rpc_client: &RpcClient,
    pump_fun_token_container: &PumpFunTokenContainer,
) -> Result<(i64, i64)> {
    let coin_data = pump_fun_token_container
        .pump_fun_coin_data
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing coin data"))?;

    let bonding_curve_pubkey = Pubkey::from_str(&coin_data.bonding_curve)?;
    let account_data = rpc_client.get_account_data(&bonding_curve_pubkey)?;

    let bonding_curve_data = BondingCurveData::try_from_slice(&account_data)?;

    // Compare with API values and check threshold
    let within_threshold = (bonding_curve_data.virtual_sol_reserves as f64
        * (1.0 - BONDING_CURVE_MARGIN_OF_ERROR)
        < coin_data.virtual_sol_reserves as f64)
        && (bonding_curve_data.virtual_token_reserves as f64
            * (1.0 - BONDING_CURVE_MARGIN_OF_ERROR)
            < coin_data.virtual_token_reserves as f64);

    if !within_threshold {
        println!("Warning: Chain values differ significantly from API values");
    }

    Ok((
        bonding_curve_data.virtual_token_reserves,
        bonding_curve_data.virtual_sol_reserves,
    ))
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

// pub async fn get_bonding_curve_data(
//     rpc_client: &Arc<RpcClient>,
//     mint: &Pubkey,
// ) -> Result<BondingCurveData> {
//     let (bonding_curve, _) =
//         Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], &PUMP_FUN_PROGRAM_ID);

//     let account_data = rpc_client.get_account_data(&bonding_curve)?;
//     let curve_data = BondingCurveData::try_from_slice(&account_data)?;

//     Ok(curve_data)
// }

// Helper function to derive necessary accounts for trading
// pub fn derive_trading_accounts(mint: &Pubkey) -> Result<(Pubkey, Pubkey)> {
//     let (bonding_curve, _) =
//         Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], &PUMP_FUN_PROGRAM_ID);

//     let associated_bonding_curve =
//         spl_associated_token_account::get_associated_token_address(&bonding_curve, mint);

//     Ok((bonding_curve, associated_bonding_curve))
// }

// pub async fn get_bonding_curve_info(
//     rpc_client: &RpcClient,
//     pump_fun_token_container: &PumpFunTokenContainer,
// ) -> Result<(i64, i64), AppError> {
//     let bonding_curve_pubkey = Pubkey::from_str(
//         &pump_fun_token_container
//             .pump_fun_coin_data
//             .as_ref()
//             .unwrap()
//             .bonding_curve,
//     )
//     .context("Failed to parse bonding curve pubkey")?;

//     println!("Bonding curve pubkey: {}", bonding_curve_pubkey);
//     let account_info = rpc_client
//         .get_account_data(&bonding_curve_pubkey)
//         .context("Failed to get account info")?;

//     if account_info.is_empty() {
//         return Err(AppError::BadRequest(
//             "Account not found or no data available".to_string(),
//         ));
//     }

//     let (virtual_token_reserves, virtual_sol_reserves) = decode_bonding_curve_data(&account_info)?;

//     // Compare with pump.fun values
//     let pump_fun_virtual_token_reserves = pump_fun_token_container
//         .pump_fun_coin_data
//         .as_ref()
//         .unwrap()
//         .virtual_token_reserves;
//     let pump_fun_virtual_sol_reserves = pump_fun_token_container
//         .pump_fun_coin_data
//         .as_ref()
//         .unwrap()
//         .virtual_sol_reserves;

//     println!(
//         "Chain values: {} {}, API values: {} {}",
//         virtual_token_reserves,
//         virtual_sol_reserves,
//         pump_fun_virtual_token_reserves,
//         pump_fun_virtual_sol_reserves
//     );

//     // Threshold check
//     let within_threshold = (virtual_sol_reserves as f64
//         * (1.0 - super::constants::BONDING_CURVE_MARGIN_OF_ERROR)
//         < pump_fun_virtual_sol_reserves as f64)
//         && (virtual_token_reserves as f64
//             * (1.0 - super::constants::BONDING_CURVE_MARGIN_OF_ERROR)
//             < pump_fun_virtual_token_reserves as f64);

//     if !within_threshold {
//         println!("Warning: Chain values differ significantly from API values");
//     }

//     Ok((virtual_token_reserves, virtual_sol_reserves))
// }

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
