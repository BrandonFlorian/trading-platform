use anyhow::Result;
use borsh::BorshDeserialize;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{pubkey::Pubkey, signature::Keypair, signer::Signer, transaction::Transaction};
use solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta;
use std::str::FromStr;

use super::{
    types::{PumpFunCoinData, PumpFunTokenContainer},
    BondingCurveData, BONDING_CURVE_MARGIN_OF_ERROR, PUMP_FUN_PROGRAM_ID,
};
use crate::{data::get_account_keys_from_message, error::AppError};

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

pub fn get_transaction_accounts(
    transaction: &EncodedConfirmedTransactionWithStatusMeta,
) -> Option<(String, String)> {
    match &transaction.transaction.transaction {
        solana_transaction_status::EncodedTransaction::Json(tx) => {
            let account_keys = get_account_keys_from_message(&tx.message);
            Some((
                account_keys.first().cloned().unwrap_or_default(),
                account_keys.get(1).cloned().unwrap_or_default(),
            ))
        }
        _ => None,
    }
}
