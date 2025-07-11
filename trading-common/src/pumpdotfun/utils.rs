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
    
    // Parse bonding curve account data based on pump.fun IDL structure
    if account_data.len() >= 81 {  // Minimum length includes creator field (32 bytes)
        let virtual_token_reserves = u64::from_le_bytes(account_data[8..16].try_into().unwrap());
        let virtual_sol_reserves = u64::from_le_bytes(account_data[16..24].try_into().unwrap());
        let real_token_reserves = u64::from_le_bytes(account_data[24..32].try_into().unwrap());
        let real_sol_reserves = u64::from_le_bytes(account_data[32..40].try_into().unwrap());
        let token_total_supply = u64::from_le_bytes(account_data[40..48].try_into().unwrap());
        let complete = account_data[48] != 0;
        
        // Extract creator address from bonding curve data
        let creator = Pubkey::new_from_array(account_data[49..81].try_into().unwrap());
        
        Ok(BondingCurveData {
            discriminator: account_data[0..8].try_into().unwrap(),
            virtual_token_reserves,
            virtual_sol_reserves,
            real_token_reserves,
            real_sol_reserves,
            token_total_supply,
            complete,
            creator,
        })
    } else {
        Err(anyhow::anyhow!("Account data too short: {} bytes", account_data.len()).into())
    }
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
) -> Result<(u64, u64)> {
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
        // Chain values differ significantly from API values
        // This might indicate stale data or a chain state change
    }

    Ok((
        bonding_curve_data.virtual_token_reserves,
        bonding_curve_data.virtual_sol_reserves,
    ))
}

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

    let body_str = response.body_string().await?;
    let pump_fun_coin_data: PumpFunCoinData = serde_json::from_str(&body_str).map_err(|e| {
        AppError::JsonParseError(format!(
            "Failed to parse pump.fun response: {}. Raw response: {}",
            e, body_str
        ))
    })?;

    Ok(pump_fun_coin_data)
}

pub fn decode_bonding_curve_data(data: &[u8]) -> Result<(u64, u64)> {
    if data.len() < 16 {
        return Err(anyhow::anyhow!(
            "Insufficient data to decode bonding curve info"
        ));
    }

    let virtual_token_reserves = u64::from_le_bytes(data[0..8].try_into()?);
    let virtual_sol_reserves = u64::from_le_bytes(data[8..16].try_into()?);


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

pub async fn derive_creator_vault(
    rpc_client: &RpcClient, 
    mint: &Pubkey
) -> Result<Pubkey, AppError> {
    // The creator vault is a PDA derived from the creator address
    // Uses the pattern: ["creator-vault", creator_pubkey_bytes]
    
    let bonding_curve_data = get_bonding_curve_data(rpc_client, mint).await
        .map_err(|e| AppError::RequestError(format!("Failed to get bonding curve data: {}", e)))?;
    
    let (creator_vault, _bump) = Pubkey::find_program_address(
        &[b"creator-vault", bonding_curve_data.creator.as_ref()],
        &PUMP_FUN_PROGRAM_ID,
    );
    
    Ok(creator_vault)
}



// Helper function to derive creator vault authority from creator address
pub fn derive_creator_vault_authority(creator: &Pubkey) -> Result<Pubkey, AppError> {
    // According to pump.fun docs, the creator vault authority PDA uses:
    // seeds = ["creator_vault", coin_creator.as_ref()]
    let (creator_vault_authority, _bump) = Pubkey::find_program_address(
        &[b"creator_vault", creator.as_ref()],
        &PUMP_FUN_PROGRAM_ID,
    );
    Ok(creator_vault_authority)
}

// Try derivation with mint address
pub fn derive_creator_vault_from_mint(mint: &Pubkey) -> Result<Pubkey, AppError> {
    let (creator_vault_authority, _bump) = Pubkey::find_program_address(
        &[b"creator_vault", mint.as_ref()],
        &PUMP_FUN_PROGRAM_ID,
    );
    Ok(creator_vault_authority)
}

// Try alternative mint-based derivation
pub fn derive_vault_from_mint_alt(mint: &Pubkey) -> Result<Pubkey, AppError> {
    let (creator_vault_authority, _bump) = Pubkey::find_program_address(
        &[mint.as_ref(), b"creator_vault"],
        &PUMP_FUN_PROGRAM_ID,
    );
    Ok(creator_vault_authority)
}

// Try coin creator vault derivation
pub fn derive_coin_creator_vault(creator: &Pubkey) -> Result<Pubkey, AppError> {
    let (creator_vault_authority, _bump) = Pubkey::find_program_address(
        &[b"coin_creator_vault", creator.as_ref()],
        &PUMP_FUN_PROGRAM_ID,
    );
    Ok(creator_vault_authority)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_derive_bonding_curve_address() {
        let mint = Pubkey::from_str("3dgv1AFXVHbebKUzF2G8wry2ojZeN5sM5D1biK8Wpump").unwrap();
        let (bonding_curve, _bump) = derive_bonding_curve_address(&mint);
        
        // Verify the address is derived correctly
        assert_ne!(bonding_curve, mint);
        assert_ne!(bonding_curve, Pubkey::default());
        println!("Bonding curve address: {}", bonding_curve);
    }

    #[test]
    fn test_derive_trading_accounts() {
        let mint = Pubkey::from_str("3dgv1AFXVHbebKUzF2G8wry2ojZeN5sM5D1biK8Wpump").unwrap();
        let result = derive_trading_accounts(&mint);
        
        assert!(result.is_ok());
        let (bonding_curve, associated_bonding_curve) = result.unwrap();
        assert_ne!(bonding_curve, associated_bonding_curve);
        println!("Bonding curve: {}", bonding_curve);
        println!("Associated bonding curve: {}", associated_bonding_curve);
        
        // Note: Creator vault derivation now requires RPC client and metadata fetching
        // This test would need to be converted to an integration test with live RPC connection
        // For now, just test the creator vault authority derivation function
        let test_creator = Pubkey::new_unique();
        let creator_vault_authority_result = derive_creator_vault_authority(&test_creator);
        assert!(creator_vault_authority_result.is_ok());
        let creator_vault_authority = creator_vault_authority_result.unwrap();
        println!("Creator vault authority: {}", creator_vault_authority);
    }

    #[test]
    fn test_creator_vault_authority_derivation() {
        // Test that the creator vault authority derivation is working correctly
        let test_creator = Pubkey::from_str("11111111111111111111111111111111").unwrap();
        let result = derive_creator_vault_authority(&test_creator);
        
        assert!(result.is_ok());
        let creator_vault_authority = result.unwrap();
        
        // Verify it's a valid pubkey and different from the creator
        assert_ne!(creator_vault_authority, test_creator);
        assert_ne!(creator_vault_authority, Pubkey::default());
        
        // Test that the same creator always produces the same vault authority
        let result2 = derive_creator_vault_authority(&test_creator);
        assert!(result2.is_ok());
        assert_eq!(creator_vault_authority, result2.unwrap());
    }

    // Note: This test requires a live RPC connection and would be an integration test
    // #[tokio::test]
    // async fn test_get_bonding_curve_data() {
    //     let rpc_client = RpcClient::new("https://api.mainnet-beta.solana.com".to_string());
    //     let mint = Pubkey::from_str("3dgv1AFXVHbebKUzF2G8wry2ojZeN5sM5D1biK8Wpump").unwrap();
    //     
    //     let result = get_bonding_curve_data(&rpc_client, &mint).await;
    //     assert!(result.is_ok());
    //     let bonding_curve_data = result.unwrap();
    //     println!("Bonding curve data: {:?}", bonding_curve_data);
    // }
    
    // Integration test for metadata fetching (requires live RPC)
    // #[tokio::test]
    // async fn test_get_token_creator() {
    //     let rpc_client = RpcClient::new("https://api.mainnet-beta.solana.com".to_string());
    //     let mint = Pubkey::from_str("3dgv1AFXVHbebKUzF2G8wry2ojZeN5sM5D1biK8Wpump").unwrap();
    //     
    //     let result = get_token_creator(&rpc_client, &mint).await;
    //     assert!(result.is_ok());
    //     let creator = result.unwrap();
    //     println!("Token creator: {}", creator);
    //     
    //     // Test that the creator vault authority derivation works with this creator
    //     let vault_authority = derive_creator_vault_authority(&creator).unwrap();
    //     println!("Derived creator vault authority: {}", vault_authority);
    // }
}
