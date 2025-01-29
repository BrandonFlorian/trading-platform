use solana_client::rpc_client::RpcClient;
use solana_sdk::{program_pack::Pack, pubkey::Pubkey};
use std::{str::FromStr, sync::Arc};
use trading_common::error::AppError;

use super::PriceData;

#[derive(Debug, Clone)]
pub struct RaydiumPool {
    pub address: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub base_vault: Pubkey,
    pub quote_vault: Pubkey,
    pub base_decimals: u8,
    pub quote_decimals: u8,
}

impl RaydiumPool {
    pub fn from_account_data(address: &Pubkey, data: &[u8]) -> Result<Self, AppError> {
        tracing::debug!("Attempting to parse pool data of length: {}", data.len());

        // First validate data length
        if data.len() != 752 {
            return Err(AppError::SerializationError(borsh::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid data length: {}, expected 752", data.len()),
            )));
        }

        // Skip 8 byte discriminator
        let data = &data[8..];

        // KENJ token is at offset 432
        let base_mint = {
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&data[432 - 8..464 - 8]); // Subtract 8 for discriminator
            Pubkey::new_from_array(bytes)
        };

        // SOL token at offset 400
        let quote_mint = Pubkey::from_str("So11111111111111111111111111111111111111112")?;

        // Base and quote vaults
        let base_vault = {
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&data[336 - 8..368 - 8]); // Adjust for discriminator
            Pubkey::new_from_array(bytes)
        };

        let quote_vault = {
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&data[368 - 8..400 - 8]); // Adjust for discriminator
            Pubkey::new_from_array(bytes)
        };

        // Decimals should be in first part
        let base_decimals = u64::from_le_bytes(data[40..48].try_into().unwrap()) as u8;
        let quote_decimals = 9; // SOL always has 9 decimals

        tracing::info!("Parsed base mint: {}", base_mint);
        tracing::info!("Parsed quote mint: {}", quote_mint);
        tracing::info!("Base decimals: {}", base_decimals);
        tracing::info!("Quote decimals: {}", quote_decimals);
        tracing::info!("Base vault: {}", base_vault);
        tracing::info!("Quote vault: {}", quote_vault);

        Ok(Self {
            address: *address,
            base_mint,
            quote_mint,
            base_vault,
            quote_vault,
            base_decimals,
            quote_decimals,
        })
    }

    pub async fn fetch_price_data(
        &self,
        rpc_client: &Arc<RpcClient>,
    ) -> Result<PriceData, AppError> {
        // Get token account balances
        let base_balance = rpc_client
            .get_token_account_balance(&self.base_vault)
            .map_err(|e| AppError::SolanaRpcError { source: e })?;

        let quote_balance = rpc_client
            .get_token_account_balance(&self.quote_vault)
            .map_err(|e| AppError::SolanaRpcError { source: e })?;

        // Convert string amounts to u64 before decimal adjustment
        let base_raw = base_balance.amount.parse::<u64>().unwrap_or(0);
        let quote_raw = quote_balance.amount.parse::<u64>().unwrap_or(0);

        // Check if this is the USDC/SOL pool
        let is_usdc_pool = self.base_mint
            == Pubkey::from_str("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v").unwrap();

        if is_usdc_pool {
            // For USDC/SOL pool
            let sol_amount = base_raw as f64 / 10f64.powi(9); // SOL has 9 decimals
            let usdc_amount = quote_raw as f64 / 10f64.powi(6); // USDC has 6 decimals

            let price_sol = if sol_amount > 0.0 {
                usdc_amount / sol_amount // This gives us USDC/SOL price
            } else {
                0.0
            };

            tracing::info!(
                "Raw calculation - USDC amount: {}, SOL amount: {}",
                usdc_amount,
                sol_amount
            );

            // Calculate market cap (not relevant for USDC/SOL pool)
            let market_cap = 0.0;

            Ok(PriceData {
                price_sol,
                price_usd: Some(price_sol), // USDC price is the USD price
                liquidity: sol_amount * 2.0,
                liquidity_usd: None,
                market_cap,
                volume_24h: None,
                volume_6h: None,
                volume_1h: None,
                volume_5m: None,
            })
        } else {
            // For other tokens
            let base_amount = base_raw as f64 / 10f64.powi(self.base_decimals as i32);
            let quote_amount = quote_raw as f64 / 10f64.powi(self.quote_decimals as i32);

            let price_sol = if base_amount > 0.0 {
                quote_amount / base_amount * 1000.0
            } else {
                0.0
            };

            let liquidity_sol = quote_amount * 0.2; // Changed from 2.0 to 0.2 to match Photon's calculation

            // Get total supply for market cap calculation
            let market_cap = if let Ok(mint_account) = rpc_client.get_account(&self.base_mint) {
                if let Ok(mint_data) = spl_token::state::Mint::unpack(&mint_account.data) {
                    let total_supply =
                        mint_data.supply as f64 / 10f64.powi(mint_data.decimals as i32);
                    total_supply * price_sol // This will be in SOL terms
                } else {
                    0.0
                }
            } else {
                0.0
            };

            tracing::info!(
                "Raw calculation - Base amount: {}, Quote amount: {}",
                base_amount,
                quote_amount
            );

            Ok(PriceData {
                price_sol,
                price_usd: None,
                liquidity: liquidity_sol,
                liquidity_usd: None,
                market_cap,
                volume_24h: None,
                volume_6h: None,
                volume_1h: None,
                volume_5m: None,
            })
        }
    }

    pub fn is_usdc_pool(&self) -> bool {
        self.base_mint == Pubkey::from_str("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v").unwrap()
    }
}
