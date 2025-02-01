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

        // Log potential scale factors and pool parameters
        tracing::debug!("Pool data analysis:");

        // Look for potential scale factors in first 100 bytes
        tracing::debug!("First 100 bytes analysis:");
        for i in (0..100).step_by(4) {
            if i + 4 <= data.len() {
                let value = u32::from_le_bytes(data[i..i + 4].try_into().unwrap());
                if value > 0 && value < 100 {
                    tracing::debug!("Potential scale/decimal factor at offset {}: {}", i, value);
                }
            }
        }

        // Look for potential price scale factors in specific ranges
        for i in (0..data.len()).step_by(8) {
            if i + 8 <= data.len() {
                let value = u64::from_le_bytes(data[i..i + 8].try_into().unwrap());
                if value > 0 && value < 1_000_000 {
                    // Potential scale factor range
                    tracing::debug!("Potential scale factor at offset {}: {} (u64)", i, value);
                }
            }
        }

        // Log the raw bytes around known offsets
        tracing::debug!("Bytes at base mint offset (424-464): {:?}", &data[424..464]);
        tracing::debug!(
            "Bytes at quote mint offset (368-400): {:?}",
            &data[368..400]
        );

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

        // Get base decimals from mint account
        let base_decimals = 6; // todo: FIX get from mint account

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
        sol_price_usd: f64,
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
                liquidity: Some(sol_amount * 2.0),
                liquidity_usd: None,
                market_cap,
                volume_24h: None,
                volume_6h: None,
                volume_1h: None,
                volume_5m: None,
            })
        } else {
            // For other tokens
            let base_amount = base_raw as f64;
            let quote_amount = quote_raw as f64;
            let base_decimals = if let Ok(mint_account) = rpc_client.get_account(&self.base_mint) {
                if let Ok(mint_data) = spl_token::state::Mint::unpack(&mint_account.data) {
                    tracing::info!("Base decimals: {}", mint_data.decimals);
                    mint_data.decimals
                } else {
                    tracing::error!("Failed to read mint data");
                    6 // Default to 6 if can't read
                }
            } else {
                tracing::error!("Failed to read mint account");
                6
            };

            // Calculate price by dividing raw amounts first
            let price_sol = if base_amount > 0.0 {
                let raw_price = base_amount / quote_amount;
                let decimal_adjustment =
                    10f64.powi(self.quote_decimals as i32 - base_decimals as i32 * 2);

                tracing::info!(
                    "Price calculation: raw_price={}, decimal_adjustment={}, final_price={}",
                    raw_price,
                    decimal_adjustment,
                    raw_price * decimal_adjustment
                );

                raw_price * decimal_adjustment
            } else {
                0.0
            };

            // Adjust liquidity by decimals
            let liquidity_sol = quote_amount / 10f64.powi(self.quote_decimals as i32);

            // Get total supply for market cap calculation
            let market_cap = if let Ok(mint_account) = rpc_client.get_account(&self.base_mint) {
                if let Ok(mint_data) = spl_token::state::Mint::unpack(&mint_account.data) {
                    let raw_supply = mint_data.supply as f64;
                    let real_supply = raw_supply / 10f64.powi(self.base_decimals as i32);

                    tracing::info!(
                        "Market cap detailed calculation:\n\
                         raw_supply={}\n\
                         base_decimals={}\n\
                         real_supply={}\n\
                         price_sol={}\n\
                         sol_price_usd={}\n\
                         raw_market_cap={}\n\
                         expected_market_cap=~120M\n\
                         decimal_adjusted_supply={}",
                        raw_supply,
                        self.base_decimals,
                        real_supply,
                        price_sol,
                        sol_price_usd,
                        real_supply * price_sol * sol_price_usd,
                        raw_supply / 10f64.powi(self.base_decimals as i32)
                    );

                    // Use decimal adjusted supply
                    (raw_supply / 10f64.powi(self.base_decimals as i32)) * price_sol * sol_price_usd
                } else {
                    0.0
                }
            } else {
                0.0
            };

            tracing::info!(
                "Raw calculation - Base amount: {}, Quote amount: {}, Real base: {}, Real quote: {}, Price: {}",
                base_amount,
                quote_amount,
                base_amount / 10f64.powi(self.base_decimals as i32),
                quote_amount / 10f64.powi(self.quote_decimals as i32),
                price_sol
            );

            Ok(PriceData {
                price_sol,
                price_usd: None,
                liquidity: Some(liquidity_sol),
                liquidity_usd: None,
                market_cap,
                volume_24h: None,
                volume_6h: None,
                volume_1h: None,
                volume_5m: None,
            })
        }
    }
}
