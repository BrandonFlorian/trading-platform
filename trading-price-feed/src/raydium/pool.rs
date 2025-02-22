use super::PriceData;
use parking_lot::RwLock;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{program_pack::Pack, pubkey::Pubkey};
use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use trading_common::{error::AppError, redis::RedisPool};

#[derive(Debug)]
pub struct AccountData {
    pub base_balance: u64,
    pub quote_balance: u64,
    pub last_updated: Instant,
}

#[derive(Debug, Clone)]
pub struct RaydiumPool {
    pub address: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub base_vault: Pubkey,
    pub quote_vault: Pubkey,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    pub account_data: Arc<RwLock<Option<AccountData>>>,
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

        println!("Base mint: {}", base_mint);
        println!("Quote mint: {}", quote_mint);
        println!("Base vault: {}", base_vault);
        println!("Quote vault: {}", quote_vault);

        Ok(Self {
            address: *address,
            base_mint,
            quote_mint,
            base_vault,
            quote_vault,
            base_decimals: 0,  // Will be loaded by load_metadata
            quote_decimals: 9, // SOL always has 9 decimals
            account_data: Arc::new(RwLock::new(None)),
        })
    }

    pub async fn load_metadata(
        &mut self,
        rpc_client: &Arc<RpcClient>,
        redis_connection: &Arc<RedisPool>,
    ) -> Result<(), AppError> {
        tracing::info!("Loading metadata for pool: {}", self.address);
        // Try cache first
        let decimals = redis_connection.get_token_decimals(&self.base_mint).await?;
        if let Some(decimals) = decimals {
            self.base_decimals = decimals;
            return Ok(());
        }

        // Fallback to RPC
        let mint_account = rpc_client.get_account(&self.base_mint)?;
        let mint_data = spl_token::state::Mint::unpack(&mint_account.data)?;
        self.base_decimals = mint_data.decimals;

        // Cache the result
        redis_connection
            .set_token_decimals(
                &self.base_mint,
                self.base_decimals,
                Duration::from_secs(3600), // 1 hour TTL
            )
            .await?;

        Ok(())
    }

    pub async fn update_from_websocket_data(&self, data: &[u8]) -> Result<(), AppError> {
        tracing::info!("Updating pool from websocket data");

        // Skip 8 byte discriminator
        if data.len() < 8 + 752 {
            return Err(AppError::SerializationError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid data length: {}, expected at least 760", data.len()),
            )));
        }
        let data = &data[8..];

        // Extract vault balances and update account data
        let base_vault = {
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&data[328..360]);
            Pubkey::new_from_array(bytes)
        };

        let quote_vault = {
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&data[360..392]);
            Pubkey::new_from_array(bytes)
        };

        // Verify vaults match
        if base_vault != self.base_vault || quote_vault != self.quote_vault {
            return Err(AppError::WebSocketError(
                "Vault addresses don't match pool data".to_string(),
            ));
        }

        // Update account data with new balances
        let mut account_data = self.account_data.write();
        *account_data = Some(AccountData {
            base_balance: u64::from_le_bytes(data[0..8].try_into().map_err(
                |e: std::array::TryFromSliceError| {
                    AppError::SerializationError(borsh::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e.to_string(),
                    ))
                },
            )?),
            quote_balance: u64::from_le_bytes(data[8..16].try_into().map_err(
                |e: std::array::TryFromSliceError| {
                    AppError::SerializationError(borsh::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e.to_string(),
                    ))
                },
            )?),
            last_updated: Instant::now(),
        });

        tracing::info!(
            "Updated pool data for {}\nBase vault: {}\nQuote vault: {}",
            self.address,
            base_vault,
            quote_vault
        );

        Ok(())
    }

    pub async fn fetch_price_data(
        &self,
        rpc_client: &Arc<RpcClient>,
        sol_price_usd: f64,
    ) -> Result<PriceData, AppError> {
        tracing::info!("Fetching price data for pool: {}", self.address);
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
