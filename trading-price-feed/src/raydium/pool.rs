use borsh::{BorshDeserialize, BorshSerialize};
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::{str::FromStr, sync::Arc};
use trading_common::error::AppError;

use super::{calculate_liquidity, calculate_price, types::PoolState, PriceData};

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

        tracing::info!(
            "Raw base balance: {}, decimals: {}",
            base_balance.amount,
            self.base_decimals
        );
        tracing::info!(
            "Raw quote balance: {}, decimals: {}",
            quote_balance.amount,
            self.quote_decimals
        );

        let price = calculate_price(
            base_balance.amount.parse().unwrap_or(0),
            self.base_decimals,
            quote_balance.amount.parse().unwrap_or(0),
            self.quote_decimals,
        );

        tracing::info!("Calculated price: {}", price);

        let liquidity = calculate_liquidity(
            base_balance.amount.parse().unwrap_or(0),
            self.base_decimals,
            quote_balance.amount.parse().unwrap_or(0),
            self.quote_decimals,
        );

        tracing::info!("Calculated liquidity: {}", liquidity);

        Ok(PriceData {
            price_sol: price,
            liquidity,
            market_cap: 0.0,
            volume_24h: None,
            volume_6h: None,
            volume_1h: None,
            volume_5m: None,
        })
    }

    pub fn to_debug_string(&self) -> String {
        format!(
            "Pool {} - Token Mint: {}, Base Vault: {}, Quote Vault: {}, Base Decimals: {}, Quote Decimals: {}",
            self.address,
            self.base_mint,
            self.base_vault,
            self.quote_vault,
            self.base_decimals,
            self.quote_decimals
        )
    }
}
