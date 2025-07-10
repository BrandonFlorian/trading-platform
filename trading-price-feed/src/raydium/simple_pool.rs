use solana_sdk::{pubkey::Pubkey, program_pack::Pack};
use std::str::FromStr;
use trading_common::{error::AppError, redis::RedisPool};
use solana_client::rpc_client::RpcClient;
use std::sync::Arc;
use std::time::Duration;


/// Simplified Raydium pool structure for vault monitoring
#[derive(Debug, Clone)]
pub struct SimpleRaydiumPool {
    pub address: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub base_vault: Pubkey,
    pub quote_vault: Pubkey,
    pub base_decimals: u8,
    pub quote_decimals: u8,
}

impl SimpleRaydiumPool {
    pub fn from_account_data(address: &Pubkey, data: &[u8]) -> Result<Self, AppError> {
        // Validate data length
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

        // Base and quote vaults - NOTE: Swapped based on actual vault contents
        let quote_vault = {
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&data[336 - 8..368 - 8]); // Adjust for discriminator - this actually contains SOL
            Pubkey::new_from_array(bytes)
        };

        let base_vault = {
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&data[368 - 8..400 - 8]); // Adjust for discriminator - this actually contains the token
            Pubkey::new_from_array(bytes)
        };

        tracing::debug!(
            "Parsed Raydium pool: base_mint={}, quote_mint={}, base_vault={}, quote_vault={}",
            base_mint,
            quote_mint,
            base_vault,
            quote_vault
        );

        Ok(Self {
            address: *address,
            base_mint,
            quote_mint,
            base_vault,
            quote_vault,
            base_decimals: 0,  // Will be loaded by load_metadata
            quote_decimals: 9, // SOL always has 9 decimals
        })
    }

    pub async fn load_metadata(
        &mut self,
        rpc_client: &Arc<RpcClient>,
        redis_connection: &Arc<RedisPool>,
    ) -> Result<(), AppError> {
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
}