use solana_client::rpc_client::RpcClient;
use solana_sdk::{borsh, program_pack::Pack, pubkey::Pubkey};
use std::str::FromStr;
use trading_common::error::AppError;

const USDC_SOL_POOL: &str = "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2";
const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[derive(Debug)]
pub struct UsdcSolPool {
    pub address: Pubkey,
    pub usdc_vault: Pubkey,
    pub sol_vault: Pubkey,
    pub usdc_mint: Pubkey,
    pub sol_mint: Pubkey,
}

impl UsdcSolPool {
    pub fn new() -> Result<Self, AppError> {
        Ok(Self {
            address: Pubkey::from_str(USDC_SOL_POOL)?,
            usdc_mint: Pubkey::from_str(USDC_MINT)?,
            sol_mint: Pubkey::from_str(WSOL_MINT)?,
            usdc_vault: Pubkey::default(), // Will be populated from account data
            sol_vault: Pubkey::default(),  // Will be populated from account data
        })
    }

    pub fn from_account_data(data: &[u8]) -> Result<Self, AppError> {
        if data.len() != 752 {
            return Err(AppError::SerializationError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid data length: {}, expected 752", data.len()),
            )));
        }

        // Skip 8 byte discriminator
        let data = &data[8..];

        // Extract vault addresses
        let sol_vault = {
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&data[336 - 8..368 - 8]);
            Pubkey::new_from_array(bytes)
        };

        let usdc_vault = {
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&data[368 - 8..400 - 8]);
            Pubkey::new_from_array(bytes)
        };

        Ok(Self {
            address: Pubkey::from_str(USDC_SOL_POOL)?,
            usdc_mint: Pubkey::from_str(USDC_MINT)?,
            sol_mint: Pubkey::from_str(WSOL_MINT)?,
            usdc_vault,
            sol_vault,
        })
    }

    pub async fn get_price(&self, rpc_client: &RpcClient) -> Result<f64, AppError> {
        // Get vault balances
        let sol_balance = rpc_client
            .get_token_account_balance(&self.sol_vault)
            .map_err(|e| AppError::SolanaRpcError { source: e })?;

        let usdc_balance = rpc_client
            .get_token_account_balance(&self.usdc_vault)
            .map_err(|e| AppError::SolanaRpcError { source: e })?;

        // Convert amounts
        let sol_amount = sol_balance
            .amount
            .parse::<u64>()
            .map_err(|_| AppError::PriceNotAvailable("Invalid SOL balance".to_string()))?
            as f64
            / 1e9; // SOL has 9 decimals

        let usdc_amount = usdc_balance
            .amount
            .parse::<u64>()
            .map_err(|_| AppError::PriceNotAvailable("Invalid USDC balance".to_string()))?
            as f64
            / 1e6; // USDC has 6 decimals

        if sol_amount == 0.0 {
            return Err(AppError::PriceNotAvailable(
                "Zero SOL liquidity".to_string(),
            ));
        }

        // Calculate price (USDC per SOL)
        Ok(usdc_amount / sol_amount)
    }
}
