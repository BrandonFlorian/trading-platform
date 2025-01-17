use anyhow::Result;
use serde::{Deserialize, Serialize};
use solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta;

use crate::{
    pumpdotfun::{self},
    raydium, TransactionType,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DexType {
    PumpFun,
    Raydium,
    Unknown,
}

impl std::fmt::Display for DexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DexType::PumpFun => write!(f, "pump_fun"),
            DexType::Raydium => write!(f, "raydium"),
            DexType::Unknown => write!(f, "unknown"),
        }
    }
}

#[derive(Debug)]
pub struct DexTransaction {
    pub dex_type: DexType,
    pub transaction_type: TransactionType,
    pub token_address: String,
    pub amount_token: f64,
    pub amount_sol: f64,
    pub price_per_token: f64,
}

impl DexTransaction {
    pub fn detect_dex_type(transaction: &EncodedConfirmedTransactionWithStatusMeta) -> DexType {
        if let Some(meta) = &transaction.transaction.meta {
            let empty_logs = Vec::new();
            let logs = meta.log_messages.as_ref().unwrap_or(&empty_logs);

            println!("Checking DEX type...");

            // Check for Pump.fun signatures
            let is_pump_fun = logs
                .iter()
                .any(|log| log.contains(&pumpdotfun::constants::PUMP_FUN_PROGRAM_ID.to_string()));
            if is_pump_fun {
                println!("Detected Pump.fun transaction");
                return DexType::PumpFun;
            }

            // Check for Raydium signatures
            let is_raydium = logs
                .iter()
                .any(|log| log.contains(&raydium::constants::RAY_V4_PROGRAM_ID.to_string()));
            if is_raydium {
                println!("Detected Raydium transaction");
                return DexType::Raydium;
            }

            println!("No matching DEX found in transaction");
        }
        DexType::Unknown
    }

    pub fn from_transaction(
        transaction: &EncodedConfirmedTransactionWithStatusMeta,
    ) -> Result<Option<DexTransaction>> {
        let dex_type = Self::detect_dex_type(transaction);

        match dex_type {
            DexType::Unknown => Ok(None),
            _ => {
                let (transaction_type, token_address, amount_token, amount_sol, price_per_token) =
                    match dex_type {
                        DexType::PumpFun => {
                            pumpdotfun::transaction::extract_transaction_details(transaction)?
                        }
                        DexType::Raydium => {
                            raydium::transaction::extract_transaction_details(transaction)?
                        }
                        DexType::Unknown => unreachable!(),
                    };

                if transaction_type == TransactionType::Unknown {
                    return Ok(None);
                }

                Ok(Some(DexTransaction {
                    dex_type,
                    transaction_type,
                    token_address,
                    amount_token,
                    amount_sol,
                    price_per_token,
                }))
            }
        }
    }
}
