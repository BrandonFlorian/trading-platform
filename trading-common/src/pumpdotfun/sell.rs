use crate::error::AppError;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use solana_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::Keypair;
use solana_sdk::{
    instruction::AccountMeta, instruction::Instruction, message::Message, pubkey::Pubkey,
    signer::Signer, transaction::Transaction,
};
use solana_transaction_status::UiTransactionEncoding;
use std::str::FromStr;
use thiserror::Error;

use crate::models::{BuyRequest, BuyResponse, SellRequest, SellResponse};
use crate::utils::{confirm_transaction, get_token_balance};
use solana_client::rpc_config::RpcSendTransactionConfig;

const UNIT_PRICE: u64 = 1_000;
const UNIT_BUDGET: u32 = 200_000;
const LAMPORTS_PER_SOL: u64 = 1_000_000_000;
