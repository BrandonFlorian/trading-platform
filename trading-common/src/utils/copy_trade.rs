use anyhow::Result;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{pubkey::Pubkey, signature::Keypair, signer::Signer};
use std::str::FromStr;
use std::sync::Arc;

use crate::data::get_token_balance;
use crate::models::SellRequest;
use crate::pumpdotfun::{process_buy_request, process_sell_request};
use crate::wallet::server_wallet_manager::ServerWalletManager;
use crate::{models::BuyRequest, ClientTxInfo, CopyTradeSettings, TransactionType};
pub async fn should_copy_trade(
    tx_info: &ClientTxInfo,
    settings: &CopyTradeSettings,
    server_wallet_manager: &Arc<tokio::sync::Mutex<ServerWalletManager>>,
) -> Result<bool> {
    // Token allowlist check
    if settings.use_allowed_tokens_list {
        if let Some(allowed_tokens) = &settings.allowed_tokens {
            if !allowed_tokens.contains(&tx_info.token_address) {
                println!("Token not in allowed list: {}", tx_info.token_address);
                return Ok(false);
            }
        }
    }

    match tx_info.transaction_type {
        TransactionType::Buy => {
            // Check if the current number of open positions is less than max allowed
            let wallet_manager = Arc::clone(server_wallet_manager);
            let manager = wallet_manager.lock().await;
            let current_positions = manager.get_tokens().len();

            if current_positions >= settings.max_open_positions as usize {
                println!(
                    "Maximum open positions reached: Current {} of {}",
                    current_positions, settings.max_open_positions
                );
                return Ok(false);
            }

            if !settings.allow_additional_buys {
                // Check if we already hold this token
                if manager.get_tokens().contains_key(&tx_info.token_address) {
                    println!("Additional buys not allowed and token already held");
                    return Ok(false);
                }
            }
        }
        TransactionType::Sell => {
            // Sell-specific validation goes here
        }
        _ => return Ok(false),
    }

    Ok(true)
}

pub async fn execute_copy_trade(
    rpc_client: &Arc<RpcClient>,
    server_keypair: &Keypair,
    tx_info: &ClientTxInfo,
    settings: &CopyTradeSettings,
) -> Result<()> {
    match tx_info.transaction_type {
        TransactionType::Buy => {
            let request = BuyRequest {
                token_address: tx_info.token_address.clone(),
                sol_quantity: settings.trade_amount_sol,
                slippage_tolerance: settings.max_slippage,
            };

            let response = process_buy_request(rpc_client, server_keypair, request).await?;
            if response.success {
                println!("Copy trade buy executed: {}", response.signature);
            }
        }
        TransactionType::Sell => {
            println!("Preparing to execute copy trade sell");
            let token_mint = Pubkey::from_str(&tx_info.token_address)?;

            // Get the associated token account for our wallet
            let token_account = spl_associated_token_account::get_associated_token_address(
                &server_keypair.pubkey(),
                &token_mint,
            );

            println!("Using token account: {}", token_account);
            let token_balance = get_token_balance(rpc_client, &token_account).await?;
            println!(
                "Found token balance to sell: {} {}",
                token_balance, tx_info.token_symbol
            );
            println!("Using max slippage: {}%", settings.max_slippage * 100.0);

            let request = SellRequest {
                token_address: tx_info.token_address.clone(),
                token_quantity: token_balance,
                slippage_tolerance: settings.max_slippage,
            };

            println!("Executing sell request...");
            let response = process_sell_request(rpc_client, server_keypair, request).await?;
            if response.success {
                println!("Copy trade sell executed successfully:");
                println!("  Signature: {}", response.signature);
                println!(
                    "  Amount sold: {} {}",
                    response.token_quantity, tx_info.token_symbol
                );
                println!("  SOL received: {} SOL", response.sol_received);
            }
        }
        _ => {}
    }

    Ok(())
}
