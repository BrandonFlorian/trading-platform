use anyhow::Result;
use solana_client::rpc_client::RpcClient;
use solana_sdk::transaction::Transaction;
use solana_sdk::{pubkey::Pubkey, signature::Keypair, signer::Signer};
use std::str::FromStr;
use std::sync::Arc;

use crate::dex::DexType;
use crate::models::SellRequest;
use crate::server_wallet_client::WalletClient;
use crate::utils::data::get_token_balance;
use crate::{jupiter, pumpdotfun, WalletInfoResponse};
use crate::{models::BuyRequest, ClientTxInfo, CopyTradeSettings, TransactionType};
use crate::{raydium, TradeExecutionRequest};

pub async fn should_copy_trade(
    tx_info: &ClientTxInfo,
    settings: &CopyTradeSettings,
    wallet_info: &WalletInfoResponse,
) -> Result<bool> {
    // Token allowlist check
    if settings.use_allowed_tokens_list {
        if let Some(allowed_tokens) = &settings.allowed_tokens {
            if !allowed_tokens.contains(&tx_info.token_address) {
                return Ok(false);
            }
        }
    }

    match tx_info.transaction_type {
        TransactionType::Buy => {
            // Check current positions using wallet_info
            let current_positions = wallet_info.tokens.len();

            if current_positions >= settings.max_open_positions as usize {
                println!(
                    "Maximum open positions reached: Current {} of {}",
                    current_positions, settings.max_open_positions
                );
                return Ok(false);
            }

            if !settings.allow_additional_buys {
                // Check if we already hold this token
                if wallet_info
                    .tokens
                    .iter()
                    .any(|t| t.address == tx_info.token_address)
                {
                    println!("Additional buys not allowed and token already held");
                    return Ok(false);
                }
            }
        }
        TransactionType::Sell => {
            // Sell-specific validation
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
    dex_type: DexType,
    wallet_client: &Arc<WalletClient>,
) -> Result<()> {
    match tx_info.transaction_type {
        TransactionType::Buy => {
            let request = BuyRequest {
                token_address: tx_info.token_address.clone(),
                sol_quantity: settings.trade_amount_sol,
                slippage_tolerance: settings.max_slippage,
            };

            let response = match dex_type {
                DexType::PumpFun => {
                    pumpdotfun::process_buy_request(rpc_client, server_keypair, request).await?
                }
                DexType::Raydium => {
                    raydium::process_buy_request(rpc_client, server_keypair, &request).await?
                }
                DexType::Jupiter => {
                    let jupiter = jupiter::swap::Jupiter::default();
                    jupiter
                        .process_buy_request(rpc_client, server_keypair, &request)
                        .await?
                }
                DexType::Unknown => {
                    return Err(anyhow::anyhow!("Unknown DEX type"));
                }
            };

            if response.success {
                let wallet_client = wallet_client.clone();
                let trade_request = TradeExecutionRequest {
                    signature: response.signature.clone(),
                    token_address: tx_info.token_address.clone(),
                    transaction_type: "buy".to_string(),
                    amount_token: response.token_quantity,
                    amount_sol: response.sol_spent,
                    price_per_token: response.sol_spent / response.token_quantity,
                    token_name: tx_info.token_name.clone(),
                    token_symbol: tx_info.token_symbol.clone(),
                    token_image_uri: tx_info.token_image_uri.clone(),
                };

                tokio::spawn(async move {
                    if let Err(e) = wallet_client.handle_trade_execution(trade_request).await {
                        eprintln!("Error updating wallet state: {}", e);
                    }
                });
            }
        }
        TransactionType::Sell => {
            println!("Preparing to execute copy trade sell");
            let token_mint = Pubkey::from_str(&tx_info.token_address)?;

            // Create token account if needed
            let token_account = spl_associated_token_account::get_associated_token_address(
                &server_keypair.pubkey(),
                &token_mint,
            );

            // Create ATA if it doesn't exist
            if rpc_client.get_account(&token_account).is_err() {
                println!("Creating token account for {}", tx_info.token_symbol);
                let create_ata_ix =
                    spl_associated_token_account::instruction::create_associated_token_account(
                        &server_keypair.pubkey(),
                        &server_keypair.pubkey(),
                        &token_mint,
                        &spl_token::id(),
                    );

                let recent_blockhash = rpc_client.get_latest_blockhash()?;
                let create_ata_tx = Transaction::new_signed_with_payer(
                    &[create_ata_ix],
                    Some(&server_keypair.pubkey()),
                    &[server_keypair],
                    recent_blockhash,
                );

                rpc_client.send_and_confirm_transaction(&create_ata_tx)?;
                println!("Token account created successfully");
            }

            println!("Using token account: {}", token_account);
            let token_balance = get_token_balance(rpc_client, &token_account).await?;
            println!(
                "Found token balance to sell: {} {}",
                token_balance, tx_info.token_symbol
            );
            println!("Using max slippage: {}%", settings.max_slippage * 100.0);

            if token_balance > 0.0 {
                let request = SellRequest {
                    token_address: tx_info.token_address.clone(),
                    token_quantity: token_balance,
                    slippage_tolerance: settings.max_slippage,
                };

                let response = match dex_type {
                    DexType::PumpFun => {
                        pumpdotfun::process_sell_request(rpc_client, server_keypair, request)
                            .await?
                    }
                    DexType::Raydium => {
                        raydium::process_sell_request(rpc_client, server_keypair, &request).await?
                    }
                    DexType::Jupiter => {
                        let jupiter = jupiter::swap::Jupiter::default();
                        jupiter
                            .process_sell_request(rpc_client, server_keypair, &request)
                            .await?
                    }
                    DexType::Unknown => {
                        return Err(anyhow::anyhow!("Unknown DEX type"));
                    }
                };

                if response.success {
                    println!("Copy trade sell executed: {}", response.signature);
                    let wallet_client = wallet_client.clone();
                    let trade_request = TradeExecutionRequest {
                        signature: response.signature.clone(),
                        token_address: tx_info.token_address.clone(),
                        transaction_type: "sell".to_string(),
                        amount_token: token_balance,
                        amount_sol: response.sol_received,
                        price_per_token: response.sol_received / token_balance,
                        token_name: tx_info.token_name.clone(),
                        token_symbol: tx_info.token_symbol.clone(),
                        token_image_uri: tx_info.token_image_uri.clone(),
                    };

                    tokio::spawn(async move {
                        if let Err(e) = wallet_client.handle_trade_execution(trade_request).await {
                            eprintln!("Error updating wallet state: {}", e);
                        }
                    });
                }
            } else {
                println!("No tokens to sell");
            }
        }
        _ => {}
    }

    Ok(())
}
