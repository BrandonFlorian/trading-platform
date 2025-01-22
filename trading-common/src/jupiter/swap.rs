use super::{client::JupiterClient, types::*};
use crate::error::AppError;
use crate::models::{BuyRequest, BuyResponse, SellRequest, SellResponse};
use base64::{engine::general_purpose::STANDARD, Engine};
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_client::rpc_request::TokenAccountsFilter;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::instruction::AccountMeta;
use solana_sdk::{
    commitment_config::CommitmentLevel,
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    message::Message,
    pubkey::Pubkey,
    signer::Signer,
    transaction::{Transaction, VersionedTransaction},
};
use std::str::FromStr;

#[derive(Default)]
pub struct Jupiter {
    client: JupiterClient,
}

impl Jupiter {
    pub async fn process_buy_request(
        &self,
        rpc_client: &RpcClient,
        server_keypair: &impl Signer,
        request: &BuyRequest,
    ) -> Result<BuyResponse, AppError> {
        // Create quote request
        let quote_request = JupiterQuoteRequest {
            input_mint: "So11111111111111111111111111111111111111112".to_string(), // WSOL
            output_mint: request.token_address.clone(),
            amount: ((request.sol_quantity * 1_000_000_000.0) as u64).to_string(),
            slippage_bps: (request.slippage_tolerance * 10_000.0) as u16,
            platform_fee_bps: None,
        };

        // Get quote
        let quote_response = self.client.get_quote(&quote_request).await?;

        // Get swap instructions instead of full transaction
        let swap_request = JupiterSwapRequest {
            user_public_key: server_keypair.pubkey().to_string(),
            quote_response: quote_response.clone(),
            config: JupiterTransactionConfig {
                wrap_and_unwrap_sol: true,
                compute_unit_price_micro_lamports: Some(1_000_000), // We'll override this anyway
                compute_unit_limit: Some(1_400_000),
                prioritization_fee: None,
            },
        };

        let swap_instructions = self.client.get_swap_instructions(&swap_request).await?;

        // Build our own transaction with higher priority
        let mut all_instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(swap_instructions.compute_unit_limit),
            ComputeBudgetInstruction::set_compute_unit_price(
                swap_instructions
                    .prioritization_fee_lamports
                    .saturating_mul(2), // Use Jupiter's suggestion with a multiplier
            ),
        ];

        // Convert encoded instructions to Solana Instructions
        let convert_encoded = |encoded: EncodedInstruction| -> Result<Instruction, AppError> {
            let program_id = Pubkey::from_str(&encoded.program_id)
                .map_err(|e| AppError::TransactionError(format!("Invalid program id: {}", e)))?;

            let accounts = encoded
                .accounts
                .into_iter()
                .map(|acc| {
                    let pubkey = Pubkey::from_str(&acc.pubkey).map_err(|e| {
                        AppError::TransactionError(format!("Invalid account pubkey: {}", e))
                    })?;
                    Ok(AccountMeta {
                        pubkey,
                        is_signer: acc.is_signer,
                        is_writable: acc.is_writable,
                    })
                })
                .collect::<Result<Vec<_>, AppError>>()?;

            let data = STANDARD.decode(&encoded.data).map_err(|e| {
                AppError::TransactionError(format!("Invalid instruction data: {}", e))
            })?;

            Ok(Instruction {
                program_id,
                accounts,
                data,
            })
        };

        // Add all Jupiter instructions in order
        if let Some(ledger) = swap_instructions.token_ledger_instruction {
            all_instructions.push(convert_encoded(ledger)?);
        }

        for ix in swap_instructions.setup_instructions {
            all_instructions.push(convert_encoded(ix)?);
        }

        all_instructions.push(convert_encoded(swap_instructions.swap_instruction)?);

        if let Some(cleanup) = swap_instructions.cleanup_instruction {
            all_instructions.push(convert_encoded(cleanup)?);
        }

        // Get fresh blockhash and create transaction
        let recent_blockhash = rpc_client.get_latest_blockhash()?;
        let message = Message::new(&all_instructions, Some(&server_keypair.pubkey()));
        let transaction = Transaction::new(&[server_keypair], message, recent_blockhash);
        let versioned_transaction = VersionedTransaction::from(transaction);

        // Send with optimized config
        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Finalized),
            encoding: None,
            max_retries: Some(5),
            min_context_slot: None,
        };

        let signature = rpc_client.send_transaction_with_config(&versioned_transaction, config)?;

        let confirmation_config = CommitmentConfig::finalized();
        let timeout = std::time::Duration::from_secs(30);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            match rpc_client.get_signature_status_with_commitment(&signature, confirmation_config) {
                Ok(Some(Ok(()))) => {
                    println!("Transaction confirmed successfully: {}", signature);
                    println!("Buy successful, checking token accounts...");
                    let token_pubkey = Pubkey::from_str(&request.token_address).map_err(|e| {
                        AppError::TransactionError(format!("Invalid token address: {}", e))
                    })?;

                    // Check all possible token accounts
                    let token_accounts = rpc_client
                        .get_token_accounts_by_owner(
                            &server_keypair.pubkey(),
                            TokenAccountsFilter::Mint(token_pubkey),
                        )
                        .map_err(|e| {
                            AppError::TransactionError(format!(
                                "Failed to get token accounts: {}",
                                e
                            ))
                        })?;

                    println!("Found {} token accounts after buy:", token_accounts.len());
                    for account in token_accounts {
                        println!("Token account: {}", account.pubkey);
                        if let Ok(balance) = rpc_client
                            .get_token_account_balance(&Pubkey::from_str(&account.pubkey).unwrap())
                        {
                            println!("Balance: {}", balance.ui_amount.unwrap_or_default());
                        }
                    }
                    return Ok(BuyResponse {
                        success: true,
                        signature: signature.to_string(),
                        token_quantity: quote_response
                            .out_amount
                            .parse::<f64>()
                            .unwrap_or_default(),
                        sol_spent: quote_response.in_amount.parse::<f64>().unwrap_or_default()
                            / 1_000_000_000.0,
                        solscan_tx_url: format!("https://solscan.io/tx/{}", signature),
                        error: None,
                    });
                }
                Ok(Some(Err(e))) => {
                    return Err(AppError::TransactionError(format!(
                        "Transaction failed: {:?}",
                        e
                    )));
                }
                Ok(None) => {
                    println!(
                        "Transaction still pending... elapsed: {:?}",
                        start.elapsed()
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                    continue;
                }
                Err(e) => {
                    println!("Error checking signature status: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                }
            }
        }

        // If we reach here, we've timed out
        Err(AppError::TransactionError(
            "Transaction confirmation timed out".to_string(),
        ))
    }

    pub async fn process_sell_request(
        &self,
        rpc_client: &RpcClient,
        server_keypair: &impl Signer,
        request: &SellRequest,
    ) -> Result<SellResponse, AppError> {
        // Create quote request
        let quote_request = JupiterQuoteRequest {
            input_mint: request.token_address.clone(),
            output_mint: "So11111111111111111111111111111111111111112".to_string(), // WSOL
            amount: ((request.token_quantity * 1_000_000.0) as u64).to_string(),
            slippage_bps: (request.slippage_tolerance * 10_000.0) as u16,
            platform_fee_bps: None,
        };

        // Get quote
        let quote_response = self.client.get_quote(&quote_request).await?;

        // Get swap instructions instead of full transaction
        let swap_request = JupiterSwapRequest {
            user_public_key: server_keypair.pubkey().to_string(),
            quote_response: quote_response.clone(),
            config: JupiterTransactionConfig {
                wrap_and_unwrap_sol: true,
                compute_unit_price_micro_lamports: Some(1_000_000), // We'll override this anyway
                compute_unit_limit: Some(1_400_000),
                prioritization_fee: None,
            },
        };

        let swap_instructions = self.client.get_swap_instructions(&swap_request).await?;

        // Build our own transaction with higher priority
        let mut all_instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(swap_instructions.compute_unit_limit),
            ComputeBudgetInstruction::set_compute_unit_price(
                swap_instructions
                    .prioritization_fee_lamports
                    .saturating_mul(2), // Use Jupiter's suggestion with a multiplier
            ),
        ];

        // Convert encoded instructions to Solana Instructions
        let convert_encoded = |encoded: EncodedInstruction| -> Result<Instruction, AppError> {
            let program_id = Pubkey::from_str(&encoded.program_id)
                .map_err(|e| AppError::TransactionError(format!("Invalid program id: {}", e)))?;

            let accounts = encoded
                .accounts
                .into_iter()
                .map(|acc| {
                    let pubkey = Pubkey::from_str(&acc.pubkey).map_err(|e| {
                        AppError::TransactionError(format!("Invalid account pubkey: {}", e))
                    })?;
                    Ok(AccountMeta {
                        pubkey,
                        is_signer: acc.is_signer,
                        is_writable: acc.is_writable,
                    })
                })
                .collect::<Result<Vec<_>, AppError>>()?;

            let data = STANDARD.decode(&encoded.data).map_err(|e| {
                AppError::TransactionError(format!("Invalid instruction data: {}", e))
            })?;

            Ok(Instruction {
                program_id,
                accounts,
                data,
            })
        };

        // Add all Jupiter instructions in order
        if let Some(ledger) = swap_instructions.token_ledger_instruction {
            all_instructions.push(convert_encoded(ledger)?);
        }

        for ix in swap_instructions.setup_instructions {
            all_instructions.push(convert_encoded(ix)?);
        }

        all_instructions.push(convert_encoded(swap_instructions.swap_instruction)?);

        if let Some(cleanup) = swap_instructions.cleanup_instruction {
            all_instructions.push(convert_encoded(cleanup)?);
        }

        // Get fresh blockhash and create transaction
        let recent_blockhash = rpc_client.get_latest_blockhash()?;
        let message = Message::new(&all_instructions, Some(&server_keypair.pubkey()));
        let transaction = Transaction::new(&[server_keypair], message, recent_blockhash);
        let versioned_transaction = VersionedTransaction::from(transaction);

        // Send with optimized config
        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Finalized),
            encoding: None,
            max_retries: Some(5),
            min_context_slot: None,
        };

        let signature = rpc_client.send_transaction_with_config(&versioned_transaction, config)?;

        // Wait for confirmation with increased timeout
        let confirmation_config = CommitmentConfig::finalized();
        let timeout = std::time::Duration::from_secs(30);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            match rpc_client.get_signature_status_with_commitment(&signature, confirmation_config) {
                Ok(Some(Ok(()))) => {
                    println!("Transaction confirmed successfully: {}", signature);
                    println!("Sell successful, checking token accounts...");
                    let token_pubkey = Pubkey::from_str(&request.token_address).map_err(|e| {
                        AppError::TransactionError(format!("Invalid token address: {}", e))
                    })?;

                    // Check all possible token accounts
                    let token_accounts = rpc_client
                        .get_token_accounts_by_owner(
                            &server_keypair.pubkey(),
                            TokenAccountsFilter::Mint(token_pubkey),
                        )
                        .map_err(|e| {
                            AppError::TransactionError(format!(
                                "Failed to get token accounts: {}",
                                e
                            ))
                        })?;

                    println!("Found {} token accounts after sell:", token_accounts.len());
                    for account in token_accounts {
                        println!("Token account: {}", account.pubkey);
                        if let Ok(balance) = rpc_client
                            .get_token_account_balance(&Pubkey::from_str(&account.pubkey).unwrap())
                        {
                            println!("Balance: {}", balance.ui_amount.unwrap_or_default());
                        }
                    }
                    return Ok(SellResponse {
                        success: true,
                        signature: signature.to_string(),
                        token_quantity: request.token_quantity,
                        sol_received: quote_response.out_amount.parse::<f64>().unwrap_or_default()
                            / 1_000_000_000.0,
                        solscan_tx_url: format!("https://solscan.io/tx/{}", signature),
                        error: None,
                    });
                }
                Ok(Some(Err(e))) => {
                    return Err(AppError::TransactionError(format!(
                        "Transaction failed: {:?}",
                        e
                    )));
                }
                Ok(None) => {
                    println!(
                        "Transaction still pending... elapsed: {:?}",
                        start.elapsed()
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                    continue;
                }
                Err(e) => {
                    println!("Error checking signature status: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                }
            }
        }

        // If we reach here, we've timed out
        Err(AppError::TransactionError(
            "Transaction confirmation timed out".to_string(),
        ))
    }
}
