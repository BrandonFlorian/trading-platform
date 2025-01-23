use super::convert_encoded;
use super::{client::JupiterClient, types::*};
use crate::error::AppError;
use crate::models::{BuyRequest, BuyResponse, SellRequest, SellResponse};
use solana_client::{
    rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig, rpc_request::TokenAccountsFilter,
};
use solana_sdk::{
    address_lookup_table::{state::AddressLookupTable, AddressLookupTableAccount},
    commitment_config::{CommitmentConfig, CommitmentLevel},
    instruction::Instruction,
    message::{v0, VersionedMessage},
    pubkey::Pubkey,
    signer::Signer,
    transaction::VersionedTransaction,
};
use solana_transaction_status::UiTransactionEncoding;
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
            input_mint: "So11111111111111111111111111111111111111112".to_string(),
            output_mint: request.token_address.clone(),
            amount: ((request.sol_quantity * 1_000_000_000.0) as u64).to_string(),
            slippage_bps: (request.slippage_tolerance * 10_000.0) as u16,
            platform_fee_bps: None,
        };

        let quote_response = self.client.get_quote(&quote_request).await?;

        let swap_request = JupiterSwapRequest {
            user_public_key: server_keypair.pubkey().to_string(),
            quote_response: quote_response.clone(),
            config: JupiterTransactionConfig::default(),
        };

        let swap_instructions = self.client.get_swap_instructions(&swap_request).await?;

        // Load ALTs
        let lookup_tables: Vec<AddressLookupTableAccount> = swap_instructions
            .address_lookup_table_addresses
            .iter()
            .filter_map(|addr| {
                let pubkey = Pubkey::from_str(addr).ok()?;
                rpc_client.get_account(&pubkey).ok().and_then(|acc| {
                    AddressLookupTable::deserialize(&acc.data)
                        .ok()
                        .map(|table| AddressLookupTableAccount {
                            key: pubkey,
                            addresses: table.addresses.to_vec(),
                        })
                })
            })
            .collect();
        let recent_blockhash = rpc_client.get_latest_blockhash();

        // Build transaction with higher priority
        let mut all_instructions = Vec::new();

        // Add Jupiter's compute budget instructions first
        for ix in swap_instructions.compute_budget_instructions {
            all_instructions.push(convert_encoded(ix)?);
        }

        // Add token ledger if present
        if let Some(ledger) = swap_instructions.token_ledger_instruction {
            all_instructions.push(convert_encoded(ledger)?);
        }

        // Add setup instructions
        let setup_instructions: Vec<Instruction> = swap_instructions
            .setup_instructions
            .iter()
            .map(|ix| convert_encoded(ix.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        all_instructions.extend(setup_instructions);

        // Add swap instruction
        all_instructions.push(convert_encoded(swap_instructions.swap_instruction)?);

        // Add cleanup if present
        if let Some(cleanup) = swap_instructions.cleanup_instruction {
            all_instructions.push(convert_encoded(cleanup)?);
        }

        // Add any other instructions
        for ix in swap_instructions.other_instructions {
            all_instructions.push(convert_encoded(ix)?);
        }

        // Use all_instructions in the message compilation
        let v0_message = v0::Message::try_compile(
            &server_keypair.pubkey(),
            &all_instructions,
            &lookup_tables,
            recent_blockhash.unwrap(),
        )
        .map_err(|e| AppError::TransactionError(format!("Failed to compile message: {}", e)))?;

        let versioned_transaction =
            VersionedTransaction::try_new(VersionedMessage::V0(v0_message), &[server_keypair])
                .map_err(|e| {
                    AppError::TransactionError(format!(
                        "Failed to create versioned transaction: {}",
                        e
                    ))
                })?;

        // Use faster commitment level
        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Processed),
            encoding: Some(UiTransactionEncoding::Base64),
            max_retries: Some(5),
            min_context_slot: Some(swap_instructions.simulation_slot.unwrap_or_default()),
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

        // Timed out
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
        let token_mint = Pubkey::from_str(&request.token_address)
            .map_err(|e| AppError::TransactionError(format!("Invalid token address: {}", e)))?;

        // Find all token accounts for this mint
        let token_accounts = rpc_client
            .get_token_accounts_by_owner(
                &server_keypair.pubkey(),
                TokenAccountsFilter::Mint(token_mint),
            )
            .map_err(|e| {
                AppError::TransactionError(format!("Failed to get token accounts: {}", e))
            })?;

        println!(
            "Found {} token accounts for mint {}",
            token_accounts.len(),
            token_mint
        );

        // Find first account with sufficient balance and verify it's still valid
        let mut valid_token_account = None;
        for account in token_accounts {
            let pubkey = Pubkey::from_str(&account.pubkey)
                .map_err(|e| AppError::TransactionError(format!("Invalid pubkey: {}", e)))?;

            match rpc_client.get_token_account_balance(&pubkey) {
                Ok(balance) => {
                    println!(
                        "Token account {} has balance {}",
                        pubkey,
                        balance.ui_amount.unwrap_or_default()
                    );
                    if balance.ui_amount.unwrap_or_default() >= request.token_quantity {
                        valid_token_account = Some(account.pubkey);
                        break;
                    }
                }
                Err(e) => {
                    println!("Token account {} is invalid: {}", pubkey, e);
                    continue;
                }
            }
        }

        let token_account = valid_token_account.ok_or_else(|| {
            AppError::TransactionError(
                "No valid token account with sufficient balance found".to_string(),
            )
        })?;

        println!("Using token account {} for sell", token_account);

        // Verify the balance one last time
        let token_balance = rpc_client
            .get_token_account_balance(&Pubkey::from_str(&token_account).unwrap())
            .map_err(|e| AppError::TransactionError(format!("Failed to get token balance: {}", e)))?
            .ui_amount
            .unwrap_or_default();

        if token_balance < request.token_quantity {
            return Err(AppError::TransactionError(format!(
                "Insufficient balance: Have {} tokens, trying to sell {}",
                token_balance, request.token_quantity
            )));
        }

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

        // Get swap instructions
        let swap_request = JupiterSwapRequest {
            user_public_key: server_keypair.pubkey().to_string(),
            quote_response: quote_response.clone(),
            config: JupiterTransactionConfig::default(),
        };

        let swap_instructions = self.client.get_swap_instructions(&swap_request).await?;

        // Load ALTs
        let lookup_tables: Vec<AddressLookupTableAccount> = swap_instructions
            .address_lookup_table_addresses
            .iter()
            .filter_map(|addr| {
                let pubkey = Pubkey::from_str(addr).ok()?;
                rpc_client.get_account(&pubkey).ok().and_then(|acc| {
                    AddressLookupTable::deserialize(&acc.data)
                        .ok()
                        .map(|table| AddressLookupTableAccount {
                            key: pubkey,
                            addresses: table.addresses.to_vec(),
                        })
                })
            })
            .collect();

        let recent_blockhash = rpc_client.get_latest_blockhash()?;

        // Build transaction
        let mut all_instructions = Vec::new();

        // Add Jupiter's compute budget instructions first
        for ix in swap_instructions.compute_budget_instructions {
            all_instructions.push(convert_encoded(ix)?);
        }

        // Add token ledger if present
        if let Some(ledger) = swap_instructions.token_ledger_instruction {
            all_instructions.push(convert_encoded(ledger)?);
        }

        // Add setup instructions
        let setup_instructions: Vec<Instruction> = swap_instructions
            .setup_instructions
            .iter()
            .map(|ix| convert_encoded(ix.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        all_instructions.extend(setup_instructions);

        // Add swap instruction
        all_instructions.push(convert_encoded(swap_instructions.swap_instruction)?);

        // Add cleanup if present
        if let Some(cleanup) = swap_instructions.cleanup_instruction {
            all_instructions.push(convert_encoded(cleanup)?);
        }

        // Add any other instructions
        for ix in swap_instructions.other_instructions {
            all_instructions.push(convert_encoded(ix)?);
        }

        // Use all_instructions in the message compilation
        let v0_message = v0::Message::try_compile(
            &server_keypair.pubkey(),
            &all_instructions,
            &lookup_tables,
            recent_blockhash,
        )
        .map_err(|e| AppError::TransactionError(format!("Failed to compile message: {}", e)))?;

        let versioned_transaction =
            VersionedTransaction::try_new(VersionedMessage::V0(v0_message), &[server_keypair])
                .map_err(|e| {
                    AppError::TransactionError(format!(
                        "Failed to create versioned transaction: {}",
                        e
                    ))
                })?;

        // Send with config
        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Processed),
            encoding: Some(UiTransactionEncoding::Base64),
            max_retries: Some(5),
            min_context_slot: Some(swap_instructions.simulation_slot.unwrap_or_default()),
        };

        let signature = rpc_client
            .send_transaction_with_config(&versioned_transaction, config)
            .map_err(|e| {
                AppError::TransactionError(format!("Failed to send transaction: {}", e))
            })?;
        println!("Transaction sent: {}", signature);

        let confirmation_config = CommitmentConfig::finalized();
        let timeout = std::time::Duration::from_secs(30);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            match rpc_client.get_signature_status_with_commitment(&signature, confirmation_config) {
                Ok(Some(Ok(_))) => {
                    println!("Transaction confirmed in {:?}", start.elapsed());

                    // Don't try to check token balance after sell - account might be closed
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
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    continue;
                }
                Err(e) => {
                    println!("Error checking signature status: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }

        Err(AppError::TransactionError(
            "Transaction confirmation timed out".to_string(),
        ))
    }
}
