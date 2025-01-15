use crate::event_system::{Event, EventSystem};
use crate::models::{TokenInfo, WalletUpdate, WalletUpdateNotification};
use crate::utils::data::{
    extract_token_account_info, format_balance, format_token_amount, get_metadata,
};
use crate::ClientTxInfo;
use anyhow::{Context, Result};
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_request::TokenAccountsFilter;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use surf::Client;

pub struct ServerWalletManager {
    pub rpc_client: Arc<RpcClient>,
    pub _http_client: Client,
    pub public_key: Pubkey,
    pub balance: f64,
    pub tokens: HashMap<String, TokenInfo>,
    pub event_system: Arc<EventSystem>,
}

impl ServerWalletManager {
    pub async fn new(
        rpc_client: Arc<RpcClient>,
        public_key: Pubkey,
        event_system: Arc<EventSystem>,
    ) -> Result<Self> {
        let mut manager = Self {
            rpc_client,
            _http_client: Client::new(),
            public_key,
            balance: 0.0,
            tokens: HashMap::new(),
            event_system,
        };
        manager.refresh_balances().await?;
        Ok(manager)
    }

    pub async fn refresh_balances(&mut self) -> Result<()> {
        // Update SOL balance
        self.balance = self.get_sol_balance().await?;

        // Update token balances
        self.get_token_balances().await?;

        // Emit wallet update event
        self.emit_wallet_update();

        Ok(())
    }

    pub async fn get_sol_balance(&self) -> Result<f64> {
        let balance = self.rpc_client.get_balance(&self.public_key)?;
        Ok((balance as f64) / 1e9)
    }

    pub async fn get_token_balances(&mut self) -> Result<()> {
        // Create a new map for the updated state
        let mut updated_tokens = HashMap::new();

        // Get all token accounts
        let token_accounts = self.rpc_client.get_token_accounts_by_owner(
            &self.public_key,
            TokenAccountsFilter::ProgramId(spl_token::id()),
        )?;

        let count = token_accounts.len();
        println!("Processing {} token accounts", count);

        // Process only accounts that have non-zero balance
        for account in token_accounts {
            let (mint, balance, decimals) = extract_token_account_info(&account.account.data)
                .context("Failed to extract token account info")?;

            if balance > 0 {
                let mint_pubkey = Pubkey::from_str(&mint)?;

                // Try to get existing token info first for efficiency
                let token_info = if let Some(existing) = self.tokens.get(&mint) {
                    TokenInfo {
                        balance: format_balance(format_token_amount(balance, decimals), decimals),
                        ..existing.clone()
                    }
                } else {
                    // Only fetch metadata for new tokens with balance
                    let metadata = get_metadata(&self.rpc_client, &mint_pubkey).await?;
                    TokenInfo {
                        address: mint.clone(),
                        symbol: metadata.symbol,
                        name: metadata.name,
                        balance: format_balance(format_token_amount(balance, decimals), decimals),
                        metadata_uri: Some(metadata.uri),
                        decimals,
                        market_cap: 0.0,
                    }
                };

                updated_tokens.insert(mint.clone(), token_info);
                println!(
                    "Added token {} with balance {}",
                    mint,
                    format_token_amount(balance, decimals)
                );
            }
        }

        // Replace the old map with the new one
        let token_count = updated_tokens.len();
        self.tokens = updated_tokens;
        println!(
            "Updated balances for {} tokens with non-zero balance",
            token_count
        );

        Ok(())
    }

    pub fn update_balance(&mut self, amount: f64) {
        self.balance += amount;
        self.emit_wallet_update();
    }
    pub fn update_token_balance(
        &mut self,
        token_address: &str,
        new_balance: f64,
        decimals: u8,
        token_info: Option<HashMap<String, String>>,
    ) {
        let formatted_balance = format_balance(new_balance, decimals);

        if let Some(token) = self.tokens.get_mut(token_address) {
            token.balance = formatted_balance;
        } else if let Some(info) = token_info {
            self.tokens.insert(
                token_address.to_string(),
                TokenInfo {
                    address: token_address.to_string(),
                    symbol: info
                        .get("symbol")
                        .cloned()
                        .unwrap_or_else(|| "Unknown".to_string()),
                    name: info
                        .get("name")
                        .cloned()
                        .unwrap_or_else(|| "Unknown".to_string()),
                    balance: formatted_balance,
                    metadata_uri: info.get("metadataUri").cloned(),
                    decimals,
                    market_cap: 0.0,
                },
            );
        }

        self.emit_wallet_update();
    }

    pub fn emit_wallet_update(&self) {
        let wallet_info = self.get_wallet_info();
        println!("Emitting wallet update with data: {:?}", wallet_info);

        let notification = WalletUpdateNotification {
            data: wallet_info,
            type_: "wallet_update".to_string(),
        };

        println!("Created wallet update notification");
        self.event_system.emit(Event::WalletUpdate(notification));
        println!("Wallet update event emitted");
    }

    pub fn get_wallet_info(&self) -> WalletUpdate {
        WalletUpdate {
            balance: self.balance,
            tokens: self.tokens.values().cloned().collect(),
            address: self.public_key.to_string(),
        }
    }

    // Helper methods for querying state
    pub fn get_tokens(&self) -> &HashMap<String, TokenInfo> {
        &self.tokens
    }

    pub fn get_token_values(&self) -> impl Iterator<Item = &TokenInfo> {
        self.tokens.values()
    }

    pub async fn handle_trade_execution(&mut self, tx_info: &ClientTxInfo) -> Result<()> {
        println!("Server wallet handling trade execution: {:?}", tx_info);

        let signature = Signature::from_str(&tx_info.signature)?;

        // Wait for finality using signature status
        loop {
            let status = self.rpc_client.get_signature_status(&signature)?;
            match status {
                Some(Ok(_)) => break, // Transaction is confirmed
                Some(Err(e)) => return Err(anyhow::anyhow!("Transaction failed: {}", e)),
                None => tokio::time::sleep(tokio::time::Duration::from_millis(500)).await,
            }
        }

        // Single refresh once we know the transaction is final
        self.refresh_balances().await?;
        println!(
            "Final state - SOL: {}, Tokens: {:?}",
            self.balance, self.tokens
        );

        Ok(())
    }
}
