use crate::event_system::{Event, EventSystem};
use anyhow::{Context, Result};
use serde::Serialize;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_request::TokenAccountsFilter;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use surf::Client;
use trading_common::models::WalletUpdateNotification;
use trading_common::utils::{
    extract_token_account_info, format_balance, format_token_amount, get_metadata,
};
use trading_common::{ClientTxInfo, TransactionType};

#[derive(Debug, Clone, Serialize)]
pub struct TokenInfo {
    pub address: String,
    pub symbol: String,
    pub name: String,
    pub balance: String,
    pub metadata_uri: Option<String>,
    pub decimals: u8,
    pub market_cap: f64,
}

pub struct ServerWalletManager {
    rpc_client: Arc<RpcClient>,
    http_client: Client,
    public_key: Pubkey,
    balance: f64,
    tokens: HashMap<String, TokenInfo>,
    event_system: Arc<EventSystem>,
}

impl ServerWalletManager {
    pub async fn new(
        rpc_client: Arc<RpcClient>,
        public_key: Pubkey,
        event_system: Arc<EventSystem>,
    ) -> Result<Self> {
        let mut manager = Self {
            rpc_client,
            http_client: Client::new(),
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
        // Clear existing tokens
        self.tokens.clear();

        // Get all token accounts
        let token_accounts = self.rpc_client.get_token_accounts_by_owner(
            &self.public_key,
            TokenAccountsFilter::ProgramId(spl_token::id()),
        )?;

        // Process each token account
        for account in token_accounts {
            let (mint, balance, decimals) = extract_token_account_info(&account.account.data)
                .context("Failed to extract token account info")?;

            if balance > 0 {
                let mint_pubkey = Pubkey::from_str(&mint)?;
                let metadata = get_metadata(&self.rpc_client, &mint_pubkey).await?;

                self.tokens.insert(
                    mint.clone(),
                    TokenInfo {
                        address: mint,
                        symbol: metadata.symbol,
                        name: metadata.name,
                        balance: format_balance(format_token_amount(balance, decimals), decimals),
                        metadata_uri: Some(metadata.uri),
                        decimals,
                        market_cap: 0.0,
                    },
                );
            }
        }

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
        let notification = WalletUpdateNotification {
            data: self.get_wallet_info(),
            type_: "wallet_update".to_string(),
        };
        self.event_system.emit(Event::WalletUpdate(notification));
    }

    pub fn get_wallet_info(&self) -> serde_json::Value {
        serde_json::json!({
            "balance": self.balance,
            "tokens": self.tokens.values().collect::<Vec<_>>(),
        })
    }

    // Helper methods for querying state
    pub fn get_tokens(&self) -> &HashMap<String, TokenInfo> {
        &self.tokens
    }

    pub fn get_token_values(&self) -> impl Iterator<Item = &TokenInfo> {
        self.tokens.values()
    }

    pub async fn handle_trade_execution(&mut self, tx_info: &ClientTxInfo) -> Result<()> {
        match tx_info.transaction_type {
            TransactionType::Buy => {
                // Update SOL balance (subtract)
                self.update_balance(-tx_info.amount_sol);

                // Update or add token balance
                self.update_token_balance(
                    &tx_info.token_address,
                    tx_info.amount_token,
                    9, // Need to make this dynamic
                    Some(HashMap::from([
                        ("name".to_string(), tx_info.token_name.clone()),
                        ("symbol".to_string(), tx_info.token_symbol.clone()),
                        ("metadataUri".to_string(), tx_info.token_image_uri.clone()),
                    ])),
                );
            }
            TransactionType::Sell => {
                // Update SOL balance (add)
                self.update_balance(tx_info.amount_sol);

                // Update token balance (should be zero after sell)
                self.update_token_balance(&tx_info.token_address, 0.0, 9, None);
            }
            _ => {}
        }

        // Refresh actual balances to ensure accuracy
        self.refresh_balances().await?;

        Ok(())
    }
}
