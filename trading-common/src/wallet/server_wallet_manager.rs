use crate::event_system::{Event, EventSystem};
use crate::models::WalletUpdateNotification;
use crate::utils::data::{
    extract_token_account_info, format_balance, format_token_amount, get_metadata,
};
use crate::{ClientTxInfo, TransactionType};
use anyhow::{Context, Result};
use atomic_float::AtomicF64;
use dashmap::DashMap;
use reqwest::Client;
use serde::Serialize;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_request::TokenAccountsFilter;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

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
    pub rpc_client: Arc<RpcClient>,
    pub http_client: Client,
    pub public_key: Pubkey,
    pub balance: AtomicF64,
    pub tokens: DashMap<String, TokenInfo>,
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
            http_client: Client::new(),
            public_key,
            balance: AtomicF64::new(0.0),
            tokens: DashMap::new(),
            event_system,
        };
        manager.refresh_balances().await?;
        Ok(manager)
    }

    pub async fn refresh_balances(&mut self) -> Result<()> {
        // Update SOL balance
        let new_balance = self.get_sol_balance().await?;
        self.balance
            .store(new_balance, std::sync::atomic::Ordering::SeqCst);

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

    pub fn update_balance(&self, amount: f64) {
        let current = self.balance.load(std::sync::atomic::Ordering::SeqCst);
        self.balance
            .store(current + amount, std::sync::atomic::Ordering::SeqCst);
        self.emit_wallet_update();
    }

    pub fn update_token_balance(
        &self,
        token_address: &str,
        new_balance: f64,
        decimals: u8,
        token_info: Option<HashMap<String, String>>,
    ) {
        let formatted_balance = format_balance(new_balance, decimals);

        if let Some(mut token) = self.tokens.get_mut(token_address) {
            *token.value_mut() = TokenInfo {
                address: token.address.clone(),
                symbol: token.symbol.clone(),
                name: token.name.clone(),
                balance: formatted_balance,
                metadata_uri: token.metadata_uri.clone(),
                decimals: token.decimals,
                market_cap: token.market_cap,
            };
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
            "balance": self.balance.load(std::sync::atomic::Ordering::SeqCst),
            "tokens": self.tokens.iter().map(|e| e.value().clone()).collect::<Vec<_>>(),
        })
    }

    pub fn get_tokens(
        &self,
    ) -> impl Iterator<Item = dashmap::mapref::multiple::RefMulti<String, TokenInfo>> {
        self.tokens.iter()
    }

    pub fn get_token_values(
        &self,
    ) -> impl Iterator<Item = dashmap::mapref::multiple::RefMulti<String, TokenInfo>> {
        self.tokens.iter()
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
