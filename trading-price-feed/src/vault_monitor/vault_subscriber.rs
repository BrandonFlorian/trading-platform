use base64::Engine;
use serde_json::Value;
use solana_sdk::{program_pack::Pack, pubkey::Pubkey};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use trading_common::error::AppError;

use super::{
    PoolMonitorState, SubscriptionRoute, VaultBalance, VaultPriceUpdate, VaultSubscription,
    VaultType, WebSocketManager,
};

/// Handles vault account subscriptions and balance tracking
pub struct VaultSubscriber {
    ws_manager: Arc<WebSocketManager>,
    pool_states: Arc<RwLock<HashMap<String, PoolMonitorState>>>,
    vault_balances: Arc<RwLock<HashMap<Pubkey, VaultBalance>>>,
    price_sender: mpsc::Sender<VaultPriceUpdate>,
    redis_client: Arc<trading_common::redis::RedisPool>,
    subscription_to_vault: Arc<RwLock<HashMap<String, Pubkey>>>, // subscription_id -> vault_address
}

impl VaultSubscriber {
    pub fn new(
        ws_manager: Arc<WebSocketManager>,
        price_sender: mpsc::Sender<VaultPriceUpdate>,
        redis_client: Arc<trading_common::redis::RedisPool>,
    ) -> Self {
        Self {
            ws_manager,
            pool_states: Arc::new(RwLock::new(HashMap::new())),
            vault_balances: Arc::new(RwLock::new(HashMap::new())),
            price_sender,
            redis_client,
            subscription_to_vault: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Subscribe to vault accounts for a specific token pool
    pub async fn subscribe_to_pool(
        &self,
        token_address: String,
        pool_state: PoolMonitorState,
    ) -> Result<(), AppError> {
        tracing::info!("Subscribing to vault accounts for token: {}", token_address);

        // Create routes for both vault subscriptions
        let base_route = SubscriptionRoute {
            subscription_id: String::new(), // Will be set by WebSocketManager
            token_address: token_address.clone(),
            vault_type: VaultType::Base,
            vault_address: pool_state.base_vault,
        };

        let quote_route = SubscriptionRoute {
            subscription_id: String::new(),
            token_address: token_address.clone(),
            vault_type: VaultType::Quote,
            vault_address: pool_state.quote_vault,
        };

        // Subscribe to both vault accounts
        let base_subscription_id = self
            .ws_manager
            .subscribe_to_account(&pool_state.base_vault, base_route)
            .await?;

        let quote_subscription_id = self
            .ws_manager
            .subscribe_to_account(&pool_state.quote_vault, quote_route)
            .await?;

        // Store subscription mappings
        {
            let mut mappings = self.subscription_to_vault.write().await;
            mappings.insert(base_subscription_id.clone(), pool_state.base_vault);
            mappings.insert(quote_subscription_id.clone(), pool_state.quote_vault);
        }

        // Store the pool state
        {
            let mut states = self.pool_states.write().await;
            states.insert(token_address.clone(), pool_state);
        }

        tracing::info!(
            "Successfully subscribed to vaults for token {}: base={}, quote={}",
            token_address,
            base_subscription_id,
            quote_subscription_id
        );

        Ok(())
    }

    /// Process incoming vault account notifications
    pub async fn process_vault_notification(
        &self,
        subscription_id: String,
        message: Value,
    ) -> Result<(), AppError> {
        // Extract account data from the WebSocket message
        let account_data = self.extract_account_data(&message)?;
        
        // Parse the SPL token account
        let token_account = spl_token::state::Account::unpack(&account_data).map_err(|e| {
            AppError::SerializationError(borsh::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse SPL token account: {}", e),
            ))
        })?;

        // Get vault address from subscription routes
        // Note: This may fail if messages are being forwarded to wrong subscriptions
        let vault_address_result = self.get_vault_address_from_subscription(&subscription_id).await;
        
        if let Err(e) = &vault_address_result {
            tracing::warn!(
                "Failed to get vault address for subscription {}: {}. This may be due to message forwarding to wrong subscription.",
                subscription_id, e
            );
            return Ok(()); // Silently ignore - likely forwarded to wrong subscription
        }
        
        let vault_address = vault_address_result.unwrap();

        // Create vault balance update
        let vault_balance = VaultBalance {
            vault_address,
            balance: token_account.amount,
            owner: token_account.owner,
            mint: token_account.mint,
            last_updated: std::time::Instant::now(),
        };

        // Find which token this vault belongs to
        let token_address_result = self.find_token_for_vault(&vault_balance.vault_address).await;
        
        if let Err(e) = &token_address_result {
            tracing::warn!(
                "Failed to find token for vault {}: {}. This may be due to message forwarding to wrong subscription.",
                vault_address, e
            );
            return Ok(()); // Silently ignore - likely forwarded to wrong subscription
        }
        
        let token_address = token_address_result.unwrap();
        
        tracing::info!(
            "📊 Vault update for {}: {} balance = {} (mint: {}, owner: {})",
            token_address,
            vault_address,
            token_account.amount,
            token_account.mint,
            token_account.owner
        );

        // Debug: Compare with expected vault addresses and mints
        let pool_state = {
            let pool_states = self.pool_states.read().await;
            pool_states.get(&token_address).cloned()
        };
        
        if let Some(pool_state) = pool_state {
            let expected_mint = if vault_address == pool_state.base_vault {
                pool_state.token_address.clone()
            } else if vault_address == pool_state.quote_vault {
                "So11111111111111111111111111111111111111112".to_string()
            } else {
                "Unknown".to_string()
            };
            
            tracing::info!(
                "🔍 Vault debug - Address: {}, Expected mint: {}, Actual mint: {}, Match: {}",
                vault_address,
                expected_mint,
                token_account.mint,
                expected_mint == token_account.mint.to_string()
            );
        }
        
        // Update cached balance
        {
            let mut balances = self.vault_balances.write().await;
            balances.insert(vault_balance.vault_address, vault_balance.clone());
        }

        // Check if we have both balances for price calculation
        self.try_calculate_price(&token_address).await?;

        Ok(())
    }

    /// Extract account data from WebSocket message
    fn extract_account_data(&self, message: &Value) -> Result<Vec<u8>, AppError> {
        let account_data = message
            .get("params")
            .and_then(|p| p.get("result"))
            .and_then(|r| r.get("value"))
            .and_then(|v| v.get("data"))
            .and_then(|d| d.as_array())
            .and_then(|a| a.first())
            .and_then(|s| s.as_str())
            .ok_or_else(|| AppError::JsonParseError("Invalid account data format".to_string()))?;

        base64::engine::general_purpose::STANDARD
            .decode(account_data)
            .map_err(|e| {
                AppError::SerializationError(borsh::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to decode base64 data: {}", e),
                ))
            })
    }

    /// Get vault address from subscription ID
    async fn get_vault_address_from_subscription(&self, subscription_id: &str) -> Result<Pubkey, AppError> {
        let mappings = self.subscription_to_vault.read().await;
        mappings.get(subscription_id)
            .copied()
            .ok_or_else(|| AppError::InternalError(format!("No vault found for subscription: {}", subscription_id)))
    }


    /// Find which token address corresponds to a vault address
    async fn find_token_for_vault(&self, vault_address: &Pubkey) -> Result<String, AppError> {
        let states = self.pool_states.read().await;
        
        for (token_address, pool_state) in states.iter() {
            if pool_state.base_vault == *vault_address || pool_state.quote_vault == *vault_address {
                return Ok(token_address.clone());
            }
        }

        Err(AppError::InternalError(format!(
            "No token found for vault: {}",
            vault_address
        )))
    }

    /// Try to calculate price if both vault balances are available
    async fn try_calculate_price(&self, token_address: &str) -> Result<(), AppError> {
        let (base_balance, quote_balance, pool_state) = {
            let states = self.pool_states.read().await;
            let balances = self.vault_balances.read().await;
            
            let pool_state = states.get(token_address).ok_or_else(|| {
                AppError::InternalError(format!("Pool state not found for token: {}", token_address))
            })?;

            let base_balance = balances.get(&pool_state.base_vault).map(|b| b.balance);
            let quote_balance = balances.get(&pool_state.quote_vault).map(|b| b.balance);

            (base_balance, quote_balance, pool_state.clone())
        };

        // Only calculate price if we have both balances
        if let (Some(base_balance), Some(quote_balance)) = (base_balance, quote_balance) {
            let price_update = self.calculate_price_from_balances(
                token_address,
                base_balance,
                quote_balance,
                &pool_state,
            ).await?;

            // Send price update
            if let Err(e) = self.price_sender.send(price_update).await {
                tracing::error!("Failed to send price update: {}", e);
            }

            // Update pool state with new balances
            {
                let mut states = self.pool_states.write().await;
                if let Some(state) = states.get_mut(token_address) {
                    state.base_balance = Some(base_balance);
                    state.quote_balance = Some(quote_balance);
                    state.last_price_update = Some(std::time::Instant::now());
                }
            }
        }

        Ok(())
    }

    /// Calculate price from vault balances
    async fn calculate_price_from_balances(
        &self,
        token_address: &str,
        base_balance: u64,
        quote_balance: u64,
        pool_state: &PoolMonitorState,
    ) -> Result<VaultPriceUpdate, AppError> {
        // Convert raw balances to decimal-adjusted amounts
        let base_amount = base_balance as f64 / 10f64.powi(pool_state.base_decimals as i32);
        let quote_amount = quote_balance as f64 / 10f64.powi(pool_state.quote_decimals as i32);

        // Calculate price: quote_amount / base_amount
        let price_sol = if base_amount > 0.0 {
            quote_amount / base_amount
        } else {
            0.0
        };

        // Get SOL price in USD
        let sol_price_usd = self.get_sol_price_usd().await?;
        let price_usd = price_sol * sol_price_usd;

        // Calculate liquidity (total value of both sides)
        let liquidity_sol = quote_amount * 2.0; // Total liquidity approximation

        let price_update = VaultPriceUpdate {
            token_address: token_address.to_string(),
            price_sol,
            price_usd: Some(price_usd),
            base_balance,
            quote_balance,
            liquidity_sol,
            timestamp: chrono::Utc::now().timestamp(),
        };

        tracing::info!(
            "💰 Calculated price for {}: ${:.6} SOL (${:.4} USD), Liquidity: {:.2} SOL | Base: {} | Quote: {}",
            token_address,
            price_sol,
            price_usd,
            liquidity_sol,
            base_balance,
            quote_balance
        );

        Ok(price_update)
    }

    /// Get SOL price in USD from Redis
    async fn get_sol_price_usd(&self) -> Result<f64, AppError> {
        self.redis_client
            .get_sol_price()
            .await?
            .ok_or_else(|| AppError::RedisError("SOL price not found in Redis".to_string()))
    }

    /// Unsubscribe from a token's vault accounts
    pub async fn unsubscribe_from_pool(&self, token_address: &str) -> Result<(), AppError> {
        // Remove pool state
        {
            let mut states = self.pool_states.write().await;
            if let Some(pool_state) = states.remove(token_address) {
                // Remove cached balances
                let mut balances = self.vault_balances.write().await;
                balances.remove(&pool_state.base_vault);
                balances.remove(&pool_state.quote_vault);
                
                // Remove subscription mappings
                let mut mappings = self.subscription_to_vault.write().await;
                mappings.retain(|_, vault_addr| {
                    *vault_addr != pool_state.base_vault && *vault_addr != pool_state.quote_vault
                });
            }
        }

        tracing::info!("Unsubscribed from pool: {}", token_address);
        Ok(())
    }

    /// Get current cached balance for a vault
    pub async fn get_vault_balance(&self, vault_address: &Pubkey) -> Option<VaultBalance> {
        let balances = self.vault_balances.read().await;
        balances.get(vault_address).cloned()
    }

    /// Get all monitored tokens
    pub async fn get_monitored_tokens(&self) -> Vec<String> {
        let states = self.pool_states.read().await;
        states.keys().cloned().collect()
    }

    /// Get health status of all subscriptions
    pub async fn get_subscription_health(&self) -> HashMap<String, bool> {
        let states = self.pool_states.read().await;
        let mut health = HashMap::new();

        for (token_address, pool_state) in states.iter() {
            let has_recent_update = pool_state
                .last_price_update
                .map(|t| t.elapsed().as_secs() < 60)
                .unwrap_or(false);
            
            health.insert(token_address.clone(), has_recent_update);
        }

        health
    }
}