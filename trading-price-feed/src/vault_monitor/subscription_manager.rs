use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use trading_common::error::AppError;
use trading_common::models::PriceUpdate;

use super::{
    PoolMonitorState, VaultPriceUpdate, VaultSubscriber, WebSocketManager, PriceCalculator,
};

/// High-level subscription manager that coordinates WebSocket connections and vault monitoring
pub struct SubscriptionManager {
    vault_subscriber: Arc<VaultSubscriber>,
    ws_manager: Arc<WebSocketManager>,
    active_subscriptions: Arc<RwLock<HashMap<String, PoolMonitorState>>>,
    price_sender: tokio::sync::broadcast::Sender<PriceUpdate>,
    redis_client: Arc<trading_common::redis::RedisPool>,
    rpc_client: Arc<solana_client::rpc_client::RpcClient>,
}

impl SubscriptionManager {
    pub fn new(
        ws_url: String,
        redis_client: Arc<trading_common::redis::RedisPool>,
        rpc_client: Arc<solana_client::rpc_client::RpcClient>,
    ) -> (Self, tokio::sync::broadcast::Receiver<PriceUpdate>) {
        // Create WebSocket manager
        let (ws_manager, mut ws_message_receiver) = WebSocketManager::new(ws_url);
        let ws_manager = Arc::new(ws_manager);

        // Create price broadcast channel
        let (price_sender, price_receiver) = tokio::sync::broadcast::channel(1000);

        // Create internal vault price channel
        let (vault_price_sender, mut vault_price_receiver) = mpsc::channel(1000);

        // Create vault subscriber
        let vault_subscriber = Arc::new(VaultSubscriber::new(
            Arc::clone(&ws_manager),
            vault_price_sender,
            Arc::clone(&redis_client),
        ));

        let manager = Self {
            vault_subscriber: Arc::clone(&vault_subscriber),
            ws_manager,
            active_subscriptions: Arc::new(RwLock::new(HashMap::new())),
            price_sender: price_sender.clone(),
            redis_client: Arc::clone(&redis_client),
            rpc_client: Arc::clone(&rpc_client),
        };

        // Start WebSocket message processing task
        let vault_subscriber_clone = Arc::clone(&vault_subscriber);
        tokio::spawn(async move {
            while let Some((subscription_id, message)) = ws_message_receiver.recv().await {
                if let Err(e) = vault_subscriber_clone
                    .process_vault_notification(subscription_id, message)
                    .await
                {
                    tracing::error!("Failed to process vault notification: {}", e);
                }
            }
        });

        // Start vault price update processing task
        let active_subscriptions = Arc::clone(&manager.active_subscriptions);
        let redis_client_clone = Arc::clone(&redis_client);
        let rpc_client_clone = Arc::clone(&rpc_client);
        let price_sender_clone = price_sender.clone();
        
        tokio::spawn(async move {
            while let Some(vault_update) = vault_price_receiver.recv().await {
                if let Err(e) = Self::process_vault_price_update(
                    vault_update,
                    &active_subscriptions,
                    &redis_client_clone,
                    &rpc_client_clone,
                    &price_sender_clone,
                ).await {
                    tracing::error!("Failed to process vault price update: {}", e);
                }
            }
        });

        (manager, price_receiver)
    }

    /// Subscribe to price updates for a token
    pub async fn subscribe_to_token(
        &self,
        token_address: String,
        pool_state: PoolMonitorState,
    ) -> Result<(), AppError> {
        tracing::info!("Starting subscription for token: {}", token_address);

        // Subscribe to vault accounts
        self.vault_subscriber
            .subscribe_to_pool(token_address.clone(), pool_state.clone())
            .await?;

        // Store the subscription
        {
            let mut subscriptions = self.active_subscriptions.write().await;
            subscriptions.insert(token_address.clone(), pool_state);
        }

        tracing::info!("Successfully subscribed to token: {}", token_address);
        Ok(())
    }

    /// Unsubscribe from token price updates
    pub async fn unsubscribe_from_token(&self, token_address: &str) -> Result<(), AppError> {
        tracing::info!("Unsubscribing from token: {}", token_address);

        // Remove from vault subscriber
        self.vault_subscriber
            .unsubscribe_from_pool(token_address)
            .await?;

        // Remove from active subscriptions
        {
            let mut subscriptions = self.active_subscriptions.write().await;
            subscriptions.remove(token_address);
        }

        tracing::info!("Successfully unsubscribed from token: {}", token_address);
        Ok(())
    }

    /// Process vault price update and convert to standard format
    async fn process_vault_price_update(
        vault_update: VaultPriceUpdate,
        active_subscriptions: &Arc<RwLock<HashMap<String, PoolMonitorState>>>,
        redis_client: &Arc<trading_common::redis::RedisPool>,
        rpc_client: &Arc<solana_client::rpc_client::RpcClient>,
        price_sender: &tokio::sync::broadcast::Sender<PriceUpdate>,
    ) -> Result<(), AppError> {
        let pool_state = {
            let subscriptions = active_subscriptions.read().await;
            subscriptions.get(&vault_update.token_address).cloned()
        };

        let pool_state = match pool_state {
            Some(state) => state,
            None => {
                tracing::warn!("Received price update for untracked token: {}", vault_update.token_address);
                return Ok(());
            }
        };

        // Get SOL price
        let sol_price_usd = redis_client
            .get_sol_price()
            .await?
            .unwrap_or(0.0);

        // Convert to standard price update format
        let price_update = PriceCalculator::convert_to_price_update(
            vault_update.clone(),
            &pool_state,
            sol_price_usd,
            rpc_client,
        ).await?;

        // Validate price data using the actual balances from the vault update
        PriceCalculator::validate_price_data(
            price_update.price_sol,
            vault_update.base_balance,
            vault_update.quote_balance,
        )?;

        // Log the price update before broadcasting
        tracing::info!(
            "🎯 PRICE UPDATE: {} = ${:.6} SOL (${:.4} USD) | Market Cap: ${:.0} | Liquidity: {:.2} SOL (${:.0} USD)",
            price_update.token_address,
            price_update.price_sol,
            price_update.price_usd.unwrap_or(0.0),
            price_update.market_cap,
            price_update.liquidity.unwrap_or(0.0),
            price_update.liquidity_usd.unwrap_or(0.0)
        );

        // Broadcast the price update
        if let Err(e) = price_sender.send(price_update) {
            tracing::error!("Failed to broadcast price update: {}", e);
        } else {
            tracing::debug!("Successfully broadcasted price update to {} subscribers", price_sender.receiver_count());
        }

        Ok(())
    }

    /// Get list of actively monitored tokens
    pub async fn get_active_tokens(&self) -> Vec<String> {
        let subscriptions = self.active_subscriptions.read().await;
        subscriptions.keys().cloned().collect()
    }

    /// Get subscription health status
    pub async fn get_health_status(&self) -> SubscriptionHealthStatus {
        let ws_health = self.ws_manager.get_connection_health().await;
        let vault_health = self.vault_subscriber.get_subscription_health().await;
        let active_tokens = self.get_active_tokens().await;

        let healthy_connections = ws_health.values().filter(|&&healthy| healthy).count();
        let total_connections = ws_health.len();
        
        let healthy_subscriptions = vault_health.values().filter(|&&healthy| healthy).count();
        let total_subscriptions = vault_health.len();

        SubscriptionHealthStatus {
            total_connections,
            healthy_connections,
            total_subscriptions,
            healthy_subscriptions,
            active_tokens: active_tokens.len(),
            connection_details: ws_health,
            subscription_details: vault_health,
        }
    }

    /// Force refresh all subscriptions (useful for recovery)
    pub async fn refresh_all_subscriptions(&self) -> Result<(), AppError> {
        let tokens = self.get_active_tokens().await;
        
        for token_address in tokens {
            if let Err(e) = self.refresh_token_subscription(&token_address).await {
                tracing::error!("Failed to refresh subscription for {}: {}", token_address, e);
            }
        }

        Ok(())
    }

    /// Refresh subscription for a specific token
    async fn refresh_token_subscription(&self, token_address: &str) -> Result<(), AppError> {
        let pool_state = {
            let subscriptions = self.active_subscriptions.read().await;
            subscriptions.get(token_address).cloned()
        };

        if let Some(pool_state) = pool_state {
            // Unsubscribe and resubscribe
            self.unsubscribe_from_token(token_address).await?;
            self.subscribe_to_token(token_address.to_string(), pool_state).await?;
        }

        Ok(())
    }

    /// Get current cached price for a token
    pub async fn get_cached_price(&self, token_address: &str) -> Option<VaultPriceUpdate> {
        // This would require adding price caching to the vault subscriber
        // For now, returning None as this would need additional implementation
        None
    }

    /// Get subscription metrics
    pub async fn get_metrics(&self) -> SubscriptionMetrics {
        let health = self.get_health_status().await;
        
        SubscriptionMetrics {
            total_tokens_monitored: health.active_tokens,
            healthy_subscriptions: health.healthy_subscriptions,
            total_subscriptions: health.total_subscriptions,
            active_connections: health.healthy_connections,
            total_connections: health.total_connections,
            uptime_percentage: if health.total_subscriptions > 0 {
                (health.healthy_subscriptions as f64 / health.total_subscriptions as f64) * 100.0
            } else {
                0.0
            },
        }
    }
}

/// Health status information for all subscriptions
#[derive(Debug, Clone)]
pub struct SubscriptionHealthStatus {
    pub total_connections: usize,
    pub healthy_connections: usize,
    pub total_subscriptions: usize,
    pub healthy_subscriptions: usize,
    pub active_tokens: usize,
    pub connection_details: HashMap<String, bool>,
    pub subscription_details: HashMap<String, bool>,
}

/// Metrics for monitoring subscription performance
#[derive(Debug, Clone)]
pub struct SubscriptionMetrics {
    pub total_tokens_monitored: usize,
    pub healthy_subscriptions: usize,
    pub total_subscriptions: usize,
    pub active_connections: usize,
    pub total_connections: usize,
    pub uptime_percentage: f64,
}