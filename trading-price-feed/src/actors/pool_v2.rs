use solana_client::rpc_client::RpcClient;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot};
use trading_common::error::AppError;
use trading_common::models::PriceUpdate;
use trading_common::redis::RedisPool;

use crate::models::{PoolCommand, PoolEvent, PoolState};
use crate::raydium::RaydiumPoolFinder;
use crate::vault_monitor::{PoolMonitorState, SubscriptionManager};

/// New pool actor that uses vault-based monitoring
pub struct PoolActorV2 {
    token_address: String,
    command_rx: mpsc::Receiver<PoolCommand>,
    event_tx: broadcast::Sender<PoolEvent>,
    subscription_manager: Arc<SubscriptionManager>,
    rpc_client: Arc<RpcClient>,
    redis_client: Arc<RedisPool>,
    state: PoolState,
    price_receiver: Option<broadcast::Receiver<PriceUpdate>>,
}

impl PoolActorV2 {
    pub fn new(
        token_address: String,
        command_rx: mpsc::Receiver<PoolCommand>,
        event_tx: broadcast::Sender<PoolEvent>,
        subscription_manager: Arc<SubscriptionManager>,
        rpc_client: Arc<RpcClient>,
        redis_client: Arc<RedisPool>,
    ) -> Self {
        Self {
            token_address,
            command_rx,
            event_tx,
            subscription_manager,
            rpc_client,
            redis_client,
            state: PoolState::Initializing,
            price_receiver: None,
        }
    }

    pub async fn run(mut self) {
        tracing::info!("Starting PoolActorV2 for token {}", self.token_address);

        let mut health_check_interval = tokio::time::interval(std::time::Duration::from_secs(30));

        loop {
            tokio::select! {
                Some(cmd) = self.command_rx.recv() => {
                    match cmd {
                        PoolCommand::Initialize { response_tx } => {
                            let result = self.initialize_pool().await;
                            let _ = response_tx.send(result);
                        }
                        PoolCommand::GetPrice { response_tx } => {
                            let result = self.get_current_price().await;
                            let _ = response_tx.send(result);
                        }
                        PoolCommand::Shutdown { response_tx } => {
                            let result = self.handle_shutdown().await;
                            let _ = response_tx.send(result);
                            break;
                        }
                    }
                }
                Ok(price_update) = async {
                    if let Some(receiver) = &mut self.price_receiver {
                        receiver.recv().await
                    } else {
                        futures_util::future::pending().await
                    }
                }, if self.price_receiver.is_some() => {
                    // Forward price update to event system
                    if price_update.token_address == self.token_address {
                        self.handle_price_update(price_update).await;
                    }
                }
                _ = health_check_interval.tick() => {
                    self.check_health().await;
                }
            }
        }

        tracing::info!("PoolActorV2 for {} shutting down", self.token_address);
    }

    async fn initialize_pool(&mut self) -> Result<(), AppError> {
        tracing::info!("Initializing pool for token {}", self.token_address);
        self.update_state(PoolState::Initializing).await;

        // Find the Raydium pool
        let pool_finder = RaydiumPoolFinder::new(
            Arc::clone(&self.rpc_client),
            Arc::clone(&self.redis_client),
        );
        
        let mut pool = pool_finder.find_pool(&self.token_address).await?;
        
        // Load metadata (decimals, etc.)
        pool.load_metadata(&self.rpc_client, &self.redis_client).await?;

        tracing::info!(
            "Found Raydium pool {} for token {}",
            pool.address.to_string(),
            self.token_address
        );

        // Create pool monitor state
        let pool_state = PoolMonitorState {
            token_address: self.token_address.clone(),
            pool_address: pool.address,
            base_vault: pool.base_vault,
            quote_vault: pool.quote_vault,
            base_decimals: pool.base_decimals,
            quote_decimals: pool.quote_decimals,
            base_balance: None,
            quote_balance: None,
            last_price_update: None,
        };

        // Subscribe to vault monitoring
        self.subscription_manager
            .subscribe_to_token(self.token_address.clone(), pool_state)
            .await?;

        // Get a price receiver for this specific token
        let (_, price_receiver) = SubscriptionManager::new(
            std::env::var("SOLANA_RPC_WS_URL").unwrap_or_default(),
            Arc::clone(&self.redis_client),
        );
        self.price_receiver = Some(price_receiver);

        self.update_state(PoolState::Active).await;
        tracing::info!("Pool for token {} initialized successfully", self.token_address);

        Ok(())
    }

    async fn handle_price_update(&mut self, price_update: PriceUpdate) {
        tracing::debug!(
            "Received price update for {}: ${:.6} SOL",
            self.token_address,
            price_update.price_sol
        );

        // Publish price update event
        let event = PoolEvent::PriceUpdated(price_update);
        if let Err(e) = self.event_tx.send(event) {
            tracing::error!("Failed to publish price update: {}", e);
        }
    }

    async fn get_current_price(&self) -> Result<PriceUpdate, AppError> {
        // Try to get cached price from subscription manager
        if let Some(cached_price) = self.subscription_manager.get_cached_price(&self.token_address).await {
            let sol_price = self.redis_client
                .get_sol_price()
                .await?
                .unwrap_or(0.0);

            return Ok(PriceUpdate {
                token_address: self.token_address.clone(),
                price_sol: cached_price.price_sol,
                price_usd: cached_price.price_usd,
                market_cap: 0.0, // Would need additional calculation
                timestamp: cached_price.timestamp,
                dex_type: trading_common::dex::DexType::Raydium,
                liquidity: Some(cached_price.liquidity_sol),
                liquidity_usd: Some(cached_price.liquidity_sol * sol_price),
                pool_address: None, // Would need to store this
                volume_24h: None,
                volume_6h: None,
                volume_1h: None,
                volume_5m: None,
            });
        }

        Err(AppError::InternalError("No cached price available".to_string()))
    }

    async fn check_health(&mut self) {
        let health = self.subscription_manager.get_health_status().await;
        
        if let Some(is_healthy) = health.subscription_details.get(&self.token_address) {
            if !is_healthy && matches!(self.state, PoolState::Active) {
                tracing::warn!("Pool {} subscription is unhealthy", self.token_address);
                self.update_state(PoolState::Reconnecting { attempt: 1 }).await;
                
                // Try to refresh the subscription
                if let Err(e) = self.subscription_manager.refresh_all_subscriptions().await {
                    tracing::error!("Failed to refresh subscriptions: {}", e);
                    self.update_state(PoolState::Failed {
                        error: e.to_string(),
                    }).await;
                } else {
                    self.update_state(PoolState::Active).await;
                }
            }
        }
    }

    async fn handle_shutdown(&mut self) -> Result<(), AppError> {
        tracing::info!("Shutting down pool actor for {}", self.token_address);

        // Unsubscribe from vault monitoring
        self.subscription_manager
            .unsubscribe_from_token(&self.token_address)
            .await?;

        Ok(())
    }

    async fn update_state(&mut self, new_state: PoolState) {
        if self.state != new_state {
            tracing::info!(
                "Pool {} state changed from {:?} to {:?}",
                self.token_address,
                self.state,
                new_state
            );

            self.state = new_state.clone();

            let event = PoolEvent::StateChanged {
                token_address: self.token_address.clone(),
                state: new_state,
            };

            if let Err(e) = self.event_tx.send(event) {
                tracing::error!("Failed to publish state change: {}", e);
            }
        }
    }
}

/// Factory for creating new V2 pool actors
pub struct PoolFactoryV2 {
    subscription_manager: Arc<SubscriptionManager>,
    rpc_client: Arc<RpcClient>,
    redis_client: Arc<RedisPool>,
    event_tx: broadcast::Sender<PoolEvent>,
}

impl PoolFactoryV2 {
    pub fn new(
        subscription_manager: Arc<SubscriptionManager>,
        rpc_client: Arc<RpcClient>,
        redis_client: Arc<RedisPool>,
    ) -> (Self, broadcast::Receiver<PoolEvent>) {
        let (event_tx, event_rx) = broadcast::channel(1000);

        let factory = Self {
            subscription_manager,
            rpc_client,
            redis_client,
            event_tx,
        };

        (factory, event_rx)
    }

    pub fn create_pool(
        &self,
        token_address: String,
    ) -> (mpsc::Sender<PoolCommand>, tokio::task::JoinHandle<()>) {
        let (command_tx, command_rx) = mpsc::channel(100);

        let pool = PoolActorV2::new(
            token_address,
            command_rx,
            self.event_tx.clone(),
            Arc::clone(&self.subscription_manager),
            Arc::clone(&self.rpc_client),
            Arc::clone(&self.redis_client),
        );

        let handle = tokio::spawn(async move {
            pool.run().await;
        });

        (command_tx, handle)
    }
}