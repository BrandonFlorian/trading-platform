use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use trading_common::{
    error::AppError,
    models::{ConnectionStatus, ConnectionType, PriceUpdate},
    redis::RedisPool,
    ConnectionMonitor,
};

use crate::raydium::RaydiumPool;
use crate::{price_calculator::PriceCalculator, subscriptions::PartitionedSubscriptionManager};

use crate::{config::PriceFeedConfig, websocket_message_handler::WebSocketMessageHandler};

pub struct PriceFeedService {
    pub subscription_manager: Arc<PartitionedSubscriptionManager>,
    pub message_handler: Arc<WebSocketMessageHandler>,
    pub price_calculator: Arc<PriceCalculator>,
    pub connection_monitor: Arc<ConnectionMonitor>,
    pub redis_connection: Arc<RedisPool>,
    pub rpc_client: Arc<RpcClient>,
    pub config: PriceFeedConfig,
}

impl PriceFeedService {
    pub async fn new(
        rpc_client: Arc<RpcClient>,
        connection_monitor: Arc<ConnectionMonitor>,
        redis_url: &str,
        config: PriceFeedConfig,
    ) -> Result<Self, AppError> {
        // Create Redis connection
        let redis_connection =
            Arc::new(RedisPool::new(redis_url, connection_monitor.clone()).await?);

        // Create price calculator
        let price_calculator = Arc::new(PriceCalculator::new(
            redis_connection.clone(),
            rpc_client.clone(),
        ));

        // Create message handler
        let message_handler = Arc::new(WebSocketMessageHandler::new());

        // Create subscription manager
        let subscription_manager = Arc::new(PartitionedSubscriptionManager::new(
            config.rpc_ws_url.clone(),
            rpc_client.clone(),
            redis_connection.clone(),
        ));

        Ok(Self {
            subscription_manager,
            message_handler,
            price_calculator,
            connection_monitor,
            redis_connection,
            rpc_client,
            config,
        })
    }

    pub async fn start(&self) -> Result<(), AppError> {
        // Update connection status
        self.connection_monitor
            .update_status(
                ConnectionType::WebSocket,
                ConnectionStatus::Connecting,
                None,
            )
            .await;

        // Start SOL price subscription
        tracing::info!("Starting SOL price subscription...");
        self.start_sol_price_subscription().await?;

        // Wait for initial SOL price with timeout
        self.wait_for_sol_price().await?;

        // Forward price updates to message handler
        self.forward_price_updates().await?;

        // Update connection status
        self.connection_monitor
            .update_status(ConnectionType::WebSocket, ConnectionStatus::Connected, None)
            .await;

        tracing::info!("Price feed service started successfully");
        Ok(())
    }

    async fn wait_for_sol_price(&self) -> Result<(), AppError> {
        // Wait for initial SOL price with timeout
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(10);

        while start.elapsed() < timeout {
            if let Ok(Some(sol_price)) = self.redis_connection.get_sol_price().await {
                tracing::info!("Initial SOL price received: {}", sol_price);
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        Err(AppError::RedisError(
            "Failed to get initial SOL price".into(),
        ))
    }

    async fn forward_price_updates(&self) -> Result<(), AppError> {
        // Subscribe to price updates from the subscription manager
        let mut price_rx = self.subscription_manager.subscribe_to_updates();

        // Clone the message handler for the task
        let message_handler = self.message_handler.clone();

        // Spawn a task to forward price updates to clients
        tokio::spawn(async move {
            while let Ok(update) = price_rx.recv().await {
                // Get the token address
                let token_address = update.token_address.clone();

                // Convert to a format suitable for WebSocket clients
                let ws_update = serde_json::json!({
                    "type": "price_update",
                    "data": update
                });

                // Broadcast to clients subscribed to this token
                if let Err(e) = message_handler
                    .broadcast_to_token_subscribers(&token_address, &ws_update)
                    .await
                {
                    tracing::error!(
                        "Failed to broadcast price update for {}: {}",
                        token_address,
                        e
                    );
                }
            }
        });

        Ok(())
    }

    pub async fn get_price(&self, token_address: &str) -> Result<Option<PriceUpdate>, AppError> {
        // Try to find the pool first
        let pool = match self.find_pool(token_address).await {
            Ok(pool) => pool,
            Err(AppError::PoolNotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };

        // Calculate price using the pool
        let price_update = self
            .price_calculator
            .calculate_token_price(
                &pool,
                &Pubkey::try_from(token_address.as_bytes()).map_err(|e| {
                    AppError::SerializationError(borsh::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e.to_string(),
                    ))
                })?,
            )
            .await?;

        Ok(Some(price_update))
    }

    pub async fn find_pool(&self, token_address: &str) -> Result<RaydiumPool, AppError> {
        // Delegate to subscription manager's pool finder
        self.subscription_manager.find_pool(token_address).await
    }

    pub async fn start_sol_price_subscription(&self) -> Result<(), AppError> {
        tracing::info!("Starting SOL price subscription...");
        let mut redis = self.redis_connection.subscribe_to_sol_price().await?;

        // Clone the message handler for the task
        let message_handler = self.message_handler.clone();

        tokio::spawn(async move {
            tracing::info!("Starting SOL price subscription loop...");
            while let Ok(update) = redis.recv().await {
                tracing::info!("Received SOL price update: ${:.2}", update.price_usd);

                // Convert to a format suitable for WebSocket clients
                let ws_update = serde_json::json!({
                    "type": "sol_price_update",
                    "data": {
                        "price_usd": update.price_usd,
                        "timestamp": update.timestamp
                    }
                });

                // Broadcast to all clients
                if let Err(e) = message_handler.broadcast(&ws_update).await {
                    tracing::error!("Failed to broadcast SOL price update: {}", e);
                }
            }
        });

        tracing::info!("SOL price subscription initialized");
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), AppError> {
        tracing::info!("Shutting down price feed service");

        // Shutdown the subscription manager
        if let Err(e) = self.subscription_manager.shutdown().await {
            tracing::error!("Error shutting down subscription manager: {}", e);
        }

        // Shutdown the message handler
        if let Err(e) = self.message_handler.shutdown().await {
            tracing::error!("Error shutting down message handler: {}", e);
        }

        tracing::info!("Price feed service shutdown complete");
        Ok(())
    }
}
