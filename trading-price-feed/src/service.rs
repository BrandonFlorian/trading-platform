use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::time::Duration;
use std::{str::FromStr, sync::Arc};
use tokio::sync::{broadcast, RwLock};
use trading_common::dex::DexType;
use trading_common::{
    error::AppError,
    event_system::EventSystem,
    models::{ConnectionStatus, ConnectionType, PriceUpdate},
    redis::RedisConnection,
    ConnectionMonitor,
};

use crate::config::PriceFeedConfig;
use crate::pool_monitor_websocket::PoolWebSocketMonitor;
use crate::price_calculator::PriceCalculator;

pub struct PriceFeedService {
    pub pool_monitor: Arc<PoolWebSocketMonitor>,
    pub price_calculator: Arc<PriceCalculator>,
    pub event_system: Arc<EventSystem>,
    pub connection_monitor: Arc<ConnectionMonitor>,
    pub redis_connection: Arc<RedisConnection>,
    pub config: PriceFeedConfig,
    pub rpc_client: Arc<RpcClient>,
}

impl PriceFeedService {
    pub async fn new(
        rpc_client: Arc<RpcClient>,
        event_system: Arc<EventSystem>,
        connection_monitor: Arc<ConnectionMonitor>,
        redis_url: &str,
        config: PriceFeedConfig,
    ) -> Result<Self, AppError> {
        let redis_connection =
            Arc::new(RedisConnection::new(redis_url, connection_monitor.clone()).await?);

        // Create price broadcast channel
        let (price_sender, _) = broadcast::channel(1000);

        let price_calculator = Arc::new(PriceCalculator::new(
            price_sender.clone(),
            redis_connection.clone(),
            rpc_client.clone(),
        ));

        // Create the client subscriptions map
        let client_subscriptions = Arc::new(RwLock::new(HashMap::new()));

        // Create pool monitor with correct parameters
        let pool_monitor = Arc::new(PoolWebSocketMonitor::new(
            config.rpc_ws_url.clone(),
            price_sender,
            rpc_client.clone(),
            redis_connection.clone(),
            client_subscriptions.clone(),
        ));

        Ok(Self {
            pool_monitor,
            price_calculator,
            event_system,
            connection_monitor,
            redis_connection,
            config,
            rpc_client,
        })
    }

    pub async fn start(&self) -> Result<(), AppError> {
        self.connection_monitor
            .update_status(
                ConnectionType::WebSocket,
                ConnectionStatus::Connecting,
                None,
            )
            .await;

        // Start sol price subscription first
        self.start_sol_price_subscription().await?;

        // Start WebSocket pool monitoring with error handling and reconnection
        let monitor_handle = tokio::spawn({
            let pool_monitor = self.pool_monitor.clone();
            let connection_monitor = self.connection_monitor.clone();
            async move {
                loop {
                    match pool_monitor.start().await {
                        Ok(()) => {
                            tracing::info!("Pool monitor stopped normally");
                            break;
                        }
                        Err(e) => {
                            tracing::error!("Pool monitor error: {}", e);
                            connection_monitor
                                .update_status(
                                    ConnectionType::WebSocket,
                                    ConnectionStatus::Error,
                                    Some(e.to_string()),
                                )
                                .await;

                            // Wait before retry
                            tokio::time::sleep(Duration::from_secs(5)).await;

                            connection_monitor
                                .update_status(
                                    ConnectionType::WebSocket,
                                    ConnectionStatus::Connecting,
                                    None,
                                )
                                .await;
                        }
                    }
                }
            }
        });

        // Just monitor the pool monitor task
        tokio::spawn(async move {
            if let Err(e) = monitor_handle.await {
                tracing::error!("Monitor task error: {}", e);
            }
        });

        self.connection_monitor
            .update_status(ConnectionType::WebSocket, ConnectionStatus::Connected, None)
            .await;

        Ok(())
    }

    pub async fn get_price(&self, token_address: &str) -> Result<Option<PriceUpdate>, AppError> {
        let pubkey = Pubkey::from_str(token_address)?;

        // Handle the potential error from find_pool
        match self.pool_monitor.find_pool(&pubkey.to_string()).await {
            Ok(pool) => {
                let price_update = self
                    .price_calculator
                    .calculate_token_price(&pool, &pubkey)
                    .await?;
                Ok(Some(price_update))
            }
            Err(AppError::PoolNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn start_sol_price_subscription(&self) -> Result<(), AppError> {
        let mut redis = self.redis_connection.subscribe_to_sol_price().await?;
        let price_calculator = Arc::clone(&self.price_calculator);

        tokio::spawn(async move {
            while let Ok(update) = redis.recv().await {
                price_calculator.update_sol_price(update).await;
            }
        });
        Ok(())
    }
}
