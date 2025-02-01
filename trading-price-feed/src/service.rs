use parking_lot::RwLock as ParkingLotRwLock;
use solana_client::rpc_client::RpcClient;
use std::sync::Arc;
use tokio::sync::RwLock;
use trading_common::{
    error::AppError,
    event_system::{Event, EventSystem},
    models::{
        ConnectionStatus, ConnectionType, PriceUpdate, PriceUpdateNotification, SolPriceUpdate,
    },
    ConnectionMonitor, RedisConnection,
};

use crate::{cache::PriceCache, config::PriceFeedConfig, pool_monitor::PoolMonitor};

pub struct PriceFeedService {
    pub pool_monitor: Arc<RwLock<PoolMonitor>>,
    price_cache: Arc<ParkingLotRwLock<PriceCache>>,
    event_system: Arc<EventSystem>,
    connection_monitor: Arc<ConnectionMonitor>,
    redis_connection: RedisConnection,
    config: PriceFeedConfig,
    current_sol_price: Arc<RwLock<Option<f64>>>,
}

impl PriceFeedService {
    pub async fn new(
        rpc_client: Arc<RpcClient>,
        event_system: Arc<EventSystem>,
        connection_monitor: Arc<ConnectionMonitor>,
        redis_url: &str,
        config: PriceFeedConfig,
    ) -> Result<Self, AppError> {
        let redis_connection = RedisConnection::new(redis_url, connection_monitor.clone()).await?;

        let service = Self {
            pool_monitor: Arc::new(RwLock::new(PoolMonitor::new(
                rpc_client,
                event_system.clone(),
                redis_connection.clone(),
                config.clone(),
            ))),
            price_cache: Arc::new(ParkingLotRwLock::new(PriceCache::new())),
            event_system,
            connection_monitor,
            redis_connection,
            config,
            current_sol_price: Arc::new(RwLock::new(None)),
        };

        // Subscribe to SOL price updates via Redis
        service.subscribe_to_sol_price().await?;

        Ok(service)
    }

    pub async fn start(&self) -> Result<(), AppError> {
        self.connection_monitor
            .update_status(
                ConnectionType::WebSocket,
                ConnectionStatus::Connecting,
                None,
            )
            .await;

        let mut pool_monitor = self.pool_monitor.write().await;
        pool_monitor.start_monitoring().await?;
        let mut price_updates = pool_monitor.subscribe_to_updates();
        drop(pool_monitor);

        // Start processing price updates
        tokio::spawn({
            let event_system = self.event_system.clone();
            let price_cache = self.price_cache.clone();
            let mut redis = self.redis_connection.clone();

            async move {
                while let Ok(price_update) = price_updates.recv().await {
                    // Update cache
                    price_cache.write().update(price_update.clone());

                    // Publish to Redis
                    if let Err(e) = redis.publish_price_update(&price_update).await {
                        tracing::error!("Failed to publish price update to Redis: {}", e);
                    }

                    // Emit event
                    event_system.emit(Event::PriceUpdate(PriceUpdateNotification {
                        data: price_update,
                        type_: "price_update".to_string(),
                    }));
                }
            }
        });

        self.connection_monitor
            .update_status(ConnectionType::WebSocket, ConnectionStatus::Connected, None)
            .await;

        Ok(())
    }

    pub async fn stop(&self) -> Result<(), AppError> {
        self.pool_monitor.write().await.stop_monitoring().await?;

        self.connection_monitor
            .update_status(
                ConnectionType::WebSocket,
                ConnectionStatus::Disconnected,
                None,
            )
            .await;

        Ok(())
    }

    async fn subscribe_to_sol_price(&self) -> Result<(), AppError> {
        let current_sol_price = self.current_sol_price.clone();
        let price_cache = self.price_cache.clone();
        let mut redis = self.redis_connection.clone(); // Just clone it here

        // Get subscription receiver
        let mut rx = redis.subscribe_to_sol_price().await?;

        tokio::spawn(async move {
            while let Ok(update) = rx.recv().await {
                tracing::debug!("Received SOL price update: ${:.2}", update.price_usd);

                // Update current SOL price
                *current_sol_price.write().await = Some(update.price_usd);

                // Update USD prices in cache
                let mut cache = price_cache.write();
                let prices = cache.get_all_prices();

                // Create updated prices with USD values
                for mut price in prices {
                    price.price_usd = Some(price.price_sol * update.price_usd);
                    price.liquidity_usd = price.liquidity.map(|l| l * update.price_usd);
                    cache.update(price);
                }

                tracing::debug!("Received message from Redis: {:?}", update);
            }
        });

        Ok(())
    }

    pub async fn get_price(&self, token_address: &str) -> Result<Option<PriceUpdate>, AppError> {
        Ok(self.price_cache.read().get_price(token_address))
    }

    pub async fn get_all_prices(&self) -> Vec<PriceUpdate> {
        self.price_cache.read().get_all_prices()
    }
}
