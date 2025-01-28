use parking_lot::RwLock as ParkingLotRwLock;
use solana_client::rpc_client::RpcClient;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use trading_common::{
    error::AppError,
    event_system::{Event, EventSystem},
    models::{ConnectionStatus, ConnectionType, PriceUpdate, PriceUpdateNotification},
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

        let pool_monitor = Arc::new(RwLock::new(PoolMonitor::new(
            rpc_client,
            event_system.clone(),
            connection_monitor.clone(),
            config.clone(),
        )));

        let price_cache = Arc::new(ParkingLotRwLock::new(PriceCache::new()));

        Ok(Self {
            pool_monitor,
            price_cache,
            event_system,
            connection_monitor,
            redis_connection,
            config,
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

        // Start monitoring pools
        let mut pool_monitor = self.pool_monitor.write().await;
        pool_monitor.start_monitoring().await?;

        // Subscribe to price updates from pool monitor
        let mut price_updates = pool_monitor.subscribe_to_updates();

        // Start processing price updates
        tokio::spawn({
            let event_system = self.event_system.clone();
            let price_cache = self.price_cache.clone();
            let mut redis = self.redis_connection.clone();

            async move {
                while let Some(price_update) = price_updates.next().await {
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

    pub async fn get_price(&self, token_address: &str) -> Result<Option<PriceUpdate>, AppError> {
        Ok(self.price_cache.read().get_price(token_address))
    }

    pub async fn get_all_prices(&self) -> Vec<PriceUpdate> {
        self.price_cache.read().get_all_prices()
    }
}
