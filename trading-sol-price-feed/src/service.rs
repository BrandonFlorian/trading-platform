use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use trading_common::{
    error::AppError,
    event_system::{Event, EventSystem},
    models::{ConnectionStatus, ConnectionType, SolPriceUpdate, SolPriceUpdateNotification},
    redis::RedisPool,
    ConnectionMonitor,
};

use crate::price_monitor::PriceMonitor;

pub struct SolPriceFeedService {
    price_monitor: Arc<PriceMonitor>,
    event_system: Arc<EventSystem>,
    connection_monitor: Arc<ConnectionMonitor>,
    redis_connection: Arc<RedisPool>,
    current_price: Arc<RwLock<Option<SolPriceUpdate>>>,
    price_sender: broadcast::Sender<SolPriceUpdate>,
    price_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

impl SolPriceFeedService {
    pub async fn new(
        rpc_client: Arc<solana_client::rpc_client::RpcClient>,
        event_system: Arc<EventSystem>,
        connection_monitor: Arc<ConnectionMonitor>,
        redis_url: &str,
    ) -> Result<Self, AppError> {
        let redis_connection = RedisPool::new(redis_url, connection_monitor.clone()).await?;
        let (price_sender, _) = broadcast::channel::<SolPriceUpdate>(100);

        let monitor_price_sender = price_sender.clone();
        let price_monitor = Arc::new(PriceMonitor::new(
            rpc_client,
            event_system.clone(),
            connection_monitor.clone(),
            monitor_price_sender,
        ));

        Ok(Self {
            price_monitor,
            event_system,
            connection_monitor,
            redis_connection: Arc::new(redis_connection),
            current_price: Arc::new(RwLock::new(None)),
            price_sender,
            price_task: Arc::new(RwLock::new(None)),
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

        self.price_monitor.start_monitoring().await?;

        // Subscribe to price updates to maintain current price
        let mut price_rx = self.price_sender.subscribe();
        let current_price = self.current_price.clone();
        let event_system = self.event_system.clone();

        let task = tokio::spawn({
            let redis_connection = self.redis_connection.clone();
            async move {
                tracing::info!("Price update task started");
                while let Ok(price_update) = price_rx.recv().await {
                    tracing::info!("Received price update: ${:.2}", price_update.price_usd);

                    // Update current price
                    *current_price.write().await = Some(price_update.clone());

                    // Publish to Redis
                    let price_json = serde_json::to_string(&price_update).unwrap();
                    tracing::info!("Attempting to publish to Redis: {}", price_json);
                    if let Err(e) = redis_connection
                        .publish_sol_price_update(&price_update)
                        .await
                    {
                        tracing::error!("Failed to publish SOL price update to Redis: {}", e);
                    } else {
                        tracing::debug!("Published SOL price to Redis: {}", price_json);
                    }

                    let notification = SolPriceUpdateNotification {
                        data: price_update,
                        type_: "sol_price_update".to_string(),
                    };

                    // Emit event
                    event_system.emit(Event::SolPriceUpdate(notification));
                }
            }
        });

        // Store task handle
        *self.price_task.write().await = Some(task);

        self.connection_monitor
            .update_status(ConnectionType::WebSocket, ConnectionStatus::Connected, None)
            .await;

        Ok(())
    }

    pub async fn stop(&self) -> Result<(), AppError> {
        // Stop the price monitor
        self.price_monitor.stop_monitoring().await?;

        // Abort the price update task if it exists
        if let Some(task) = self.price_task.write().await.take() {
            task.abort();
        }

        self.connection_monitor
            .update_status(
                ConnectionType::WebSocket,
                ConnectionStatus::Disconnected,
                None,
            )
            .await;

        Ok(())
    }

    /// Get the current price (async version - preferred)
    pub async fn get_current_price(&self) -> Option<SolPriceUpdate> {
        self.current_price.read().await.clone()
    }

    /// Try to get the current price without blocking (non-blocking version)
    /// Returns None if the lock is currently held by another task
    pub fn try_get_current_price(&self) -> Option<SolPriceUpdate> {
        self.current_price.try_read().ok()?.clone()
    }

    /// Get the current price with a timeout (fallback for edge cases)
    pub async fn get_current_price_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Option<SolPriceUpdate>, AppError> {
        match tokio::time::timeout(timeout, self.current_price.read()).await {
            Ok(guard) => Ok(guard.clone()),
            Err(_) => Err(AppError::TimeoutError(
                "Timeout waiting for price lock".to_string(),
            )),
        }
    }

    pub fn subscribe_to_updates(&self) -> broadcast::Receiver<SolPriceUpdate> {
        self.price_sender.subscribe()
    }
}
