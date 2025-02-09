use super::RedisConnection;
use crate::{
    constants::{PRICE_UPDATES_CHANNEL, SETTINGS_CHANNEL, TRACKED_WALLETS_CHANNEL},
    error::AppError,
    event_system::{Event, EventSystem},
    models::{
        CopyTradeSettings, PriceUpdate, PriceUpdateNotification, SettingsUpdateNotification,
        SolPriceUpdate, WalletStateChange, WalletStateChangeType, WalletStateNotification,
    },
};
use redis::{aio::ConnectionManagerConfig, AsyncCommands};
use redis::{PushInfo, PushKind};
use serde_json::{self, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::{broadcast, mpsc};
use tokio::time::Instant;

const RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRIES: u32 = 5;

impl RedisConnection {
    // Publishing methods
    pub async fn publish_with_retry<T: serde::Serialize>(
        &self,
        channel: &str,
        payload: &T,
    ) -> Result<(), AppError> {
        let msg = serde_json::to_string(payload)
            .map_err(|e| AppError::JsonParseError(format!("Failed to serialize payload: {}", e)))?;

        let mut retries = 0;
        loop {
            let mut connection = self.connection.lock().await;
            match connection.publish::<_, _, i32>(channel, msg.clone()).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if retries >= MAX_RETRIES {
                        return Err(AppError::RedisError(format!(
                            "Failed to publish after {} retries: {}",
                            MAX_RETRIES, e
                        )));
                    }
                    retries += 1;
                    tokio::time::sleep(RECONNECT_DELAY).await;
                }
            }
        }
    }

    pub async fn publish_tracked_wallet_update(
        &self,
        wallet: &str,
        action: &str,
    ) -> Result<(), AppError> {
        let payload = json!({
            "wallet_address": wallet,
            "action": action,
        });
        self.publish_with_retry(TRACKED_WALLETS_CHANNEL, &payload)
            .await
    }

    pub async fn publish_settings_update(
        &self,
        settings: &CopyTradeSettings,
        action: &str,
    ) -> Result<(), AppError> {
        let payload = json!({
            "settings": settings,
            "action": action,
        });
        self.publish_with_retry(SETTINGS_CHANNEL, &payload).await
    }

    pub async fn publish_price_update(
        &mut self,
        price_update: &PriceUpdate,
    ) -> Result<(), AppError> {
        self.publish_with_retry(PRICE_UPDATES_CHANNEL, price_update)
            .await
    }

    pub async fn publish_sol_price_update(
        &self,
        price_update: &SolPriceUpdate,
    ) -> Result<(), AppError> {
        self.publish_with_retry("sol_price_updates", price_update)
            .await
    }

    // Subscription methods
    pub async fn subscribe_to_updates(
        redis_url: &str,
        event_system: Arc<EventSystem>,
    ) -> Result<(), AppError> {
        let (tx, rx) = mpsc::unbounded_channel();
        let client = Self::create_redis_client(redis_url)?;
        let config = ConnectionManagerConfig::new().set_push_sender(tx);

        let mut con = client
            .get_connection_manager_with_config(config)
            .await
            .map_err(|e| AppError::RedisError(e.to_string()))?;

        // Subscribe to channels
        for channel in [
            SETTINGS_CHANNEL,
            TRACKED_WALLETS_CHANNEL,
            PRICE_UPDATES_CHANNEL,
        ] {
            con.subscribe(channel)
                .await
                .map_err(|e| AppError::RedisError(format!("Failed to subscribe: {}", e)))?;
        }

        // Start message handler
        Self::spawn_message_handler(rx, event_system);

        // Keep connection alive with ping
        Self::spawn_keepalive(con);

        Ok(())
    }

    pub async fn subscribe_to_sol_price(
        &self,
    ) -> Result<broadcast::Receiver<SolPriceUpdate>, AppError> {
        let (tx, rx) = broadcast::channel(100);
        let connection = self.connection.clone();

        tokio::spawn(async move {
            let mut connection = connection.lock().await;
            let mut last_error_time = None;

            loop {
                match Self::handle_sol_price_subscription(&mut connection, &tx).await {
                    Ok(()) => break,
                    Err(e) => {
                        let now = Instant::now();
                        if last_error_time
                            .map_or(true, |t: Instant| now.duration_since(t).as_secs() > 60)
                        {
                            tracing::error!("SOL price subscription error: {}", e);
                            last_error_time = Some(now);
                        }
                        tokio::time::sleep(RECONNECT_DELAY).await;
                    }
                }
            }
        });

        Ok(rx)
    }

    // Private helper methods
    async fn handle_sol_price_subscription(
        connection: &mut redis::aio::ConnectionManager,
        tx: &broadcast::Sender<SolPriceUpdate>,
    ) -> Result<(), AppError> {
        let (_, mut push_rx) = mpsc::unbounded_channel::<PushInfo>();

        connection
            .subscribe("sol_price_updates")
            .await
            .map_err(|e| AppError::RedisError(e.to_string()))?;

        while let Some(msg) = push_rx.recv().await {
            if msg.kind == redis::PushKind::Message && msg.data.len() >= 2 {
                if let Ok(payload) = redis::from_redis_value::<String>(&msg.data[1]) {
                    if let Ok(update) = serde_json::from_str::<SolPriceUpdate>(&payload) {
                        if let Err(e) = tx.send(update) {
                            tracing::error!("Failed to forward SOL price update: {}", e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn spawn_message_handler(
        mut rx: mpsc::UnboundedReceiver<PushInfo>,
        event_system: Arc<EventSystem>,
    ) {
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if msg.kind == PushKind::Message && msg.data.len() >= 2 {
                    if let Ok(payload) = redis::from_redis_value::<String>(&msg.data[1]) {
                        if let Ok(channel) = redis::from_redis_value::<String>(&msg.data[0]) {
                            Self::handle_channel_message(&channel, &payload, &event_system).await;
                        }
                    }
                }
            }
        });
    }

    async fn handle_channel_message(channel: &str, payload: &str, event_system: &EventSystem) {
        match channel {
            SETTINGS_CHANNEL => Self::handle_settings_update(payload, event_system),
            TRACKED_WALLETS_CHANNEL => Self::handle_wallet_update(payload, event_system),
            PRICE_UPDATES_CHANNEL => Self::handle_price_update(payload, event_system),
            _ => tracing::warn!("Unknown channel: {}", channel),
        }
    }

    fn handle_settings_update(payload: &str, event_system: &EventSystem) {
        if let Ok(settings) = serde_json::from_str::<CopyTradeSettings>(payload) {
            event_system.emit(Event::SettingsUpdate(SettingsUpdateNotification {
                data: settings,
                type_: "settings_updated".to_string(),
            }));
        }
    }

    fn handle_wallet_update(payload: &str, event_system: &EventSystem) {
        if let Ok(update) = serde_json::from_str::<serde_json::Value>(payload) {
            if let Some(action) = update["action"].as_str() {
                let wallet_type = match action {
                    "add" => WalletStateChangeType::Added,
                    "archive" => WalletStateChangeType::Archived,
                    "unarchive" => WalletStateChangeType::Unarchived,
                    "delete" => WalletStateChangeType::Deleted,
                    _ => return,
                };

                event_system.emit(Event::WalletStateChange(WalletStateNotification {
                    data: WalletStateChange::new(
                        update["wallet_address"].as_str().unwrap_or("").to_string(),
                        wallet_type,
                    )
                    .with_details(update.clone()),
                    type_: "wallet_state_change".to_string(),
                }));
            }
        }
    }

    fn handle_price_update(payload: &str, event_system: &EventSystem) {
        if let Ok(price_update) = serde_json::from_str::<PriceUpdate>(payload) {
            event_system.emit(Event::PriceUpdate(PriceUpdateNotification {
                data: price_update,
                type_: "price_update".to_string(),
            }));
        }
    }
}
