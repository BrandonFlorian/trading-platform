use crate::{
    constants::{PRICE_UPDATES_CHANNEL, SETTINGS_CHANNEL, TRACKED_WALLETS_CHANNEL},
    error::AppError,
    event_system::{Event, EventSystem},
    models::{
        ConnectionStatus, ConnectionType, CopyTradeSettings, PriceUpdate, PriceUpdateNotification,
        SettingsUpdateNotification, SolPriceUpdate, SolPriceUpdateNotification, WalletStateChange,
        WalletStateChangeType, WalletStateNotification,
    },
    ConnectionMonitor, TrackedWallet,
};

use redis::AsyncConnectionConfig;
use redis::{aio::ConnectionManager, AsyncCommands};
use serde_json::{self, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::{
    sync::{broadcast, mpsc},
    time::Instant,
};

const RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRIES: u32 = 5;

#[derive(Clone)]
pub struct RedisConnection {
    connection: ConnectionManager,
    connection_monitor: Arc<ConnectionMonitor>,
}

impl RedisConnection {
    pub async fn new(
        redis_url: &str,
        connection_monitor: Arc<ConnectionMonitor>,
    ) -> Result<Self, AppError> {
        println!("Creating Redis connection");
        let redis_url = if !redis_url.contains("protocol=resp3") {
            if redis_url.contains('?') {
                format!("{}&protocol=resp3", redis_url)
            } else {
                format!("{}?protocol=resp3", redis_url)
            }
        } else {
            redis_url.to_string()
        };

        let client = redis::Client::open(redis_url)
            .map_err(|e| AppError::Generic(format!("Failed to create Redis client: {}", e)))?;

        match ConnectionManager::new(client.clone()).await {
            Ok(connection) => {
                connection_monitor
                    .update_status(ConnectionType::Redis, ConnectionStatus::Connected, None)
                    .await;

                Ok(Self {
                    connection,
                    connection_monitor,
                })
            }
            Err(e) => {
                connection_monitor
                    .update_status(
                        ConnectionType::Redis,
                        ConnectionStatus::Error,
                        Some(e.to_string()),
                    )
                    .await;
                Err(AppError::Generic(format!(
                    "Failed to create Redis connection: {}",
                    e
                )))
            }
        }
    }

    pub async fn publish_tracked_wallet_update(
        &mut self,
        wallet: &TrackedWallet,
        action: &str, // "add", "archive", "unarchive", "delete"
    ) -> Result<(), AppError> {
        println!("Publishing tracked wallet update: {:?}", wallet.clone());
        let payload = json!({
            "wallet_address": wallet.wallet_address,
            "action": action,
            "is_active": wallet.is_active,
            "id": wallet.id,
        });
        println!("Publishing tracked wallet update: {:?}", payload);
        let msg = serde_json::to_string(&payload)
            .map_err(|e| AppError::Generic(format!("Failed to serialize wallet update: {}", e)))?;

        let mut retries = 0;
        loop {
            match self
                .connection
                .publish::<_, _, i32>(TRACKED_WALLETS_CHANNEL, msg.clone())
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if retries >= MAX_RETRIES {
                        return Err(AppError::Generic(format!(
                            "Failed to publish wallet update after {} retries: {}",
                            MAX_RETRIES, e
                        )));
                    }
                    retries += 1;
                    tokio::time::sleep(RECONNECT_DELAY).await;
                }
            }
        }
    }

    pub async fn publish_settings_update(
        &mut self,
        settings: &CopyTradeSettings,
    ) -> Result<(), AppError> {
        println!("Publishing settings update: {:?}", settings.clone());
        let msg = serde_json::to_string(settings)
            .map_err(|e| AppError::Generic(format!("Failed to serialize settings: {}", e)))?;

        let mut retries = 0;
        loop {
            match self
                .connection
                .publish::<_, _, i32>(SETTINGS_CHANNEL, msg.clone())
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if retries >= MAX_RETRIES {
                        self.connection_monitor
                            .update_status(
                                ConnectionType::Redis,
                                ConnectionStatus::Error,
                                Some(format!(
                                    "Failed to publish after {} retries: {}",
                                    MAX_RETRIES, e
                                )),
                            )
                            .await;
                        return Err(AppError::Generic(format!(
                            "Failed to publish settings after {} retries: {}",
                            MAX_RETRIES, e
                        )));
                    }
                    retries += 1;
                    tokio::time::sleep(RECONNECT_DELAY).await;
                }
            }
        }
    }

    pub async fn publish_wallet_address_update(
        &mut self,
        wallet_address: &str,
        action: &str,
    ) -> Result<(), AppError> {
        println!("Publishing wallet address update: {:?}", wallet_address);
        let payload = json!({
            "wallet_address": wallet_address,
            "action": action,
        });

        let msg = serde_json::to_string(&payload)
            .map_err(|e| AppError::Generic(format!("Failed to serialize wallet update: {}", e)))?;

        let mut retries = 0;
        loop {
            match self
                .connection
                .publish::<_, _, i32>(TRACKED_WALLETS_CHANNEL, msg.clone())
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if retries >= MAX_RETRIES {
                        return Err(AppError::Generic(format!(
                            "Failed to publish wallet update after {} retries: {}",
                            MAX_RETRIES, e
                        )));
                    }
                    retries += 1;
                    tokio::time::sleep(RECONNECT_DELAY).await;
                }
            }
        }
    }

    pub async fn publish_settings_delete(&mut self, settings_id: &str) -> Result<(), AppError> {
        println!("Publishing settings delete: {:?}", settings_id);
        let payload = json!({
            "settings_id": settings_id,
        });

        let msg = serde_json::to_string(&payload)
            .map_err(|e| AppError::Generic(format!("Failed to serialize wallet update: {}", e)))?;

        let mut retries = 0;
        loop {
            match self
                .connection
                .publish::<_, _, i32>(SETTINGS_CHANNEL, msg.clone())
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if retries >= MAX_RETRIES {
                        return Err(AppError::Generic(format!(
                            "Failed to publish settings delete after {} retries: {}",
                            MAX_RETRIES, e
                        )));
                    }
                    retries += 1;
                    tokio::time::sleep(RECONNECT_DELAY).await;
                }
            }
        }
    }

    pub async fn subscribe_to_updates(
        redis_url: &str,
        event_system: Arc<EventSystem>,
    ) -> Result<(), AppError> {
        println!("Starting Redis subscription setup");
        // Create channel for push messages
        let (tx, mut rx) = mpsc::unbounded_channel();

        // Configure connection with push support
        let redis_url = if !redis_url.contains("protocol=resp3") {
            if redis_url.contains('?') {
                format!("{}&protocol=resp3", redis_url)
            } else {
                format!("{}?protocol=resp3", redis_url)
            }
        } else {
            redis_url.to_string()
        };

        println!("Creating Redis client with URL: {}", redis_url);
        let client = redis::Client::open(redis_url)
            .map_err(|e| AppError::Generic(format!("Failed to create Redis client: {}", e)))?;

        let config = AsyncConnectionConfig::new().set_push_sender(tx);

        println!("Establishing Redis connection...");
        let mut con = client
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .map_err(|e| AppError::Generic(format!("Failed to create connection: {}", e)))?;

        // Subscribe to both channels
        for channel in [
            SETTINGS_CHANNEL,
            TRACKED_WALLETS_CHANNEL,
            PRICE_UPDATES_CHANNEL,
        ] {
            println!("Subscribing to channel: {}", channel);
            con.subscribe(channel)
                .await
                .map_err(|e| AppError::Generic(format!("Failed to subscribe: {}", e)))?;
        }

        // Keep connection alive
        let connection = Arc::new(tokio::sync::Mutex::new(con));
        let connection_clone = connection.clone();

        // Spawn keep-alive task
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                let mut con = connection_clone.lock().await;
                if let Err(e) = redis::cmd("PING").query_async::<String>(&mut *con).await {
                    println!("Redis keep-alive failed: {}", e);
                    break;
                }
            }
        });

        // Handle push messages
        tokio::spawn(async move {
            println!("Starting Redis message handler loop");
            while let Some(push_info) = rx.recv().await {
                println!("Received Redis push message: {:?}", push_info);
                match push_info.kind {
                    redis::PushKind::Message if push_info.data.len() >= 2 => {
                        if let Ok(payload) = redis::from_redis_value::<String>(&push_info.data[1]) {
                            println!("Decoded Redis payload: {}", payload);
                            // Handle different channel messages
                            if let Ok(channel) =
                                redis::from_redis_value::<String>(&push_info.data[0])
                            {
                                println!("Message from channel: {}", channel);
                                match channel.as_str() {
                                    SETTINGS_CHANNEL => {
                                        println!("Processing settings update");
                                        if let Ok(settings) =
                                            serde_json::from_str::<CopyTradeSettings>(&payload)
                                        {
                                            println!(
                                                "Successfully deserialized settings update: {:?}",
                                                settings
                                            );
                                            event_system.emit(Event::SettingsUpdate(
                                                SettingsUpdateNotification {
                                                    data: settings,
                                                    type_: "settings_updated".to_string(),
                                                },
                                            ));
                                        } else {
                                            println!("Failed to deserialize settings update");
                                        }
                                    }
                                    TRACKED_WALLETS_CHANNEL => {
                                        println!("Processing tracked wallet update");
                                        if let Ok(update) =
                                            serde_json::from_str::<serde_json::Value>(&payload)
                                        {
                                            println!("Successfully deserialized tracked wallet update: {:?}", update);
                                            if let Some(action) = update["action"].as_str() {
                                                println!("Extracted action: {}", action);
                                                let wallet_type = match action {
                                                    "add" => WalletStateChangeType::Added,
                                                    "archive" => WalletStateChangeType::Archived,
                                                    "unarchive" => {
                                                        WalletStateChangeType::Unarchived
                                                    }
                                                    "delete" => WalletStateChangeType::Deleted,
                                                    _ => continue,
                                                };
                                                println!("Emitting wallet state change event");
                                                event_system.emit(Event::WalletStateChange(
                                                    WalletStateNotification {
                                                        data: WalletStateChange::new(
                                                            update["wallet_address"]
                                                                .as_str()
                                                                .unwrap_or("")
                                                                .to_string(),
                                                            wallet_type,
                                                        )
                                                        .with_details(update.clone()),
                                                        type_: "wallet_state_change".to_string(),
                                                    },
                                                ));
                                            }
                                        } else {
                                            println!("Failed to deserialize tracked wallet update");
                                        }
                                    }
                                    PRICE_UPDATES_CHANNEL => {
                                        println!("Processing price update");
                                        if let Ok(price_update) =
                                            serde_json::from_str::<PriceUpdate>(&payload)
                                        {
                                            println!(
                                                "Successfully deserialized price update for token: {}",
                                                price_update.token_address
                                            );
                                            event_system.emit(Event::PriceUpdate(
                                                PriceUpdateNotification {
                                                    data: price_update,
                                                    type_: "price_update".to_string(),
                                                },
                                            ));
                                        } else {
                                            println!("Failed to deserialize price update");
                                        }
                                    }
                                    _ => {
                                        println!("Unknown channel: {}", channel);
                                    }
                                }
                            }
                        }
                    }
                    redis::PushKind::Subscribe => {
                        println!("Received subscription confirmation, continuing...");
                        continue;
                    }
                    _ => {
                        println!("Received other push message type: {:?}", push_info.kind);
                        continue;
                    }
                }
            }
            println!("Redis message handler ended");
        });

        // Keep the connection in scope
        tokio::spawn(async move {
            let _con = connection; // Keep connection alive
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        });

        println!("Redis subscription setup complete");
        Ok(())
    }

    pub async fn subscribe_to_sol_price_updates(
        redis_url: &str,
        event_system: Arc<EventSystem>,
    ) -> Result<(), AppError> {
        let redis_url = if !redis_url.contains("protocol=resp3") {
            if redis_url.contains('?') {
                format!("{}&protocol=resp3", redis_url)
            } else {
                format!("{}?protocol=resp3", redis_url)
            }
        } else {
            redis_url.to_string()
        };

        let client = redis::Client::open(redis_url)
            .map_err(|e| AppError::Generic(format!("Failed to create Redis client: {}", e)))?;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let config = redis::AsyncConnectionConfig::new().set_push_sender(tx);

        println!("Establishing Redis connection for SOL price updates...");
        let mut con = client
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .map_err(|e| AppError::Generic(format!("Failed to create connection: {}", e)))?;

        // Subscribe to SOL price update channel
        con.subscribe("sol_price_updates")
            .await
            .map_err(|e| AppError::Generic(format!("Failed to subscribe: {}", e)))?;

        // Keep connection alive and process messages
        tokio::spawn(async move {
            println!("Starting SOL price update handler loop");
            while let Some(push_info) = rx.recv().await {
                match push_info.kind {
                    redis::PushKind::Message if push_info.data.len() >= 2 => {
                        if let Ok(payload) = redis::from_redis_value::<String>(&push_info.data[1]) {
                            if let Ok(price_update) =
                                serde_json::from_str::<SolPriceUpdate>(&payload)
                            {
                                event_system.emit(Event::SolPriceUpdate(
                                    SolPriceUpdateNotification {
                                        data: price_update,
                                        type_: "sol_price_update".to_string(),
                                    },
                                ));
                            }
                        }
                    }
                    _ => continue,
                }
            }
            println!("SOL price update handler ended");
        });

        Ok(())
    }

    pub async fn is_healthy(&mut self) -> Result<bool, AppError> {
        println!("Checking Redis health");
        match redis::cmd("PING")
            .query_async::<String>(&mut self.connection)
            .await
        {
            Ok(response) => Ok(response == "PONG"),
            Err(e) => {
                self.connection_monitor
                    .update_status(
                        ConnectionType::Redis,
                        ConnectionStatus::Error,
                        Some(format!("Redis health check failed: {}", e)),
                    )
                    .await;
                Err(AppError::Generic(format!(
                    "Redis health check failed: {}",
                    e
                )))
            }
        }
    }

    pub async fn publish_price_update(
        &mut self,
        price_update: &PriceUpdate,
    ) -> Result<(), AppError> {
        println!(
            "Publishing price update for token: {}",
            price_update.token_address
        );

        let msg = serde_json::to_string(price_update)
            .map_err(|e| AppError::Generic(format!("Failed to serialize price update: {}", e)))?;

        let mut retries = 0;
        loop {
            match self
                .connection
                .publish::<_, _, i32>(PRICE_UPDATES_CHANNEL, msg.clone())
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if retries >= MAX_RETRIES {
                        self.connection_monitor
                            .update_status(
                                ConnectionType::Redis,
                                ConnectionStatus::Error,
                                Some(format!(
                                    "Failed to publish price update after {} retries: {}",
                                    MAX_RETRIES, e
                                )),
                            )
                            .await;
                        return Err(AppError::RedisError(format!(
                            "Failed to publish price update after {} retries: {}",
                            MAX_RETRIES, e
                        )));
                    }
                    retries += 1;
                    tokio::time::sleep(RECONNECT_DELAY).await;
                }
            }
        }
    }

    pub async fn publish_sol_price_update(
        &mut self,
        price_update: &SolPriceUpdate,
    ) -> Result<(), AppError> {
        let channel = "sol_price_updates";
        let msg = serde_json::to_string(&price_update).map_err(|e| {
            AppError::JsonParseError(format!("Failed to serialize price update: {}", e))
        })?;

        let mut retries = 0;
        const MAX_RETRIES: u32 = 3;

        loop {
            match self
                .connection
                .publish::<_, _, i32>(channel, msg.clone())
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if retries >= MAX_RETRIES {
                        return Err(AppError::RedisError(format!(
                            "Failed to publish SOL price update after {} retries: {}",
                            MAX_RETRIES, e
                        )));
                    }
                    retries += 1;
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    pub async fn subscribe_to_sol_price(
        &mut self,
    ) -> Result<broadcast::Receiver<SolPriceUpdate>, AppError> {
        let (tx, rx) = broadcast::channel(100);
        let mut connection = self.connection.clone();

        tokio::spawn(async move {
            let mut last_error_time = None;

            loop {
                match Self::subscribe_and_forward(&mut connection, &tx).await {
                    Ok(()) => {
                        // Successful completion - unlikely in practice
                        break;
                    }
                    Err(e) => {
                        let now = Instant::now();
                        // Only log errors once per minute to avoid spam
                        if last_error_time
                            .map_or(true, |t: Instant| now.duration_since(t).as_secs() > 60)
                        {
                            tracing::error!("Error in SOL price subscription: {}", e);
                            last_error_time = Some(now);
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });

        Ok(rx)
    }

    async fn subscribe_and_forward(
        connection: &mut ConnectionManager,
        tx: &broadcast::Sender<SolPriceUpdate>,
    ) -> Result<(), AppError> {
        tracing::info!("Starting subscription to sol_price_updates");

        // Configure connection for RESP3
        let client = redis::Client::open("redis://127.0.0.1/?protocol=resp3")
            .map_err(|e| AppError::RedisError(format!("Failed to create Redis client: {}", e)))?;

        let (push_tx, mut push_rx) = tokio::sync::mpsc::unbounded_channel();
        let config = redis::AsyncConnectionConfig::new().set_push_sender(push_tx);

        let mut con = client
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .map_err(|e| AppError::RedisError(format!("Failed to create connection: {}", e)))?;

        tracing::info!("Subscribing to sol_price_updates channel");
        con.subscribe("sol_price_updates")
            .await
            .map_err(|e| AppError::RedisError(format!("Failed to subscribe: {}", e)))?;

        while let Some(msg) = push_rx.recv().await {
            tracing::debug!("Received push message: {:?}", msg);
            if msg.kind == redis::PushKind::Message && msg.data.len() >= 2 {
                if let Ok(payload) = redis::from_redis_value::<String>(&msg.data[1]) {
                    tracing::info!("Received payload: {}", payload);
                    if let Ok(update) = serde_json::from_str::<SolPriceUpdate>(&payload) {
                        if let Err(e) = tx.send(update) {
                            tracing::error!("Failed to forward update: {}", e);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
