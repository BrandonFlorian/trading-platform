use crate::{
    error::AppError,
    event_system::{Event, EventSystem},
    models::{CopyTradeSettings, SettingsUpdateNotification},
};
use redis::AsyncConnectionConfig;
use redis::{aio::ConnectionManager, AsyncCommands, Client};
use serde_json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
const SETTINGS_CHANNEL: &str = "copy_trade_settings";
const RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRIES: u32 = 5;

#[derive(Clone)]
pub struct RedisConnection {
    _client: Client,
    connection: ConnectionManager,
}

impl RedisConnection {
    pub async fn new(redis_url: &str) -> Result<Self, AppError> {
        // Add protocol=resp3 to URL if not present
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

        let connection = ConnectionManager::new(client.clone())
            .await
            .map_err(|e| AppError::Generic(format!("Failed to create Redis connection: {}", e)))?;

        Ok(Self {
            _client: client,
            connection,
        })
    }

    pub async fn publish_settings_update(
        &mut self,
        settings: &CopyTradeSettings,
    ) -> Result<(), AppError> {
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

    pub async fn subscribe_to_settings_updates(
        redis_url: &str,
        event_system: Arc<EventSystem>,
    ) -> Result<(), AppError> {
        // Create channel for push messages
        let (tx, mut rx) = mpsc::unbounded_channel();

        // Add debug logging
        println!("Setting up Redis subscription for settings updates");

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

        let client = redis::Client::open(redis_url)
            .map_err(|e| AppError::Generic(format!("Failed to create Redis client: {}", e)))?;

        // Create a connection with push message support
        let config = AsyncConnectionConfig::new().set_push_sender(tx);

        let mut con = client
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .map_err(|e| AppError::Generic(format!("Failed to create connection: {}", e)))?;

        println!(
            "Redis connection established, subscribing to channel {}",
            SETTINGS_CHANNEL
        );

        // Subscribe to the channel
        con.subscribe(SETTINGS_CHANNEL)
            .await
            .map_err(|e| AppError::Generic(format!("Failed to subscribe: {}", e)))?;

        println!("Successfully subscribed to Redis channel");

        // Clone connection for keep-alive task
        let mut keep_alive_con = con.clone();

        // Spawn keep-alive task
        tokio::spawn(async move {
            println!("Starting Redis keep-alive task");
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                match redis::cmd("PING")
                    .query_async::<String>(&mut keep_alive_con)
                    .await
                {
                    Ok(_) => {
                        //This can be used to check if the connection is healthy but is annoying to leave uncommented
                        //println!("Redis keep-alive ping successful");
                    }
                    Err(e) => {
                        eprintln!("Redis keep-alive ping failed: {}", e);
                        break;
                    }
                }
            }
            println!("Redis keep-alive task ended");
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
                            match serde_json::from_str::<CopyTradeSettings>(&payload) {
                                Ok(settings) => {
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
                                }
                                Err(e) => {
                                    eprintln!("Error deserializing settings update: {}", e);
                                }
                            }
                        }
                    }
                    redis::PushKind::Subscribe => {
                        println!("Received subscription confirmation, continuing...");
                        // Just continue the loop for subscription messages
                        continue;
                    }
                    _ => {
                        println!("Received other push message type: {:?}", push_info.kind);
                        continue;
                    }
                }
            }
            println!("Redis message handler loop ended");
        });

        Ok(())
    }

    pub async fn is_healthy(&mut self) -> Result<bool, AppError> {
        match redis::cmd("PING")
            .query_async::<String>(&mut self.connection)
            .await
        {
            Ok(response) => Ok(response == "PONG"),
            Err(e) => Err(AppError::Generic(format!(
                "Redis health check failed: {}",
                e
            ))),
        }
    }
}
