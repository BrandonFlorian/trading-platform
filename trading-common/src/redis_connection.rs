use crate::{
    event_system::{Event, EventSystem},
    models::{CopyTradeSettings, SettingsUpdateNotification},
};
use redis::{aio::ConnectionManager, RedisResult, Value};
use serde_json;
use std::sync::Arc;

#[derive(Clone)]
pub struct RedisConnection {
    pub connection: ConnectionManager,
}

impl RedisConnection {
    pub async fn new(redis_url: &str) -> Result<Self, redis::RedisError> {
        let client = redis::Client::open(redis_url)?;
        let connection = ConnectionManager::new(client).await?;

        Ok(Self { connection })
    }

    pub async fn publish_settings_update(
        &mut self,
        settings: &CopyTradeSettings,
    ) -> RedisResult<()> {
        let msg: String = serde_json::to_string(settings).unwrap_or_default();
        // Use `Value` as the return type instead of unit
        let _: Value = redis::cmd("PUBLISH")
            .arg("settings_updates")
            .arg(msg)
            .query_async(&mut self.connection)
            .await?;
        Ok(())
    }

    pub async fn subscribe_to_settings_updates(
        redis_url: &str,
        event_system: Arc<EventSystem>,
    ) -> RedisResult<()> {
        let client = redis::Client::open(redis_url)?;
        let mut connection = client.get_multiplexed_tokio_connection().await?;

        // Set up RESP3 for push messages
        let _: Value = redis::cmd("HELLO")
            .arg(3)
            .query_async(&mut connection)
            .await?;

        // Create a separate connection for receiving messages
        let mut msg_conn = connection.clone();

        // Subscribe to the channel
        let _: Value = redis::cmd("SUBSCRIBE")
            .arg("settings_updates")
            .query_async(&mut connection)
            .await?;

        // Spawn message processing task
        tokio::spawn(async move {
            loop {
                match redis::cmd("GET")
                    .arg("settings_updates")
                    .query_async(&mut msg_conn)
                    .await
                {
                    Ok(value) => {
                        println!("Received value: {:?}", value);
                        if let Ok(payload) = redis::from_redis_value::<String>(&value) {
                            match serde_json::from_str::<CopyTradeSettings>(&payload) {
                                Ok(settings) => {
                                    println!("Received settings update: {:?}", settings);
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
                    Err(e) => {
                        eprintln!("Error reading message: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
            }
        });

        // Keep the connection alive
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                match redis::cmd("PING").exec_async(&mut connection).await {
                    Ok(_) => {
                        println!("Redis ping successful");
                    }
                    Err(e) => {
                        eprintln!("Redis ping failed: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(())
    }
}
