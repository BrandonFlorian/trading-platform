use super::RedisPool;
use crate::{
    constants::{
        SETTINGS_CHANNEL, SOL_PRICE_CHANNEL, TOKEN_PRICE_CHANNEL, TRACKED_WALLETS_CHANNEL,
    },
    error::AppError,
    models::{CopyTradeSettings, PriceUpdate, SolPriceUpdate, WalletStateChange},
};
use futures_util::StreamExt;
use serde_json::json;
use std::time::Duration;
use tokio::sync::broadcast;
const RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRIES: u32 = 5;

impl RedisPool {
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
            match self.publish(channel, &msg).await {
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

    pub async fn publish_price_update(&self, price_update: &PriceUpdate) -> Result<(), AppError> {
        self.publish_with_retry(TOKEN_PRICE_CHANNEL, price_update)
            .await
    }

    pub async fn publish_sol_price_update(
        &self,
        price_update: &SolPriceUpdate,
    ) -> Result<(), AppError> {
        tracing::info!(
            "Publishing SOL price update: ${:.2}",
            price_update.price_usd
        );

        // Update the cache first
        self.set_sol_price(price_update.price_usd, Duration::from_secs(60))
            .await?;

        // Then publish the update
        self.publish_with_retry(SOL_PRICE_CHANNEL, price_update)
            .await?;

        tracing::info!("Published SOL price update: ${:.2}", price_update.price_usd);
        Ok(())
    }

    // Subscription methods
    pub async fn subscribe_to_updates(&self) -> Result<broadcast::Receiver<PriceUpdate>, AppError> {
        let (tx, rx) = broadcast::channel(100);
        let mut pubsub = self.subscribe(TOKEN_PRICE_CHANNEL).await?;

        tokio::spawn(async move {
            let mut msg_stream = pubsub.on_message();
            while let Some(msg) = msg_stream.next().await {
                if let Ok(payload) = redis::from_redis_value::<String>(&msg.get_payload().unwrap())
                {
                    if let Ok(update) = serde_json::from_str::<PriceUpdate>(&payload) {
                        if let Err(e) = tx.send(update) {
                            tracing::error!("Failed to forward price update: {}", e);
                            break;
                        }
                    }
                }
            }
        });

        Ok(rx)
    }

    pub async fn subscribe_to_sol_price(
        &self,
    ) -> Result<broadcast::Receiver<SolPriceUpdate>, AppError> {
        let (tx, rx) = broadcast::channel(100);
        let mut pubsub = self.subscribe(SOL_PRICE_CHANNEL).await?;

        tokio::spawn(async move {
            let mut msg_stream = pubsub.on_message();
            while let Some(msg) = msg_stream.next().await {
                if let Ok(payload) = redis::from_redis_value::<String>(&msg.get_payload().unwrap())
                {
                    if let Ok(update) = serde_json::from_str::<SolPriceUpdate>(&payload) {
                        if let Err(e) = tx.send(update) {
                            tracing::error!("Failed to forward SOL price update: {}", e);
                            break;
                        }
                    }
                }
            }
        });

        Ok(rx)
    }

    pub async fn subscribe_to_wallet_updates(
        &self,
    ) -> Result<broadcast::Receiver<WalletStateChange>, AppError> {
        let (tx, rx) = broadcast::channel(100);
        let mut pubsub = self.subscribe(TRACKED_WALLETS_CHANNEL).await?;

        tokio::spawn(async move {
            let mut msg_stream = pubsub.on_message();
            while let Some(msg) = msg_stream.next().await {
                if let Ok(payload) = redis::from_redis_value::<String>(&msg.get_payload().unwrap())
                {
                    if let Ok(update) = serde_json::from_str::<WalletStateChange>(&payload) {
                        if let Err(e) = tx.send(update) {
                            tracing::error!("Failed to forward wallet update: {}", e);
                            break;
                        }
                    }
                }
            }
        });

        Ok(rx)
    }
}
