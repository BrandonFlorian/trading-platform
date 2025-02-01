use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Utc;
use parking_lot::Mutex;
use solana_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{str::FromStr, sync::Arc};
use tokio::sync::broadcast;
use trading_common::{
    error::AppError,
    event_system::EventSystem,
    models::{ConnectionStatus, ConnectionType, PriceSource, SolPriceUpdate},
    ConnectionMonitor,
};

use crate::raydium::UsdcSolPool;
#[derive(BorshSerialize, BorshDeserialize, Debug)]
struct PriceUpdate {
    write_authority: [u8; 32],
    // verification_level: u8, // 0x00 for Partial (with extra byte), 0x01 for Full
    // num_signatures: Option<u8>, // Only present if verification_level is 0x00
    price_message: PriceFeedMessage,
    posted_slot: u64,
}

#[derive(BorshSerialize, BorshDeserialize, Debug)]
struct PriceFeedMessage {
    feed_id: [u8; 32],
    price: i64,
    conf: u64,
    exponent: i32,
    publish_time: i64,
    prev_publish_time: i64,
    ema_price: i64,
    ema_conf: u64,
}

#[derive(BorshDeserialize, Debug)]
struct PythPriceMessage {
    feed_id: [u8; 32],
    price: i64,
    conf: u64,
    exponent: i32,
    publish_time: i64,
    prev_publish_time: i64,
    ema_price: i64,
    ema_conf: u64,
}

// Use only V2 price feed
const PYTH_SOL_USD_PRICE_ACCOUNT: &str = "7UVimffxr9ow1uXYxsr4LHAcV58mLzhmwaeKvJ1pjLiE";
const STALE_AFTER_SECONDS: i64 = 120; // 2 minutes
const UPDATE_INTERVAL: u64 = 1;
struct MonitorState {
    is_running: AtomicBool,
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

pub struct PriceMonitor {
    rpc_client: Arc<RpcClient>,
    event_system: Arc<EventSystem>,
    connection_monitor: Arc<ConnectionMonitor>,
    price_sender: broadcast::Sender<SolPriceUpdate>,
    state: Mutex<MonitorState>,
}

impl PriceMonitor {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        event_system: Arc<EventSystem>,
        connection_monitor: Arc<ConnectionMonitor>,
        price_sender: broadcast::Sender<SolPriceUpdate>,
    ) -> Self {
        Self {
            rpc_client,
            event_system,
            connection_monitor,
            price_sender,
            state: Mutex::new(MonitorState {
                is_running: AtomicBool::new(false),
                task_handle: None,
            }),
        }
    }

    pub async fn start_monitoring(&self) -> Result<(), AppError> {
        let mut state = self.state.lock();
        if state.is_running.load(Ordering::SeqCst) {
            return Ok(());
        }

        state.is_running.store(true, Ordering::SeqCst);

        let rpc_client = Arc::clone(&self.rpc_client);
        let price_sender = self.price_sender.clone();
        let connection_monitor = Arc::clone(&self.connection_monitor);
        let is_running = Arc::new(AtomicBool::new(true));
        let is_running_clone = is_running.clone();

        let handle = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(UPDATE_INTERVAL));
            let mut last_source = None;

            while is_running_clone.load(Ordering::SeqCst) {
                tokio::select! {
                    _ = interval.tick() => {
                        // Try Pyth first
                        match Self::get_pyth_price(&rpc_client).await {
                            Ok(price) => {
                                if last_source != Some(PriceSource::Pyth) {
                                    tracing::info!("Successfully switched to Pyth as price source");
                                    last_source = Some(PriceSource::Pyth);
                                }
                                tracing::debug!("Pyth SOL price: ${:.2} (confidence: {:.4})",
                                    price.price_usd,
                                    price.confidence.unwrap_or_default()
                                );
                                if let Err(e) = price_sender.send(price) {
                                    tracing::error!("Failed to send Pyth price update: {}", e);
                                }
                                continue;
                            }
                            Err(e) => {
                                tracing::warn!("Pyth price source failed, falling back to Raydium: {}", e);
                            }
                        }

                        // Fallback to Raydium
                        match Self::get_raydium_price(&rpc_client).await {
                            Ok(price) => {
                                if last_source != Some(PriceSource::Raydium) {
                                    tracing::info!("Successfully switched to Raydium as price source");
                                    last_source = Some(PriceSource::Raydium);
                                }
                                tracing::debug!("Raydium SOL price: ${:.2}", price.price_usd);
                                if let Err(e) = price_sender.send(price) {
                                    tracing::error!("Failed to send Raydium price update: {}", e);
                                }
                            }
                            Err(e) => {
                                tracing::error!("Failed to get SOL price from any source: {}", e);
                                last_source = None;
                                connection_monitor
                                    .update_status(
                                        ConnectionType::WebSocket,
                                        ConnectionStatus::Error,
                                        Some(e.to_string()),
                                    )
                                    .await;
                            }
                        }
                    }
                }
            }
        });

        state.task_handle = Some(handle);
        Ok(())
    }

    pub async fn stop_monitoring(&self) -> Result<(), AppError> {
        let mut state = self.state.lock();
        state.is_running.store(false, Ordering::SeqCst);

        if let Some(handle) = state.task_handle.take() {
            handle.abort();
        }

        Ok(())
    }

    async fn get_pyth_price(rpc_client: &RpcClient) -> Result<SolPriceUpdate, AppError> {
        let pyth_account_pubkey = Pubkey::from_str(PYTH_SOL_USD_PRICE_ACCOUNT)?;
        let account = rpc_client
            .get_account_with_commitment(&pyth_account_pubkey, CommitmentConfig::processed())?;

        let account_data = account
            .value
            .ok_or_else(|| AppError::PriceNotAvailable("Pyth V2 account not found".to_string()))?
            .data;

        // Get verification level
        let verification_level = account_data[40];
        let is_fully_verified = verification_level == 0x01;

        // Skip discriminator (8) + authority (32) + verification (1)
        let message_start = 41;
        match PythPriceMessage::try_from_slice(&account_data[message_start..message_start + 84]) {
            Ok(msg) => {
                let current_time = Utc::now().timestamp();
                if current_time - msg.publish_time > STALE_AFTER_SECONDS {
                    return Err(AppError::PriceNotAvailable(format!(
                        "Pyth V2 price is stale by {}s",
                        current_time - msg.publish_time
                    )));
                }

                let price_value = (msg.price as f64) * 10f64.powi(msg.exponent);
                let confidence = (msg.conf as f64) * 10f64.powi(msg.exponent);
                let ema_price = (msg.ema_price as f64) * 10f64.powi(msg.exponent);
                let ema_conf = (msg.ema_conf as f64) * 10f64.powi(msg.exponent);

                tracing::info!(
                    "Pyth V2 price details:\n\
                    Price: ${:.2} (±${:.3})\n\
                    EMA Price: ${:.2} (±${:.3})\n\
                    Publish time: {} ({}s ago)\n\
                    Verification: {}\n\
                    Exponent: {}",
                    price_value,
                    confidence,
                    ema_price,
                    ema_conf,
                    msg.publish_time,
                    current_time - msg.publish_time,
                    if is_fully_verified { "Full" } else { "Partial" },
                    msg.exponent
                );

                Ok(SolPriceUpdate {
                    price_usd: price_value,
                    source: PriceSource::Pyth,
                    timestamp: current_time,
                    confidence: Some(confidence),
                })
            }
            Err(e) => {
                tracing::error!("Failed to parse Pyth V2 price feed: {:?}", e);
                tracing::debug!("Account data: {:?}", account_data);
                Err(AppError::PriceNotAvailable(format!(
                    "Failed to parse Pyth V2 price feed: {}",
                    e
                )))
            }
        }
    }

    async fn get_raydium_price(rpc_client: &RpcClient) -> Result<SolPriceUpdate, AppError> {
        // Get USDC/SOL pool account
        let pool = UsdcSolPool::new()?;
        let account = rpc_client.get_account(&pool.address)?;

        let pool = UsdcSolPool::from_account_data(&account.data)?;
        let price = pool.get_price(rpc_client).await?;

        Ok(SolPriceUpdate {
            price_usd: price,
            source: PriceSource::Raydium,
            timestamp: Utc::now().timestamp(),
            confidence: None, // Raydium doesn't provide confidence intervals
        })
    }
}
