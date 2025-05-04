use base64::Engine;
use serde::{Deserialize, Serialize};
use solana_account_decoder::UiAccountEncoding;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, oneshot, RwLock};
use tokio_stream::StreamExt;
use tokio_tungstenite::tungstenite::Message;
use trading_common::error::AppError;
use trading_common::models::PriceUpdate;
use trading_common::{RAYDIUM_V4, WSOL};

use crate::raydium::{PoolFinder, RaydiumPool};
use trading_common::redis::RedisPool;

// Number of worker partitions
const NUM_PARTITIONS: usize = 8;

// Commands that can be sent to workers
#[derive(Debug)]
pub enum WorkerCommand {
    Subscribe {
        client_id: String,
        token_address: String,
        response_tx: oneshot::Sender<Result<(), AppError>>,
    },
    Unsubscribe {
        client_id: String,
        token_address: String,
    },
    HealthCheck {
        response_tx: oneshot::Sender<WorkerStatus>,
    },
    Shutdown,
}

// Commands that can be sent to the manager
#[derive(Debug)]
pub enum ManagerCommand {
    Subscribe {
        client_id: String,
        token_address: String,
        response_tx: oneshot::Sender<Result<(), AppError>>,
    },
    Unsubscribe {
        client_id: String,
        token_address: String,
    },
    ClientDisconnected {
        client_id: String,
    },
    GetWorkerStatus {
        response_tx: oneshot::Sender<Vec<WorkerStatus>>,
    },
    Shutdown,
}

// Status information for each worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerStatus {
    pub partition_id: usize,
    pub active_tokens: usize,
    pub client_connections: usize,
    pub last_update: chrono::DateTime<chrono::Utc>,
}

// Subscription state for a token
#[derive(Clone, Debug)]
pub struct TokenSubscription {
    pub pool: RaydiumPool,
    pub clients: HashSet<String>,
    pub rpc_subscription_id: Option<String>,
    pub last_update: Instant,
    pub state: SubscriptionState,
    pub retry_count: u32,
}

#[derive(Clone)]
struct WorkerProcessingContext {
    partition_id: usize,
    token_subscriptions: Arc<RwLock<HashMap<String, TokenSubscription>>>,
    price_tx: mpsc::Sender<PriceUpdate>,
    rpc_client: Arc<RpcClient>,
    redis_connection: Arc<RedisPool>,
}

// State of a subscription
#[derive(Debug, Clone)]
pub enum SubscriptionState {
    Initializing,
    Active,
    Failed(String),
    Reconnecting,
}

// Worker that manages a partition of token subscriptions
pub struct SubscriptionWorker {
    partition_id: usize,
    token_subscriptions: HashMap<String, TokenSubscription>,
    client_subscriptions: HashMap<String, HashSet<String>>,
    command_rx: mpsc::Receiver<WorkerCommand>,
    price_tx: mpsc::Sender<PriceUpdate>,
    rpc_client: Arc<RpcClient>,
    redis_connection: Arc<RedisPool>,
    ws_connection: Arc<RwLock<trading_common::WebSocketConnectionManager>>,
}

// Manager that routes commands to the appropriate worker
#[derive(Clone)]
pub struct PartitionedSubscriptionManager {
    command_tx: mpsc::Sender<ManagerCommand>,
    price_tx: broadcast::Sender<PriceUpdate>,
    worker_command_txs: Vec<mpsc::Sender<WorkerCommand>>,
    pool_finder: Arc<PoolFinder>,
}

impl PartitionedSubscriptionManager {
    pub fn new(
        rpc_ws_url: String,
        rpc_client: Arc<RpcClient>,
        redis_connection: Arc<RedisPool>,
    ) -> Self {
        let (price_tx, _) = broadcast::channel(1000);
        let (command_tx, command_rx) = mpsc::channel(100);
        let pool_finder = Arc::new(PoolFinder::new(
            rpc_client.clone(),
            redis_connection.clone(),
        ));

        // Create the manager and spawn its task
        let manager = Self {
            command_tx,
            price_tx: price_tx.clone(),
            worker_command_txs: Vec::with_capacity(NUM_PARTITIONS),
            pool_finder,
        };

        // Clone manager for task
        let manager_clone = manager.clone();

        // Spawn manager task
        tokio::spawn(async move {
            manager_clone
                .run(
                    command_rx,
                    rpc_ws_url,
                    rpc_client,
                    redis_connection,
                    price_tx,
                )
                .await;
        });

        manager
    }

    // Run the manager loop
    async fn run(
        mut self,
        mut command_rx: mpsc::Receiver<ManagerCommand>,
        rpc_ws_url: String,
        rpc_client: Arc<RpcClient>,
        redis_connection: Arc<RedisPool>,
        price_tx: broadcast::Sender<PriceUpdate>,
    ) {
        // Initialize workers
        let mut worker_command_txs = Vec::with_capacity(NUM_PARTITIONS);
        let mut worker_handles = Vec::with_capacity(NUM_PARTITIONS);

        for i in 0..NUM_PARTITIONS {
            let (worker_tx, worker_rx) = mpsc::channel(100);
            worker_command_txs.push(worker_tx);

            // Create worker and spawn its task
            let worker_handle = Self::spawn_worker(
                i,
                worker_rx,
                rpc_ws_url.clone(),
                rpc_client.clone(),
                redis_connection.clone(),
                price_tx.clone(),
            );

            worker_handles.push(worker_handle);
        }

        self.worker_command_txs = worker_command_txs;

        // Process commands
        while let Some(command) = command_rx.recv().await {
            match command {
                ManagerCommand::Subscribe {
                    client_id,
                    token_address,
                    response_tx,
                } => {
                    let worker_id = self.get_worker_id(&token_address);
                    if let Some(worker_tx) = self.worker_command_txs.get(worker_id) {
                        // Forward the command to the appropriate worker
                        if let Err(e) = worker_tx
                            .send(WorkerCommand::Subscribe {
                                client_id,
                                token_address,
                                response_tx,
                            })
                            .await
                        {
                            tracing::error!(
                                "Failed to forward subscribe command to worker {}: {}",
                                worker_id,
                                e
                            );
                        }
                    } else {
                        // This shouldn't happen if workers are properly initialized
                        let _ = response_tx.send(Err(AppError::InternalError(
                            "Worker not available".to_string(),
                        )));
                    }
                }
                ManagerCommand::Unsubscribe {
                    client_id,
                    token_address,
                } => {
                    let worker_id = self.get_worker_id(&token_address);
                    if let Some(worker_tx) = self.worker_command_txs.get(worker_id) {
                        if let Err(e) = worker_tx
                            .send(WorkerCommand::Unsubscribe {
                                client_id,
                                token_address,
                            })
                            .await
                        {
                            tracing::error!(
                                "Failed to forward unsubscribe command to worker {}: {}",
                                worker_id,
                                e
                            );
                        }
                    }
                }
                ManagerCommand::ClientDisconnected { client_id } => {
                    // We need to notify all workers since we don't know which tokens the client was subscribed to
                    for (worker_id, worker_tx) in self.worker_command_txs.iter().enumerate() {
                        if let Err(e) = worker_tx
                            .send(WorkerCommand::Unsubscribe {
                                client_id: client_id.clone(),
                                token_address: String::new(), // Special case: empty token address means unsubscribe from all
                            })
                            .await
                        {
                            tracing::error!(
                                "Failed to forward client disconnection to worker {}: {}",
                                worker_id,
                                e
                            );
                        }
                    }
                }
                ManagerCommand::GetWorkerStatus { response_tx } => {
                    // Collect status from all workers
                    let mut statuses = Vec::with_capacity(NUM_PARTITIONS);
                    for (worker_id, worker_tx) in self.worker_command_txs.iter().enumerate() {
                        let (status_tx, status_rx) = oneshot::channel();
                        if let Err(e) = worker_tx
                            .send(WorkerCommand::HealthCheck {
                                response_tx: status_tx,
                            })
                            .await
                        {
                            tracing::error!(
                                "Failed to get status from worker {}: {}",
                                worker_id,
                                e
                            );
                            continue;
                        }

                        match tokio::time::timeout(Duration::from_secs(5), status_rx).await {
                            Ok(Ok(status)) => statuses.push(status),
                            _ => tracing::error!(
                                "Failed to receive status from worker {}",
                                worker_id
                            ),
                        }
                    }

                    let _ = response_tx.send(statuses);
                }
                ManagerCommand::Shutdown => {
                    // Signal all workers to shut down
                    for (worker_id, worker_tx) in self.worker_command_txs.iter().enumerate() {
                        if let Err(e) = worker_tx.send(WorkerCommand::Shutdown).await {
                            tracing::error!(
                                "Failed to send shutdown to worker {}: {}",
                                worker_id,
                                e
                            );
                        }
                    }

                    // Wait for all workers to finish
                    for (i, handle) in worker_handles.into_iter().enumerate() {
                        if let Err(e) = handle.await {
                            tracing::error!("Worker {} failed during shutdown: {}", i, e);
                        }
                    }

                    tracing::info!("Subscription manager shutdown complete");
                    break;
                }
            }
        }
    }

    // Spawn a worker task
    fn spawn_worker(
        partition_id: usize,
        worker_rx: mpsc::Receiver<WorkerCommand>,
        rpc_ws_url: String,
        rpc_client: Arc<RpcClient>,
        redis_connection: Arc<RedisPool>,
        price_tx: broadcast::Sender<PriceUpdate>,
    ) -> tokio::task::JoinHandle<()> {
        let (price_worker_tx, mut price_worker_rx) = mpsc::channel(100);

        // Create WebSocket connection manager for this worker
        let ws_config = trading_common::WebSocketConfig {
            health_check_interval: Duration::from_secs(30),
            connection_timeout: Duration::from_secs(5),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            max_retries: 3,
        };

        let ws_connection = Arc::new(RwLock::new(
            trading_common::WebSocketConnectionManager::new(rpc_ws_url, Some(ws_config)),
        ));

        // Create the worker
        let worker = SubscriptionWorker {
            partition_id,
            token_subscriptions: HashMap::new(),
            client_subscriptions: HashMap::new(),
            command_rx: worker_rx,
            price_tx: price_worker_tx,
            rpc_client,
            redis_connection,
            ws_connection,
        };

        // Spawn the worker task
        let worker_handle = tokio::spawn(async move {
            worker.run().await;
        });

        // Spawn a task to forward price updates to the broadcast channel
        tokio::spawn(async move {
            while let Some(update) = price_worker_rx.recv().await {
                if let Err(e) = price_tx.send(update) {
                    tracing::error!("Failed to broadcast price update: {}", e);
                }
            }
        });

        worker_handle
    }

    // Determine which worker should handle a token based on its address
    fn get_worker_id(&self, token_address: &str) -> usize {
        // Simple hash function to distribute tokens among workers
        let mut hash = 0u64;
        for b in token_address.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(b as u64);
        }
        (hash % NUM_PARTITIONS as u64) as usize
    }

    // Public API for subscribing to a token
    pub async fn subscribe_to_token(
        &self,
        client_id: &str,
        token_address: &str,
    ) -> Result<(), AppError> {
        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send(ManagerCommand::Subscribe {
                client_id: client_id.to_string(),
                token_address: token_address.to_string(),
                response_tx: tx,
            })
            .await
            .map_err(|_| AppError::InternalError("Failed to send subscription request".into()))?;

        tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .map_err(|_| AppError::TimeoutError("Subscription request timed out".into()))?
            .map_err(|_| {
                AppError::InternalError("Failed to receive subscription response".into())
            })?
    }

    // Public API for unsubscribing from a token
    pub async fn unsubscribe_from_token(
        &self,
        client_id: &str,
        token_address: &str,
    ) -> Result<(), AppError> {
        self.command_tx
            .send(ManagerCommand::Unsubscribe {
                client_id: client_id.to_string(),
                token_address: token_address.to_string(),
            })
            .await
            .map_err(|_| AppError::InternalError("Failed to send unsubscribe request".into()))?;

        Ok(())
    }

    // Public API for notifying that a client has disconnected
    pub async fn client_disconnected(&self, client_id: &str) -> Result<(), AppError> {
        self.command_tx
            .send(ManagerCommand::ClientDisconnected {
                client_id: client_id.to_string(),
            })
            .await
            .map_err(|_| {
                AppError::InternalError("Failed to send client disconnection notice".into())
            })?;

        Ok(())
    }

    // Public API for subscribing to price updates
    pub fn subscribe_to_updates(&self) -> broadcast::Receiver<PriceUpdate> {
        self.price_tx.subscribe()
    }

    // Public API for getting worker status
    pub async fn get_worker_status(&self) -> Result<Vec<WorkerStatus>, AppError> {
        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send(ManagerCommand::GetWorkerStatus { response_tx: tx })
            .await
            .map_err(|_| AppError::InternalError("Failed to send status request".into()))?;

        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| AppError::TimeoutError("Status request timed out".into()))?
            .map_err(|_| AppError::InternalError("Failed to receive status response".into()))
    }

    // Public API for shutting down
    pub async fn shutdown(&self) -> Result<(), AppError> {
        if let Err(e) = self.command_tx.send(ManagerCommand::Shutdown).await {
            tracing::error!("Failed to send shutdown command: {}", e);
            return Err(AppError::InternalError(
                "Failed to initiate shutdown".into(),
            ));
        }

        Ok(())
    }
}

// Implementation of the worker
impl SubscriptionWorker {
    async fn run(mut self) {
        tracing::info!("Starting subscription worker {}", self.partition_id);

        // Create a processing context that can be cloned
        let processing_context = WorkerProcessingContext {
            partition_id: self.partition_id,
            token_subscriptions: Arc::new(RwLock::new(self.token_subscriptions.clone())),
            price_tx: self.price_tx.clone(),
            rpc_client: self.rpc_client.clone(),
            redis_connection: self.redis_connection.clone(),
        };

        // Set up periodic health check
        let mut health_check_interval = tokio::time::interval(Duration::from_secs(30));

        // Set up WebSocket message processing
        let ws_connection = self.ws_connection.clone();
        let context = processing_context.clone();

        // Spawn WebSocket message processing task
        let message_task = tokio::spawn(async move {
            loop {
                let mut connection = ws_connection.write().await;
                match connection.ensure_connection().await {
                    Ok(conn) => {
                        while let Some(msg) = conn.next().await {
                            match msg {
                                Ok(Message::Text(text)) => {
                                    if let Err(e) = Self::process_websocket_message_with_context(
                                        &text, &context,
                                    )
                                    .await
                                    {
                                        tracing::error!(
                                            "Error processing WebSocket message: {}",
                                            e
                                        );
                                    }
                                }
                                Ok(Message::Close(_)) => break,
                                Err(e) => {
                                    tracing::error!("WebSocket error: {}", e);
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to ensure WebSocket connection: {}", e);
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        });

        // Main loop
        loop {
            tokio::select! {
                Some(command) = self.command_rx.recv() => {
                    match command {
                        WorkerCommand::Subscribe { client_id, token_address, response_tx } => {
                            let result = self.handle_subscribe(client_id, token_address).await;
                            let _ = response_tx.send(result);
                        },
                        WorkerCommand::Unsubscribe { client_id, token_address } => {
                            if token_address.is_empty() {
                                // Special case: unsubscribe client from all tokens
                                self.handle_client_disconnected(&client_id).await;
                            } else {
                                self.handle_unsubscribe(&client_id, &token_address).await;
                            }
                        },
                        WorkerCommand::HealthCheck { response_tx } => {
                            let status = self.get_status();
                            let _ = response_tx.send(status);
                        },
                        WorkerCommand::Shutdown => {
                            self.handle_shutdown().await;
                            return;
                        }
                    }
                },
                _ = health_check_interval.tick() => {
                    self.perform_health_check().await;
                },
                else => break,
            }
        }

        // Cleanup if the loop exits unexpectedly
        self.handle_shutdown().await;
    }

    async fn process_websocket_message_with_context(
        message: &str,
        context: &WorkerProcessingContext,
    ) -> Result<(), AppError> {
        // Parse the message
        let json: serde_json::Value = serde_json::from_str(message)
            .map_err(|e| AppError::JsonParseError(format!("Failed to parse message: {}", e)))?;

        // Process subscription notification
        if let Some(params) = json.get("params") {
            if let Some(subscription) = params.get("subscription").and_then(|s| s.as_str()) {
                // Find which token this subscription belongs to
                let mut token_address = None;

                // Get read lock for token subscriptions
                let token_subscriptions = context.token_subscriptions.read().await;

                for (addr, sub) in token_subscriptions.iter() {
                    if let Some(sub_id) = &sub.rpc_subscription_id {
                        if sub_id == subscription {
                            token_address = Some(addr.clone());
                            break;
                        }
                    }
                }

                // Drop read lock before processing
                drop(token_subscriptions);

                if let Some(token_addr) = token_address {
                    // Process account data
                    if let Some(account_data) = params
                        .get("result")
                        .and_then(|r| r.get("value"))
                        .and_then(|v| v.get("data"))
                    {
                        // Extract data based on expected format
                        let data_str = account_data
                            .as_array()
                            .and_then(|arr| arr.first())
                            .and_then(|v| v.as_str())
                            .ok_or_else(|| {
                                AppError::SerializationError(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "Invalid data format",
                                ))
                            })?;

                        // Decode base64 data
                        let decoded_data = base64::engine::general_purpose::STANDARD
                            .decode(data_str)
                            .map_err(|e| {
                                AppError::SerializationError(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!("Failed to decode base64 data: {}", e),
                                ))
                            })?;

                        // Get write lock for updating subscription data
                        let mut token_subscriptions = context.token_subscriptions.write().await;
                        let subscription =
                            token_subscriptions.get_mut(&token_addr).ok_or_else(|| {
                                AppError::InternalError(format!(
                                    "Subscription for {} not found",
                                    token_addr
                                ))
                            })?;

                        // Update pool data in our subscription
                        subscription
                            .pool
                            .update_from_websocket_data(&decoded_data)
                            .await?;

                        // Get current SOL price
                        let current_sol_price = context
                            .redis_connection
                            .get_sol_price()
                            .await?
                            .ok_or_else(|| {
                                AppError::RedisError("SOL price not found".to_string())
                            })?;

                        // Calculate new price data
                        let price_data = subscription
                            .pool
                            .fetch_price_data(&context.rpc_client, current_sol_price)
                            .await?;

                        // Create price update
                        let update = PriceUpdate {
                            token_address: subscription.pool.base_mint.to_string(),
                            price_sol: price_data.price_sol,
                            price_usd: price_data.price_usd,
                            market_cap: price_data.market_cap,
                            timestamp: chrono::Utc::now().timestamp(),
                            dex_type: trading_common::dex::DexType::Raydium,
                            liquidity: price_data.liquidity,
                            liquidity_usd: price_data.liquidity_usd,
                            pool_address: Some(subscription.pool.address.to_string()),
                            volume_24h: price_data.volume_24h,
                            volume_6h: price_data.volume_6h,
                            volume_1h: price_data.volume_1h,
                            volume_5m: price_data.volume_5m,
                        };

                        // Update subscription state
                        subscription.last_update = Instant::now();

                        // Drop write lock before sending update
                        drop(token_subscriptions);

                        // Send update to price channel
                        if let Err(e) = context.price_tx.send(update.clone()).await {
                            tracing::error!("Failed to send price update: {}", e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    // Handle a subscribe command
    async fn handle_subscribe(
        &mut self,
        client_id: String,
        token_address: String,
    ) -> Result<(), AppError> {
        // Check if client is already subscribed to this token
        if let Some(client_subs) = self.client_subscriptions.get_mut(&client_id) {
            if client_subs.contains(&token_address) {
                tracing::debug!(
                    "Client {} already subscribed to token {}",
                    client_id,
                    token_address
                );
                return Ok(());
            }
            client_subs.insert(token_address.clone());
        } else {
            // New client
            let mut subs = HashSet::new();
            subs.insert(token_address.clone());
            self.client_subscriptions.insert(client_id.clone(), subs);
        }

        // Check if we already have this token
        if let Some(sub) = self.token_subscriptions.get_mut(&token_address) {
            // Add client to existing subscription
            sub.clients.insert(client_id.clone());
            tracing::info!(
                "Added client {} to existing subscription for token {}",
                client_id,
                token_address
            );
            return Ok(());
        }

        // New token - need to find the pool and set up subscription
        match self.find_and_initialize_pool(&token_address).await {
            Ok(pool) => {
                // Create subscription
                let mut clients = HashSet::new();
                clients.insert(client_id.clone());

                let subscription = TokenSubscription {
                    pool,
                    clients,
                    rpc_subscription_id: None,
                    last_update: Instant::now(),
                    state: SubscriptionState::Initializing,
                    retry_count: 0,
                };

                // Add to our subscriptions
                self.token_subscriptions
                    .insert(token_address.clone(), subscription);

                // Set up the RPC subscription
                if let Err(e) = self.setup_rpc_subscription(&token_address).await {
                    tracing::error!(
                        "Failed to set up RPC subscription for token {}: {}",
                        token_address,
                        e
                    );
                    // We still return Ok because the subscription is technically created,
                    // but in an initializing state. The health check will retry.
                }

                tracing::info!(
                    "Created new subscription for token {} with client {}",
                    token_address,
                    client_id
                );
                Ok(())
            }
            Err(e) => {
                // Remove client subscription since we couldn't find the pool
                self.remove_client_subscription(&client_id, &token_address);
                Err(e)
            }
        }
    }

    // Handle an unsubscribe command
    async fn handle_unsubscribe(&mut self, client_id: &str, token_address: &str) {
        self.remove_client_subscription(client_id, token_address);

        // Check if we need to clean up the token subscription
        if let Some(sub) = self.token_subscriptions.get_mut(token_address) {
            sub.clients.remove(client_id);

            if sub.clients.is_empty() {
                // No more clients for this token, unsubscribe from RPC
                if let Some(subscription_id) = sub.rpc_subscription_id.clone() {
                    if let Err(e) = self
                        .unsubscribe_from_rpc(token_address, &subscription_id)
                        .await
                    {
                        tracing::error!(
                            "Failed to unsubscribe from RPC for token {}: {}",
                            token_address,
                            e
                        );
                    }
                }

                // Remove the subscription
                self.token_subscriptions.remove(token_address);
                tracing::info!(
                    "Removed subscription for token {} as it has no more clients",
                    token_address
                );
            }
        }
    }

    // Handle a client disconnection
    async fn handle_client_disconnected(&mut self, client_id: &str) {
        // Get all tokens this client was subscribed to
        let token_addresses = match self.client_subscriptions.remove(client_id) {
            Some(addresses) => addresses.into_iter().collect::<Vec<_>>(),
            None => return,
        };

        // Unsubscribe from each token
        for token_address in token_addresses {
            self.handle_unsubscribe(client_id, &token_address).await;
        }
    }

    // Perform a health check on all subscriptions
    async fn perform_health_check(&mut self) {
        let tokens_to_check: Vec<String> = self.token_subscriptions.keys().cloned().collect();

        for token_address in tokens_to_check {
            let needs_resubscribe = match self.token_subscriptions.get(&token_address) {
                Some(sub) => {
                    match sub.state {
                        SubscriptionState::Initializing => true,
                        SubscriptionState::Failed(_) => true,
                        SubscriptionState::Reconnecting => false, // Already reconnecting
                        SubscriptionState::Active => {
                            // Check if we've received updates recently
                            sub.last_update.elapsed() > Duration::from_secs(60)
                        }
                    }
                }
                None => continue,
            };

            if needs_resubscribe {
                if let Err(e) = self.setup_rpc_subscription(&token_address).await {
                    tracing::error!(
                        "Failed to resubscribe to token {} during health check: {}",
                        token_address,
                        e
                    );

                    // Update retry count and check if we should give up
                    if let Some(sub) = self.token_subscriptions.get_mut(&token_address) {
                        sub.retry_count += 1;
                        sub.state = SubscriptionState::Failed(e.to_string());

                        if sub.retry_count > 5 {
                            tracing::error!(
                                "Too many failed attempts for token {}, giving up",
                                token_address
                            );
                            // We could decide to remove the subscription entirely here
                        }
                    }
                }
            }
        }
    }

    // Get status information for this worker
    fn get_status(&self) -> WorkerStatus {
        WorkerStatus {
            partition_id: self.partition_id,
            active_tokens: self.token_subscriptions.len(),
            client_connections: self.client_subscriptions.len(),
            last_update: chrono::Utc::now(),
        }
    }

    // Handle shutdown request
    async fn handle_shutdown(&mut self) {
        tracing::info!("Shutting down worker {}", self.partition_id);

        // Unsubscribe from all RPC subscriptions
        for (token_address, sub) in &self.token_subscriptions {
            if let Some(subscription_id) = &sub.rpc_subscription_id {
                if let Err(e) = self
                    .unsubscribe_from_rpc(token_address, subscription_id)
                    .await
                {
                    tracing::error!(
                        "Failed to unsubscribe from RPC for token {} during shutdown: {}",
                        token_address,
                        e
                    );
                }
            }
        }

        // Clean up state
        self.token_subscriptions.clear();
        self.client_subscriptions.clear();

        // Close WebSocket connection
        let mut ws_connection = self.ws_connection.write().await;
        if let Err(e) = ws_connection.shutdown().await {
            tracing::error!("Failed to shut down WebSocket connection: {}", e);
        }

        tracing::info!("Worker {} shutdown complete", self.partition_id);
    }

    // Helper methods

    // Remove a client's subscription to a token
    fn remove_client_subscription(&mut self, client_id: &str, token_address: &str) {
        if let Some(subscriptions) = self.client_subscriptions.get_mut(client_id) {
            subscriptions.remove(token_address);
            if subscriptions.is_empty() {
                self.client_subscriptions.remove(client_id);
            }
        }
    }

    // Find and initialize a pool for a token
    async fn find_and_initialize_pool(&self, token_address: &str) -> Result<RaydiumPool, AppError> {
        tracing::info!("Finding pool for token {}", token_address);
        let token_pubkey = Pubkey::from_str(token_address)
            .map_err(|_| AppError::InvalidPoolAddress(token_address.to_string()))?;

        // SOL token pubkey - we'll use this to validate quote mint
        let sol_pubkey = Pubkey::from_str(WSOL)?;

        // Search for pool with token as base mint
        let memcmp = Memcmp::new(432, MemcmpEncodedBytes::Base58(token_pubkey.to_string()));
        let filters = vec![RpcFilterType::DataSize(752), RpcFilterType::Memcmp(memcmp)];

        tracing::info!("Searching for pools with base mint {}", token_pubkey);

        let config = RpcProgramAccountsConfig {
            filters: Some(filters),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                commitment: Some(CommitmentConfig::confirmed()),
                ..Default::default()
            },
            ..Default::default()
        };

        let raydium_program = Pubkey::from_str(RAYDIUM_V4)?;

        // Get all matching pool accounts
        let accounts = self
            .rpc_client
            .get_program_accounts_with_config(&raydium_program, config)
            .map_err(|e| AppError::SolanaRpcError { source: e })?;

        tracing::info!("Found {} potential pool accounts", accounts.len());

        // Track the best pool based on liquidity
        let mut best_pool: Option<(RaydiumPool, u64)> = None;

        // Find pool with highest liquidity
        for (pubkey, account) in accounts {
            if let Ok(mut pool) = RaydiumPool::from_account_data(&pubkey, &account.data) {
                // Verify this is a SOL pool
                if pool.quote_mint != sol_pubkey {
                    continue;
                }

                tracing::info!("Loading metadata for pool candidate: {}", pubkey);
                pool.load_metadata(&self.rpc_client, &self.redis_connection)
                    .await?;

                // Get pool liquidity
                let quote_balance = self
                    .rpc_client
                    .get_token_account_balance(&pool.quote_vault)
                    .map_err(|e| AppError::SolanaRpcError { source: e })?;

                let liquidity = quote_balance.amount.parse::<u64>().unwrap_or(0);

                // Update best pool if this has higher liquidity
                match &best_pool {
                    None => best_pool = Some((pool, liquidity)),
                    Some((_, current_liquidity)) if liquidity > *current_liquidity => {
                        best_pool = Some((pool, liquidity));
                    }
                    _ => {}
                }
            }
        }

        // Return the pool with highest liquidity
        best_pool
            .map(|(pool, _)| pool)
            .ok_or_else(|| AppError::PoolNotFound(token_address.to_string()))
    }

    // Set up an RPC subscription for a token
    async fn setup_rpc_subscription(&mut self, token_address: &str) -> Result<(), AppError> {
        let subscription = self
            .token_subscriptions
            .get_mut(token_address)
            .ok_or_else(|| {
                AppError::InternalError(format!("Subscription for {} not found", token_address))
            })?;

        // Mark as reconnecting
        subscription.state = SubscriptionState::Reconnecting;

        // Get the pool address
        let pool_address = subscription.pool.address.to_string();

        // Prepare subscription request
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": token_address,
            "method": "accountSubscribe",
            "params": [
                pool_address,
                {
                    "encoding": "base64",
                    "commitment": "confirmed"
                }
            ]
        });

        // Get WebSocket connection
        let mut ws_connection = self.ws_connection.write().await;

        // Send subscription request
        ws_connection
            .send_message(Message::Text(request.to_string().into()))
            .await?;

        // Wait for confirmation response
        let response =
            tokio::time::timeout(Duration::from_secs(5), ws_connection.receive_message())
                .await
                .map_err(|_| AppError::TimeoutError("Subscription request timed out".into()))?;

        match response {
            Ok(Some(Message::Text(text))) => {
                let response: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
                    AppError::JsonParseError(format!("Failed to parse response: {}", e))
                })?;

                if let Some(result) = response.get("result") {
                    if let Some(id) = result.as_str() {
                        tracing::info!(
                            "Successfully subscribed to token {} with ID {}",
                            token_address,
                            id
                        );

                        // Update subscription state
                        if let Some(sub) = self.token_subscriptions.get_mut(token_address) {
                            sub.rpc_subscription_id = Some(id.to_string());
                            sub.state = SubscriptionState::Active;
                            sub.last_update = Instant::now();
                            sub.retry_count = 0;
                        }

                        return Ok(());
                    }
                }

                Err(AppError::WebSocketError(format!(
                    "Invalid subscription response: {}",
                    text
                )))
            }
            Ok(Some(_)) => Err(AppError::WebSocketError("Unexpected response type".into())),
            Ok(None) => Err(AppError::WebSocketError("Connection closed".into())),
            Err(e) => Err(AppError::WebSocketError(format!("WebSocket error: {}", e))),
        }
    }

    // Unsubscribe from an RPC subscription
    async fn unsubscribe_from_rpc(
        &self,
        token_address: &str,
        subscription_id: &str,
    ) -> Result<(), AppError> {
        // Prepare unsubscribe request
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": token_address,
            "method": "accountUnsubscribe",
            "params": [subscription_id]
        });

        // Get WebSocket connection
        let mut ws_connection = self.ws_connection.write().await;

        // Send unsubscribe request
        ws_connection
            .send_message(Message::Text(request.to_string().into()))
            .await?;

        // We don't wait for a response as we're cleaning up

        Ok(())
    }

    // Process a WebSocket message (would be called from a message handler task)
    async fn process_websocket_message(&mut self, message: &str) -> Result<(), AppError> {
        // Parse the message
        let json: serde_json::Value = serde_json::from_str(message)
            .map_err(|e| AppError::JsonParseError(format!("Failed to parse message: {}", e)))?;

        // Process subscription notification
        if let Some(params) = json.get("params") {
            if let Some(subscription) = params.get("subscription").and_then(|s| s.as_str()) {
                // Find which token this subscription belongs to
                let mut token_address = None;

                for (addr, sub) in &self.token_subscriptions {
                    if let Some(sub_id) = &sub.rpc_subscription_id {
                        if sub_id == subscription {
                            token_address = Some(addr.clone());
                            break;
                        }
                    }
                }

                if let Some(token_addr) = token_address {
                    // Process account data
                    if let Some(account_data) = params
                        .get("result")
                        .and_then(|r| r.get("value"))
                        .and_then(|v| v.get("data"))
                    {
                        // Update token data and calculate price
                        self.update_token_data(&token_addr, account_data).await?;
                    }
                }
            }
        }

        Ok(())
    }

    // Update token data and calculate price
    async fn update_token_data(
        &mut self,
        token_address: &str,
        data: &serde_json::Value,
    ) -> Result<(), AppError> {
        let subscription = self
            .token_subscriptions
            .get_mut(token_address)
            .ok_or_else(|| {
                AppError::InternalError(format!("Subscription for {} not found", token_address))
            })?;

        // Extract data based on expected format
        let data_str = data
            .as_array()
            .and_then(|arr| arr.first())
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AppError::SerializationError(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid data format",
                ))
            })?;

        // Decode base64 data
        let decoded_data = base64::engine::general_purpose::STANDARD
            .decode(data_str)
            .map_err(|e| {
                AppError::SerializationError(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to decode base64 data: {}", e),
                ))
            })?;

        // Update pool data in our subscription
        subscription
            .pool
            .update_from_websocket_data(&decoded_data)
            .await?;

        // Get current SOL price
        let current_sol_price = self
            .redis_connection
            .get_sol_price()
            .await?
            .ok_or_else(|| AppError::RedisError("SOL price not found".to_string()))?;

        // Calculate new price data
        let price_data = subscription
            .pool
            .fetch_price_data(&self.rpc_client, current_sol_price)
            .await?;

        // Create price update
        let update = PriceUpdate {
            token_address: subscription.pool.base_mint.to_string(),
            price_sol: price_data.price_sol,
            price_usd: price_data.price_usd,
            market_cap: price_data.market_cap,
            timestamp: chrono::Utc::now().timestamp(),
            dex_type: trading_common::dex::DexType::Raydium,
            liquidity: price_data.liquidity,
            liquidity_usd: price_data.liquidity_usd,
            pool_address: Some(subscription.pool.address.to_string()),
            volume_24h: price_data.volume_24h,
            volume_6h: price_data.volume_6h,
            volume_1h: price_data.volume_1h,
            volume_5m: price_data.volume_5m,
        };

        // Send update to price channel
        if let Err(e) = self.price_tx.send(update.clone()).await {
            tracing::error!("Failed to send price update: {}", e);
        }

        Ok(())
    }
}

// Helper methods for the manager
impl PartitionedSubscriptionManager {
    pub async fn find_pool(&self, token_address: &str) -> Result<RaydiumPool, AppError> {
        self.pool_finder.find_pool(token_address).await
    }

    // Batch subscribe to multiple tokens
    pub async fn batch_subscribe(
        &self,
        token_addresses: Vec<String>,
        client_id: &str,
    ) -> Result<(), AppError> {
        let mut results = Vec::with_capacity(token_addresses.len());

        for token_address in token_addresses {
            let result = self.subscribe_to_token(client_id, &token_address).await;
            results.push((token_address, result));
        }

        // Check if any subscriptions failed
        let failed: Vec<_> = results
            .iter()
            .filter(|(_, result)| result.is_err())
            .map(|(addr, _)| addr.clone())
            .collect();

        if !failed.is_empty() {
            return Err(AppError::PartialFailure(format!(
                "Failed to subscribe to {} tokens: {}",
                failed.len(),
                failed.join(", ")
            )));
        }

        Ok(())
    }

    // Unsubscribe from all tokens
    pub async fn unsubscribe_all(&self, client_id: &str) -> Result<(), AppError> {
        self.command_tx
            .send(ManagerCommand::ClientDisconnected {
                client_id: client_id.to_string(),
            })
            .await
            .map_err(|_| {
                AppError::InternalError("Failed to send client disconnection notice".into())
            })?;

        Ok(())
    }
}
