use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
};
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, RwLock};
use trading_common::{error::AppError, models::PriceUpdate, redis::RedisPool, RAYDIUM_V4, WSOL};

use crate::{
    connection_pool::{ConnectionPoolConfig, PooledConnectionManager},
    raydium::RaydiumPool,
    websocket_message_handler::WebSocketMessage,
};

use super::subscription_manager::{
    SubscriptionState, TokenSubscription, WorkerCommand, WorkerStatus,
};

/// Subscription worker configuration
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub partition_id: usize,
    pub health_check_interval: Duration,
    pub subscription_timeout: Duration,
    pub max_retries: u32,
    pub batch_size: usize,
    pub batch_interval: Duration,
    pub rpc_ws_url: String,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            partition_id: 0,
            health_check_interval: Duration::from_secs(30),
            subscription_timeout: Duration::from_secs(60),
            max_retries: 5,
            batch_size: 100,
            batch_interval: Duration::from_millis(100),
            rpc_ws_url: "".to_string(),
        }
    }
}

/// Worker state machine for managing subscriptions

/// Internal worker state
#[derive(Debug)]
struct WorkerState {
    token_subscriptions: HashMap<String, TokenSubscription>,
    client_subscriptions: HashMap<String, HashSet<String>>,
    pending_operations: HashMap<String, PendingOperation>,
    last_health_check: Instant,
}

impl Default for WorkerState {
    fn default() -> Self {
        Self {
            token_subscriptions: HashMap::new(),
            client_subscriptions: HashMap::new(),
            pending_operations: HashMap::new(),
            last_health_check: Instant::now(),
        }
    }
}
/// Metrics for monitoring worker performance
#[derive(Debug)]
pub struct WorkerMetrics {
    pub active_subscriptions: AtomicUsize,
    pub connected_clients: AtomicUsize,
    pub messages_processed: AtomicU64,
    pub errors_encountered: AtomicU64,
    pub last_update: parking_lot::RwLock<Instant>,
}

impl Default for WorkerMetrics {
    fn default() -> Self {
        Self {
            active_subscriptions: AtomicUsize::new(0),
            connected_clients: AtomicUsize::new(0),
            messages_processed: AtomicU64::new(0),
            errors_encountered: AtomicU64::new(0),
            last_update: parking_lot::RwLock::new(Instant::now()),
        }
    }
}

/// Pending operations tracking
#[derive(Debug)]
struct PendingOperation {
    operation_type: OperationType,
    started_at: Instant,
    retries: u32,
}

#[derive(Debug, Clone)]
enum OperationType {
    Subscribe(String),   // client_id
    Unsubscribe(String), // client_id
    Reconnect,
}

pub struct SubscriptionWorker {
    config: WorkerConfig,
    state: Arc<RwLock<WorkerState>>,
    command_rx: mpsc::Receiver<WorkerCommand>,
    price_tx: mpsc::Sender<PriceUpdate>,
    rpc_client: Arc<RpcClient>,
    redis_connection: Arc<RedisPool>,
    ws_connection: Arc<RwLock<PooledConnectionManager>>,
    metrics: Arc<WorkerMetrics>,
}

impl SubscriptionWorker {
    pub fn new(
        config: WorkerConfig,
        command_rx: mpsc::Receiver<WorkerCommand>,
        price_tx: mpsc::Sender<PriceUpdate>,
        rpc_client: Arc<RpcClient>,
        redis_connection: Arc<RedisPool>,
        ws_connection: Arc<RwLock<PooledConnectionManager>>,
    ) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(WorkerState {
                token_subscriptions: HashMap::new(),
                client_subscriptions: HashMap::new(),
                pending_operations: HashMap::new(),
                last_health_check: Instant::now(),
            })),
            command_rx,
            price_tx,
            rpc_client,
            redis_connection,
            ws_connection,
            metrics: Arc::new(WorkerMetrics::default()),
        }
    }

    pub fn new_with_pool(
        config: WorkerConfig,
        command_rx: mpsc::Receiver<WorkerCommand>,
        price_tx: mpsc::Sender<PriceUpdate>,
        rpc_client: Arc<RpcClient>,
        redis_connection: Arc<RedisPool>,
        pool_config: ConnectionPoolConfig,
    ) -> Self {
        let ws_connection = Arc::new(RwLock::new(PooledConnectionManager::new(
            config.rpc_ws_url.clone(),
            pool_config,
        )));

        Self {
            config,
            state: Arc::new(RwLock::new(WorkerState::default())),
            command_rx,
            price_tx,
            rpc_client,
            redis_connection,
            ws_connection,
            metrics: Arc::new(WorkerMetrics::default()),
        }
    }

    // Update connection handling methods to use the pool
    async fn setup_rpc_subscription(&self, token_address: &str) -> Result<(), AppError> {
        // Get a connection from the pool
        self.ws_connection
            .write()
            .await
            .get_connection_for_token(token_address)
            .await?;

        // Continue with subscription setup...
        Ok(())
    }

    async fn cleanup_subscription(&self, token_address: &str) -> Result<(), AppError> {
        // Release the connection back to the pool
        self.ws_connection
            .write()
            .await
            .release_connection(token_address)
            .await?;

        Ok(())
    }

    /// Main worker loop with improved error handling and state management
    pub async fn run(mut self) {
        tracing::info!(
            "Starting subscription worker {} with config: {:?}",
            self.config.partition_id,
            self.config
        );

        let mut health_check_interval = tokio::time::interval(self.config.health_check_interval);
        let mut batch_interval = tokio::time::interval(self.config.batch_interval);
        let mut pending_updates = Vec::with_capacity(self.config.batch_size);

        loop {
            tokio::select! {
                Some(command) = self.command_rx.recv() => {
                    if let Err(e) = self.handle_command(command).await {
                        self.metrics.errors_encountered.fetch_add(1, Ordering::Relaxed);
                        tracing::error!("Error handling command: {}", e);
                    }
                }
                _ = health_check_interval.tick() => {
                    if let Err(e) = self.perform_health_check().await {
                        tracing::error!("Health check failed: {}", e);
                    }
                }
                _ = batch_interval.tick() => {
                    if !pending_updates.is_empty() {
                        if let Err(e) = self.process_batch_updates(&pending_updates).await {
                            tracing::error!("Failed to process batch updates: {}", e);
                        }
                        pending_updates.clear();
                    }
                }
                else => break,
            }
        }

        // Graceful shutdown
        self.handle_shutdown().await;
    }

    /// Handle incoming worker commands with proper state transitions
    async fn handle_command(&mut self, command: WorkerCommand) -> Result<(), AppError> {
        match command {
            WorkerCommand::Subscribe {
                client_id,
                token_address,
                response_tx,
            } => {
                let result = self
                    .handle_subscribe(client_id.clone(), token_address.clone())
                    .await;

                // Track the operation
                if result.is_ok() {
                    let mut state = self.state.write().await;
                    state.pending_operations.insert(
                        format!("{}:{}", token_address, client_id),
                        PendingOperation {
                            operation_type: OperationType::Subscribe(client_id),
                            started_at: Instant::now(),
                            retries: 0,
                        },
                    );
                }

                // Send response
                if let Err(e) = response_tx.send(result) {
                    tracing::error!("Failed to send subscription response: {:?}", e);
                }
            }

            WorkerCommand::Unsubscribe {
                client_id,
                token_address,
            } => {
                self.handle_unsubscribe(&client_id, &token_address).await?;
            }

            WorkerCommand::HealthCheck { response_tx } => {
                let status = self.get_status().await;
                if let Err(e) = response_tx.send(status) {
                    tracing::error!("Failed to send health check response: {:?}", e);
                }
            }

            WorkerCommand::Shutdown => {
                self.handle_shutdown().await;
                return Ok(());
            }
        }

        Ok(())
    }

    /// Process a batch of updates efficiently
    async fn process_batch_updates(&self, updates: &[PriceUpdate]) -> Result<(), AppError> {
        // Group updates by token
        let mut updates_by_token: HashMap<String, Vec<&PriceUpdate>> = HashMap::new();
        for update in updates {
            updates_by_token
                .entry(update.token_address.clone())
                .or_default()
                .push(update);
        }

        // Process each token's updates
        for (token_address, token_updates) in updates_by_token {
            // Get the latest update only
            if let Some(latest_update) = token_updates.last() {
                self.broadcast_price_update(latest_update).await?;
            }
        }

        Ok(())
    }

    /// Perform regular health checks and maintenance
    async fn perform_health_check(&mut self) -> Result<(), AppError> {
        let mut state = self.state.write().await;
        let now = Instant::now();
        state.last_health_check = now;

        // Check subscriptions
        let tokens_to_check: Vec<String> = state.token_subscriptions.keys().cloned().collect();
        drop(state); // Release lock before async operations

        for token_address in tokens_to_check {
            self.check_subscription_health(&token_address).await?;
        }

        // Clean up stale pending operations
        self.cleanup_stale_operations().await?;

        // Update metrics
        let state = self.state.read().await;
        self.metrics
            .active_subscriptions
            .store(state.token_subscriptions.len(), Ordering::Relaxed);
        self.metrics
            .connected_clients
            .store(state.client_subscriptions.len(), Ordering::Relaxed);
        *self.metrics.last_update.write() = Instant::now();

        Ok(())
    }

    /// Clean up stale pending operations
    async fn cleanup_stale_operations(&mut self) -> Result<(), AppError> {
        let mut state = self.state.write().await;
        let now = Instant::now();
        let stale_ops: Vec<String> = state
            .pending_operations
            .iter()
            .filter(|(_, op)| {
                op.started_at.elapsed() > Duration::from_secs(300) // 5 minutes timeout
            })
            .map(|(k, _)| k.clone())
            .collect();

        for op_key in stale_ops {
            if let Some(op) = state.pending_operations.remove(&op_key) {
                tracing::warn!("Removing stale operation: {:?}", op);
            }
        }

        Ok(())
    }

    /// Check the health of a subscription and attempt recovery if needed
    async fn check_subscription_health(&mut self, token_address: &str) -> Result<(), AppError> {
        let subscription = {
            let state = self.state.read().await;
            state.token_subscriptions.get(token_address).cloned()
        };

        if let Some(sub) = subscription {
            let needs_reconnect = match sub.state {
                SubscriptionState::Active => {
                    sub.last_update.elapsed() > self.config.subscription_timeout
                }
                SubscriptionState::Failed(_) => true,
                SubscriptionState::Reconnecting => sub.retry_count < self.config.max_retries,
                SubscriptionState::Initializing => {
                    sub.last_update.elapsed() > Duration::from_secs(30)
                }
            };

            if needs_reconnect {
                self.attempt_subscription_recovery(token_address).await?;
            }
        }

        Ok(())
    }

    /// Attempt to recover a failed subscription
    async fn attempt_subscription_recovery(&mut self, token_address: &str) -> Result<(), AppError> {
        let mut state = self.state.write().await;

        if let Some(subscription) = state.token_subscriptions.get_mut(token_address) {
            if subscription.retry_count >= self.config.max_retries {
                // Too many retries, mark as failed
                subscription.state =
                    SubscriptionState::Failed("Max retry attempts exceeded".to_string());
                return Ok(());
            }

            subscription.retry_count += 1;
            subscription.state = SubscriptionState::Reconnecting;

            // Release lock before attempting reconnection
            drop(state);

            match self.setup_rpc_subscription(token_address).await {
                Ok(_) => {
                    let mut state = self.state.write().await;
                    if let Some(sub) = state.token_subscriptions.get_mut(token_address) {
                        sub.state = SubscriptionState::Active;
                        sub.retry_count = 0;
                        sub.last_update = Instant::now();
                    }
                    Ok(())
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to recover subscription for {}: {}",
                        token_address,
                        e
                    );
                    Err(e)
                }
            }
        } else {
            Ok(())
        }
    }

    /// Handle graceful shutdown
    async fn handle_shutdown(&mut self) {
        tracing::info!(
            "Initiating shutdown for worker {}",
            self.config.partition_id
        );

        let state = self.state.read().await;

        // Clean up all subscriptions
        for (token_address, subscription) in &state.token_subscriptions {
            if let Some(sub_id) = &subscription.rpc_subscription_id {
                if let Err(e) = self.unsubscribe_from_rpc(token_address, sub_id).await {
                    tracing::error!(
                        "Error unsubscribing from {} during shutdown: {}",
                        token_address,
                        e
                    );
                }
            }
        }

        // Close WebSocket connection
        let mut ws_connection = self.ws_connection.write().await;
        if let Err(e) = ws_connection.shutdown().await {
            tracing::error!("Error shutting down WebSocket connection: {}", e);
        }

        tracing::info!("Worker {} shutdown complete", self.config.partition_id);
    }

    /// Get current worker status
    async fn get_status(&self) -> WorkerStatus {
        let state = self.state.read().await;
        WorkerStatus {
            partition_id: self.config.partition_id,
            active_tokens: state.token_subscriptions.len(),
            client_connections: state.client_subscriptions.len(),
            last_update: chrono::Utc::now(),
        }
    }

    /// Handle unsubscribe request
    async fn handle_unsubscribe(
        &mut self,
        client_id: &str,
        token_address: &str,
    ) -> Result<(), AppError> {
        let mut state = self.state.write().await;
        self.remove_client_subscription(client_id, token_address, &mut state);

        // Check if we need to clean up the token subscription
        if let Some(sub) = state.token_subscriptions.get_mut(token_address) {
            sub.clients.remove(client_id);

            if sub.clients.is_empty() {
                // No more clients for this token, unsubscribe from RPC
                if let Some(subscription_id) = sub.rpc_subscription_id.clone() {
                    drop(state);
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
                    let mut state = self.state.write().await;
                    // Remove the subscription
                    state.token_subscriptions.remove(token_address);
                    tracing::info!(
                        "Removed subscription for token {} as it has no more clients",
                        token_address
                    );
                }
            }
        }

        Ok(())
    }

    /// Remove client subscription while holding state lock
    fn remove_client_subscription(
        &self,
        client_id: &str,
        token_address: &str,
        state: &mut WorkerState,
    ) {
        if let Some(subscriptions) = state.client_subscriptions.get_mut(client_id) {
            subscriptions.remove(token_address);
            if subscriptions.is_empty() {
                state.client_subscriptions.remove(client_id);
            }
        }
    }

    /// Broadcast price update to subscribers
    async fn broadcast_price_update(&self, update: &PriceUpdate) -> Result<(), AppError> {
        // Send through price channel
        if let Err(e) = self.price_tx.send(update.clone()).await {
            tracing::error!("Failed to send price update: {}", e);
            return Err(AppError::MessageBatchError(format!(
                "Failed to send price update: {}",
                e
            )));
        }

        Ok(())
    }

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
        let ws_connection = self.ws_connection.write().await;

        // Send unsubscribe request
        ws_connection
            .send_message(
                token_address,
                WebSocketMessage::Text(request.to_string().into()),
            )
            .await?;

        // We don't wait for a response as we're cleaning up
        Ok(())
    }

    async fn handle_subscribe(
        &mut self,
        client_id: String,
        token_address: String,
    ) -> Result<(), AppError> {
        // Check if client is already subscribed to this token
        let mut state = self.state.write().await;
        if let Some(client_subs) = state.client_subscriptions.get_mut(&client_id) {
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
            state.client_subscriptions.insert(client_id.clone(), subs);
        }

        // Check if we already have this token
        if let Some(sub) = state.token_subscriptions.get_mut(&token_address) {
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
        drop(state); // Release lock before async operations

        match self.find_and_initialize_pool(&token_address).await {
            Ok(pool) => {
                let mut state = self.state.write().await;
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
                state
                    .token_subscriptions
                    .insert(token_address.clone(), subscription);
                drop(state);

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
                let mut state = self.state.write().await;
                self.remove_client_subscription(&client_id, &token_address, &mut state);
                Err(e)
            }
        }
    }

    async fn find_and_initialize_pool(&self, token_address: &str) -> Result<RaydiumPool, AppError> {
        tracing::info!("Finding pool for token {}", token_address);
        let token_pubkey = Pubkey::from_str(token_address)
            .map_err(|_| AppError::InvalidPoolAddress(token_address.to_string()))?;

        // SOL token pubkey - we'll use this to validate quote mint
        let sol_pubkey = Pubkey::from_str(WSOL)?;

        // Search for pool with token as base mint
        let memcmp = Memcmp::new(432, MemcmpEncodedBytes::Base58(token_pubkey.to_string()));
        let filters = vec![RpcFilterType::DataSize(752), RpcFilterType::Memcmp(memcmp)];

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

        best_pool
            .map(|(pool, _)| pool)
            .ok_or_else(|| AppError::PoolNotFound(token_address.to_string()))
    }
}
