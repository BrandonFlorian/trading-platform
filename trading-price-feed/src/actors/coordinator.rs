use solana_client::rpc_client::RpcClient;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use trading_common::error::AppError;
use trading_common::models::PriceUpdate;
use trading_common::redis::RedisPool;

use crate::models::{CoordinatorCommand, PoolCommand, PoolEvent, PoolState};

use super::pool::PoolFactory;

/// Structure to track pool information
struct PoolInfo {
    /// Command channel to the pool actor
    command_tx: mpsc::Sender<PoolCommand>,

    /// Task handle for the pool actor
    task: JoinHandle<()>,

    /// Set of client IDs subscribed to this pool
    subscribers: HashSet<String>,

    /// Current state of the pool
    state: PoolState,
}

/// Structure to track client information
struct ClientInfo {
    /// Set of token addresses this client is subscribed to
    subscriptions: HashSet<String>,
}

/// Subscription coordinator - manages all subscriptions and routes price updates
pub struct SubscriptionCoordinator {
    /// Receiver for coordinator commands
    command_rx: mpsc::Receiver<CoordinatorCommand>,

    /// Sender for coordinator commands (for cloning)
    command_tx: mpsc::Sender<CoordinatorCommand>,

    /// Factory for creating pool actors
    pool_factory: PoolFactory,

    /// Receiver for pool events
    pool_event_rx: broadcast::Receiver<PoolEvent>,

    /// Sender for price updates to clients
    price_tx: broadcast::Sender<PriceUpdate>,

    /// Map of token addresses to pool information
    pools: HashMap<String, PoolInfo>,

    /// Map of client IDs to client information
    clients: HashMap<String, ClientInfo>,
}

impl SubscriptionCoordinator {
    /// Create a new subscription coordinator
    pub fn new(
        rpc_client: Arc<RpcClient>,
        redis_client: Arc<RedisPool>,
    ) -> (
        Self,
        mpsc::Sender<CoordinatorCommand>,
        broadcast::Sender<PriceUpdate>,
    ) {
        let (command_tx, command_rx) = mpsc::channel(100);
        let (price_tx, price_rx) = broadcast::channel(1000);

        // Create pool factory
        let (pool_factory, pool_event_rx) = PoolFactory::new(rpc_client, redis_client);

        let coordinator = Self {
            command_rx,
            command_tx: command_tx.clone(),
            pool_factory,
            pool_event_rx,
            price_tx: price_tx.clone(),
            pools: HashMap::new(),
            clients: HashMap::new(),
        };

        (coordinator, command_tx, price_tx)
    }

    /// Start the subscription coordinator
    pub async fn run(mut self) {
        tracing::info!("Starting subscription coordinator");

        // Set up task to process pool events
        let price_tx = self.price_tx.clone();
        let mut pool_event_rx = self.pool_event_rx.resubscribe();

        let event_task = tokio::spawn(async move {
            while let Ok(event) = pool_event_rx.recv().await {
                match event {
                    PoolEvent::PriceUpdated(price_update) => {
                        if let Err(e) = price_tx.send(price_update) {
                            tracing::error!("Failed to forward price update: {}", e);
                        }
                    }
                    PoolEvent::StateChanged {
                        token_address,
                        state,
                    } => {
                        tracing::info!("Pool {} state changed to {:?}", token_address, state);
                    }
                }
            }
        });

        // Main loop to process coordinator commands
        while let Some(cmd) = self.command_rx.recv().await {
            match cmd {
                CoordinatorCommand::Subscribe {
                    client_id,
                    token_address,
                    response_tx,
                } => {
                    let result = self.handle_subscribe(&client_id, &token_address).await;
                    let _ = response_tx.send(result);
                }
                CoordinatorCommand::Unsubscribe {
                    client_id,
                    token_address,
                } => {
                    let _ = self.handle_unsubscribe(&client_id, &token_address).await;
                }
                CoordinatorCommand::ClientDisconnected { client_id } => {
                    let _ = self.handle_client_disconnected(&client_id).await;
                }
                CoordinatorCommand::Shutdown { response_tx } => {
                    let result = self.handle_shutdown().await;
                    let _ = response_tx.send(result);
                    break;
                }
            }
        }

        // Cancel event task
        event_task.abort();

        tracing::info!("Subscription coordinator shut down");
    }

    /// Handle a subscribe command
    async fn handle_subscribe(
        &mut self,
        client_id: &str,
        token_address: &str,
    ) -> Result<(), AppError> {
        tracing::info!(
            "Handling subscription from {} to {}",
            client_id,
            token_address
        );

        // Register client if not already registered
        if !self.clients.contains_key(client_id) {
            self.clients.insert(
                client_id.to_string(),
                ClientInfo {
                    subscriptions: HashSet::new(),
                },
            );
        }

        // Add token to client's subscriptions
        if let Some(client_info) = self.clients.get_mut(client_id) {
            client_info.subscriptions.insert(token_address.to_string());
        }

        // Create or update pool for this token
        if !self.pools.contains_key(token_address) {
            // Create new pool actor
            let (command_tx, task) = self.pool_factory.create_pool(token_address.to_string());

            // Initialize the pool
            let (init_tx, init_rx) = oneshot::channel();
            command_tx
                .send(PoolCommand::Initialize {
                    response_tx: init_tx,
                })
                .await
                .map_err(|_| {
                    AppError::ChannelSendError("Failed to send initialize command".to_string())
                })?;

            init_rx.await.map_err(|_| {
                AppError::ChannelReceiveError("Failed to receive initialize response".to_string())
            })??;

            // Create pool info
            let mut subscribers = HashSet::new();
            subscribers.insert(client_id.to_string());

            self.pools.insert(
                token_address.to_string(),
                PoolInfo {
                    command_tx,
                    task,
                    subscribers,
                    state: PoolState::Active,
                },
            );

            tracing::info!("Created new pool actor for {}", token_address);
        } else if let Some(pool_info) = self.pools.get_mut(token_address) {
            // Add client to existing pool's subscribers
            pool_info.subscribers.insert(client_id.to_string());
            tracing::info!(
                "Added client {} to existing pool {}",
                client_id,
                token_address
            );
        }

        Ok(())
    }

    /// Handle an unsubscribe command
    async fn handle_unsubscribe(
        &mut self,
        client_id: &str,
        token_address: &str,
    ) -> Result<(), AppError> {
        tracing::info!(
            "Handling unsubscription from {} to {}",
            client_id,
            token_address
        );

        // Remove token from client's subscriptions
        if let Some(client_info) = self.clients.get_mut(client_id) {
            client_info.subscriptions.remove(token_address);

            // Remove client if they have no more subscriptions
            if client_info.subscriptions.is_empty() {
                self.clients.remove(client_id);
                tracing::info!(
                    "Removed client {} as they have no more subscriptions",
                    client_id
                );
            }
        }

        // Remove client from pool's subscribers
        if let Some(pool_info) = self.pools.get_mut(token_address) {
            pool_info.subscribers.remove(client_id);

            // Shut down pool if it has no more subscribers
            if pool_info.subscribers.is_empty() {
                self.shutdown_pool(token_address).await?;
                tracing::info!(
                    "Shut down pool {} as it has no more subscribers",
                    token_address
                );
            }
        }

        Ok(())
    }

    /// Handle a client disconnected command
    async fn handle_client_disconnected(&mut self, client_id: &str) -> Result<(), AppError> {
        tracing::info!("Handling disconnection of client {}", client_id);

        // Get all tokens this client was subscribed to
        let tokens = if let Some(client_info) = self.clients.get(client_id) {
            client_info.subscriptions.clone()
        } else {
            return Ok(());
        };

        // Remove client from all pools
        for token_address in tokens {
            self.handle_unsubscribe(client_id, &token_address).await?;
        }

        // Remove client
        self.clients.remove(client_id);

        Ok(())
    }

    /// Shut down a pool
    async fn shutdown_pool(&mut self, token_address: &str) -> Result<(), AppError> {
        if let Some(pool_info) = self.pools.remove(token_address) {
            // Send shutdown command
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            pool_info
                .command_tx
                .send(PoolCommand::Shutdown {
                    response_tx: shutdown_tx,
                })
                .await
                .map_err(|_| {
                    AppError::ChannelSendError("Failed to send shutdown command".to_string())
                })?;

            // Wait for response
            let _ = shutdown_rx.await.map_err(|_| {
                AppError::ChannelReceiveError("Failed to receive shutdown response".to_string())
            })?;

            // Abort task if it's still running
            pool_info.task.abort();

            tracing::info!("Pool {} shut down", token_address);
        }

        Ok(())
    }

    /// Handle a shutdown command
    async fn handle_shutdown(&mut self) -> Result<(), AppError> {
        tracing::info!("Shutting down subscription coordinator");

        // Shut down all pools
        let tokens: Vec<String> = self.pools.keys().cloned().collect();
        for token_address in tokens {
            self.shutdown_pool(&token_address).await?;
        }

        Ok(())
    }
}
