use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use trading_common::error::AppError;

use crate::raydium::RaydiumPool;

/// Core subscription state management
#[derive(Debug)]
pub struct SubscriptionStateManager {
    states: Arc<RwLock<HashMap<String, SubscriptionState>>>,
}

#[derive(Debug, Clone)]
pub struct SubscriptionState {
    pub pool: RaydiumPool,
    pub clients: HashSet<String>,
    pub rpc_subscription_id: Option<String>,
    pub last_update: Instant,
    pub status: ConnectionStatus,
    pub retry_count: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionStatus {
    Initializing,
    Connected,
    Disconnected { reason: String },
    Failed { error: String },
    Reconnecting { attempt: u32 },
}

impl SubscriptionStateManager {
    pub fn new() -> Self {
        Self {
            states: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Atomic subscription state update
    pub async fn update_subscription_state<F, T>(
        &self,
        token_address: &str,
        f: F,
    ) -> Result<T, AppError>
    where
        F: FnOnce(&mut SubscriptionState) -> Result<T, AppError>,
    {
        let mut states = self.states.write().await;

        if let Some(state) = states.get_mut(token_address) {
            f(state)
        } else {
            Err(AppError::SubscriptionNotFound(token_address.to_string()))
        }
    }

    /// Atomic client addition to subscription
    pub async fn add_client(&self, token_address: &str, client_id: &str) -> Result<(), AppError> {
        let mut states = self.states.write().await;

        if let Some(state) = states.get_mut(token_address) {
            state.clients.insert(client_id.to_string());
            Ok(())
        } else {
            Err(AppError::SubscriptionNotFound(token_address.to_string()))
        }
    }

    /// Atomic client removal from subscription
    pub async fn remove_client(
        &self,
        token_address: &str,
        client_id: &str,
    ) -> Result<bool, AppError> {
        let mut states = self.states.write().await;

        if let Some(state) = states.get_mut(token_address) {
            let removed = state.clients.remove(client_id);

            // If no clients left, return true to signal subscription can be removed
            Ok(removed && state.clients.is_empty())
        } else {
            Ok(false)
        }
    }

    /// Thread-safe subscription creation
    pub async fn create_subscription(
        &self,
        token_address: String,
        pool: RaydiumPool,
    ) -> Result<(), AppError> {
        let mut states = self.states.write().await;

        if states.contains_key(&token_address) {
            return Err(AppError::SubscriptionExists(token_address));
        }

        states.insert(
            token_address,
            SubscriptionState {
                pool,
                clients: HashSet::new(),
                rpc_subscription_id: None,
                last_update: Instant::now(),
                status: ConnectionStatus::Initializing,
                retry_count: 0,
            },
        );

        Ok(())
    }

    /// Atomic subscription removal
    pub async fn remove_subscription(&self, token_address: &str) -> Result<(), AppError> {
        let mut states = self.states.write().await;
        states.remove(token_address);
        Ok(())
    }

    /// Get all active subscriptions safely
    pub async fn get_active_subscriptions(&self) -> Vec<(String, HashSet<String>)> {
        let states = self.states.read().await;
        states
            .iter()
            .filter(|(_, state)| matches!(state.status, ConnectionStatus::Connected))
            .map(|(token, state)| (token.clone(), state.clients.clone()))
            .collect()
    }

    /// Atomic status update
    pub async fn update_status(
        &self,
        token_address: &str,
        new_status: ConnectionStatus,
    ) -> Result<(), AppError> {
        self.update_subscription_state(token_address, |state| {
            state.status = new_status;
            Ok(())
        })
        .await
    }

    /// Safe batch operation wrapper
    pub async fn batch_operation<F, T>(
        &self,
        token_addresses: &[String],
        f: F,
    ) -> Result<Vec<Result<T, AppError>>, AppError>
    where
        F: Fn(&str) -> Result<T, AppError> + Send + Sync,
    {
        let mut results = Vec::with_capacity(token_addresses.len());

        // Take write lock once for entire batch
        let states = self.states.write().await;

        for token_address in token_addresses {
            if states.contains_key(token_address) {
                results.push(f(token_address));
            } else {
                results.push(Err(AppError::SubscriptionNotFound(token_address.clone())));
            }
        }

        Ok(results)
    }

    /// Attempt to reconnect a subscription
    pub async fn reconnect_subscription(&self, token_address: &str) -> Result<(), AppError> {
        self.update_subscription_state(token_address, |state| {
            state.status = ConnectionStatus::Reconnecting {
                attempt: state.retry_count + 1,
            };
            state.retry_count += 1;
            Ok(())
        })
        .await?;

        // Return success - actual reconnection will be handled by the worker
        Ok(())
    }

    /// Check subscription health
    pub async fn check_subscription_health(&self, token_address: &str) -> Result<bool, AppError> {
        let states = self.states.read().await;

        if let Some(state) = states.get(token_address) {
            Ok(match state.status {
                ConnectionStatus::Connected => {
                    state.last_update.elapsed() <= Duration::from_secs(60)
                }
                _ => false,
            })
        } else {
            Ok(false)
        }
    }
}
