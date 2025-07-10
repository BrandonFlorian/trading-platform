// New main.rs using vault monitoring system
mod actors;
mod gateway;
mod messages;
mod models;
mod raydium;
mod vault_monitor;

// use actors::SubscriptionCoordinator; // Removed old implementation
use dotenv::dotenv;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
// use tokio::sync::oneshot; // Not used in new implementation
use trading_common::error::AppError;
use trading_common::redis::RedisPool;
use vault_monitor::SubscriptionManager;

#[tokio::main]
async fn main() -> Result<(), AppError> {
    // Load environment variables
    dotenv().ok();

    // Initialize tracing with appropriate level for production
    tracing_subscriber::fmt()
        .with_target(false)
        .init();

    // Get configuration from environment with validation
    let rpc_url = std::env::var("SOLANA_RPC_HTTP_URL")
        .map_err(|_| AppError::InternalError("SOLANA_RPC_HTTP_URL environment variable is required".to_string()))?;
    
    let ws_url = std::env::var("SOLANA_RPC_WS_URL")
        .map_err(|_| AppError::InternalError("SOLANA_RPC_WS_URL environment variable is required".to_string()))?;
    
    let redis_url = std::env::var("REDIS_URL")
        .map_err(|_| AppError::InternalError("REDIS_URL environment variable is required".to_string()))?;
    
    let port = std::env::var("PRICE_FEED_PORT")
        .map_err(|_| AppError::InternalError("PRICE_FEED_PORT environment variable is required".to_string()))?
        .parse::<u16>()
        .map_err(|e| AppError::InternalError(format!("Invalid PRICE_FEED_PORT (must be 1-65535): {}", e)))?;

    // Validate URLs
    if !rpc_url.starts_with("http://") && !rpc_url.starts_with("https://") {
        return Err(AppError::InternalError("SOLANA_RPC_HTTP_URL must start with http:// or https://".to_string()));
    }
    
    if !ws_url.starts_with("ws://") && !ws_url.starts_with("wss://") {
        return Err(AppError::InternalError("SOLANA_RPC_WS_URL must start with ws:// or wss://".to_string()));
    }
    
    if !redis_url.starts_with("redis://") && !redis_url.starts_with("rediss://") {
        return Err(AppError::InternalError("REDIS_URL must start with redis:// or rediss://".to_string()));
    }

    // Create clients
    let rpc_client = Arc::new(solana_client::rpc_client::RpcClient::new(rpc_url));
    let connection_monitor = Arc::new(trading_common::ConnectionMonitor::new(Arc::new(
        trading_common::event_system::EventSystem::new(),
    )));
    let redis_client = Arc::new(RedisPool::new(&redis_url, connection_monitor).await?);

    // Create the new vault-based subscription manager
    let (subscription_manager, price_receiver) = SubscriptionManager::new(
        ws_url,
        Arc::clone(&redis_client),
        Arc::clone(&rpc_client),
    );
    let subscription_manager = Arc::new(subscription_manager);

    // Create a broadcast channel for price updates to forward to clients
    let (price_tx, _) = tokio::sync::broadcast::channel(1000);

    // Start a task to forward price updates from subscription manager to the broadcast channel
    let price_tx_clone = price_tx.clone();
    let mut price_receiver = price_receiver;
    tokio::spawn(async move {
        while let Ok(price_update) = price_receiver.recv().await {
            if let Err(e) = price_tx_clone.send(price_update) {
                tracing::error!("Failed to broadcast price update: {}", e);
            }
        }
    });

    // Create placeholder coordinator channel (for gateway compatibility)
    let (coordinator_tx, _) = tokio::sync::mpsc::channel::<String>(100);

    // Create router with the existing gateway (we can update this later to use the new system directly)
    let app = gateway::create_router(coordinator_tx.clone(), price_tx.clone());

    // Create server address
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    // Create the listener
    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|e| AppError::InternalError(format!("Failed to bind to address: {}", e)))?;

    tracing::info!("Starting vault-based price feed server on {}", addr);

    // Create a shutdown channel
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    // Spawn the server task with shutdown signal
    let server_task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .map_err(|e| AppError::InternalError(format!("Server error: {}", e)))
    });

    // Example: Subscribe to a token for testing
    // You can remove this or make it configurable
    if let Ok(test_token) = std::env::var("TEST_TOKEN_ADDRESS") {
        tracing::info!("Test mode: subscribing to token {}", test_token);
        
        // Find the pool first
        let pool_finder = raydium::RaydiumPoolFinder::new(
            Arc::clone(&rpc_client),
            Arc::clone(&redis_client),
        );
        
        match pool_finder.find_pool(&test_token).await {
            Ok(mut pool) => {
                pool.load_metadata(&rpc_client, &redis_client).await?;
                
                tracing::info!("Found pool for {}:", test_token);
                tracing::info!("  Pool address: {}", pool.address);
                tracing::info!("  Base vault: {}", pool.base_vault);
                tracing::info!("  Quote vault: {}", pool.quote_vault);
                tracing::info!("  Base decimals: {}", pool.base_decimals);
                tracing::info!("  Quote decimals: {}", pool.quote_decimals);
                
                // Check vault balances immediately
                match rpc_client.get_token_account_balance(&pool.base_vault) {
                    Ok(base_balance) => {
                        tracing::info!("  Base vault balance: {}", base_balance.amount);
                    }
                    Err(e) => {
                        tracing::error!("  Failed to get base vault balance: {}", e);
                    }
                }
                
                match rpc_client.get_token_account_balance(&pool.quote_vault) {
                    Ok(quote_balance) => {
                        tracing::info!("  Quote vault balance: {}", quote_balance.amount);
                    }
                    Err(e) => {
                        tracing::error!("  Failed to get quote vault balance: {}", e);
                    }
                }
                
                let pool_state = vault_monitor::PoolMonitorState {
                    token_address: test_token.clone(),
                    pool_address: pool.address,
                    base_vault: pool.base_vault,
                    quote_vault: pool.quote_vault,
                    base_decimals: pool.base_decimals,
                    quote_decimals: pool.quote_decimals,
                    base_balance: None,
                    quote_balance: None,
                    last_price_update: None,
                };
                
                if let Err(e) = subscription_manager.subscribe_to_token(test_token.clone(), pool_state).await {
                    tracing::error!("Failed to subscribe to test token {}: {}", test_token, e);
                } else {
                    tracing::info!("Successfully subscribed to test token: {}", test_token);
                }
            }
            Err(e) => {
                tracing::error!("Failed to find pool for test token {}: {}", test_token, e);
            }
        }
    }

    // Start health monitoring task
    let subscription_manager_clone = Arc::clone(&subscription_manager);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        
        loop {
            interval.tick().await;
            
            let health = subscription_manager_clone.get_health_status().await;
            let metrics = subscription_manager_clone.get_metrics().await;
            
            tracing::info!(
                "Subscription Health: {}/{} connections healthy, {}/{} subscriptions healthy, {:.1}% uptime",
                health.healthy_connections,
                health.total_connections,
                health.healthy_subscriptions,
                health.total_subscriptions,
                metrics.uptime_percentage
            );
            
            // Auto-recovery for unhealthy subscriptions
            if metrics.uptime_percentage < 80.0 {
                tracing::warn!("Low subscription health, attempting recovery...");
                if let Err(e) = subscription_manager_clone.refresh_all_subscriptions().await {
                    tracing::error!("Failed to refresh subscriptions: {}", e);
                }
            }
        }
    });

    // Wait for shutdown signal
    shutdown_signal().await;

    // Send the shutdown signal to the server task
    let _ = shutdown_tx.send(());

    // Wait for server to shut down
    if let Err(e) = server_task.await {
        tracing::error!("Error waiting for server task: {}", e);
    }

    tracing::info!("Vault-based price feed server shut down gracefully");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("Received Ctrl+C, starting graceful shutdown");
        }
        _ = terminate => {
            tracing::info!("Received SIGTERM, starting graceful shutdown");
        }
    }
}