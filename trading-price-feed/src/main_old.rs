// src/main.rs
mod actors;
mod gateway;
mod messages;
mod models;
mod raydium;
mod vault_monitor;

use actors::SubscriptionCoordinator;
use dotenv::dotenv;

use models::CoordinatorCommand;
use solana_client::rpc_client::RpcClient;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::oneshot;
use trading_common::error::AppError;
use trading_common::redis::RedisPool;

#[tokio::main]
async fn main() -> Result<(), AppError> {
    // Load environment variables
    dotenv().ok();

    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Get configuration from environment
    let rpc_url = std::env::var("SOLANA_RPC_HTTP_URL")
        .map_err(|_| AppError::InternalError("SOLANA_RPC_HTTP_URL not set".to_string()))?;
    let redis_url = std::env::var("REDIS_URL")
        .map_err(|_| AppError::InternalError("REDIS_URL not set".to_string()))?;
    let port = std::env::var("PRICE_FEED_PORT")
        .map_err(|_| AppError::InternalError("PRICE_FEED_PORT not set".to_string()))?
        .parse::<u16>()
        .map_err(|_| AppError::InternalError("Invalid PRICE_FEED_PORT".to_string()))?;

    // Create clients
    let rpc_client = Arc::new(RpcClient::new(rpc_url));
    let connection_monitor = Arc::new(trading_common::ConnectionMonitor::new(Arc::new(
        trading_common::event_system::EventSystem::new(),
    )));
    let redis_client = Arc::new(RedisPool::new(&redis_url, connection_monitor).await?);

    // Create subscription coordinator
    let (coordinator, coordinator_tx, price_tx) =
        SubscriptionCoordinator::new(rpc_client.clone(), redis_client.clone());

    // Start coordinator task
    let coordinator_handle = tokio::spawn(async move {
        coordinator.run().await;
    });

    // Create router
    let app = gateway::create_router(coordinator_tx.clone(), price_tx.clone());

    // Create server address
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    // Create the listener
    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|e| AppError::InternalError(format!("Failed to bind to address: {}", e)))?;

    tracing::info!("Starting server on {}", addr);

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

    // Wait for shutdown signal
    shutdown_signal().await;

    // Send the shutdown signal to the server task
    let _ = shutdown_tx.send(());

    // Wait for server to shut down
    if let Err(e) = server_task.await {
        tracing::error!("Error waiting for server task: {}", e);
    }

    // Shutdown coordinator
    tracing::info!("Shutting down coordinator");
    let (tx, rx) = oneshot::channel();
    if let Err(e) = coordinator_tx
        .send(CoordinatorCommand::Shutdown { response_tx: tx })
        .await
    {
        tracing::error!("Failed to send shutdown command: {}", e);
    }

    // Wait for coordinator to shut down
    if let Err(e) = rx.await {
        tracing::error!("Failed to receive shutdown response: {}", e);
    }

    // Wait for coordinator task to finish
    if let Err(e) = coordinator_handle.await {
        tracing::error!("Error waiting for coordinator task: {}", e);
    }

    tracing::info!("Server shut down gracefully");
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
