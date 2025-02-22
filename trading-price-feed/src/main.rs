mod config;
mod pool_monitor_websocket;
mod price_calculator;

mod raydium;
mod routes;
mod service;

use anyhow::{Context, Result};
use dotenv::dotenv;
use solana_client::rpc_client::RpcClient;
use std::{net::SocketAddr, sync::Arc};
use tokio::{net::TcpListener, signal};
use trading_common::{event_system::EventSystem, ConnectionMonitor};

use crate::{config::PriceFeedConfig, service::PriceFeedService};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Load configuration from environment
    let rpc_url =
        std::env::var("SOLANA_RPC_HTTP_URL").context("SOLANA_RPC_HTTP_URL must be set")?;
    let redis_url = std::env::var("REDIS_URL").context("REDIS_URL must be set")?;

    // Create RPC client
    let rpc_client = Arc::new(RpcClient::new(rpc_url));

    // Create event system
    let event_system = Arc::new(EventSystem::new());
    let _event_rx = event_system.subscribe();

    // Create connection monitor
    let connection_monitor = Arc::new(ConnectionMonitor::new(event_system.clone()));

    // Create configuration with correct WebSocket URL
    let config = PriceFeedConfig::from_env()
        .context("Failed to create price feed config")?
        .with_update_interval(std::time::Duration::from_secs(1))
        .with_cache_duration(std::time::Duration::from_secs(60));

    let http_addr = SocketAddr::from(([0, 0, 0, 0], config.http_port));

    // Create service
    let service = Arc::new(
        PriceFeedService::new(rpc_client, connection_monitor.clone(), &redis_url, config).await?,
    );

    // Start service
    service.start().await?;

    // Create router with both HTTP and WebSocket endpoints
    let app = routes::create_router(service.clone());

    // Create and bind TCP listener
    let listener = TcpListener::bind(http_addr).await?;
    println!("Server listening on {}", http_addr);

    // Start HTTP and WebSocket server
    let server = axum::serve(listener, app.into_make_service());

    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;

    tokio::select! {
        result = server => {
            if let Err(e) = result {
                eprintln!("Server error: {}", e);
            }
        }
        _ = signal::ctrl_c() => {
            println!("Received Ctrl+C, initiating shutdown...");
        }
        _ = sigterm.recv() => {
            println!("Received termination signal, initiating shutdown...");
        }
    }

    // Graceful shutdown
    service.pool_monitor.stop().await?;
    println!("Service stopped. Goodbye!");

    Ok(())
}
