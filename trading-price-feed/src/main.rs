mod cache;
mod config;
mod pool_monitor;
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

    // Create connection monitor
    let connection_monitor = Arc::new(ConnectionMonitor::new(event_system.clone()));

    // Create configuration
    let config = PriceFeedConfig::new()
        .with_update_interval(std::time::Duration::from_secs(1))
        .with_cache_duration(std::time::Duration::from_secs(60));

    // Create and start price feed service
    let service = Arc::new(
        PriceFeedService::new(
            rpc_client,
            event_system,
            connection_monitor.clone(),
            &redis_url,
            config,
        )
        .await?,
    );

    // Start the service
    service.start().await?;

    let port = std::env::var("PRICE_FEED_PORT").context("PRICE_FEED_PORT must be set")?;
    // Create API router
    let app = routes::create_router(service.clone());
    let addr = SocketAddr::from(([0, 0, 0, 0], port.parse::<u16>()?));
    println!("Price feed API listening on {}", addr);

    // Create and bind TCP listener
    let listener = TcpListener::bind(addr).await?;

    // Run server
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app.into_make_service()).await {
            eprintln!("Server error: {}", e);
        }
    });

    // Handle shutdown signals
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;

    tokio::select! {
        _ = signal::ctrl_c() => {
            println!("Received Ctrl+C, initiating shutdown...");
        }
        _ = sigterm.recv() => {
            println!("Received termination signal, initiating shutdown...");
        }
    }

    // Graceful shutdown
    service.stop().await?;
    println!("Service stopped. Goodbye!");

    Ok(())
}
