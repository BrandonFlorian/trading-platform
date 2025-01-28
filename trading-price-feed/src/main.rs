mod cache;
mod config;
mod pool_monitor;
mod raydium;
mod service;

use anyhow::{Context, Result};
use dotenv::dotenv;
use solana_client::rpc_client::RpcClient;
use std::sync::Arc;
use tokio::signal;
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
        .with_cache_duration(std::time::Duration::from_secs(60))
        .with_token_addresses(vec![
            "KENJSUYLASHUMfHyy5o4Hp2FdNqZg1AsUPhfH2kYvEP".to_string()
        ]);

    // Create and start price feed service
    let service = PriceFeedService::new(
        rpc_client,
        event_system,
        connection_monitor.clone(),
        &redis_url,
        config,
    )
    .await?;

    println!("Price feed service starting...");
    service.start().await?;

    service
        .pool_monitor
        .write()
        .await
        .add_pool("CpsMssqi3P9VMvNqxrdWVbSBCwyUHbGgNcrw7MorBq3g")
        .await?;

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
