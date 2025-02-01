mod price_monitor;
mod raydium;
mod routes;
mod service;

use crate::service::SolPriceFeedService;
use anyhow::{Context, Result};
use dotenv::dotenv;
use solana_client::rpc_client::RpcClient;
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    net::TcpListener,
    signal::{
        self,
        unix::{signal, SignalKind},
    },
};
use trading_common::{event_system::EventSystem, ConnectionMonitor};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Load configuration
    let rpc_url =
        std::env::var("SOLANA_RPC_HTTP_URL").context("SOLANA_RPC_HTTP_URL must be set")?;
    let redis_url = std::env::var("REDIS_URL").context("REDIS_URL must be set")?;
    let port = std::env::var("SOL_PRICE_FEED_PORT").context("SOL_PRICE_FEED_PORT must be set")?;

    // Create RPC client
    let rpc_client = Arc::new(RpcClient::new(rpc_url));

    // Create event system
    let event_system = Arc::new(EventSystem::new());

    // Create connection monitor
    let connection_monitor = Arc::new(ConnectionMonitor::new(event_system.clone()));

    // Create and start price feed service
    let service = Arc::new(
        SolPriceFeedService::new(
            rpc_client,
            event_system,
            connection_monitor.clone(),
            &redis_url,
        )
        .await?,
    );

    service.start().await?;

    // Create API router
    let app = routes::create_router(service.clone());
    let addr = SocketAddr::from(([0, 0, 0, 0], port.parse::<u16>()?));
    println!("SOL price feed API listening on {}", addr);

    // Create and bind TCP listener
    let listener = TcpListener::bind(addr).await?;

    // Create signal handler for graceful shutdown
    let mut sigterm = signal(SignalKind::terminate())?;
    let server_handle = tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .context("Server error")
    });

    // Wait for shutdown signal
    tokio::select! {
        _ = signal::ctrl_c() => {
            println!("Received Ctrl+C, initiating graceful shutdown...");
        }
        _ = sigterm.recv() => {
            println!("Received SIGTERM, initiating graceful shutdown...");
        }
        res = server_handle => {
            if let Err(e) = res {
                println!("Server error: {}", e);
            }
        }
    }

    // Graceful shutdown
    println!("Stopping SOL price feed service...");
    if let Err(e) = service.stop().await {
        eprintln!("Error during service shutdown: {}", e);
    }
    println!("Service stopped successfully");

    Ok(())
}
