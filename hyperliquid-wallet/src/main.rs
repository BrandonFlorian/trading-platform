mod service;

use anyhow::Result;
use hyperliquid_common::{HyperliquidClient, generated::wallet::wallet_service_server::WalletServiceServer};
use service::HyperliquidWalletService;
use std::{env, net::SocketAddr, sync::Arc};
use tonic::transport::Server;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    
    info!("Starting Hyperliquid Wallet service...");
    
    // Get configuration
    let private_key = env::var("HYPERLIQUID_PRIVATE_KEY")
        .expect("HYPERLIQUID_PRIVATE_KEY must be set");
    let testnet = env::var("HYPERLIQUID_TESTNET")
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()
        .unwrap_or(false);
    
    // Initialize client
    let client = Arc::new(HyperliquidClient::new(&private_key, testnet).await?);
    
    info!("Hyperliquid Wallet service initialized");
    
    // Set up gRPC server
    let wallet_service = HyperliquidWalletService::new(client);
    let wallet_server = WalletServiceServer::new(wallet_service);
    
    let port = env::var("HYPERLIQUID_WALLET_PORT")
        .unwrap_or_else(|_| "50052".to_string())
        .parse::<u16>()?;
    
    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    
    info!("Hyperliquid Wallet gRPC service listening on {}", addr);
    
    // Start the gRPC server
    Server::builder()
        .add_service(wallet_server)
        .serve_with_shutdown(addr, async {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down Hyperliquid Wallet service...");
        })
        .await?;
    
    Ok(())
}