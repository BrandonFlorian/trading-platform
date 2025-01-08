use anyhow::{Context, Result};
use dotenv::dotenv;
use tonic::transport::Server;
use tracing::info;
use trading_common::proto::wallet;
use wallet_service::WalletServiceImpl;

mod wallet_service;

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables
    dotenv().ok();

    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Get service address from environment or use default
    let addr: std::net::SocketAddr = std::env::var("WALLET_SERVICE_URL")
        .context("WALLET_SERVICE_URL must be set")?
        .replace("http://", "") // Remove http:// prefix if present
        .parse()
        .context("Invalid address")?;
    let wallet_service = WalletServiceImpl::new().await?;

    info!("Wallet service starting on {}", addr);

    Server::builder()
        .add_service(wallet::wallet_service_server::WalletServiceServer::new(
            wallet_service,
        ))
        .serve(addr)
        .await?;

    Ok(())
}
