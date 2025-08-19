mod handlers;

use axum::{
    routing::{get, post},
    Router,
};
use hyperliquid_common::HyperliquidClient;
use std::{net::SocketAddr, sync::Arc};
use tower_http::cors::{Any, CorsLayer};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    
    info!("Starting Hyperliquid API server...");
    
    // Initialize Hyperliquid client
    let private_key = std::env::var("HYPERLIQUID_PRIVATE_KEY")
        .expect("HYPERLIQUID_PRIVATE_KEY must be set");
    let testnet = std::env::var("HYPERLIQUID_TESTNET")
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()
        .unwrap_or(false);
    
    let client = Arc::new(
        HyperliquidClient::new(&private_key, testnet)
            .await
            .expect("Failed to initialize Hyperliquid client")
    );
    
    // Build our application with routes
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/api/trade/market", post(handlers::place_market_order))
        .route("/api/trade/limit", post(handlers::place_limit_order))
        .route("/api/positions", get(handlers::get_positions))
        .route("/api/account", get(handlers::get_account_info))
        .route("/api/orders/{order_id}/cancel", post(handlers::cancel_order))
        .route("/api/orders", get(handlers::get_open_orders))
        .route("/api/stop-loss", post(handlers::place_stop_loss))
        .route("/api/stop-loss", get(handlers::get_stop_losses))
        .route("/api/stop-loss/modify", post(handlers::modify_stop_loss))
        .route("/api/stop-loss/{order_id}/cancel", post(handlers::cancel_stop_loss))
        .route("/api/universe", get(handlers::get_universe))
        .with_state(client)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );
    
    let port = std::env::var("HYPERLIQUID_API_PORT")
        .unwrap_or_else(|_| "3100".to_string())
        .parse::<u16>()?;
    
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("Hyperliquid API listening on {}", addr);
    
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    
    Ok(())
}

async fn health_check() -> &'static str {
    "Hyperliquid API is healthy"
}