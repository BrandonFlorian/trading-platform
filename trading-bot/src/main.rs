mod wallet_monitor;
use anyhow::{Context, Result};
use dotenv::dotenv;
use solana_client::rpc_client::RpcClient;
use solana_sdk::signer::Signer;
use solana_sdk::{pubkey::Pubkey, signature::Keypair};
use std::{env, sync::Arc};
use tokio::signal;
use trading_common::wallet_client::WalletClient;
use trading_common::RedisConnection;
use trading_common::{
    database::SupabaseClient, event_system::EventSystem,
    server_wallet_manager::ServerWalletManager, websocket::WebSocketServer,
};
use wallet_monitor::WalletMonitor;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let rpc_http_url = env::var("SOLANA_RPC_HTTP_URL").context("SOLANA_RPC_URL must be set")?;
    let rpc_ws_url = env::var("SOLANA_RPC_WS_URL").context("SOLANA_RPC_WS_URL must be set")?;
    let server_secret_key =
        env::var("SERVER_WALLET_SECRET_KEY").context("SERVER_WALLET_SECRET_KEY must be set")?;
    let supabase_url = env::var("SUPABASE_URL").context("SUPABASE_URL must be set")?;
    let supabase_key =
        env::var("SUPABASE_ANON_PUBLIC_KEY").context("SUPABASE_ANON_PUBLIC_KEY must be set")?;
    let supabase_service_role_key =
        env::var("SUPABASE_SERVICE_ROLE_KEY").context("SUPABASE_SERVICE_ROLE_KEY must be set")?;
    let redis_url = env::var("REDIS_URL").context("REDIS_URL must be set")?;
    let server_keypair = Keypair::from_base58_string(&server_secret_key);
    if server_keypair.pubkey() == Pubkey::default() {
        return Err(anyhow::anyhow!("Invalid server secret key"));
    }
    let user_id = server_keypair.pubkey().to_string();

    // Create a single event system
    let event_system = Arc::new(EventSystem::new());

    let wallet_addr =
        std::env::var("WALLET_SERVICE_URL").context("WALLET_SERVICE_URL must be set")?;

    let wallet_client = WalletClient::connect(wallet_addr).await?;

    // Subscribe to settings updates from the API
    println!("Setting up Redis subscription...");
    if let Err(e) =
        RedisConnection::subscribe_to_settings_updates(&redis_url, event_system.clone()).await
    {
        eprintln!("Failed to set up Redis subscription: {}", e);
        // Don't fail startup, just log the error
    } else {
        println!("Redis subscription set up successfully");
    }

    let supabase_client = Arc::new(SupabaseClient::new(
        &supabase_url,
        &supabase_key,
        &supabase_service_role_key,
        &user_id,
        event_system.clone(),
    ));

    let rpc_client = Arc::new(RpcClient::new(rpc_http_url));

    // Initialize wallet manager with the same event system
    let server_wallet_manager = Arc::new(tokio::sync::Mutex::new(
        ServerWalletManager::new(
            Arc::clone(&rpc_client),
            server_keypair.pubkey(),
            event_system.clone(), // Share the event system
        )
        .await
        .context("Failed to initialize ServerWalletManager")?,
    ));

    // Print initial wallet state
    {
        let wallet_manager = server_wallet_manager.lock().await;
        println!("Server Wallet Address: {}", server_keypair.pubkey());
        println!(
            "SOL Balance: {} SOL",
            wallet_manager.get_sol_balance().await?
        );
        println!("Token Balances:");
        for token_info in wallet_manager.get_token_values() {
            println!(
                "  {}: {} {}",
                token_info.name, token_info.balance, token_info.symbol
            );
        }
    }

    let mut monitor = WalletMonitor::new(
        Arc::clone(&rpc_client),
        rpc_ws_url,
        Arc::clone(&supabase_client),
        server_keypair,
        event_system.clone(),
        Arc::clone(&server_wallet_manager),
        Arc::new(wallet_client),
    )
    .await?;

    let websocket_port = env::var("WS_PORT")
        .unwrap_or_else(|_| "3001".to_string())
        .parse()?;

    let ws_server = WebSocketServer::new(
        Arc::clone(&event_system),
        Arc::clone(&server_wallet_manager),
        supabase_client,
        websocket_port,
    );

    // Start WebSocket server
    tokio::spawn(async move {
        if let Err(e) = ws_server.start().await {
            eprintln!("WebSocket server error: {}", e);
        }
    });

    println!("WebSocket server started on port {}", websocket_port);

    let mut shutdown_monitor = monitor.clone();

    // Create signal handler before select
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
        .context("Failed to create SIGTERM signal handler")?;

    let monitor_handle = tokio::spawn(async move {
        if let Err(e) = monitor.start().await {
            eprintln!("Wallet monitor error: {:?}", e);
        }
    });

    // Handle shutdown signals
    tokio::select! {
        _ = signal::ctrl_c() => {
            println!("\nReceived Ctrl+C, initiating graceful shutdown...");
        }
        _ = sigterm.recv() => {
            println!("\nReceived termination signal, initiating graceful shutdown...");
        }
        _ = monitor_handle => {
            println!("\nMonitor task completed.");
        }
    }

    // Perform graceful shutdown
    if let Err(e) = shutdown_monitor.stop().await {
        eprintln!("Error during shutdown: {:?}", e);
    }
    println!("Shutdown complete.");

    Ok(())
}
