mod wallet_monitor;
use anyhow::{Context, Result};
use dotenv::dotenv;
use solana_client::rpc_client::RpcClient;
use solana_sdk::signer::Signer;
use solana_sdk::{pubkey::Pubkey, signature::Keypair};
use std::{env, sync::Arc};
use tokio::signal;
use trading_common::{
    database::SupabaseClient, event_system::EventSystem, server_wallet_client::WalletClient,
    websocket::WebSocketServer,
};
use trading_common::{redis::RedisPool, ConnectionMonitor};
use wallet_monitor::WalletMonitor;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    // Solana
    let rpc_http_url = env::var("SOLANA_RPC_HTTP_URL").context("SOLANA_RPC_URL must be set")?;
    let rpc_ws_url = env::var("SOLANA_RPC_WS_URL").context("SOLANA_RPC_WS_URL must be set")?;

    // Server wallet
    let server_secret_key =
        env::var("SERVER_WALLET_SECRET_KEY").context("SERVER_WALLET_SECRET_KEY must be set")?;

    let server_keypair = Keypair::from_base58_string(&server_secret_key);
    if server_keypair.pubkey() == Pubkey::default() {
        return Err(anyhow::anyhow!("Invalid server secret key"));
    }
    let user_id = server_keypair.pubkey().to_string();

    // Supabase
    let supabase_url = env::var("SUPABASE_URL").context("SUPABASE_URL must be set")?;
    println!("Supabase URL: {}", supabase_url);
    let supabase_key =
        env::var("SUPABASE_ANON_PUBLIC_KEY").context("SUPABASE_ANON_PUBLIC_KEY must be set")?;
    println!("Supabase anon public key: {}", supabase_key);
    let supabase_service_role_key =
        env::var("SUPABASE_SERVICE_ROLE_KEY").context("SUPABASE_SERVICE_ROLE_KEY must be set")?;
    println!("Supabase service role key: {}", supabase_service_role_key);
    // Redis
    let redis_url = env::var("REDIS_URL").context("REDIS_URL must be set")?;
    println!("Redis URL: {}", redis_url);
    // Event system
    let event_system = Arc::new(EventSystem::new());

    // Connection monitor
    let connection_monitor = Arc::new(ConnectionMonitor::new(event_system.clone()));

    // Initialize Redis Pool
    println!("Initializing Redis pool...");
    let redis_pool = Arc::new(
        RedisPool::new(&redis_url, connection_monitor.clone())
            .await
            .context("Failed to create Redis pool")?,
    );

    // Subscribe to updates
    println!("Setting up Redis subscriptions...");
    if let Err(e) = redis_pool.subscribe_to_updates().await {
        eprintln!("Failed to set up Redis subscription: {}", e);
    } else {
        println!("Redis subscription set up successfully");
    }

    // Wallet client
    let wallet_addr =
        std::env::var("WALLET_SERVICE_URL").context("WALLET_SERVICE_URL must be set")?;
    let wallet_client =
        Arc::new(WalletClient::connect(wallet_addr.clone(), connection_monitor.clone()).await?);

    println!(
        "Wallet client connected successfully with address: {}",
        wallet_addr
    );

    // Supabase client
    let mut supabase_client = SupabaseClient::new(
        &supabase_url,
        &supabase_key,
        &supabase_service_role_key,
        &user_id,
        event_system.clone(),
    );
    
    // Initialize user
    supabase_client.initialize_user().await?;
    let supabase_client = Arc::new(supabase_client);

    println!("Supabase client initialized successfully");
    // RPC client
    let rpc_client = Arc::new(RpcClient::new(rpc_http_url));

    println!("RPC client initialized successfully");

    // Wallet monitor
    let mut monitor = WalletMonitor::new(
        Arc::clone(&rpc_client),
        rpc_ws_url,
        Arc::clone(&supabase_client),
        server_keypair,
        event_system.clone(),
        Arc::clone(&wallet_client),
        Arc::clone(&connection_monitor),
    )
    .await?;

    println!("Wallet monitor initialized successfully");

    // WebSocket server
    let websocket_port = env::var("WS_PORT")
        .unwrap_or_else(|_| "3001".to_string())
        .parse()?;

    let ws_server = WebSocketServer::new(
        Arc::clone(&event_system),
        Arc::clone(&wallet_client),
        websocket_port,
        Arc::clone(&connection_monitor),
    );

    println!("WebSocket server initialized successfully");

    //Start WebSocket server
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

    // Store redis_pool in a variable that will live until shutdown
    let redis_pool_for_shutdown = Arc::clone(&redis_pool);

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

    // Clean up Redis connections
    drop(redis_pool_for_shutdown);
    println!("Shutdown complete.");

    Ok(())
}
