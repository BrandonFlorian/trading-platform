mod event_system;
mod server_wallet_manager;
mod wallet_monitor;
use anyhow::{Context, Result};
use dotenv::dotenv;
use event_system::EventSystem;
use server_wallet_manager::ServerWalletManager;
use solana_client::rpc_client::RpcClient;
use solana_sdk::signer::Signer;
use solana_sdk::{pubkey::Pubkey, signature::Keypair};
use std::{env, sync::Arc};
use tokio::sync::mpsc;
use trading_common::database::SupabaseClient;
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

    let server_keypair = Keypair::from_base58_string(&server_secret_key);
    let user_id = server_keypair.pubkey().to_string();

    let supabase_client = SupabaseClient::new(
        &supabase_url,
        &supabase_key,
        &supabase_service_role_key,
        &user_id,
    );

    let rpc_client = Arc::new(RpcClient::new(rpc_http_url));
    let server_keypair = Keypair::from_base58_string(&server_secret_key);
    if server_keypair.pubkey() == Pubkey::default() {
        return Err(anyhow::anyhow!("Invalid server secret key"));
    }

    let event_system = Arc::new(EventSystem::new());

    let mut server_wallet_manager = ServerWalletManager::new(
        Arc::clone(&rpc_client),
        server_keypair.pubkey(),
        event_system.clone(),
    )
    .await
    .context("Failed to initialize ServerWalletManager")?;

    // Refresh balances before printing
    //server_wallet_manager.refresh_balances().await?;

    // Print server wallet balances
    println!("Server Wallet Address: {}", server_keypair.pubkey());
    println!(
        "SOL Balance: {} SOL",
        server_wallet_manager.get_sol_balance().await?
    );
    println!("Token Balances:");
    for token_info in server_wallet_manager.get_token_values() {
        println!(
            "  {}: {} {}",
            token_info.name, token_info.balance, token_info.symbol
        );
    }

    let (tx_sender, mut tx_receiver) = mpsc::channel(100);

    let mut monitor = WalletMonitor::new(
        Arc::clone(&rpc_client),
        rpc_ws_url,
        supabase_client,
        server_keypair,
        tx_sender,
        event_system.clone(),
    )
    .await?;

    tokio::spawn(async move {
        if let Err(e) = monitor.start().await {
            eprintln!("Wallet monitor error: {:?}", e);
        }
    });

    while let Some(transaction) = tx_receiver.recv().await {
        println!("Received transaction: {:?}", transaction);
        // Implement logic to handle received transactions
        // - Update database
        // - Send notifications
        // - etc.
    }

    Ok(())
}
