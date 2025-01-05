use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use axum::{
    routing::{delete, get, post, put},
    Router,
};
use dotenv::dotenv;
use solana_client::rpc_client::RpcClient;
use solana_sdk::signer::Signer;
use std::net::SocketAddr;
use std::{env, sync::Arc};
use tokio::net::TcpListener;
use trading_common::{data::get_server_keypair, event_system::EventSystem, SupabaseClient};
mod routes;

#[derive(Clone)]
struct AppState {
    rpc_client: Arc<ArcSwap<RpcClient>>,
    supabase_client: SupabaseClient,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let supabase_url = env::var("SUPABASE_URL").context("SUPABASE_URL must be set")?;
    let supabase_service_role_key =
        env::var("SUPABASE_SERVICE_ROLE_KEY").context("SUPABASE_SERVICE_ROLE_KEY must be set")?;

    let supabase_key =
        env::var("SUPABASE_ANON_PUBLIC_KEY").context("SUPABASE_ANON_PUBLIC_KEY must be set")?;

    let rpc_url = env::var("SOLANA_RPC_HTTP_URL").context("SOLANA_RPC_HTTP_URL must be set")?;
    println!("rpc_url: {}", rpc_url);

    let server_keypair = get_server_keypair();
    let user_id = server_keypair.pubkey().to_string();
    println!("user_id: {}", user_id);

    let event_system = Arc::new(EventSystem::new());

    let supabase_client = SupabaseClient::new(
        &supabase_url,
        &supabase_key,
        &supabase_service_role_key,
        &user_id,
        event_system,
    );
    let rpc_client = RpcClient::new(rpc_url);
    let shared_rpc_client = Arc::new(ArcSwap::from_pointee(rpc_client));

    let state = AppState {
        rpc_client: shared_rpc_client,
        supabase_client,
    };

    let app = Router::new()
        .route("/tracked_wallets", get(routes::get_tracked_wallets))
        .route("/tracked_wallets", post(routes::add_tracked_wallet))
        .route(
            "/tracked_wallets/archive/:wallet_address",
            put(routes::archive_tracked_wallet),
        )
        .route(
            "/tracked_wallets/unarchive/:wallet_address",
            put(routes::unarchive_tracked_wallet),
        )
        .route(
            "/tracked_wallets/:wallet_address",
            delete(routes::delete_tracked_wallet),
        )
        .route(
            "/tracked_wallets/update",
            put(routes::update_tracked_wallet),
        )
        .route("/copy_trade_settings", get(routes::get_copy_trade_settings))
        .route(
            "/copy_trade_settings",
            post(routes::create_copy_trade_settings),
        )
        .route(
            "/copy_trade_settings",
            put(routes::update_copy_trade_settings),
        )
        .route(
            "/copy_trade_settings/:tracked_wallet_id",
            delete(routes::delete_copy_trade_settings),
        )
        .route("/transaction_history", get(routes::get_transaction_history))
        .route("/pump_fun/buy", post(routes::pump_fun_buy))
        .route("/pump_fun/sell", post(routes::pump_fun_sell))
        .route("/raydium/buy", post(routes::raydium_buy))
        .route("/raydium/sell", post(routes::raydium_sell))
        .with_state(state);

    let port = env::var("API_PORT").unwrap_or_else(|_| "3000".to_string());
    let addr = SocketAddr::from(([0, 0, 0, 0], port.parse()?));

    println!("Server running on {}", addr);
    let listener = TcpListener::bind(addr)
        .await
        .context("Failed to bind to address")?;

    axum::serve(listener, app).await.context("Server error")?;

    Ok(())
}
