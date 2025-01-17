use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use axum::{
    http::{HeaderValue, Method},
    routing::{delete, get, post, put},
    Router,
};
use dotenv::dotenv;
use solana_client::rpc_client::RpcClient;
use solana_sdk::signer::Signer;
use std::net::SocketAddr;
use std::{env, sync::Arc};
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use trading_common::ConnectionMonitor;
use trading_common::{
    data::get_server_keypair, event_system::EventSystem, redis_connection::RedisConnection,
    server_wallet_client::WalletClient, SupabaseClient,
};

mod routes;

#[derive(Clone)]
struct AppState {
    rpc_client: Arc<ArcSwap<RpcClient>>,
    supabase_client: SupabaseClient,
    redis_connection: RedisConnection,
    wallet_client: WalletClient,
    event_system: Arc<EventSystem>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    // Supabase
    let supabase_url = env::var("SUPABASE_URL").context("SUPABASE_URL must be set")?;
    let supabase_service_role_key =
        env::var("SUPABASE_SERVICE_ROLE_KEY").context("SUPABASE_SERVICE_ROLE_KEY must be set")?;

    let supabase_key =
        env::var("SUPABASE_ANON_PUBLIC_KEY").context("SUPABASE_ANON_PUBLIC_KEY must be set")?;

    // Solana
    let rpc_url = env::var("SOLANA_RPC_HTTP_URL").context("SOLANA_RPC_HTTP_URL must be set")?;

    // Redis
    let redis_url = env::var("REDIS_URL").context("REDIS_URL must be set")?;
    println!("rpc_url: {}", rpc_url);

    // Create server keypair
    let server_keypair = get_server_keypair();
    let user_id = server_keypair.pubkey().to_string();
    println!("user_id: {}", user_id);

    // Create event system
    let event_system = Arc::new(EventSystem::new());

    // Create connection monitor
    let connection_monitor = Arc::new(ConnectionMonitor::new(event_system.clone()));

    // Create wallet client
    let wallet_service_url =
        env::var("WALLET_SERVICE_URL").context("WALLET_SERVICE_URL must be set")?;
    let wallet_client =
        WalletClient::connect(wallet_service_url, connection_monitor.clone()).await?;

    // Create supabase client
    let supabase_client = SupabaseClient::new(
        &supabase_url,
        &supabase_key,
        &supabase_service_role_key,
        &user_id,
        event_system.clone(),
    );

    // Create rpc client
    let rpc_client = RpcClient::new(rpc_url);
    let shared_rpc_client = Arc::new(ArcSwap::from_pointee(rpc_client));

    println!("redis_url: {}", redis_url);
    // Create redis connection
    let redis_connection = RedisConnection::new(&redis_url, connection_monitor.clone()).await?;
    println!("redis_connection created");

    let state = AppState {
        rpc_client: shared_rpc_client,
        supabase_client,
        redis_connection,
        wallet_client,
        event_system,
    };

    // CORS
    let cors_allowed_origins = env::var("CORS_ALLOWED_ORIGINS").unwrap_or_else(|_| "".to_string());
    let cors = CorsLayer::new()
        .allow_origin(cors_allowed_origins.parse::<HeaderValue>().unwrap())
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([
            axum::http::header::CONTENT_TYPE,
            axum::http::header::AUTHORIZATION,
        ])
        .allow_credentials(true);

    // Create app
    let app = Router::new()
        .route("/wallet/info", get(routes::get_wallet_info))
        .route("/tracked_wallets", get(routes::get_tracked_wallets))
        .route("/tracked_wallets", post(routes::add_tracked_wallet))
        .route(
            "/tracked_wallets/archive/{wallet_address}",
            put(routes::archive_tracked_wallet),
        )
        .route(
            "/tracked_wallets/unarchive/{wallet_address}",
            put(routes::unarchive_tracked_wallet),
        )
        .route(
            "/tracked_wallets/{wallet_address}",
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
            "/copy_trade_settings/{tracked_wallet_id}",
            delete(routes::delete_copy_trade_settings),
        )
        .route("/transaction_history", get(routes::get_transaction_history))
        .route("/pump_fun/buy", post(routes::pump_fun_buy))
        .route("/pump_fun/sell", post(routes::pump_fun_sell))
        .route("/raydium/buy", post(routes::raydium_buy))
        .route("/raydium/sell", post(routes::raydium_sell))
        .route("/wallet/{wallet_address}", get(routes::get_wallet_details))
        .route(
            "/token_metadata/{token_address}",
            get(routes::get_token_metadata),
        )
        .route("/wallet/update", post(routes::trigger_wallet_update))
        .with_state(state)
        .layer(cors);
    let port = env::var("API_PORT").unwrap_or_else(|_| "3000".to_string());
    let addr = SocketAddr::from(([0, 0, 0, 0], port.parse()?));

    println!("Server running on {}", addr);
    let listener = TcpListener::bind(addr)
        .await
        .context("Failed to bind to address")?;

    axum::serve(listener, app).await.context("Server error")?;

    Ok(())
}
