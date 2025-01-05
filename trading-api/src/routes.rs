use crate::AppState;
use axum::{
    extract::{Path, State},
    Json,
};
use chrono::Utc;
use serde_json::json;
use solana_sdk::signer::Signer;
use trading_common::{
    data::get_server_keypair,
    error::AppError,
    models::{BuyRequest, BuyResponse, SellRequest, SellResponse},
    pumpdotfun::{buy::process_buy_request, sell::process_sell_request},
    raydium::{
        buy::process_buy_request as process_raydium_buy,
        sell::process_sell_request as process_raydium_sell,
    },
    CopyTradeSettings, TrackedWallet, TransactionLog,
};
use uuid::Uuid;

pub async fn get_tracked_wallets(
    State(state): State<AppState>,
) -> Result<Json<Vec<TrackedWallet>>, AppError> {
    let wallets = state.supabase_client.get_tracked_wallets().await?;
    Ok(Json(wallets))
}

pub async fn add_tracked_wallet(
    State(state): State<AppState>,
    Json(wallet): Json<TrackedWallet>,
) -> Result<Json<serde_json::Value>, AppError> {
    let result = state.supabase_client.add_tracked_wallet(wallet).await?;
    Ok(Json(
        json!({ "success": true, "tracked_wallet_id": result }),
    ))
}

pub async fn archive_tracked_wallet(
    State(state): State<AppState>,
    Path(wallet_address): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let result = state
        .supabase_client
        .archive_tracked_wallet(&wallet_address)
        .await?;
    Ok(Json(json!({ "success": true, "message": result })))
}

pub async fn unarchive_tracked_wallet(
    State(state): State<AppState>,
    Path(wallet_address): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let result = state
        .supabase_client
        .unarchive_tracked_wallet(&wallet_address)
        .await?;
    Ok(Json(json!({ "success": true, "message": result })))
}

pub async fn delete_tracked_wallet(
    State(state): State<AppState>,
    Path(wallet_address): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let result = state
        .supabase_client
        .delete_tracked_wallet(&wallet_address)
        .await?;
    Ok(Json(json!({ "success": true, "message": result })))
}

pub async fn update_tracked_wallet(
    State(state): State<AppState>,
    Json(update): Json<TrackedWallet>,
) -> Result<Json<serde_json::Value>, AppError> {
    println!("update_tracked_wallet() called");
    let result = state.supabase_client.update_tracked_wallet(update).await?;
    println!("update_tracked_wallet() result: {:?}", result);
    Ok(Json(
        json!({ "success": true, "tracked_wallet_id": result }),
    ))
}

pub async fn get_copy_trade_settings(
    State(state): State<AppState>,
) -> Result<Json<Vec<CopyTradeSettings>>, AppError> {
    let settings = state.supabase_client.get_copy_trade_settings().await?;
    Ok(Json(settings))
}

pub async fn create_copy_trade_settings(
    State(state): State<AppState>,
    Json(settings): Json<CopyTradeSettings>,
) -> Result<Json<serde_json::Value>, AppError> {
    let result = state
        .supabase_client
        .create_copy_trade_settings(settings)
        .await?;
    Ok(Json(json!({ "success": true, "settings_id": result })))
}

pub async fn update_copy_trade_settings(
    State(state): State<AppState>,
    Json(settings): Json<CopyTradeSettings>,
) -> Result<Json<serde_json::Value>, AppError> {
    let result = state
        .supabase_client
        .update_copy_trade_settings(settings)
        .await?;
    Ok(Json(json!({ "success": true, "settings_id": result })))
}

pub async fn delete_copy_trade_settings(
    State(state): State<AppState>,
    Path(tracked_wallet_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, AppError> {
    let result = state
        .supabase_client
        .delete_copy_trade_settings(tracked_wallet_id)
        .await?;
    Ok(Json(json!({ "success": true, "message": result })))
}

pub async fn get_transaction_history(
    State(state): State<AppState>,
) -> Result<Json<Vec<TransactionLog>>, AppError> {
    let transactions = state.supabase_client.get_transaction_history().await?;
    Ok(Json(transactions))
}

pub async fn pump_fun_buy(
    State(state): State<AppState>,
    Json(request): Json<BuyRequest>,
) -> Result<Json<BuyResponse>, AppError> {
    let rpc_client = state.rpc_client.load();
    let server_keypair = get_server_keypair();

    let token_address = request.token_address.clone();
    println!("request: {:?}", request);

    let response = process_buy_request(&rpc_client, &server_keypair, request).await?;

    if response.success {
        // Log successful manual trade
        let transaction_log = TransactionLog {
            id: Uuid::new_v4(),
            user_id: server_keypair.pubkey().to_string(),
            tracked_wallet_id: None, // None indicates manual trade
            signature: response.signature.clone(),
            transaction_type: "Buy".to_string(),
            token_address,
            amount: response.token_quantity,
            price_sol: response.sol_spent,
            timestamp: Utc::now(),
        };

        if let Err(e) = state.supabase_client.log_transaction(transaction_log).await {
            println!("Failed to log transaction: {}", e);
            // Continue with response even if logging fails
        }
    }

    Ok(Json(response))
}

pub async fn pump_fun_sell(
    State(state): State<AppState>,
    Json(request): Json<SellRequest>,
) -> Result<Json<SellResponse>, AppError> {
    let rpc_client = state.rpc_client.load();
    let server_keypair = get_server_keypair();

    println!("request: {:?}", request);

    let token_address = request.token_address.clone();

    let response = process_sell_request(&rpc_client, &server_keypair, request).await?;

    if response.success {
        // Log successful manual trade
        let transaction_log = TransactionLog {
            id: Uuid::new_v4(),
            user_id: server_keypair.pubkey().to_string(),
            tracked_wallet_id: None, // None indicates manual trade
            signature: response.signature.clone(),
            transaction_type: "Sell".to_string(),
            token_address,
            amount: response.token_quantity,
            price_sol: response.sol_received,
            timestamp: Utc::now(),
        };

        if let Err(e) = state.supabase_client.log_transaction(transaction_log).await {
            println!("Failed to log transaction: {}", e);
            // Continue with response even if logging fails
        }
    }
    Ok(Json(response))
}

pub async fn raydium_buy(
    State(state): State<AppState>,
    Json(request): Json<BuyRequest>,
) -> Result<Json<BuyResponse>, AppError> {
    let rpc_client = state.rpc_client.load();
    let server_keypair = get_server_keypair();

    println!("Processing Raydium buy request: {:?}", request);

    let token_address = request.token_address.clone();

    let response = process_raydium_buy(&rpc_client, &server_keypair, &request).await?;

    if response.success {
        // Log successful manual trade
        let transaction_log = TransactionLog {
            id: Uuid::new_v4(),
            user_id: server_keypair.pubkey().to_string(),
            tracked_wallet_id: None, // None indicates manual trade
            signature: response.signature.clone(),
            transaction_type: "Buy".to_string(),
            token_address,
            amount: response.token_quantity,
            price_sol: response.sol_spent,
            timestamp: Utc::now(),
        };

        if let Err(e) = state.supabase_client.log_transaction(transaction_log).await {
            println!("Failed to log transaction: {}", e);
            // Continue with response even if logging fails
        }
    }

    Ok(Json(response))
}

pub async fn raydium_sell(
    State(state): State<AppState>,
    Json(request): Json<SellRequest>,
) -> Result<Json<SellResponse>, AppError> {
    let rpc_client = state.rpc_client.load();
    let server_keypair = get_server_keypair();

    println!("Processing Raydium sell request: {:?}", request);
    let token_address = request.token_address.clone();
    let response = process_raydium_sell(&rpc_client, &server_keypair, &request).await?;

    if response.success {
        // Log successful manual trade
        let transaction_log = TransactionLog {
            id: Uuid::new_v4(),
            user_id: server_keypair.pubkey().to_string(),
            tracked_wallet_id: None, // None indicates manual trade
            signature: response.signature.clone(),
            transaction_type: "Sell".to_string(),
            token_address,
            amount: response.token_quantity,
            price_sol: response.sol_received,
            timestamp: Utc::now(),
        };

        if let Err(e) = state.supabase_client.log_transaction(transaction_log).await {
            println!("Failed to log transaction: {}", e);
            // Continue with response even if logging fails
        }
    }

    Ok(Json(response))
}
