use crate::AppState;
use axum::{
    extract::{Path, State},
    Json,
};
use serde_json::json;
use std::sync::Arc;
use trading_common::{
    error::AppError,
    models::{BuyRequest, BuyResponse, SellRequest, SellResponse},
    process_buy_request, process_sell_request,
    utils::get_server_keypair,
    CopyTradeSettings, SupabaseClient, TrackedWallet, TransactionLog,
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
    println!("request: {:?}", request);
    let response = process_buy_request(&rpc_client, &server_keypair, request).await?;
    Ok(Json(response))
}

pub async fn pump_fun_sell(
    State(state): State<AppState>,
    Json(request): Json<SellRequest>,
) -> Result<Json<SellResponse>, AppError> {
    let rpc_client = state.rpc_client.load();
    let server_keypair = get_server_keypair();
    println!("request: {:?}", request);
    let response = process_sell_request(&rpc_client, &server_keypair, request).await?;
    Ok(Json(response))
}
