use axum::{
    extract::{Path, State},
    Json,
};
use serde_json::json;
use trading_common::{
    error::AppError,
    models::{BuyRequest, SellRequest},
    CopyTradeSettings, SupabaseClient, TrackedWallet, TransactionLog,
};
use uuid::Uuid;

pub async fn get_tracked_wallets(
    State(client): State<SupabaseClient>,
) -> Result<Json<Vec<TrackedWallet>>, AppError> {
    let wallets = client.get_tracked_wallets().await?;
    Ok(Json(wallets))
}

pub async fn add_tracked_wallet(
    State(client): State<SupabaseClient>,
    Json(wallet): Json<TrackedWallet>,
) -> Result<Json<serde_json::Value>, AppError> {
    let result = client.add_tracked_wallet(wallet).await?;
    Ok(Json(
        json!({ "success": true, "tracked_wallet_id": result }),
    ))
}

pub async fn archive_tracked_wallet(
    State(client): State<SupabaseClient>,
    Path(wallet_address): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let result = client.archive_tracked_wallet(&wallet_address).await?;
    Ok(Json(json!({ "success": true, "message": result })))
}

pub async fn unarchive_tracked_wallet(
    State(client): State<SupabaseClient>,
    Path(wallet_address): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let result = client.unarchive_tracked_wallet(&wallet_address).await?;
    Ok(Json(json!({ "success": true, "message": result })))
}

pub async fn delete_tracked_wallet(
    State(client): State<SupabaseClient>,
    Path(wallet_address): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let result = client.delete_tracked_wallet(&wallet_address).await?;
    Ok(Json(json!({ "success": true, "message": result })))
}

pub async fn update_tracked_wallet(
    State(client): State<SupabaseClient>,
    Json(update): Json<TrackedWallet>,
) -> Result<Json<serde_json::Value>, AppError> {
    println!("update_tracked_wallet() called");
    let result = client.update_tracked_wallet(update).await?;
    println!("update_tracked_wallet() result: {:?}", result);
    Ok(Json(
        json!({ "success": true, "tracked_wallet_id": result }),
    ))
}

pub async fn get_copy_trade_settings(
    State(client): State<SupabaseClient>,
) -> Result<Json<Vec<CopyTradeSettings>>, AppError> {
    let settings = client.get_copy_trade_settings().await?;
    Ok(Json(settings))
}

pub async fn create_copy_trade_settings(
    State(client): State<SupabaseClient>,
    Json(settings): Json<CopyTradeSettings>,
) -> Result<Json<serde_json::Value>, AppError> {
    let result = client.create_copy_trade_settings(settings).await?;
    Ok(Json(json!({ "success": true, "settings_id": result })))
}

pub async fn update_copy_trade_settings(
    State(client): State<SupabaseClient>,
    Json(settings): Json<CopyTradeSettings>,
) -> Result<Json<serde_json::Value>, AppError> {
    let result = client.update_copy_trade_settings(settings).await?;
    Ok(Json(json!({ "success": true, "settings_id": result })))
}

pub async fn delete_copy_trade_settings(
    State(client): State<SupabaseClient>,
    Path(tracked_wallet_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, AppError> {
    let result = client.delete_copy_trade_settings(tracked_wallet_id).await?;
    Ok(Json(json!({ "success": true, "message": result })))
}

pub async fn get_transaction_history(
    State(client): State<SupabaseClient>,
) -> Result<Json<Vec<TransactionLog>>, AppError> {
    let transactions = client.get_transaction_history().await?;
    Ok(Json(transactions))
}

pub async fn buy_pump_fun_token(
    Json(buy_request): Json<BuyRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    unimplemented!()
}

pub async fn sell_pump_fun_token(
    Json(sell_request): Json<SellRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    unimplemented!()
}
