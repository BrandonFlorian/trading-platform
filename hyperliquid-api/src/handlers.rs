use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
};
use hyperliquid_common::{
    types::{SimpleOrderRequest, OrderResponse, Position, Account, OrderSide, OrderType, LimitOrderType, TimeInForce, TriggerOrderType, TpSl, OpenOrder, StopLossRequest, StopLossModifyRequest},
    HyperliquidClient,
};
use serde::{Deserialize, Serialize};
use rust_decimal::Decimal;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct MarketOrderRequest {
    pub asset: String,
    pub side: String, // "long" or "short"
    pub size: Decimal,
    pub reduce_only: Option<bool>,
    pub slippage_tolerance: Option<Decimal>,
}

#[derive(Debug, Deserialize)]
pub struct LimitOrderRequest {
    pub asset: String,
    pub side: String, // "long" or "short"
    pub size: Decimal,
    pub price: Decimal,
    pub reduce_only: Option<bool>,
    pub time_in_force: Option<String>, // "GTC", "IOC", "ALO"
}

#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }
    
    pub fn error(error: String) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(error),
        }
    }
}

pub async fn place_market_order(
    State(client): State<Arc<HyperliquidClient>>,
    Json(request): Json<MarketOrderRequest>,
) -> Result<Json<ApiResponse<OrderResponse>>, StatusCode> {
    // Convert to SimpleOrderRequest
    let order_request = SimpleOrderRequest {
        asset: request.asset,
        side: if request.side.to_lowercase() == "long" {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        },
        order_type: OrderType::Trigger {
            trigger: TriggerOrderType {
                is_market: true,
                trigger_px: "0".to_string(), // Market order
                tpsl: TpSl::Tp, // Default, not used for market orders
            },
        },
        size: request.size,
        price: None,
        reduce_only: request.reduce_only.unwrap_or(false),
        client_order_id: None,
        slippage_tolerance: request.slippage_tolerance,
    };
    
    match client.place_order(order_request).await {
        Ok(response) => Ok(Json(ApiResponse::success(response))),
        Err(e) => Ok(Json(ApiResponse::error(e.to_string()))),
    }
}

pub async fn place_limit_order(
    State(client): State<Arc<HyperliquidClient>>,
    Json(request): Json<LimitOrderRequest>,
) -> Result<Json<ApiResponse<OrderResponse>>, StatusCode> {
    // Convert to SimpleOrderRequest
    let tif = match request.time_in_force.as_deref() {
        Some("IOC") => TimeInForce::Ioc,
        Some("ALO") => TimeInForce::Alo,
        _ => TimeInForce::Gtc, // Default to GTC
    };
    
    let order_request = SimpleOrderRequest {
        asset: request.asset,
        side: if request.side.to_lowercase() == "long" {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        },
        order_type: OrderType::Limit {
            limit: LimitOrderType { tif },
        },
        size: request.size,
        price: Some(request.price),
        reduce_only: request.reduce_only.unwrap_or(false),
        client_order_id: None,
        slippage_tolerance: None,
    };
    
    match client.place_order(order_request).await {
        Ok(response) => Ok(Json(ApiResponse::success(response))),
        Err(e) => Ok(Json(ApiResponse::error(e.to_string()))),
    }
}

pub async fn get_positions(
    State(client): State<Arc<HyperliquidClient>>,
) -> Result<Json<ApiResponse<Vec<Position>>>, StatusCode> {

    let wallet_address = client.get_wallet_address();
    
    match client.get_positions(&wallet_address).await {
        Ok(positions) => Ok(Json(ApiResponse::success(positions))),
        Err(e) => Ok(Json(ApiResponse::error(e.to_string()))),
    }
}

pub async fn get_account_info(
    State(client): State<Arc<HyperliquidClient>>,
) -> Result<Json<ApiResponse<Account>>, StatusCode> {

    let wallet_address = client.get_wallet_address();
    
    match client.get_account_info(&wallet_address).await {
        Ok(account) => Ok(Json(ApiResponse::success(account))),
        Err(e) => Ok(Json(ApiResponse::error(e.to_string()))),
    }
}

pub async fn cancel_order(
    State(client): State<Arc<HyperliquidClient>>,
    Path(order_id): Path<String>,
) -> Result<Json<ApiResponse<()>>, StatusCode> {
    match client.cancel_order(&order_id).await {
        Ok(()) => Ok(Json(ApiResponse::success(()))),
        Err(e) => Ok(Json(ApiResponse::error(e.to_string()))),
    }
}

pub async fn get_universe(
    State(client): State<Arc<HyperliquidClient>>,
) -> Result<Json<ApiResponse<Vec<String>>>, StatusCode> {
    match client.get_universe().await {
        Ok(assets) => Ok(Json(ApiResponse::success(assets))),
        Err(e) => Ok(Json(ApiResponse::error(e.to_string()))),
    }
}

pub async fn get_open_orders(
    State(client): State<Arc<HyperliquidClient>>,
) -> Result<Json<ApiResponse<Vec<OpenOrder>>>, StatusCode> {
    match client.get_open_orders(None).await {
        Ok(orders) => Ok(Json(ApiResponse::success(orders))),
        Err(e) => Ok(Json(ApiResponse::error(e.to_string()))),
    }
}

pub async fn place_stop_loss(
    State(client): State<Arc<HyperliquidClient>>,
    Json(request): Json<StopLossRequest>,
) -> Result<Json<ApiResponse<OrderResponse>>, StatusCode> {
    match client.place_stop_loss(request).await {
        Ok(response) => Ok(Json(ApiResponse::success(response))),
        Err(e) => Ok(Json(ApiResponse::error(e.to_string()))),
    }
}

pub async fn modify_stop_loss(
    State(client): State<Arc<HyperliquidClient>>,
    Json(request): Json<StopLossModifyRequest>,
) -> Result<Json<ApiResponse<OrderResponse>>, StatusCode> {
    match client.modify_stop_loss(request).await {
        Ok(response) => Ok(Json(ApiResponse::success(response))),
        Err(e) => Ok(Json(ApiResponse::error(e.to_string()))),
    }
}

#[derive(Debug, Deserialize)]
pub struct CancelStopLossRequest {
    pub asset: String,
}

pub async fn cancel_stop_loss(
    State(client): State<Arc<HyperliquidClient>>,
    Path(order_id): Path<String>,
    Json(request): Json<CancelStopLossRequest>,
) -> Result<Json<ApiResponse<()>>, StatusCode> {
    match client.cancel_stop_loss(&request.asset, &order_id).await {
        Ok(()) => Ok(Json(ApiResponse::success(()))),
        Err(e) => Ok(Json(ApiResponse::error(e.to_string()))),
    }
}

pub async fn get_stop_losses(
    State(client): State<Arc<HyperliquidClient>>,
) -> Result<Json<ApiResponse<Vec<OpenOrder>>>, StatusCode> {
    match client.get_stop_losses(None).await {
        Ok(stop_losses) => Ok(Json(ApiResponse::success(stop_losses))),
        Err(e) => Ok(Json(ApiResponse::error(e.to_string()))),
    }
}