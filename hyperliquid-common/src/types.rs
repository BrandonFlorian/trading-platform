use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum OrderSide {
    Buy,
    Sell,
}

// Hyperliquid specific order structures based on API docs
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitOrderType {
    pub tif: TimeInForce,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TriggerOrderType {
    #[serde(rename = "isMarket")]
    pub is_market: bool,
    #[serde(rename = "triggerPx")]
    pub trigger_px: String,
    pub tpsl: TpSl,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OrderType {
    Limit { limit: LimitOrderType },
    Trigger { trigger: TriggerOrderType },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimeInForce {
    Alo,  // Add Liquidity Only (Post Only)
    Ioc,  // Immediate or Cancel
    Gtc,  // Good Till Canceled
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TpSl {
    Tp,  // Take Profit
    Sl,  // Stop Loss
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PositionSide {
    Long,
    Short,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub asset: String,
    pub side: PositionSide,
    pub size: Decimal,
    pub entry_price: Decimal,
    pub mark_price: Decimal,
    pub unrealized_pnl: Decimal,
    pub margin: Decimal,
    pub leverage: u8,
}

// Hyperliquid API order structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperliquidOrder {
    pub a: u32,  // Asset index
    pub b: bool, // Buy = true, Sell = false
    pub p: String, // Price
    pub s: String, // Size
    pub r: bool, // Reduce only
    pub t: OrderType, // Order type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub c: Option<String>, // Client order ID (128-bit hex)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderAction {
    #[serde(rename = "type")]
    pub action_type: String, // "order"
    pub orders: Vec<HyperliquidOrder>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub builder: Option<BuilderInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuilderInfo {
    pub b: String, // Builder address
    pub f: u32,    // Fee in tenths of basis point
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub action: OrderAction,
    pub nonce: u64,
    pub signature: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vault_address: Option<String>,
}

// Simplified order request for our API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleOrderRequest {
    pub asset: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub size: Decimal,
    pub price: Option<Decimal>,
    pub reduce_only: bool,
    pub client_order_id: Option<String>,
    pub slippage_tolerance: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResponse {
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub status: String,
    pub filled_size: Decimal,
    pub remaining_size: Decimal,
    pub average_fill_price: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub address: String,
    pub equity: Decimal,
    pub margin_used: Decimal,
    pub available_margin: Decimal,
    pub positions: Vec<Position>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenOrder {
    pub order_id: String,
    pub asset: String,
    pub side: OrderSide,
    pub size: Decimal,
    pub price: Decimal,
    pub timestamp: u64,
    pub reduce_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopLossRequest {
    pub asset: String,
    pub trigger_price: Decimal,
    pub size: Option<Decimal>, // If None, closes entire position
    pub client_order_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopLossModifyRequest {
    pub asset: String,
    pub order_id: String,
    pub new_trigger_price: Decimal,
}