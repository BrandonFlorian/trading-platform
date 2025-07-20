use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperliquidTransaction {
    pub id: Uuid,
    pub user_id: Uuid,
    pub order_id: String,
    pub asset: String,
    pub side: String,  // "long" or "short"
    pub size: Decimal,
    pub price: Decimal,
    pub transaction_type: String, // "open", "close", "liquidation"
    pub status: String,
    pub fee: Decimal,
    pub realized_pnl: Option<Decimal>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperliquidTradeSettings {
    pub id: Uuid,
    pub user_id: Uuid,
    pub max_position_size: Decimal,
    pub default_leverage: u8,
    pub max_leverage: u8,
    pub default_slippage: Decimal,
    pub stop_loss_percentage: Option<Decimal>,
    pub take_profit_percentage: Option<Decimal>,
    pub is_enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperliquidApiKey {
    pub id: Uuid,
    pub user_id: Uuid,
    pub address: String,
    pub encrypted_private_key: String,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
}