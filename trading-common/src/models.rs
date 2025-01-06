use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::dex::DexType;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum TransactionType {
    Buy,
    Sell,
    Transfer,
    Unknown,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientTxInfo {
    pub signature: String,
    pub token_address: String,
    pub token_name: String,
    pub token_symbol: String,
    pub transaction_type: TransactionType,
    pub amount_token: f64,
    pub amount_sol: f64,
    pub price_per_token: f64,
    pub token_image_uri: String,
    pub market_cap: f64,
    pub usd_market_cap: f64,
    pub timestamp: i64,
    pub seller: String,
    pub buyer: String,
    pub dex_type: DexType,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct User {
    pub id: Option<Uuid>,
    pub wallet_address: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrackedWallet {
    pub id: Option<Uuid>,
    pub user_id: Option<String>,
    pub wallet_address: String,
    pub is_active: bool,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CopyTradeSettings {
    pub id: Option<Uuid>,
    pub user_id: Option<String>,
    pub tracked_wallet_id: Uuid,
    pub is_enabled: bool,
    pub trade_amount_sol: f64,
    pub max_slippage: f64,
    #[serde(default)]
    pub max_open_positions: i32,
    pub allowed_tokens: Option<Vec<String>>,
    pub use_allowed_tokens_list: bool,
    pub allow_additional_buys: bool,
    pub match_sell_percentage: bool,
    pub min_sol_balance: f64,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

impl Default for CopyTradeSettings {
    fn default() -> Self {
        Self {
            id: None,
            user_id: None,
            tracked_wallet_id: Uuid::nil(),
            is_enabled: false,
            trade_amount_sol: 0.0,
            max_slippage: 0.1,
            max_open_positions: 1,
            allowed_tokens: None,
            use_allowed_tokens_list: false,
            allow_additional_buys: false,
            match_sell_percentage: false,
            min_sol_balance: 0.01,
            created_at: None,
            updated_at: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransactionLog {
    pub id: Uuid,
    pub user_id: String,
    pub tracked_wallet_id: Option<Uuid>,
    pub signature: String,
    pub transaction_type: String,
    pub token_address: String,
    pub amount: f64,
    pub price_sol: f64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CopyTradeNotification {
    pub data: ClientTxInfo,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrackedWalletNotification {
    pub data: ClientTxInfo,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransactionLoggedNotification {
    pub data: TransactionLog,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WalletUpdateNotification {
    pub data: serde_json::Value,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug)]
pub struct TokenTransactionInfo {
    pub token_address: String,
    pub amount: f64,
    pub sol_amount: f64,
    pub price_per_token: f64,
    pub buyer: String,
    pub seller: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BuyRequest {
    pub token_address: String,
    pub sol_quantity: f64,
    pub slippage_tolerance: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BuyResponse {
    pub success: bool,
    pub signature: String,
    pub solscan_tx_url: String,
    pub token_quantity: f64,
    pub sol_spent: f64,
    pub error: Option<String>,
}

//sell request
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SellRequest {
    pub token_address: String,
    pub token_quantity: f64,
    pub slippage_tolerance: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SellResponse {
    pub success: bool,
    pub signature: String,
    pub token_quantity: f64,
    pub sol_received: f64,
    pub solscan_tx_url: String,
    pub error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BuyTokenCalculations {
    pub token_out: u64,
    pub max_sol_cost: u64,
    pub price_per_token: f64,
    pub max_token_output: f64,
    pub min_token_output: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatabaseOperationEvent {
    pub operation_type: String, // "insert", "update", "delete", "select"
    pub table: String,
    pub success: bool,
    pub duration_ms: u64,
    pub error: Option<String>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ErrorEvent {
    pub error_type: String,
    pub message: String,
    pub context: serde_json::Value,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatabaseNotification {
    pub data: DatabaseOperationEvent,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ErrorNotification {
    pub data: ErrorEvent,
    #[serde(rename = "type")]
    pub type_: String,
}
