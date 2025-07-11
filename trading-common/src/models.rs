use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator::Validate;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalletStateChangeType {
    Added,
    Archived,
    Unarchived,
    Updated,
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletStateChange {
    pub wallet_address: String,
    pub change_type: WalletStateChangeType,
    pub timestamp: DateTime<Utc>,
    pub details: Option<serde_json::Value>,
}

impl WalletStateChange {
    pub fn new(wallet_address: String, change_type: WalletStateChangeType) -> Self {
        Self {
            wallet_address,
            change_type,
            timestamp: Utc::now(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: impl Into<serde_json::Value>) -> Self {
        self.details = Some(details.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletStateNotification {
    pub data: WalletStateChange,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Validate)]
pub struct TrackedWallet {
    pub id: Option<Uuid>,
    pub user_id: Option<Uuid>,

    #[validate(custom(function = "crate::validation::validate_solana_address"))]
    pub wallet_address: String,

    pub is_active: bool,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Validate)]
pub struct CopyTradeSettings {
    pub id: Option<Uuid>,
    pub user_id: Option<Uuid>,
    pub tracked_wallet_id: Uuid,
    pub is_enabled: bool,

    #[validate(custom(function = "crate::validation::validate_sol_amount_safe"))]
    pub trade_amount_sol: f64,

    #[validate(custom(function = "crate::validation::validate_slippage_percentage"))]
    pub max_slippage: f64,

    #[serde(default)]
    #[validate(custom(function = "crate::validation::validate_max_positions"))]
    pub max_open_positions: i32,

    pub allowed_tokens: Option<Vec<String>>,

    pub use_allowed_tokens_list: bool,
    pub allow_additional_buys: bool,
    pub match_sell_percentage: bool,

    #[validate(custom(function = "crate::validation::validate_min_sol_balance"))]
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
            trade_amount_sol: 0.01,
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
pub struct SettingsUpdateNotification {
    pub data: CopyTradeSettings,
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
pub struct WalletUpdate {
    pub balance: f64,
    pub tokens: Vec<TokenInfo>,
    pub address: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WalletUpdateNotification {
    pub data: WalletUpdate,
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

#[derive(Debug, Serialize, Deserialize, Clone, Validate)]
pub struct BuyRequest {
    #[validate(custom(function = "crate::validation::validate_solana_address"))]
    pub token_address: String,

    #[validate(custom(function = "crate::validation::validate_sol_amount_safe"))]
    pub sol_quantity: f64,

    #[validate(custom(function = "crate::validation::validate_slippage_percentage"))]
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
#[derive(Debug, Serialize, Deserialize, Clone, Validate)]
pub struct SellRequest {
    #[validate(custom(function = "crate::validation::validate_solana_address"))]
    pub token_address: String,

    #[validate(custom(function = "crate::validation::validate_token_quantity"))]
    pub token_quantity: f64,

    #[validate(custom(function = "crate::validation::validate_slippage_percentage"))]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TokenInfo {
    pub address: String,
    pub symbol: String,
    pub name: String,
    pub balance: String,
    pub metadata_uri: Option<String>,
    pub decimals: u8,
    pub market_cap: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum ConnectionType {
    WebSocket,
    Grpc,
    Redis,
    Database,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConnectionStatus {
    Connected,
    Disconnected,
    Error,
    Reconnecting,
    Connecting,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionStatusChange {
    pub connection_type: ConnectionType,
    pub status: ConnectionStatus,
    pub timestamp: DateTime<Utc>,
    pub details: Option<String>,
}

impl ConnectionStatusChange {
    pub fn new(connection_type: ConnectionType, status: ConnectionStatus) -> Self {
        Self {
            connection_type,
            status,
            timestamp: Utc::now(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }
}

/// Price update for a token
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceUpdate {
    /// Token address (base mint)
    pub token_address: String,

    /// Price in SOL
    pub price_sol: f64,

    /// Price in USD
    pub price_usd: Option<f64>,

    /// Market cap in USD
    pub market_cap: f64,

    /// Timestamp of update
    pub timestamp: i64,

    /// DEx type
    pub dex_type: DexType,

    /// Liquidity in SOL
    pub liquidity: Option<f64>,

    /// Liquidity in USD
    pub liquidity_usd: Option<f64>,

    /// Pool address
    pub pool_address: Option<String>,

    /// 24h volume in USD
    pub volume_24h: Option<f64>,

    /// 6h volume in USD
    pub volume_6h: Option<f64>,

    /// 1h volume in USD
    pub volume_1h: Option<f64>,

    /// 5m volume in USD
    pub volume_5m: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceUpdateNotification {
    pub data: PriceUpdate,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionStatusNotification {
    pub data: ConnectionStatusChange,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeExecution {
    pub id: Uuid,
    pub trade_type: String,       // "manual" or "copy"
    pub dex_type: String,         // "pump_fun" or "raydium"
    pub transaction_type: String, // "buy" or "sell"
    pub token_address: String,
    pub amount: f64,
    pub price_sol: f64,
    pub signature: String,
    pub timestamp: DateTime<Utc>,
    pub status: String, // "success", "failed", "pending"
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeExecutionNotification {
    pub data: TradeExecution,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TransactionState {
    Submitted,      // Transaction sent to network
    Confirmed,      // Transaction confirmed on chain
    Failed(String), // Transaction failed with error message
    Dropped,        // Transaction dropped from mempool
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionStateChange {
    pub signature: String,
    pub state: TransactionState,
    pub timestamp: DateTime<Utc>,
    pub details: Option<serde_json::Value>,
}

impl TransactionStateChange {
    pub fn new(signature: String, state: TransactionState) -> Self {
        Self {
            signature,
            state,
            timestamp: Utc::now(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: impl Into<serde_json::Value>) -> Self {
        self.details = Some(details.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionStateNotification {
    pub data: TransactionStateChange,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Watchlist {
    pub id: Option<Uuid>,
    pub user_id: Option<String>,
    pub name: String,
    pub description: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchlistToken {
    pub id: Option<Uuid>,
    pub watchlist_id: Uuid,
    pub token_address: String,
    pub added_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchlistWithTokens {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub tokens: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolPriceUpdate {
    pub price_usd: f64,
    pub source: PriceSource,
    pub timestamp: i64,
    pub confidence: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PriceSource {
    Pyth,
    Raydium,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolPriceUpdateNotification {
    pub data: SolPriceUpdate,
    #[serde(rename = "type")]
    pub type_: String,
}
