use std::time::Instant;

use tokio::sync::broadcast;

use crate::models::{
    ConnectionStatusNotification, CopyTradeNotification, DatabaseNotification,
    DatabaseOperationEvent, ErrorEvent, ErrorNotification, SettingsUpdateNotification,
    TrackedWalletNotification, TradeExecutionNotification, TransactionLoggedNotification,
    WalletStateNotification, WalletUpdateNotification,
};

#[derive(Clone)]
pub enum Event {
    TrackedWalletTransaction(TrackedWalletNotification),
    CopyTradeExecution(CopyTradeNotification),
    WalletUpdate(WalletUpdateNotification),
    TransactionLogged(TransactionLoggedNotification),
    DatabaseOperation(DatabaseNotification),
    Error(ErrorNotification),
    SettingsUpdate(SettingsUpdateNotification),
    WalletStateChange(WalletStateNotification),
    ConnectionStatus(ConnectionStatusNotification),
    TradeExecution(TradeExecutionNotification),
}
pub struct EventSystem {
    sender: broadcast::Sender<Event>,
}

impl EventSystem {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(100);
        Self { sender }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        println!("Subscribing to events");
        self.sender.subscribe()
    }

    pub fn emit(&self, event: Event) {
        let _ = self.sender.send(event);
    }

    pub async fn handle_transaction_logged(&self, notification: TransactionLoggedNotification) {
        println!("Handling transaction logged");
        self.emit(Event::TransactionLogged(notification));
    }

    pub async fn handle_copy_trade_executed(&self, notification: CopyTradeNotification) {
        println!("Handling copy trade executed");
        self.emit(Event::CopyTradeExecution(notification));
    }

    pub async fn handle_tracked_wallet_trade(&self, notification: TrackedWalletNotification) {
        println!("Handling tracked wallet trade");
        self.emit(Event::TrackedWalletTransaction(notification));
    }

    pub async fn handle_settings_update(&self, notification: SettingsUpdateNotification) {
        println!("Handling settings update");
        self.emit(Event::SettingsUpdate(notification));
    }

    pub async fn handle_wallet_updated(&self, notification: WalletUpdateNotification) {
        println!("Handling wallet update");
        self.emit(Event::WalletUpdate(notification));
    }

    pub async fn handle_database_operation(&self, notification: DatabaseNotification) {
        println!("Handling database operation");
        self.emit(Event::DatabaseOperation(notification));
    }

    pub async fn handle_error(&self, notification: ErrorNotification) {
        println!("Handling error");
        self.emit(Event::Error(notification));
    }

    pub fn emit_db_event(
        &self,
        operation: &str,
        table: &str,
        start_time: Instant,
        error: Option<String>,
    ) {
        let duration = start_time.elapsed().as_millis() as u64;

        let event = DatabaseOperationEvent {
            operation_type: operation.to_string(),
            table: table.to_string(),
            success: error.is_none(),
            duration_ms: duration,
            error,
            timestamp: chrono::Utc::now(),
        };

        let notification = DatabaseNotification {
            data: event,
            type_: "database_operation".to_string(),
        };

        self.emit(Event::DatabaseOperation(notification));
    }

    pub fn emit_error(&self, error_type: &str, message: &str, context: serde_json::Value) {
        let event = ErrorEvent {
            error_type: error_type.to_string(),
            message: message.to_string(),
            context,
            timestamp: chrono::Utc::now(),
        };

        let notification = ErrorNotification {
            data: event,
            type_: "error".to_string(),
        };

        self.emit(Event::Error(notification));
    }
}

impl Default for EventSystem {
    fn default() -> Self {
        Self::new()
    }
}
