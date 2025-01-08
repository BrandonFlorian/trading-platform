use std::time::Instant;

use tokio::sync::broadcast;

use crate::models::{
    CopyTradeNotification, DatabaseNotification, DatabaseOperationEvent, ErrorEvent,
    ErrorNotification, SettingsUpdateNotification, TrackedWalletNotification,
    TransactionLoggedNotification, WalletUpdateNotification,
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
        self.sender.subscribe()
    }

    pub fn emit(&self, event: Event) {
        let _ = self.sender.send(event);
    }

    pub async fn handle_transaction_logged(&self, notification: TransactionLoggedNotification) {
        self.emit(Event::TransactionLogged(notification));
    }

    pub async fn handle_copy_trade_executed(&self, notification: CopyTradeNotification) {
        self.emit(Event::CopyTradeExecution(notification));
    }

    pub async fn handle_tracked_wallet_trade(&self, notification: TrackedWalletNotification) {
        self.emit(Event::TrackedWalletTransaction(notification));
    }

    pub async fn handle_settings_update(&self, notification: SettingsUpdateNotification) {
        self.emit(Event::SettingsUpdate(notification));
    }

    pub async fn handle_wallet_updated(&self, notification: WalletUpdateNotification) {
        self.emit(Event::WalletUpdate(notification));
    }

    pub async fn handle_database_operation(&self, notification: DatabaseNotification) {
        self.emit(Event::DatabaseOperation(notification));
    }

    pub async fn handle_error(&self, notification: ErrorNotification) {
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
