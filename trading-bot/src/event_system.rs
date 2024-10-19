use tokio::sync::broadcast;
use trading_common::models::{
    CopyTradeNotification, TrackedWalletNotification, TransactionLoggedNotification,
    WalletUpdateNotification,
};
#[derive(Clone)]
pub enum Event {
    TrackedWalletTransaction(TrackedWalletNotification),
    CopyTradeExecution(CopyTradeNotification),
    WalletUpdate(WalletUpdateNotification),
    TransactionLogged(TransactionLoggedNotification),
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

    pub async fn handle_wallet_updated(&self, notification: WalletUpdateNotification) {
        self.emit(Event::WalletUpdate(notification));
    }
}
