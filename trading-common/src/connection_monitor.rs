use crate::{
    event_system::EventSystem,
    models::{
        ConnectionStatus, ConnectionStatusChange, ConnectionStatusNotification, ConnectionType,
    },
};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct ConnectionMonitor {
    event_system: Arc<EventSystem>,
    statuses: Arc<RwLock<Vec<ConnectionStatusChange>>>,
}

impl ConnectionMonitor {
    pub fn new(event_system: Arc<EventSystem>) -> Self {
        Self {
            event_system,
            statuses: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn update_status(
        &self,
        connection_type: ConnectionType,
        status: ConnectionStatus,
        details: Option<String>,
    ) {
        let status_change = ConnectionStatusChange::new(connection_type, status)
            .with_details(details.unwrap_or_default());

        // Update internal state
        {
            let mut statuses = self.statuses.write().await;
            if let Some(existing) = statuses
                .iter_mut()
                .find(|s| s.connection_type == connection_type)
            {
                *existing = status_change.clone();
            } else {
                statuses.push(status_change.clone());
            }
        }

        // Emit event
        self.event_system
            .emit(crate::event_system::Event::ConnectionStatus(
                ConnectionStatusNotification {
                    data: status_change,
                    type_: "connection_status_change".to_string(),
                },
            ));
    }

    pub async fn get_status(&self, connection_type: ConnectionType) -> Option<ConnectionStatus> {
        let statuses = self.statuses.read().await;
        statuses
            .iter()
            .find(|s| s.connection_type == connection_type)
            .map(|s| s.status.clone())
    }

    pub async fn get_all_statuses(&self) -> Vec<ConnectionStatusChange> {
        self.statuses.read().await.clone()
    }
}
