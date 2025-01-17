use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;

pub use crate::proto::wallet::{
    wallet_service_client::WalletServiceClient, RefreshBalancesRequest, RefreshBalancesResponse,
    SubscribeRequest, TradeExecutionRequest, TradeExecutionResponse, WalletInfoRequest,
    WalletInfoResponse, WalletUpdate,
};
use crate::{
    models::{ConnectionStatus, ConnectionType},
    ConnectionMonitor, EmitWalletUpdateRequest, EmitWalletUpdateResponse,
};

#[derive(Clone)]
pub struct WalletClient {
    client: Arc<Mutex<WalletServiceClient<Channel>>>,
    connection_monitor: Arc<ConnectionMonitor>,
}

impl WalletClient {
    pub async fn connect(addr: String, connection_monitor: Arc<ConnectionMonitor>) -> Result<Self> {
        match WalletServiceClient::connect(addr).await {
            Ok(client) => {
                connection_monitor
                    .update_status(ConnectionType::Grpc, ConnectionStatus::Connected, None)
                    .await;

                Ok(Self {
                    client: Arc::new(Mutex::new(client)),
                    connection_monitor,
                })
            }
            Err(e) => {
                connection_monitor
                    .update_status(
                        ConnectionType::Grpc,
                        ConnectionStatus::Error,
                        Some(e.to_string()),
                    )
                    .await;
                Err(e.into())
            }
        }
    }

    pub async fn get_wallet_info(&self) -> Result<WalletInfoResponse> {
        let mut client = self.client.lock().await;
        let request = tonic::Request::new(WalletInfoRequest {});
        match client.get_wallet_info(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => {
                self.connection_monitor
                    .update_status(
                        ConnectionType::Grpc,
                        ConnectionStatus::Error,
                        Some(e.to_string()),
                    )
                    .await;
                Err(e.into())
            }
        }
    }

    pub async fn handle_trade_execution(
        &self,
        request: TradeExecutionRequest,
    ) -> Result<TradeExecutionResponse> {
        let mut client = self.client.lock().await;
        match client.handle_trade_execution(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => {
                self.connection_monitor
                    .update_status(
                        ConnectionType::Grpc,
                        ConnectionStatus::Error,
                        Some(e.to_string()),
                    )
                    .await;
                Err(e.into())
            }
        }
    }

    pub async fn subscribe_to_updates(&self) -> Result<tonic::Streaming<WalletUpdate>> {
        let mut client = self.client.lock().await;
        let request = tonic::Request::new(SubscribeRequest {});
        match client.subscribe_to_updates(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => {
                self.connection_monitor
                    .update_status(
                        ConnectionType::Grpc,
                        ConnectionStatus::Error,
                        Some(e.to_string()),
                    )
                    .await;
                Err(e.into())
            }
        }
    }

    pub async fn refresh_balances(&self) -> Result<RefreshBalancesResponse> {
        let mut client = self.client.lock().await;
        let request = tonic::Request::new(RefreshBalancesRequest {});
        match client.refresh_balances(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => {
                self.connection_monitor
                    .update_status(
                        ConnectionType::Grpc,
                        ConnectionStatus::Error,
                        Some(e.to_string()),
                    )
                    .await;
                Err(e.into())
            }
        }
    }

    pub async fn emit_wallet_update(&self) -> Result<EmitWalletUpdateResponse> {
        let mut client = self.client.lock().await;
        let request = tonic::Request::new(EmitWalletUpdateRequest {});
        match client.emit_wallet_update(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => Err(e.into()),
        }
    }
}
