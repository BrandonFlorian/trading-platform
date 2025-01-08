use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;

pub use crate::proto::wallet::{
    wallet_service_client::WalletServiceClient, RefreshBalancesRequest, RefreshBalancesResponse,
    SubscribeRequest, TradeExecutionRequest, TradeExecutionResponse, WalletInfoRequest,
    WalletInfoResponse, WalletUpdate,
};

#[derive(Clone)]
pub struct WalletClient {
    client: Arc<Mutex<WalletServiceClient<Channel>>>,
}

impl WalletClient {
    pub async fn connect(addr: String) -> Result<Self> {
        let client = WalletServiceClient::connect(addr).await?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }

    pub async fn get_wallet_info(&self) -> Result<WalletInfoResponse> {
        let mut client = self.client.lock().await;
        let request = tonic::Request::new(WalletInfoRequest {});
        let response = client.get_wallet_info(request).await?;
        Ok(response.into_inner())
    }

    pub async fn handle_trade_execution(
        &self,
        request: TradeExecutionRequest,
    ) -> Result<TradeExecutionResponse> {
        let mut client = self.client.lock().await;
        let response = client.handle_trade_execution(request).await?;
        Ok(response.into_inner())
    }

    pub async fn subscribe_to_updates(&self) -> Result<tonic::Streaming<WalletUpdate>> {
        let mut client = self.client.lock().await;
        let request = tonic::Request::new(SubscribeRequest {});
        let response = client.subscribe_to_updates(request).await?;
        Ok(response.into_inner())
    }

    pub async fn refresh_balances(&self) -> Result<RefreshBalancesResponse> {
        let mut client = self.client.lock().await;
        let request = tonic::Request::new(RefreshBalancesRequest {});
        let response = client.refresh_balances(request).await?;
        Ok(response.into_inner())
    }
}
