use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::Signer;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use trading_common::data::get_metadata;
use trading_common::data::TokenMetadata;
use trading_common::dex::DexType;
use trading_common::proto::wallet::{
    wallet_service_server::WalletService, RefreshBalancesRequest, RefreshBalancesResponse,
    SubscribeRequest, TokenInfo as ProtoTokenInfo, TradeExecutionRequest, TradeExecutionResponse,
    WalletInfoRequest, WalletInfoResponse, WalletUpdate,
};
use trading_common::EmitWalletUpdateRequest;
use trading_common::EmitWalletUpdateResponse;
use trading_common::{
    data::get_server_keypair, event_system::EventSystem, models::TokenInfo,
    server_wallet_manager::ServerWalletManager,
};
use trading_common::{ClientTxInfo, TransactionType};
pub struct WalletServiceImpl {
    wallet_manager: Arc<Mutex<ServerWalletManager>>,
    event_system: Arc<EventSystem>,
}

impl WalletServiceImpl {
    pub async fn new() -> anyhow::Result<Self> {
        let event_system = Arc::new(EventSystem::new());
        let server_keypair = get_server_keypair();
        let rpc_client = Arc::new(solana_client::rpc_client::RpcClient::new(std::env::var(
            "SOLANA_RPC_HTTP_URL",
        )?));

        let wallet_manager = ServerWalletManager::new(
            rpc_client,
            server_keypair.pubkey(),
            Arc::clone(&event_system),
        )
        .await?;

        Ok(Self {
            wallet_manager: Arc::new(Mutex::new(wallet_manager)),
            event_system,
        })
    }

    fn convert_token_info(token: &TokenInfo) -> ProtoTokenInfo {
        ProtoTokenInfo {
            address: token.address.clone(),
            symbol: token.symbol.clone(),
            name: token.name.clone(),
            balance: token.balance.clone(),
            metadata_uri: token.metadata_uri.clone(),
            decimals: token.decimals as u32,
            market_cap: token.market_cap,
        }
    }
}

#[tonic::async_trait]
impl WalletService for WalletServiceImpl {
    async fn get_wallet_info(
        &self,
        _request: Request<WalletInfoRequest>,
    ) -> Result<Response<WalletInfoResponse>, Status> {
        let wallet_manager = self.wallet_manager.lock().await;
        let wallet_info = wallet_manager.get_wallet_info();

        let response = WalletInfoResponse {
            balance: wallet_info.balance,
            tokens: wallet_info
                .tokens
                .iter()
                .map(Self::convert_token_info)
                .collect(),
            address: wallet_info.address,
        };

        Ok(Response::new(response))
    }

    async fn handle_trade_execution(
        &self,
        request: tonic::Request<TradeExecutionRequest>,
    ) -> Result<tonic::Response<TradeExecutionResponse>, tonic::Status> {
        let req = request.into_inner();
        let mut wallet_manager = self.wallet_manager.lock().await;

        // Fetch token metadata if not provided
        let token_pubkey = Pubkey::from_str(&req.token_address)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;

        let token_metadata = match get_metadata(&wallet_manager.rpc_client, &token_pubkey).await {
            Ok(metadata) => metadata,
            Err(e) => {
                println!("Failed to fetch token metadata: {}", e);
                TokenMetadata {
                    mint: token_pubkey.to_string(),
                    name: "Unknown".to_string(),
                    symbol: "???".to_string(),
                    uri: "".to_string(),
                    update_authority: "".to_string(),
                }
            }
        };

        // Convert to ClientTxInfo with enriched data
        let tx_info = ClientTxInfo {
            signature: req.signature,
            token_address: req.token_address,
            token_name: token_metadata.name,
            token_symbol: token_metadata.symbol,
            transaction_type: match req.transaction_type.as_str() {
                "Buy" => TransactionType::Buy,
                "Sell" => TransactionType::Sell,
                _ => TransactionType::Unknown,
            },
            amount_token: req.amount_token,
            amount_sol: req.amount_sol,
            price_per_token: req.price_per_token,
            token_image_uri: token_metadata.uri,
            market_cap: 0.0,
            usd_market_cap: 0.0,
            timestamp: chrono::Utc::now().timestamp(),
            seller: String::new(),
            buyer: String::new(),
            dex_type: DexType::Unknown,
        };

        // Update internal state
        if let Err(e) = wallet_manager.handle_trade_execution(&tx_info).await {
            println!("Error handling trade execution: {}", e);
            return Ok(Response::new(TradeExecutionResponse {
                success: false,
                error: Some(e.to_string()),
            }));
        }

        println!("Trade execution handled successfully");
        Ok(Response::new(TradeExecutionResponse {
            success: true,
            error: None,
        }))
    }

    type SubscribeToUpdatesStream =
        Pin<Box<dyn Stream<Item = Result<WalletUpdate, Status>> + Send + 'static>>;

    async fn subscribe_to_updates(
        &self,
        _request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeToUpdatesStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(200); // Increase buffer size
        let mut event_rx = self.event_system.subscribe();

        println!("Created wallet update subscription channel");

        tokio::spawn(async move {
            while let Ok(event) = event_rx.recv().await {
                println!(
                    "Received event in subscription: {:?}",
                    std::mem::discriminant(&event)
                );
                if let trading_common::event_system::Event::WalletUpdate(notification) = event {
                    let update = WalletUpdate {
                        balance: notification.data.balance,
                        tokens: notification
                            .data
                            .tokens
                            .iter()
                            .map(|t| ProtoTokenInfo {
                                address: t.address.clone(),
                                symbol: t.symbol.clone(),
                                name: t.name.clone(),
                                balance: t.balance.clone(),
                                metadata_uri: t.metadata_uri.clone(),
                                decimals: t.decimals as u32,
                                market_cap: t.market_cap,
                            })
                            .collect(),
                        address: notification.data.address,
                    };

                    println!("Sending wallet update through channel");
                    if let Err(e) = tx.send(Ok(update)).await {
                        println!("Failed to send wallet update: {}", e);
                        break;
                    }
                }
            }
            println!("Wallet update subscription ended");
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn refresh_balances(
        &self,
        _request: Request<RefreshBalancesRequest>,
    ) -> Result<Response<RefreshBalancesResponse>, Status> {
        let mut wallet_manager = self.wallet_manager.lock().await;

        match wallet_manager.refresh_balances().await {
            Ok(_) => Ok(Response::new(RefreshBalancesResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(RefreshBalancesResponse {
                success: false,
                error: Some(e.to_string()),
            })),
        }
    }

    async fn emit_wallet_update(
        &self,
        _request: Request<EmitWalletUpdateRequest>,
    ) -> Result<Response<EmitWalletUpdateResponse>, Status> {
        let wallet_manager = self.wallet_manager.lock().await;
        wallet_manager.emit_wallet_update();
        Ok(Response::new(EmitWalletUpdateResponse {
            success: true,
            error: None,
        }))
    }
}
