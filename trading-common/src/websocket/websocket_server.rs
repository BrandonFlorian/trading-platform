use crate::{
    event_system::{Event, EventSystem},
    models::{TokenInfo, WalletUpdate},
    server_wallet_manager::ServerWalletManager,
    wallet_client::WalletClient,
    CopyTradeSettings, SupabaseClient, TrackedWallet, TransactionLog,
};
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::handshake::server::{Callback, ErrorResponse};
use tokio_tungstenite::tungstenite::http::Response;
use tokio_tungstenite::{
    tungstenite::{protocol::WebSocketConfig, Message},
    WebSocketStream,
};
struct ConnectionContext {
    supabase_client: Arc<SupabaseClient>,
    event_system: Arc<EventSystem>,
    wallet_client: Arc<WalletClient>,
}
pub struct WebSocketServer {
    event_system: Arc<EventSystem>,
    wallet_client: Arc<WalletClient>,
    supabase_client: Arc<SupabaseClient>,
    port: u16,
}

impl WebSocketServer {
    pub fn new(
        event_system: Arc<EventSystem>,
        wallet_client: Arc<WalletClient>,
        supabase_client: Arc<SupabaseClient>,
        port: u16,
    ) -> Self {
        Self {
            event_system,
            wallet_client,
            supabase_client,
            port,
        }
    }

    pub async fn start(&self) -> Result<(), anyhow::Error> {
        let addr = format!("127.0.0.1:{}", self.port);
        let listener = TcpListener::bind(&addr).await?;
        println!("WebSocket server listening on: {}", addr);

        while let Ok((stream, _)) = listener.accept().await {
            let peer = stream.peer_addr().ok();
            println!("New WebSocket connection from: {:?}", peer);

            let event_system = Arc::clone(&self.event_system);
            let wallet_client = Arc::clone(&self.wallet_client);
            let supabase_client = Arc::clone(&self.supabase_client);

            tokio::spawn(async move {
                match tokio_tungstenite::accept_async(stream).await {
                    Ok(ws_stream) => {
                        if let Err(e) = WebSocketServer::handle_connection(
                            ws_stream,
                            event_system,
                            wallet_client,
                            supabase_client,
                        )
                        .await
                        {
                            eprintln!("Connection error: {}", e);
                        }
                    }
                    Err(e) => eprintln!("WebSocket handshake error: {}", e),
                }
            });
        }

        Ok(())
    }

    async fn handle_connection(
        ws_stream: WebSocketStream<TcpStream>,
        event_system: Arc<EventSystem>,
        wallet_client: Arc<WalletClient>, // Changed parameter
        supabase_client: Arc<SupabaseClient>,
    ) -> Result<(), anyhow::Error> {
        let (mut ws_sender, mut ws_receiver) = ws_stream.split();
        let mut event_rx = event_system.subscribe();
        let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(30));

        let context = ConnectionContext {
            supabase_client,
            event_system,
            wallet_client,
        };

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                        ws_sender.send(Message::Ping(vec![].into())).await?;
                    }
                        Ok(event) = event_rx.recv() => {
                            // Convert event to JSON and send to client
                            let msg = match event {
                                Event::TrackedWalletTransaction(notification) => {
                                    // Add more detailed payload matching your frontend type
                                    json!({
                                        "type": "tracked_wallet_trade",
                                        "data": {
                                            "signature": notification.data.signature,
                                            "tokenAddress": notification.data.token_address,
                                            "tokenName": notification.data.token_name,
                                            "tokenSymbol": notification.data.token_symbol,
                                            "transactionType": notification.data.transaction_type,
                                            "amountToken": notification.data.amount_token,
                                            "amountSol": notification.data.amount_sol,
                                            "pricePerToken": notification.data.price_per_token,
                                            "tokenImageUri": notification.data.token_image_uri,
                                            "marketCap": notification.data.market_cap,
                                            "usdMarketCap": notification.data.usd_market_cap,
                                            "seller": notification.data.seller,
                                            "buyer": notification.data.buyer,
                                            "timestamp": notification.data.timestamp,
                                        }
                                    }).to_string()
                                },
                                Event::CopyTradeExecution(notification) => {
                                    // Similar format but with copy_trade_execution type
                                    json!({
                                        "type": "copy_trade_execution",
                                        "data": {
                                            "signature": notification.data.signature,
                                            "tokenAddress": notification.data.token_address,
                                            "tokenName": notification.data.token_name,
                                            "tokenSymbol": notification.data.token_symbol,
                                            "transactionType": notification.data.transaction_type,
                                            "amountToken": notification.data.amount_token,
                                            "amountSol": notification.data.amount_sol,
                                            "pricePerToken": notification.data.price_per_token,
                                            "tokenImageUri": notification.data.token_image_uri,
                                            "marketCap": notification.data.market_cap,
                                            "usdMarketCap": notification.data.usd_market_cap,
                                            "seller": notification.data.seller,
                                            "buyer": notification.data.buyer,
                                            "timestamp": notification.data.timestamp,
                                        }
                                    }).to_string()
                                },
                                Event::WalletUpdate(notification) => {
                                    // Format to match ServerWalletInfo type
                                    json!({
                                        "type": "wallet_update",
                                        "data": {
                                            "balance": notification.data.balance,
                                            "tokens": notification.data.tokens.iter().map(|token| {
                                                json!({
                                                    "address": token.address,
                                                    "symbol": token.symbol,
                                                    "name": token.name,
                                                    "balance": token.balance,
                                                    "metadataUri": token.metadata_uri,
                                                    "decimals": token.decimals,
                                                })
                                            }).collect::<Vec<_>>(),
                                        }
                                    }).to_string()
                                },
                                Event::TransactionLogged(notification) => {
                                    json!({
                                        "type": "transaction_logged",
                                        "data": {
                                            "id": notification.data.id,
                                            "signature": notification.data.signature,
                                            "transaction_type": notification.data.transaction_type,
                                            "token_address": notification.data.token_address,
                                            "amount": notification.data.amount,
                                            "price_sol": notification.data.price_sol,
                                            "timestamp": notification.data.timestamp,
                                        }
                                    }).to_string()
                                },
                                _ => continue,
                            };

                            ws_sender.send(Message::Text(msg.into())).await?;
                        }

                        Some(msg) = ws_receiver.next() => {
                    match msg {
                        Ok(Message::Close(_)) => break,
                        Ok(Message::Pong(_)) => continue,
                        Ok(Message::Ping(_)) => {
                            if let Err(e) = ws_sender.send(Message::Pong(vec![].into())).await {
                                eprintln!("Failed to send pong: {}", e);
                                break;
                            }
                        }
                        Ok(Message::Text(text)) => {
                            if let Err(e) = Self::handle_command(
                                &context,
                                &text,
                                &mut ws_sender,
                            ).await {
                                eprintln!("Failed to handle command: {}", e);
                                // Send error message to client
                                let error_msg = json!({
                                    "type": "error",
                                    "data": {
                                        "message": format!("Failed to handle command: {}", e)
                                    }
                                });
                                if let Err(e) = ws_sender.send(Message::Text(error_msg.to_string().into())).await {
                                    eprintln!("Failed to send error message: {}", e);
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("WebSocket message error: {}", e);
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        println!("WebSocket connection closed");
        Ok(())
    }

    async fn get_initial_state(context: &ConnectionContext) -> Result<InitialState, anyhow::Error> {
        // Get wallet info using gRPC client
        let server_wallet_response = context
            .wallet_client
            .get_wallet_info()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get wallet info: {}", e))?;

        // Convert WalletInfoResponse to WalletUpdate
        let server_wallet = WalletUpdate {
            balance: server_wallet_response.balance,
            tokens: server_wallet_response
                .tokens
                .into_iter()
                .map(|t| TokenInfo {
                    address: t.address,
                    symbol: t.symbol,
                    name: t.name,
                    balance: t.balance,
                    metadata_uri: t.metadata_uri,
                    decimals: t.decimals as u8,
                    market_cap: t.market_cap,
                })
                .collect(),
            address: server_wallet_response.address,
        };

        let tracked_wallets = context.supabase_client.get_tracked_wallets().await?;
        let tracked_wallet = tracked_wallets.into_iter().find(|w| w.is_active);

        let copy_trade_settings = if let Some(wallet) = &tracked_wallet {
            context
                .supabase_client
                .get_copy_trade_settings()
                .await?
                .into_iter()
                .find(|s| s.tracked_wallet_id == wallet.id.unwrap())
        } else {
            None
        };

        let recent_transactions = context
            .supabase_client
            .get_transaction_history()
            .await?
            .into_iter()
            .take(50)
            .collect();

        Ok(InitialState {
            server_wallet,
            tracked_wallet,
            copy_trade_settings,
            recent_transactions,
        })
    }

    async fn handle_command(
        context: &ConnectionContext,
        text: &str,
        ws_sender: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
    ) -> Result<(), anyhow::Error> {
        let command: CommandMessage = serde_json::from_str(text)?;

        match command {
            CommandMessage::Start => {
                let initial_state = Self::get_initial_state(context).await?;

                ws_sender
                    .send(Message::Text(
                        serde_json::to_string(&json!({
                            "type": "start",
                            "data": initial_state
                        }))?
                        .into(),
                    ))
                    .await?;
            }
            CommandMessage::UpdateSettings { settings } => {
                context
                    .supabase_client
                    .update_copy_trade_settings(settings)
                    .await?;

                // Send confirmation
                ws_sender
                    .send(Message::Text(
                        json!({
                            "type": "update_settings",
                            "data": { "success": true }
                        })
                        .to_string()
                        .into(),
                    ))
                    .await?;
            }
            CommandMessage::RefreshState => {
                // Handle refresh state...
                let initial_state = Self::get_initial_state(context).await?;

                ws_sender
                    .send(Message::Text(
                        serde_json::to_string(&json!({
                            "type": "refresh_state",
                            "data": initial_state
                        }))?
                        .into(),
                    ))
                    .await?;
            }
            CommandMessage::ManualSell {
                token_address,
                amount,
                slippage,
            } => {
                // TODO: Implement manual sell using wallet_client
                let initial_state = Self::get_initial_state(context).await?;

                ws_sender
                    .send(Message::Text(
                        serde_json::to_string(&json!({
                            "type": "manual_sell",
                            "data": initial_state
                        }))?
                        .into(),
                    ))
                    .await?;
            }
        }

        Ok(())
    }
}

impl Drop for WebSocketServer {
    fn drop(&mut self) {
        println!("WebSocket server shutting down");
    }
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
enum CommandMessage {
    #[serde(rename = "start")]
    Start,
    #[serde(rename = "update_settings")]
    UpdateSettings { settings: CopyTradeSettings },
    #[serde(rename = "refresh_state")]
    RefreshState,
    #[serde(rename = "manual_sell")]
    ManualSell {
        token_address: String,
        amount: f64,
        slippage: f64,
    },
}

#[derive(Serialize)]
struct InitialState {
    server_wallet: WalletUpdate,
    tracked_wallet: Option<TrackedWallet>,
    copy_trade_settings: Option<CopyTradeSettings>,
    recent_transactions: Vec<TransactionLog>,
}
