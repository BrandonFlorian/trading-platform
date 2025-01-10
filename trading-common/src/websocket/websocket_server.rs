use crate::{
    error::AppError,
    event_system::{Event, EventSystem},
    models::{ConnectionStatus, ConnectionType, TokenInfo, WalletUpdate},
    wallet_client::WalletClient,
    ConnectionMonitor, CopyTradeSettings, SupabaseClient, TrackedWallet, TransactionLog,
};
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    net::SocketAddr,
    sync::Arc,
    sync::RwLock,
    time::{Duration, Instant},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::interval,
};

use tokio_tungstenite::{tungstenite::Message, WebSocketStream};
use uuid::Uuid;

const SESSION_TIMEOUT: Duration = Duration::from_secs(300);
struct ConnectionContext {
    supabase_client: Arc<SupabaseClient>,
    event_system: Arc<EventSystem>,
    wallet_client: Arc<WalletClient>,
}

#[derive(Debug)]
struct ClientSession {
    id: Uuid,
    last_pong: RwLock<Instant>,
    sender: Arc<Mutex<SplitSink<WebSocketStream<TcpStream>, Message>>>,
}

impl ClientSession {
    fn new(sender: SplitSink<WebSocketStream<TcpStream>, Message>) -> Self {
        Self {
            id: Uuid::new_v4(),
            last_pong: RwLock::new(Instant::now()),
            sender: Arc::new(Mutex::new(sender)),
        }
    }

    async fn send_message(&self, msg: Message) -> Result<(), AppError> {
        let mut sender = self.sender.lock().await;
        sender
            .send(msg)
            .await
            .map_err(|e| AppError::WebSocketError(format!("Failed to send message: {}", e)))
    }
    fn update_pong(&self) {
        let mut guard = self.last_pong.write().unwrap();
        *guard = Instant::now();
    }

    fn is_alive(&self) -> bool {
        let guard = self.last_pong.read().unwrap();
        guard.elapsed() < SESSION_TIMEOUT // 5 minute timeout
    }
}
pub struct WebSocketServer {
    event_system: Arc<EventSystem>,
    wallet_client: Arc<WalletClient>,
    supabase_client: Arc<SupabaseClient>,
    port: u16,
    connection_monitor: Arc<ConnectionMonitor>,
}

impl WebSocketServer {
    pub fn new(
        event_system: Arc<EventSystem>,
        wallet_client: Arc<WalletClient>,
        supabase_client: Arc<SupabaseClient>,
        port: u16,
        connection_monitor: Arc<ConnectionMonitor>,
    ) -> Self {
        Self {
            event_system,
            wallet_client,
            supabase_client,
            port,
            connection_monitor,
        }
    }
    pub async fn start(&self) -> Result<(), anyhow::Error> {
        let addr = format!("127.0.0.1:{}", self.port);
        let listener = TcpListener::bind(&addr).await?;
        println!("WebSocket server listening for connections on: {}", addr);

        // Just wait for connections, no active state until client connects
        while let Ok((stream, addr)) = listener.accept().await {
            println!("New client connection attempt from: {}", addr);

            let event_system = Arc::clone(&self.event_system);
            let wallet_client = Arc::clone(&self.wallet_client);
            let supabase_client = Arc::clone(&self.supabase_client);
            let connection_monitor = Arc::clone(&self.connection_monitor);

            tokio::spawn(async move {
                match tokio_tungstenite::accept_async(stream).await {
                    Ok(ws_stream) => {
                        println!("Client successfully connected and upgraded to WebSocket");
                        connection_monitor
                            .update_status(
                                ConnectionType::WebSocket,
                                ConnectionStatus::Connected,
                                Some(format!("Client connected from {}", addr)),
                            )
                            .await;

                        if let Err(e) = Self::handle_connection(
                            ws_stream,
                            event_system,
                            wallet_client,
                            supabase_client,
                            connection_monitor.clone(),
                            addr,
                        )
                        .await
                        {
                            eprintln!("Client connection error: {}", e);
                            connection_monitor
                                .update_status(
                                    ConnectionType::WebSocket,
                                    ConnectionStatus::Disconnected,
                                    Some(format!("Client disconnected due to error: {}", e)),
                                )
                                .await;
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to accept client connection: {}", e);
                        connection_monitor
                            .update_status(
                                ConnectionType::WebSocket,
                                ConnectionStatus::Error,
                                Some(format!("WebSocket upgrade failed: {}", e)),
                            )
                            .await;
                    }
                }
            });
        }

        Ok(())
    }

    async fn handle_connection(
        ws_stream: WebSocketStream<TcpStream>,
        event_system: Arc<EventSystem>,
        wallet_client: Arc<WalletClient>,
        supabase_client: Arc<SupabaseClient>,
        connection_monitor: Arc<ConnectionMonitor>,
        addr: SocketAddr,
    ) -> Result<(), AppError> {
        let (ws_sender, mut ws_receiver) = ws_stream.split();
        let mut event_rx = event_system.subscribe();

        let context = ConnectionContext {
            supabase_client: Arc::clone(&supabase_client),
            event_system: Arc::clone(&event_system),
            wallet_client: Arc::clone(&wallet_client),
        };

        // Only start ping/pong after client is connected
        let mut ping_interval = interval(Duration::from_secs(60));
        let session = Arc::new(ClientSession::new(ws_sender));

        println!("Starting client session {} for {}", session.id, addr);

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    if !session.is_alive() {
                        println!("Client {} session timeout", addr);
                        connection_monitor
                            .update_status(
                                ConnectionType::WebSocket,
                                ConnectionStatus::Error,
                                Some(format!("Client {} timeout - no pong received", addr)),
                            )
                            .await;
                        break;
                    }

                    if let Err(e) = session.send_message(Message::Ping(vec![].into())).await {
                        println!("Failed to ping client {}: {}", addr, e);
                        connection_monitor
                            .update_status(
                                ConnectionType::WebSocket,
                                ConnectionStatus::Error,
                                Some(format!("Failed to ping client {}: {}", addr, e)),
                            )
                            .await;
                        break;
                    }
                }

                Some(msg) = ws_receiver.next() => {
                    match msg {
                        Ok(Message::Pong(_)) => {
                            session.update_pong();
                            continue;
                        }
                        Ok(Message::Close(_)) => {
                            connection_monitor
                                .update_status(
                                    ConnectionType::WebSocket,
                                    ConnectionStatus::Disconnected,
                                    None,
                                )
                                .await;
                            break;
                        }
                        Ok(Message::Text(text)) => {
                            if let Err(e) = Self::handle_command(&context, &text, &session).await {
                                let error_msg = json!({
                                    "type": "error",
                                    "data": {
                                        "message": format!("Failed to handle command: {}", e)
                                    }
                                });
                                if let Err(e) = session.send_message(Message::Text(error_msg.to_string().into())).await {
                                    eprintln!("Failed to send error message: {}", e);
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            connection_monitor
                                .update_status(
                                    ConnectionType::WebSocket,
                                    ConnectionStatus::Error,
                                    Some(format!("WebSocket error: {}", e)),
                                )
                                .await;
                            break;
                        }
                        _ => continue,
                    }
                }

                Ok(event) = event_rx.recv() => {
                    if let Err(e) = Self::send_event(&session, event).await {
                        eprintln!("Failed to send event: {}", e);
                        break;
                    }
                }
            }
        }
        println!("Client {} session ended", addr);
        Ok(())
    }

    async fn send_event(session: &ClientSession, event: Event) -> Result<(), AppError> {
        let msg = match event {
            Event::TrackedWalletTransaction(notification) => {
                json!({
                    "type": "tracked_wallet_trade",
                    "data": notification.data
                })
            }
            Event::CopyTradeExecution(notification) => {
                json!({
                    "type": "copy_trade_execution",
                    "data": notification.data
                })
            }
            Event::WalletUpdate(notification) => {
                json!({
                    "type": "wallet_update",
                    "data": notification.data
                })
            }
            Event::TransactionLogged(notification) => {
                json!({
                    "type": "transaction_logged",
                    "data": notification.data
                })
            }
            Event::ConnectionStatus(notification) => {
                json!({
                    "type": "connection_status",
                    "data": notification.data
                })
            }
            Event::SettingsUpdate(notification) => {
                json!({
                    "type": "settings_update",
                    "data": notification.data
                })
            }
            Event::TradeExecution(notification) => {
                json!({
                    "type": "trade_execution",
                    "data": notification.data
                })
            }
            // Handle other event types...
            _ => return Ok(()),
        };

        session
            .send_message(Message::Text(msg.to_string().into()))
            .await?;

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
        session: &ClientSession,
    ) -> Result<(), anyhow::Error> {
        let command: CommandMessage = serde_json::from_str(text)?;

        match command {
            CommandMessage::Start => {
                let initial_state = Self::get_initial_state(context).await?;

                session
                    .send_message(Message::Text(
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
                session
                    .send_message(Message::Text(
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

                session
                    .send_message(Message::Text(
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

                session
                    .send_message(Message::Text(
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
