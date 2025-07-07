use anyhow::{Context, Result};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{signature::Keypair, signer::Signer};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_tungstenite::tungstenite::Message;
use tracing::error;
use trading_common::{
    data::get_server_keypair,
    database::SupabaseClient,
    error::AppError,
    event_system::{Event, EventSystem},
    models::{
        ClientTxInfo, ConnectionStatus, ConnectionType, CopyTradeNotification, CopyTradeSettings,
        TrackedWallet, TrackedWalletNotification, TransactionLoggedNotification,
    },
    server_wallet_client::WalletClient,
    utils::{
        copy_trade::{execute_copy_trade, should_copy_trade},
        transaction::process_websocket_message,
    },
    websocket::{WebSocketConfig, WebSocketConnectionManager},
    ConnectionMonitor, TransactionLog,
};
use uuid::Uuid;

#[derive(Clone)]
pub struct WalletMonitor {
    rpc_client: Arc<RpcClient>,
    ws_url: String,
    tracked_wallets: Arc<RwLock<Option<Vec<TrackedWallet>>>>,
    copy_trade_settings: Arc<RwLock<Option<Vec<CopyTradeSettings>>>>,
    event_system: Arc<EventSystem>,
    message_queue: mpsc::UnboundedSender<ClientTxInfo>,
    message_receiver: Arc<Mutex<Option<mpsc::UnboundedReceiver<ClientTxInfo>>>>,
    stop_signal: Arc<tokio::sync::watch::Sender<bool>>,
    stop_receiver: Arc<tokio::sync::watch::Receiver<bool>>,
    wallet_client: Arc<WalletClient>,
    connection_monitor: Arc<ConnectionMonitor>,
}

pub struct MessageProcessorContext {
    event_system: Arc<EventSystem>,
    rpc_client: Arc<RpcClient>,
    stop_receiver: Arc<tokio::sync::watch::Receiver<bool>>,
    copy_trade_settings: Arc<RwLock<Option<Vec<CopyTradeSettings>>>>,
    message_receiver: mpsc::UnboundedReceiver<ClientTxInfo>,
    server_keypair: Keypair,
    wallet_client: Arc<WalletClient>,
}

pub struct WebSocketContext {
    message_queue: mpsc::UnboundedSender<ClientTxInfo>,
    stop_receiver: Arc<tokio::sync::watch::Receiver<bool>>,
    tracked_wallets: Arc<RwLock<Option<Vec<TrackedWallet>>>>,
    rpc_client: Arc<RpcClient>,
    connection_manager: WebSocketConnectionManager,
    connection_monitor: Arc<ConnectionMonitor>,
}

impl WalletMonitor {
    pub async fn new(
        rpc_client: Arc<RpcClient>,
        ws_url: String,
        supabase_client: Arc<SupabaseClient>,
        server_keypair: Keypair,
        event_system: Arc<EventSystem>,
        wallet_client: Arc<WalletClient>,
        connection_monitor: Arc<ConnectionMonitor>,
    ) -> Result<Self> {
        let user_id = server_keypair.pubkey().to_string();
        println!("Initializing WalletMonitor for user: {}", user_id);

        let tracked_wallets = Self::fetch_tracked_wallets(&supabase_client)
            .await
            .map_err(|e| {
                AppError::InitializationError(format!("Failed to fetch wallets: {}", e))
            })?;

        let copy_trade_settings = Self::fetch_copy_trade_settings(&supabase_client)
            .await
            .map_err(|e| {
                AppError::InitializationError(format!("Failed to fetch settings: {}", e))
            })?;

        let (tx, rx) = mpsc::unbounded_channel();
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);

        Ok(Self {
            rpc_client,
            ws_url,
            tracked_wallets: Arc::new(RwLock::new(Some(tracked_wallets))),
            copy_trade_settings: Arc::new(RwLock::new(Some(copy_trade_settings))),
            event_system,
            message_queue: tx,
            message_receiver: Arc::new(Mutex::new(Some(rx))),
            stop_signal: Arc::new(stop_tx),
            stop_receiver: Arc::new(stop_rx),
            wallet_client,
            connection_monitor,
        })
    }


    pub async fn start(&mut self) -> Result<(), AppError> {
        println!("Starting WalletMonitor...");

        // Reset stop signal
        let _ = self.stop_signal.send(false);
        println!("Stop signal set to false");

        // Update connection status to Connected
        self.connection_monitor
            .update_status(ConnectionType::WebSocket, ConnectionStatus::Connected, None)
            .await;

        // Start tasks
        let message_processor = self.start_message_processor().await?;
        let websocket_monitor = self.start_websocket_monitor().await?;

        // Subscribe to events from the API
        let mut event_rx = self.event_system.subscribe();
        println!("WalletMonitor started successfully. Waiting for tasks...");

        // Wait for both tasks to complete or stop signal
        let mut rx = (*self.stop_receiver).clone();
        loop {
            tokio::select! {
                result = rx.changed() => {
                    if result.is_ok() && *rx.borrow() {
                        println!("Stop signal received, shutting down...");
                        break;
                    }
                }
                Ok(event) = event_rx.recv() => {
                    println!("Event received: {:?}", std::mem::discriminant(&event));
                    match event {
                        Event::SettingsUpdate(notification) => {
                            println!("Event - Received settings update: {:?}", notification.data);
                            // Update copy trade settings in memory
                            if let Some(settings_store) = self.copy_trade_settings.write().await.as_mut() {
                                if let Some(existing) = settings_store.iter_mut()
                                    .find(|s| s.tracked_wallet_id == notification.data.tracked_wallet_id)
                                {
                                *existing = notification.data;
                            } else {
                                settings_store.push(notification.data);
                                }
                            }
                        }
                        Event::TransactionLogged(notification) => {
                            println!("Event - Received transaction logged: {:?}", notification.data);
                        }
                        Event::WalletStateChange(notification) => {
                            println!("Event - Received wallet state change: {:?}", notification.data);
                        }
                        Event::TrackedWalletTransaction(notification) => {
                            println!("Event - Received tracked wallet transaction: {:?}", notification.data);
                        }
                        Event::TradeExecution(notification) => {
                            println!("Event - Received trade execution: {:?}", notification.data);
                        }
                        Event::ConnectionStatus(notification) => {
                            println!("Event - Received connection status: {:?}", notification.data);
                        }
                        Event::WalletUpdate(notification) => {
                            println!("Event - Received wallet update: {:?}", notification.data);
                        }
                        _ => {}
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                    // Check task status
                    if message_processor.is_finished() || websocket_monitor.is_finished() {
                        println!("One of the tasks finished unexpectedly");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn stop(&mut self) -> Result<(), AppError> {
        println!("Stopping WalletMonitor...");
        let _ = self.stop_signal.send(true);

        // Update connection status
        self.connection_monitor
            .update_status(
                ConnectionType::WebSocket,
                ConnectionStatus::Disconnected,
                None,
            )
            .await;

        println!("Waiting for tasks to complete...");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        println!("WalletMonitor stopped");
        Ok(())
    }

    async fn start_message_processor(&mut self) -> Result<tokio::task::JoinHandle<()>, AppError> {
        let context = MessageProcessorContext {
            event_system: Arc::clone(&self.event_system),
            rpc_client: Arc::clone(&self.rpc_client),

            stop_receiver: Arc::clone(&self.stop_receiver),
            copy_trade_settings: Arc::clone(&self.copy_trade_settings),
            message_receiver: self.message_receiver.lock().await.take().ok_or_else(|| {
                AppError::InitializationError("Message receiver not available".to_string())
            })?,
            server_keypair: get_server_keypair(),
            wallet_client: Arc::clone(&self.wallet_client),
        };

        Ok(tokio::spawn(Self::run_message_processor(context)))
    }

    async fn run_message_processor(context: MessageProcessorContext) {
        let MessageProcessorContext {
            event_system,
            rpc_client,
            stop_receiver,
            copy_trade_settings,
            mut message_receiver,
            server_keypair,
            wallet_client,
        } = context;

        println!("Message processor started");
        loop {
            if *stop_receiver.borrow() {
                println!("Message processor received stop signal");
                break;
            }

            tokio::select! {
            Some(client_message) = message_receiver.recv() => {
                println!("Processing message: {}", client_message.signature);
                let settings = copy_trade_settings.read().await.clone();
                println!("Current copy trade settings: {:?}", settings);
                if let Err(e) = Self::handle_transaction(
                    &rpc_client,
                    &server_keypair,
                    &event_system,
                    &settings,
                    client_message,
                    &wallet_client,
                ).await {
                    println!("Error processing transaction: {}", e);
                }
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                continue;
                }
            }
        }
        println!("Message processor shutting down");
    }

    async fn handle_transaction(
        rpc_client: &Arc<RpcClient>,
        server_keypair: &Keypair,
        event_system: &Arc<EventSystem>,
        copy_trade_settings: &Option<Vec<CopyTradeSettings>>,
        client_message: ClientTxInfo,
        wallet_client: &Arc<WalletClient>,
    ) -> Result<(), AppError> {
        // First, emit tracked wallet trade notification immediately
        event_system.emit(Event::TrackedWalletTransaction(TrackedWalletNotification {
            data: client_message.clone(),
            type_: "tracked_wallet_trade".to_string(),
        }));

        // Check copy trading settings
        if let Some(settings) = copy_trade_settings.as_ref().and_then(|s| s.first()) {
            if settings.is_enabled {
                // Process copy trade
                match Self::process_copy_trade(
                    rpc_client,
                    server_keypair,
                    settings,
                    &client_message,
                    wallet_client,
                )
                .await
                {
                    Ok(_) => {
                        let notification = CopyTradeNotification {
                            data: client_message.clone(),
                            type_: "copy_trade_executed".to_string(),
                        };
                        event_system.emit(Event::CopyTradeExecution(notification));

                        let transaction_log = TransactionLog {
                            id: Uuid::new_v4(),
                            user_id: server_keypair.pubkey().to_string(),
                            tracked_wallet_id: None,
                            signature: client_message.signature.clone(),
                            transaction_type: "buy".to_string(),
                            token_address: client_message.token_address.clone(),
                            amount: client_message.amount_token,
                            price_sol: client_message.price_per_token,
                            timestamp: chrono::Utc::now(),
                        };
                        event_system.emit(Event::TransactionLogged(
                            TransactionLoggedNotification {
                                data: transaction_log.clone(),
                                type_: "transaction_logged".to_string(),
                            },
                        ));
                    }
                    Err(e) => {
                        //should maybe also emit a notification here
                        println!("Copy trade failed: {}", e);
                        return Err(AppError::MessageProcessingError(format!(
                            "Copy trade failed: {}",
                            e
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    async fn process_copy_trade(
        rpc_client: &Arc<RpcClient>,
        server_keypair: &Keypair,
        settings: &CopyTradeSettings,
        client_message: &ClientTxInfo,
        wallet_client: &Arc<WalletClient>,
    ) -> Result<(), AppError> {
        // Check if we should copy trade
        let wallet_info = wallet_client
            .get_wallet_info()
            .await
            .map_err(|e| AppError::ServerError(format!("Failed to get wallet info: {}", e)))?;

        // Logic for should_copy_trade would need to be adapted to use wallet_info
        if !should_copy_trade(client_message, settings, &wallet_info).await? {
            return Ok(());
        }

        println!(
            "Copy trade about to execute for token: {}",
            client_message.token_address
        );

        execute_copy_trade(
            rpc_client,
            server_keypair,
            client_message,
            settings,
            client_message.dex_type.clone(),
            wallet_client,
        )
        .await
        .map_err(|e| {
            AppError::MessageProcessingError(format!("Execute copy trade failed: {}", e))
        })?;

        Ok(())
    }

    async fn start_websocket_monitor(&mut self) -> Result<tokio::task::JoinHandle<()>, AppError> {
        self.connection_monitor
            .update_status(
                ConnectionType::WebSocket,
                ConnectionStatus::Connecting,
                None,
            )
            .await;
        let ws_config = WebSocketConfig {
            health_check_interval: Duration::from_secs(30),
            connection_timeout: Duration::from_secs(5),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            max_retries: 3,
        };

        let context = WebSocketContext {
            message_queue: self.message_queue.clone(),
            stop_receiver: Arc::clone(&self.stop_receiver),
            tracked_wallets: Arc::clone(&self.tracked_wallets),
            rpc_client: Arc::clone(&self.rpc_client),
            connection_manager: WebSocketConnectionManager::new(
                self.ws_url.clone(),
                Some(ws_config),
            ),
            connection_monitor: Arc::clone(&self.connection_monitor),
        };

        Ok(tokio::spawn(Self::run_websocket_monitor(context)))
    }

    async fn run_websocket_monitor(context: WebSocketContext) {
        let WebSocketContext {
            message_queue,
            stop_receiver,
            tracked_wallets,
            rpc_client,
            mut connection_manager,
            connection_monitor,
        } = context;

        loop {
            if *stop_receiver.borrow() {
                connection_monitor
                    .update_status(
                        ConnectionType::WebSocket,
                        ConnectionStatus::Disconnected,
                        None,
                    )
                    .await;
                break;
            }

            let wallet_addresses: Vec<String> = tracked_wallets
                .read()
                .await
                .as_ref()
                .map(|w| {
                    w.iter()
                        .map(|wallet| wallet.wallet_address.clone())
                        .collect()
                })
                .unwrap_or_default();

            if wallet_addresses.is_empty() {
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            // Update status to connecting before attempt
            connection_monitor
                .update_status(
                    ConnectionType::WebSocket,
                    ConnectionStatus::Connecting,
                    None,
                )
                .await;

            match connection_manager.ensure_connection().await {
                Ok(_) => {
                    // Try to subscribe
                    match connection_manager.subscribe(wallet_addresses).await {
                        Ok(_) => {
                            connection_monitor
                                .update_status(
                                    ConnectionType::WebSocket,
                                    ConnectionStatus::Connected,
                                    None,
                                )
                                .await;

                            // Process messages until error or closure
                            loop {
                                if *stop_receiver.borrow() {
                                    break;
                                }

                                match connection_manager.receive_message().await {
                                    Ok(Some(Message::Text(text))) => {
                                        if let Err(e) = Self::handle_websocket_message(
                                            Message::Text(text),
                                            &rpc_client,
                                            &message_queue,
                                        )
                                        .await
                                        {
                                            error!("Message handling error: {}", e);
                                            connection_monitor
                                                .update_status(
                                                    ConnectionType::WebSocket,
                                                    ConnectionStatus::Error,
                                                    Some(e.to_string()),
                                                )
                                                .await;
                                            break;
                                        }
                                    }
                                    Ok(Some(Message::Close(_))) => {
                                        connection_monitor
                                            .update_status(
                                                ConnectionType::WebSocket,
                                                ConnectionStatus::Disconnected,
                                                None,
                                            )
                                            .await;
                                        break;
                                    }
                                    Ok(None) => {
                                        connection_monitor
                                            .update_status(
                                                ConnectionType::WebSocket,
                                                ConnectionStatus::Disconnected,
                                                None,
                                            )
                                            .await;
                                        break;
                                    }
                                    Err(e) => {
                                        error!("WebSocket error: {}", e);
                                        connection_monitor
                                            .update_status(
                                                ConnectionType::WebSocket,
                                                ConnectionStatus::Error,
                                                Some(e.to_string()),
                                            )
                                            .await;
                                        break;
                                    }
                                    _ => continue,
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to subscribe to wallets: {}", e);
                            connection_monitor
                                .update_status(
                                    ConnectionType::WebSocket,
                                    ConnectionStatus::Error,
                                    Some(format!("Subscription failed: {}", e)),
                                )
                                .await;
                            continue;
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to ensure connection: {}", e);
                    connection_monitor
                        .update_status(
                            ConnectionType::WebSocket,
                            ConnectionStatus::Error,
                            Some(format!("Connection failed: {}", e)),
                        )
                        .await;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        // Cleanup on exit
        if let Err(e) = connection_manager.shutdown().await {
            connection_monitor
                .update_status(
                    ConnectionType::WebSocket,
                    ConnectionStatus::Error,
                    Some(format!("Shutdown error: {}", e)),
                )
                .await;
        } else {
            connection_monitor
                .update_status(
                    ConnectionType::WebSocket,
                    ConnectionStatus::Disconnected,
                    None,
                )
                .await;
        }
    }

    async fn handle_websocket_message(
        message: Message,
        rpc_client: &Arc<RpcClient>,
        message_queue: &mpsc::UnboundedSender<ClientTxInfo>,
    ) -> Result<(), AppError> {
        match message {
            Message::Text(text) => {
                if let Some(tx_info) = process_websocket_message(text.as_str(), rpc_client)
                    .await
                    .map_err(|e| {
                        AppError::WebSocketError(format!("Failed to process message: {}", e))
                    })?
                {
                    println!("Processed transaction info: {:?}", tx_info);
                    message_queue.send(tx_info).map_err(|e| {
                        AppError::MessageProcessingError(format!("Failed to queue message: {}", e))
                    })?;
                }
            }
            Message::Close(_) => {
                return Err(AppError::WebSocketError("WebSocket closed".to_string()));
            }
            _ => {
                println!("Received non-text message: {:?}", message);
            }
        }
        Ok(())
    }

    async fn fetch_tracked_wallets(
        supabase_client: &SupabaseClient,
    ) -> Result<Vec<TrackedWallet>, AppError> {
        supabase_client
            .get_tracked_wallets()
            .await
            .context("Failed to fetch tracked wallets")
            .map_err(|e| AppError::DatabaseError(format!("Failed to fetch wallets: {}", e)))
    }

    async fn fetch_copy_trade_settings(
        supabase_client: &SupabaseClient,
    ) -> Result<Vec<CopyTradeSettings>, AppError> {
        supabase_client
            .get_copy_trade_settings()
            .await
            .context("Failed to fetch copy trade settings")
            .map_err(|e| AppError::DatabaseError(format!("Failed to fetch settings: {}", e)))
    }
}
