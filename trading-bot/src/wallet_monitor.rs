use anyhow::{Context, Result};
use parking_lot::{Mutex, RwLock};
use serde_json::json;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{signature::Keypair, signer::Signer};
use std::{sync::Arc, time::Duration, time::Instant};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use trading_common::error::AppError;
use trading_common::websocket::WebSocketConfig;
use trading_common::websocket::WebSocketConnectionManager;
use trading_common::{data::get_server_keypair, event_system::EventSystem};
use trading_common::{
    database::SupabaseClient,
    models::{ClientTxInfo, CopyTradeSettings, TrackedWallet, TrackedWalletNotification},
    server_wallet_manager::ServerWalletManager,
    utils::{
        copy_trade::{execute_copy_trade, should_copy_trade},
        transaction::process_websocket_message,
    },
};
const MAX_MESSAGE_GAP: Duration = Duration::from_secs(30);
const SUBSCRIPTION_REFRESH_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes
const PING_INTERVAL: Duration = Duration::from_secs(15);
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
    server_wallet_manager: Arc<tokio::sync::Mutex<ServerWalletManager>>,
}

pub struct MessageProcessorContext {
    event_system: Arc<EventSystem>,
    rpc_client: Arc<RpcClient>,
    server_wallet_manager: Arc<tokio::sync::Mutex<ServerWalletManager>>,
    stop_receiver: Arc<tokio::sync::watch::Receiver<bool>>,
    copy_trade_settings: Arc<RwLock<Option<Vec<CopyTradeSettings>>>>,
    message_receiver: mpsc::UnboundedReceiver<ClientTxInfo>,
    server_keypair: Keypair,
}

pub struct WebSocketContext {
    message_queue: mpsc::UnboundedSender<ClientTxInfo>,
    stop_receiver: Arc<tokio::sync::watch::Receiver<bool>>,
    tracked_wallets: Arc<RwLock<Option<Vec<TrackedWallet>>>>,
    rpc_client: Arc<RpcClient>,
    connection_manager: WebSocketConnectionManager,
}

pub struct ConnectionMetrics {
    last_message_time: Instant,
    last_subscription_refresh: Instant,
    last_pong_received: Instant,
    message_count: u64,
    error_count: u64,
}

impl ConnectionMetrics {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            last_message_time: now,
            last_subscription_refresh: now,
            last_pong_received: now,
            message_count: 0,
            error_count: 0,
        }
    }
}

impl WalletMonitor {
    pub async fn new(
        rpc_client: Arc<RpcClient>,
        ws_url: String,
        supabase_client: SupabaseClient,
        server_keypair: Keypair,
        event_system: Arc<EventSystem>,
        server_wallet_manager: Arc<tokio::sync::Mutex<ServerWalletManager>>,
    ) -> Result<Self> {
        let user_id = server_keypair.pubkey().to_string();
        println!("Initializing WalletMonitor for user: {}", user_id);

        Self::ensure_user_exists(&supabase_client, &user_id).await?;

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

        println!("Fetched {} tracked wallets", tracked_wallets.len());
        println!("Fetched {} copy trade settings", copy_trade_settings.len());

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
            server_wallet_manager,
        })
    }

    async fn ensure_user_exists(
        supabase_client: &SupabaseClient,
        user_id: &str,
    ) -> Result<(), AppError> {
        let exists = supabase_client.user_exists(user_id).await?;

        if !exists {
            println!("Creating new user in database");
            supabase_client.create_user(user_id).await.map_err(|e| {
                AppError::InitializationError(format!("Failed to create user: {}", e))
            })?;
            println!("User created successfully");
        }

        Ok(())
    }

    pub async fn start(&mut self) -> Result<(), AppError> {
        println!("Starting WalletMonitor...");

        // Reset stop signal
        let _ = self.stop_signal.send(false);
        println!("Stop signal set to false");

        // Start tasks
        let message_processor = self.start_message_processor().await?;
        let websocket_monitor = self.start_websocket_monitor().await?;

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

        println!("Waiting for tasks to complete...");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        println!("WalletMonitor stopped");
        Ok(())
    }

    async fn start_message_processor(&mut self) -> Result<tokio::task::JoinHandle<()>, AppError> {
        let context = MessageProcessorContext {
            event_system: Arc::clone(&self.event_system),
            rpc_client: Arc::clone(&self.rpc_client),
            server_wallet_manager: Arc::clone(&self.server_wallet_manager),
            stop_receiver: Arc::clone(&self.stop_receiver),
            copy_trade_settings: Arc::clone(&self.copy_trade_settings),
            message_receiver: self.message_receiver.lock().take().ok_or_else(|| {
                AppError::InitializationError("Message receiver not available".to_string())
            })?,
            server_keypair: get_server_keypair(),
        };

        Ok(tokio::spawn(Self::run_message_processor(context)))
    }

    async fn run_message_processor(context: MessageProcessorContext) {
        let MessageProcessorContext {
            event_system,
            rpc_client,
            server_wallet_manager,
            stop_receiver,
            copy_trade_settings,
            mut message_receiver,
            server_keypair,
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
                let settings = copy_trade_settings.read().clone();
                println!("Current copy trade settings: {:?}", settings);  // Add this debug line
                if let Err(e) = Self::handle_transaction(
                    &rpc_client,
                    &server_keypair,
                    &event_system,
                    &server_wallet_manager,
                    &settings,
                    client_message,
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
        server_wallet_manager: &Arc<tokio::sync::Mutex<ServerWalletManager>>,
        copy_trade_settings: &Option<Vec<CopyTradeSettings>>,
        client_message: ClientTxInfo,
    ) -> Result<(), AppError> {
        println!("----------------------");
        println!("Handling transaction: {}", client_message.signature);
        println!("Transaction type: {:?}", client_message.transaction_type);
        println!(
            "Token: {} ({}) - {}",
            client_message.token_name, client_message.token_symbol, client_message.token_address
        );

        println!("Transaction Details:");
        println!(
            "  Amount Token: {} {}",
            client_message.amount_token, client_message.token_symbol
        );
        println!("  Amount SOL: {} SOL", client_message.amount_sol);
        println!("  Price per Token: {} SOL", client_message.price_per_token);
        println!("  Seller: {}", client_message.seller);
        println!("  Buyer: {}", client_message.buyer);
        println!("  DEX Type: {:?}", client_message.dex_type);

        // Check copy trading settings
        if let Some(settings) = copy_trade_settings.as_ref().and_then(|s| s.first()) {
            println!("Copy trading settings found:");
            println!("  Enabled: {}", settings.is_enabled);
            println!("  Trade amount: {} SOL", settings.trade_amount_sol);
            println!("  Max slippage: {}%", settings.max_slippage * 100.0);
            println!("  Max open positions: {}", settings.max_open_positions);
            println!(
                "  Allow additional buys: {}",
                settings.allow_additional_buys
            );

            if settings.is_enabled {
                Self::process_copy_trade(
                    rpc_client,
                    server_keypair,
                    server_wallet_manager,
                    settings,
                    &client_message,
                )
                .await
                .map_err(|e| {
                    AppError::MessageProcessingError(format!("Copy trade failed: {}", e))
                })?;
            }
        }

        Self::send_notification(event_system, client_message)
            .await
            .map_err(|e| {
                AppError::MessageProcessingError(format!("Failed to send notification: {}", e))
            })?;

        println!("----------------------");

        Ok(())
    }

    async fn process_copy_trade(
        rpc_client: &Arc<RpcClient>,
        server_keypair: &Keypair,
        server_wallet_manager: &Arc<tokio::sync::Mutex<ServerWalletManager>>,
        settings: &CopyTradeSettings,
        client_message: &ClientTxInfo,
    ) -> Result<(), AppError> {
        if !should_copy_trade(client_message, settings, server_wallet_manager).await? {
            return Ok(());
        }

        execute_copy_trade(
            rpc_client,
            server_keypair,
            client_message,
            settings,
            client_message.dex_type.clone(),
        )
        .await
        .map_err(|e| {
            AppError::MessageProcessingError(format!("Execute copy trade failed: {}", e))
        })?;

        let mut wallet_manager = server_wallet_manager.lock().await;
        wallet_manager
            .handle_trade_execution(client_message)
            .await
            .map_err(|e| {
                AppError::MessageProcessingError(format!("Wallet update failed: {}", e))
            })?;

        Ok(())
    }

    async fn send_notification(
        event_system: &Arc<EventSystem>,
        client_message: ClientTxInfo,
    ) -> Result<(), AppError> {
        let notification = TrackedWalletNotification {
            type_: "tracked_wallet_trade".to_string(),
            data: client_message,
        };

        event_system.handle_tracked_wallet_trade(notification).await;

        Ok(())
    }

    async fn start_websocket_monitor(&mut self) -> Result<tokio::task::JoinHandle<()>, AppError> {
        let ws_config = WebSocketConfig {
            health_check_interval: Duration::from_secs(30),
            connection_timeout: Duration::from_secs(5),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
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
        } = context;

        println!("WebSocket monitor started");
        let mut metrics = ConnectionMetrics::new();

        loop {
            if *stop_receiver.borrow() {
                println!("WebSocket monitor received stop signal");
                break;
            }

            let wallet_addresses: Vec<String> = {
                let wallets = tracked_wallets.read();
                wallets
                    .as_ref()
                    .map(|w| {
                        w.iter()
                            .map(|wallet| wallet.wallet_address.clone())
                            .collect()
                    })
                    .unwrap_or_default()
            };

            if wallet_addresses.is_empty() {
                println!("No tracked wallets found");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            match connection_manager.ensure_connection().await {
                Ok(_conn) => {
                    let mut subscription_failed = false;

                    // Initial subscription
                    if !Self::subscribe_to_wallets(&mut connection_manager, &wallet_addresses).await
                    {
                        println!("Initial subscription failed, retrying...");
                        continue;
                    }

                    // Message processing loop
                    loop {
                        if *stop_receiver.borrow() {
                            break;
                        }

                        // Check if we need to refresh subscriptions
                        if metrics.last_subscription_refresh.elapsed()
                            > SUBSCRIPTION_REFRESH_INTERVAL
                        {
                            println!(
                                "Refreshing subscriptions after {:?}",
                                SUBSCRIPTION_REFRESH_INTERVAL
                            );
                            if !Self::subscribe_to_wallets(
                                &mut connection_manager,
                                &wallet_addresses,
                            )
                            .await
                            {
                                println!("Subscription refresh failed, reconnecting...");
                                break;
                            }
                            metrics.last_subscription_refresh = Instant::now();
                        }

                        // Send periodic ping
                        if metrics.last_pong_received.elapsed() > PING_INTERVAL {
                            println!("Sending ping after {:?}", PING_INTERVAL);
                            if let Err(e) = connection_manager
                                .send_message(Message::Ping(vec![].into()))
                                .await
                            {
                                println!("Failed to send ping: {}", e);
                                break;
                            }
                        }

                        tokio::select! {
                            msg = connection_manager.receive_message() => {
                                match msg {
                                    Ok(Some(message)) => {
                                        metrics.last_message_time = Instant::now();
                                        metrics.message_count += 1;

                                        match message {
                                            Message::Text(text) => {
                                                if let Err(e) = Self::handle_websocket_message(
                                                    Message::Text(text),
                                                    &rpc_client,
                                                    &message_queue
                                                ).await {
                                                    println!("Error handling websocket message: {}", e);
                                                    metrics.error_count += 1;
                                                }
                                            }
                                            Message::Pong(_) => {
                                                metrics.last_pong_received = Instant::now();
                                                println!("Received pong response");
                                            }
                                            Message::Close(frame) => {
                                                println!("Received close frame: {:?}", frame);
                                                break;
                                            }
                                            _ => {}
                                        }
                                    }
                                    Ok(None) => {
                                        println!("WebSocket stream ended");
                                        break;
                                    }
                                    Err(e) => {
                                        println!("WebSocket error: {}", e);
                                        metrics.error_count += 1;
                                        break;
                                    }
                                }
                            }
                            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                                // Check for message gap
                                if metrics.last_message_time.elapsed() > MAX_MESSAGE_GAP {
                                    println!("No messages received for {:?}, initiating reconnection", MAX_MESSAGE_GAP);
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("Connection error: {}", e);
                    metrics.error_count += 1;
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }

            // Print metrics before reconnection attempt
            println!("Connection metrics before reconnect:");
            println!("  Messages processed: {}", metrics.message_count);
            println!("  Errors encountered: {}", metrics.error_count);
            println!(
                "  Time since last message: {:?}",
                metrics.last_message_time.elapsed()
            );

            println!("Connection lost or error occurred, attempting to reconnect...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        println!("WebSocket monitor shutting down");
    }

    async fn subscribe_to_wallets(
        connection_manager: &mut WebSocketConnectionManager,
        wallet_addresses: &[String],
    ) -> bool {
        for (index, wallet_address) in wallet_addresses.iter().enumerate() {
            let subscribe_msg = json!({
                "jsonrpc": "2.0",
                "id": index + 1,
                "method": "logsSubscribe",
                "params": [
                    {"mentions": [wallet_address]},
                    {"commitment": "confirmed"}
                ]
            });

            println!(
                "Sending subscription message for {}: {}",
                wallet_address, subscribe_msg
            );

            if let Err(e) = connection_manager
                .send_message(Message::Text(subscribe_msg.to_string().into()))
                .await
            {
                println!("Error subscribing to wallet {}: {}", wallet_address, e);
                return false;
            }
            println!("Subscribed to wallet: {}", wallet_address);
        }
        true
    }

    async fn handle_websocket_message(
        message: Message,
        rpc_client: &Arc<RpcClient>,
        message_queue: &mpsc::UnboundedSender<ClientTxInfo>,
    ) -> Result<(), AppError> {
        match message {
            Message::Text(text) => {
                println!("Received WebSocket message: {}", text); // Add this debug line
                if let Some(tx_info) = process_websocket_message(text.as_str(), rpc_client)
                    .await
                    .map_err(|e| {
                        AppError::WebSocketError(format!("Failed to process message: {}", e))
                    })?
                {
                    println!("Processed transaction info: {:?}", tx_info); // Add this debug line
                    message_queue.send(tx_info).map_err(|e| {
                        AppError::MessageProcessingError(format!("Failed to queue message: {}", e))
                    })?;
                }
            }
            Message::Close(_) => {
                return Err(AppError::WebSocketError("WebSocket closed".to_string()));
            }
            _ => {
                println!("Received non-text message: {:?}", message); // Add this debug line
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
