use anyhow::{Context, Result};
use parking_lot::{Mutex, RwLock};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{signature::Keypair, signer::Signer};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::error;
use trading_common::{
    data::get_server_keypair,
    database::SupabaseClient,
    error::AppError,
    event_system::EventSystem,
    models::{
        ClientTxInfo, CopyTradeNotification, CopyTradeSettings, TrackedWallet,
        TrackedWalletNotification, TransactionLoggedNotification,
    },
    server_wallet_manager::ServerWalletManager,
    utils::{
        copy_trade::{execute_copy_trade, should_copy_trade},
        transaction::process_websocket_message,
    },
    websocket::{WebSocketConfig, WebSocketConnectionManager},
    TransactionLog,
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
                println!("Current copy trade settings: {:?}", settings);
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
            if settings.is_enabled {
                println!("Copy trading enabled with settings: {:?}", settings);

                match Self::process_copy_trade(
                    rpc_client,
                    server_keypair,
                    server_wallet_manager,
                    settings,
                    &client_message,
                )
                .await
                {
                    Ok(_) => {
                        // Emit copy trade executed event after successful execution
                        event_system
                            .handle_copy_trade_executed(CopyTradeNotification {
                                data: client_message.clone(),
                                type_: "copy_trade_executed".to_string(),
                            })
                            .await;
                    }
                    Err(e) => {
                        println!("Copy trade failed: {}", e);
                        return Err(AppError::MessageProcessingError(format!(
                            "Copy trade failed: {}",
                            e
                        )));
                    }
                }
            }
        }

        let transaction_log = TransactionLog {
            id: Uuid::new_v4(),
            user_id: server_keypair.pubkey().to_string(),
            tracked_wallet_id: None, // todo: should probably track this in ClientTxInfo
            signature: client_message.signature.clone(),
            transaction_type: format!("{:?}", client_message.transaction_type),
            token_address: client_message.token_address.clone(),
            amount: client_message.amount_token,
            price_sol: client_message.price_per_token,
            timestamp: chrono::Utc::now(),
        };

        // Log to database after successful processing
        event_system
            .handle_transaction_logged(TransactionLoggedNotification {
                data: transaction_log.clone(),
                type_: "transaction_logged".to_string(),
            })
            .await;

        // Send notification for transaction
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

        loop {
            if *stop_receiver.borrow() {
                break;
            }

            let wallet_addresses: Vec<String> = tracked_wallets
                .read()
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

            match connection_manager.ensure_connection().await {
                Ok(_) => {
                    // Try to subscribe
                    if let Err(e) = connection_manager.subscribe(wallet_addresses).await {
                        error!("Failed to subscribe to wallets: {}", e);
                        continue;
                    }

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
                                }
                            }
                            Ok(Some(Message::Close(_))) => break,
                            Ok(None) => break, // Connection closed
                            Err(e) => {
                                error!("WebSocket error: {}", e);
                                break;
                            }
                            _ => continue,
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to ensure connection: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        // Cleanup on exit
        connection_manager.shutdown().await.ok();
    }

    async fn handle_websocket_message(
        message: Message,
        rpc_client: &Arc<RpcClient>,
        message_queue: &mpsc::UnboundedSender<ClientTxInfo>,
    ) -> Result<(), AppError> {
        match message {
            Message::Text(text) => {
                println!("Received WebSocket message: {}", text);
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
