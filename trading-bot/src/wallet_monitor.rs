use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use parking_lot::{Mutex, RwLock};
use serde_json::json;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{signature::Keypair, signer::Signer};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
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

        let exists = supabase_client.user_exists(&user_id).await?;
        println!("User exists check result: {}", exists);

        if !exists {
            println!("Creating new user in database");
            match supabase_client.create_user(&user_id).await {
                Ok(uuid) => println!("Created user with UUID: {}", uuid),
                Err(e) => println!("Error creating user: {}", e),
            }
        }

        let tracked_wallets = Self::fetch_tracked_wallets(&supabase_client).await?;
        println!("Fetched {} tracked wallets", tracked_wallets.len());

        let copy_trade_settings = Self::fetch_copy_trade_settings(&supabase_client).await?;
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

    pub async fn start(&mut self) -> Result<()> {
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

    pub async fn stop(&mut self) -> Result<()> {
        println!("Stopping WalletMonitor...");
        let _ = self.stop_signal.send(true);

        println!("Waiting for tasks to complete...");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        println!("WalletMonitor stopped");
        Ok(())
    }

    async fn start_message_processor(&mut self) -> Result<tokio::task::JoinHandle<()>> {
        let mut rx = self
            .message_receiver
            .lock()
            .take()
            .ok_or_else(|| anyhow::anyhow!("Message receiver not available"))?;
        let event_system = self.event_system.clone();
        let rpc_client = self.rpc_client.clone();
        let server_wallet_manager = self.server_wallet_manager.clone();
        let stop_rx = self.stop_receiver.clone();
        let copy_trade_settings = self.copy_trade_settings.clone();
        let server_keypair = get_server_keypair();

        let handle = tokio::spawn(async move {
            println!("Message processor started");
            loop {
                if *stop_rx.borrow() {
                    println!("Message processor received stop signal");
                    break;
                }

                tokio::select! {
                    Some(client_message) = rx.recv() => {
                        println!("Processing message: {}", client_message.signature);
                        // Get settings within this scope
                        let settings = copy_trade_settings.read().clone();
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
        });

        Ok(handle)
    }

    async fn handle_transaction(
        rpc_client: &Arc<RpcClient>,
        server_keypair: &Keypair,
        event_system: &Arc<EventSystem>,
        server_wallet_manager: &Arc<tokio::sync::Mutex<ServerWalletManager>>,
        copy_trade_settings: &Option<Vec<CopyTradeSettings>>,
        client_message: ClientTxInfo,
    ) -> Result<()> {
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

            if settings.is_enabled
                && should_copy_trade(&client_message, settings, server_wallet_manager).await?
            {
                println!("Executing copy trade for {:?}", client_message.dex_type);
                let result = execute_copy_trade(
                    rpc_client,
                    server_keypair,
                    &client_message,
                    settings,
                    client_message.dex_type.clone(),
                )
                .await;

                match result {
                    Ok(()) => {
                        println!("Copy trade executed successfully");
                        let mut wallet_manager = server_wallet_manager.lock().await;
                        wallet_manager
                            .handle_trade_execution(&client_message)
                            .await?;
                        println!("Wallet manager updated");
                    }
                    Err(e) => println!("Copy trade execution failed: {}", e),
                }
            } else {
                println!("Copy trade validation failed");
            }
        } else {
            println!("No copy trade settings found");
        }

        // Send tracked wallet notification
        let notification = TrackedWalletNotification {
            type_: "tracked_wallet_trade".to_string(),
            data: client_message,
        };
        event_system.handle_tracked_wallet_trade(notification).await;
        println!("Sent tracked wallet notification");
        println!("----------------------");

        Ok(())
    }

    async fn start_websocket_monitor(&mut self) -> Result<tokio::task::JoinHandle<()>> {
        let ws_url = self.ws_url.clone();
        let message_queue = self.message_queue.clone();
        let stop_rx = self.stop_receiver.clone();
        let tracked_wallets = self.tracked_wallets.clone();
        let rpc_client = self.rpc_client.clone();

        let handle = tokio::spawn(async move {
            let mut consecutive_errors = 0;
            const MAX_CONSECUTIVE_ERRORS: u32 = 3;
            const BASE_RECONNECT_DELAY: u64 = 5;

            println!("WebSocket monitor started");
            'outer: loop {
                if *stop_rx.borrow() {
                    println!("WebSocket monitor received stop signal");
                    break;
                }

                let reconnect_delay = BASE_RECONNECT_DELAY * (1 << consecutive_errors.min(3));

                // Collect wallet addresses before websocket connection
                let wallet_addresses = {
                    let wallets = tracked_wallets.read();
                    wallets
                        .as_ref()
                        .map(|w| {
                            w.iter()
                                .map(|wallet| wallet.wallet_address.clone())
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()
                };

                if wallet_addresses.is_empty() {
                    println!("No tracked wallets found");
                    tokio::time::sleep(Duration::from_secs(reconnect_delay)).await;
                    continue;
                }

                match connect_async(&ws_url).await {
                    Ok((ws_stream, _)) => {
                        consecutive_errors = 0;
                        let (mut write, mut read) = ws_stream.split();

                        // Subscribe to tracked wallets
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

                            match write.send(Message::Text(subscribe_msg.to_string())).await {
                                Ok(_) => println!("Subscribed to wallet: {}", wallet_address),
                                Err(e) => {
                                    println!("Error subscribing to wallet: {}", e);
                                    consecutive_errors += 1;
                                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                                        println!("Too many consecutive errors, waiting longer...");
                                        tokio::time::sleep(Duration::from_secs(reconnect_delay))
                                            .await;
                                    }
                                    continue 'outer;
                                }
                            }
                        }

                        // Process messages
                        loop {
                            if *stop_rx.borrow() {
                                break 'outer;
                            }

                            tokio::select! {
                                Some(msg) = read.next() => {
                                    match msg {
                                        Ok(Message::Text(text)) => {
                                            match process_websocket_message(&text, &rpc_client).await {
                                                Ok(Some(tx_info)) => {
                                                    println!("Successfully processed transaction: {}", tx_info.signature);
                                                    let _ = message_queue.send(tx_info);
                                                }
                                                Ok(None) => { /* Not a relevant message */ }
                                                Err(e) => println!("Error processing message: {}", e),
                                            }
                                        }
                                        Err(e) => {
                                            println!("WebSocket error: {}", e);
                                            consecutive_errors += 1;
                                            break;
                                        }
                                        _ => {}
                                    }
                                }
                                _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => continue,
                            }
                        }
                    }
                    Err(e) => {
                        println!("WebSocket connection error: {}", e);
                        consecutive_errors += 1;
                        tokio::time::sleep(Duration::from_secs(reconnect_delay)).await;
                    }
                }
            }
            println!("WebSocket monitor shutting down");
        });

        Ok(handle)
    }

    async fn fetch_tracked_wallets(supabase_client: &SupabaseClient) -> Result<Vec<TrackedWallet>> {
        supabase_client
            .get_tracked_wallets()
            .await
            .context("Failed to fetch tracked wallets")
    }

    async fn fetch_copy_trade_settings(
        supabase_client: &SupabaseClient,
    ) -> Result<Vec<CopyTradeSettings>> {
        supabase_client
            .get_copy_trade_settings()
            .await
            .context("Failed to fetch copy trade settings")
    }
}
