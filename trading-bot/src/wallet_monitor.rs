use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use solana_client::{rpc_client::RpcClient, rpc_config::RpcTransactionConfig};
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
};
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding};
use std::str::FromStr;
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use trading_common::utils::{get_account_keys_from_message, get_metadata, get_server_keypair};

use crate::{
    event_system::EventSystem,
    server_wallet_manager::{ServerWalletManager, TokenInfo},
};
use trading_common::{
    constants::PUMP_FUN_PROGRAM_ID,
    database::SupabaseClient,
    models::{
        BuyRequest, ClientTxInfo, CopyTradeSettings, SellRequest, TrackedWallet,
        TrackedWalletNotification, TransactionLog, TransactionType,
    },
    pumpdotfun::{process_buy_request, process_sell_request},
    utils::get_token_balance,
};

pub struct WalletMonitor {
    rpc_client: Arc<RpcClient>,
    ws_url: String,
    supabase_client: SupabaseClient,
    server_keypair: Keypair,
    tracked_wallets: Option<Vec<TrackedWallet>>,
    copy_trade_settings: Option<Vec<CopyTradeSettings>>,
    tx_sender: mpsc::Sender<TransactionLog>,
    event_system: Arc<EventSystem>,
    message_queue: mpsc::UnboundedSender<ClientTxInfo>,
    message_receiver: Option<mpsc::UnboundedReceiver<ClientTxInfo>>,
    ws_connection: Option<WebSocketStream<TcpStream>>,
    stop_signal: tokio::sync::watch::Sender<bool>,
    stop_receiver: tokio::sync::watch::Receiver<bool>,
    server_wallet_manager: Arc<tokio::sync::Mutex<ServerWalletManager>>,
}

impl WalletMonitor {
    pub async fn new(
        rpc_client: Arc<RpcClient>,
        ws_url: String,
        supabase_client: SupabaseClient,
        server_keypair: Keypair,
        tx_sender: mpsc::Sender<TransactionLog>,
        event_system: Arc<EventSystem>,
        server_wallet_manager: Arc<tokio::sync::Mutex<ServerWalletManager>>,
    ) -> Result<Self> {
        let user_id = server_keypair.pubkey().to_string();
        println!("Initializing WalletMonitor for user: {}", user_id);

        let exists = supabase_client.user_exists(&user_id).await?;
        println!("User exists check result: {}", exists); // Add debug print

        if !exists {
            println!("Creating new user in database"); // Add debug print
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
            supabase_client,
            server_keypair,
            tracked_wallets: Some(tracked_wallets),
            copy_trade_settings: Some(copy_trade_settings),
            tx_sender,
            event_system,
            message_queue: tx,
            message_receiver: Some(rx),
            ws_connection: None,
            stop_signal: stop_tx,
            stop_receiver: stop_rx,
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
        let mut stop_rx = self.stop_receiver.clone();
        loop {
            tokio::select! {
                _ = stop_rx.changed() => {
                    if *stop_rx.borrow() {
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
            .take()
            .ok_or_else(|| anyhow::anyhow!("Message receiver not available"))?;
        let event_system = self.event_system.clone();
        let rpc_client = self.rpc_client.clone();
        let server_wallet_manager = self.server_wallet_manager.clone();
        let mut stop_rx = self.stop_receiver.clone();
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
                        if let Err(e) = Self::handle_transaction(
                            &rpc_client,
                            &server_keypair,
                            &event_system,
                            &server_wallet_manager,
                            &copy_trade_settings,
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
        println!("Handling transaction: {}", client_message.signature);
        println!("Transaction type: {:?}", client_message.transaction_type);
        println!(
            "Token: {} ({})",
            client_message.token_name, client_message.token_address
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

        // Send tracked wallet notification
        let notification = TrackedWalletNotification {
            type_: "tracked_wallet_trade".to_string(),
            data: client_message.clone(),
        };
        event_system.handle_tracked_wallet_trade(notification).await;
        println!("Sent tracked wallet notification");

        // Check copy trading settings
        if let Some(settings) = copy_trade_settings.as_ref().and_then(|s| s.first()) {
            println!("Checking copy trade settings");
            println!("Copy trading enabled: {}", settings.is_enabled);

            if settings.is_enabled
                && Self::should_copy_trade(&client_message, settings, server_wallet_manager).await?
            {
                println!("Executing copy trade");
                let result =
                    Self::execute_copy_trade(rpc_client, server_keypair, &client_message, settings)
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
                println!("Skipping copy trade due to settings or validation");
            }
        } else {
            println!("No copy trade settings found");
        }

        Ok(())
    }

    async fn start_websocket_monitor(&mut self) -> Result<tokio::task::JoinHandle<()>> {
        let ws_url = self.ws_url.clone();
        let message_queue = self.message_queue.clone();
        let mut stop_rx = self.stop_receiver.clone();
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

                match connect_async(&ws_url).await {
                    Ok((ws_stream, _)) => {
                        consecutive_errors = 0;
                        let (mut write, mut read) = ws_stream.split();

                        // Subscribe to tracked wallets
                        if let Some(wallets) = &tracked_wallets {
                            for (index, wallet) in wallets.iter().enumerate() {
                                let subscribe_msg = json!({
                                    "jsonrpc": "2.0",
                                    "id": index + 1,
                                    "method": "logsSubscribe",
                                    "params": [
                                        {"mentions": [wallet.wallet_address]},
                                        {"commitment": "confirmed"}
                                    ]
                                });

                                match write.send(Message::Text(subscribe_msg.to_string())).await {
                                    Ok(_) => {
                                        println!("Subscribed to wallet: {}", wallet.wallet_address)
                                    }
                                    Err(e) => {
                                        println!("Error subscribing to wallet: {}", e);
                                        consecutive_errors += 1;
                                        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                                            println!(
                                                "Too many consecutive errors, waiting longer..."
                                            );
                                            tokio::time::sleep(Duration::from_secs(
                                                reconnect_delay,
                                            ))
                                            .await;
                                        }
                                        continue 'outer;
                                    }
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
                                            match Self::process_websocket_message(&text, &rpc_client).await {
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

    async fn process_websocket_message(
        text: &str,
        rpc_client: &Arc<RpcClient>,
    ) -> Result<Option<ClientTxInfo>> {
        println!("Processing websocket message");
        let value: Value = serde_json::from_str(text)?;
        println!("Raw message: {}", text);

        // Check for error in json_data
        if value.get("error").is_some() {
            println!("Received error message from RPC");
            return Ok(None);
        }

        let result = match value.get("params").and_then(|p| p.get("result")) {
            Some(r) => r,
            None => {
                println!("No result in message (subscription confirmation)");
                return Ok(None);
            }
        };

        let signature = match result
            .get("value")
            .and_then(|v| v.get("signature"))
            .and_then(|s| s.as_str())
        {
            Some(s) => s.to_string(),
            None => {
                println!("No signature found");
                return Ok(None);
            }
        };

        // Configure transaction fetch
        let config = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::JsonParsed),
            commitment: Some(solana_sdk::commitment_config::CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };

        // Fetch full transaction with retries
        let signature_obj = Signature::from_str(&signature)?;
        let mut retries = 20;
        let mut transaction_data = None;

        while retries > 0 {
            match rpc_client.get_transaction_with_config(&signature_obj, config) {
                Ok(data) => {
                    transaction_data = Some(data);
                    break;
                }
                Err(e) => {
                    println!(
                        "Error fetching transaction {} (retry {}): {}",
                        signature,
                        21 - retries,
                        e
                    );
                    retries -= 1;
                    if retries > 0 {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }

        let transaction_data = match transaction_data {
            Some(data) => data,
            None => {
                println!("Failed to fetch transaction data after retries");
                return Ok(None);
            }
        };

        // Process the transaction data to create ClientTxInfo
        Self::create_client_tx_info(&transaction_data, &signature, rpc_client).await
    }

    async fn create_client_tx_info(
        transaction_data: &EncodedConfirmedTransactionWithStatusMeta,
        signature: &str,
        rpc_client: &Arc<RpcClient>,
    ) -> Result<Option<ClientTxInfo>> {
        // Get the meta data
        let meta = match &transaction_data.transaction.meta {
            Some(meta) => meta,
            None => {
                println!("No meta data in transaction");
                return Ok(None);
            }
        };

        // Create longer-lived empty vectors
        let empty_token_balances = Vec::new();
        let empty_logs = Vec::new();

        // Extract token balances with proper lifetimes
        let pre_balances = meta
            .pre_token_balances
            .as_ref()
            .unwrap_or(&empty_token_balances);
        let post_balances = meta
            .post_token_balances
            .as_ref()
            .unwrap_or(&empty_token_balances);

        // Get token info
        let token_address = match post_balances.first() {
            Some(balance) => balance.mint.clone(),
            None => {
                println!("No token balance information");
                return Ok(None);
            }
        };

        // Calculate token amount change
        let pre_amount = pre_balances
            .first()
            .and_then(|b| b.ui_token_amount.ui_amount)
            .unwrap_or(0.0);
        let post_amount = post_balances
            .first()
            .and_then(|b| b.ui_token_amount.ui_amount)
            .unwrap_or(0.0);
        let amount_token = (post_amount - pre_amount).abs();

        // Calculate SOL amount change
        let pre_sol = meta.pre_balances.first().copied().unwrap_or(0);
        let post_sol = meta.post_balances.first().copied().unwrap_or(0);
        let amount_sol = ((post_sol as i64 - pre_sol as i64).abs() as f64) / 1e9;

        // Get transaction logs and determine type using the longer-lived empty vector
        let logs = meta.log_messages.as_ref().unwrap_or(&empty_logs);
        let transaction_type = if logs.iter().any(|log| log.contains("Instruction: Buy")) {
            TransactionType::Buy
        } else if logs.iter().any(|log| log.contains("Instruction: Sell")) {
            TransactionType::Sell
        } else {
            TransactionType::Unknown
        };

        // Get token metadata
        let token_pubkey = Pubkey::from_str(&token_address)?;
        let token_metadata = get_metadata(rpc_client, &token_pubkey).await?;

        // Get account keys
        let (seller, buyer) = match &transaction_data.transaction.transaction {
            solana_transaction_status::EncodedTransaction::Json(tx) => {
                let account_keys = get_account_keys_from_message(&tx.message);

                match transaction_type {
                    TransactionType::Sell => (
                        account_keys.first().cloned().unwrap_or_default(),
                        account_keys.get(1).cloned().unwrap_or_default(),
                    ),
                    TransactionType::Buy => (
                        account_keys.get(1).cloned().unwrap_or_default(),
                        account_keys.first().cloned().unwrap_or_default(),
                    ),
                    _ => (String::new(), String::new()),
                }
            }
            _ => (String::new(), String::new()),
        };

        Ok(Some(ClientTxInfo {
            signature: signature.to_string(),
            token_address,
            token_name: token_metadata.name,
            token_symbol: token_metadata.symbol,
            transaction_type,
            amount_token,
            amount_sol,
            price_per_token: if amount_token > 0.0 {
                amount_sol / amount_token
            } else {
                0.0
            },
            token_image_uri: token_metadata.uri,
            market_cap: 0.0,
            usd_market_cap: 0.0,
            timestamp: transaction_data.block_time.unwrap_or(0),
            seller,
            buyer,
        }))
    }

    async fn should_copy_trade(
        tx_info: &ClientTxInfo,
        settings: &CopyTradeSettings,
        server_wallet_manager: &Arc<tokio::sync::Mutex<ServerWalletManager>>,
    ) -> Result<bool> {
        // Token allowlist check
        if settings.use_allowed_tokens_list {
            if let Some(allowed_tokens) = &settings.allowed_tokens {
                if !allowed_tokens.contains(&tx_info.token_address) {
                    println!("Token not in allowed list: {}", tx_info.token_address);
                    return Ok(false);
                }
            }
        }

        match tx_info.transaction_type {
            TransactionType::Buy => {
                // Check if the current number of open positions is less than max allowed
                let wallet_manager = Arc::clone(&server_wallet_manager);
                let manager = wallet_manager.lock().await;
                let current_positions = manager.get_tokens().len();

                if current_positions >= settings.max_open_positions as usize {
                    println!(
                        "Maximum open positions reached: Current {} of {}",
                        current_positions, settings.max_open_positions
                    );
                    return Ok(false);
                }

                if !settings.allow_additional_buys {
                    // Check if we already hold this token
                    if manager.get_tokens().contains_key(&tx_info.token_address) {
                        println!("Additional buys not allowed and token already held");
                        return Ok(false);
                    }
                }
            }
            TransactionType::Sell => {
                // Sell-specific validation goes here
            }
            _ => return Ok(false),
        }

        Ok(true)
    }

    async fn execute_copy_trade(
        rpc_client: &Arc<RpcClient>,
        server_keypair: &Keypair,
        tx_info: &ClientTxInfo,
        settings: &CopyTradeSettings,
    ) -> Result<()> {
        match tx_info.transaction_type {
            TransactionType::Buy => {
                let request = BuyRequest {
                    token_address: tx_info.token_address.clone(),
                    sol_quantity: settings.trade_amount_sol,
                    slippage_tolerance: settings.max_slippage,
                };

                let response = process_buy_request(rpc_client, server_keypair, request).await?;
                if response.success {
                    println!("Copy trade buy executed: {}", response.signature);
                }
            }
            TransactionType::Sell => {
                println!("Preparing to execute copy trade sell");
                let token_mint = Pubkey::from_str(&tx_info.token_address)?;

                // Get the associated token account for our wallet
                let token_account = spl_associated_token_account::get_associated_token_address(
                    &server_keypair.pubkey(),
                    &token_mint,
                );

                println!("Using token account: {}", token_account);
                let token_balance = get_token_balance(rpc_client, &token_account).await?;
                println!(
                    "Found token balance to sell: {} {}",
                    token_balance, tx_info.token_symbol
                );
                println!("Using max slippage: {}%", settings.max_slippage * 100.0);

                let request = SellRequest {
                    token_address: tx_info.token_address.clone(),
                    token_quantity: token_balance,
                    slippage_tolerance: settings.max_slippage,
                };

                println!("Executing sell request...");
                let response = process_sell_request(rpc_client, server_keypair, request).await?;
                if response.success {
                    println!("Copy trade sell executed successfully:");
                    println!("  Signature: {}", response.signature);
                    println!(
                        "  Amount sold: {} {}",
                        response.token_quantity, tx_info.token_symbol
                    );
                    println!("  SOL received: {} SOL", response.sol_received);
                }
            }
            _ => {}
        }

        Ok(())
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
