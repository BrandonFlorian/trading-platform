use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use solana_client::rpc_request::TokenAccountsFilter;
use solana_client::{rpc_client::RpcClient, rpc_config::RpcTransactionConfig};
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
};
use solana_transaction_status::UiTransactionEncoding;
use std::str::FromStr;
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpStream, sync::mpsc, time::sleep};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use trading_common::utils::get_server_keypair;

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
    utils::{extract_token_account_info, get_token_balance},
};

const MAX_RECONNECT_ATTEMPTS: u32 = 5;
const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const WEBSOCKET_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct TransactionContext {
    signature: String,
    logs: Vec<String>,
    value: Value,
}

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

        // Check if user exists, create if not
        if !supabase_client.user_exists(&user_id).await? {
            println!("Creating new user in database");
            supabase_client.create_user(&user_id).await?;
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
        let _ = self.stop_signal.send(false);

        // Start message processor
        self.start_message_processor().await?;

        // Start WebSocket monitor
        self.start_websocket_monitor().await?;

        Ok(())
    }

    pub async fn stop(&mut self) -> Result<()> {
        println!("Stopping WalletMonitor...");
        let _ = self.stop_signal.send(true);

        if let Some(mut ws) = self.ws_connection.take() {
            let _ = ws.close(None).await;
        }

        Ok(())
    }

    async fn start_message_processor(&mut self) -> Result<()> {
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

        tokio::spawn(async move {
            while !*stop_rx.borrow() {
                tokio::select! {
                    Some(client_message) = rx.recv() => {
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
                    _ = stop_rx.changed() => {
                        break;
                    }
                }
            }
            println!("Message processor stopped");
        });

        Ok(())
    }

    async fn handle_transaction(
        rpc_client: &Arc<RpcClient>,
        server_keypair: &Keypair,
        event_system: &Arc<EventSystem>,
        server_wallet_manager: &Arc<tokio::sync::Mutex<ServerWalletManager>>,
        copy_trade_settings: &Option<Vec<CopyTradeSettings>>,
        client_message: ClientTxInfo,
    ) -> Result<()> {
        // Send tracked wallet notification
        let notification = TrackedWalletNotification {
            type_: "tracked_wallet_trade".to_string(),
            data: client_message.clone(),
        };
        event_system.handle_tracked_wallet_trade(notification).await;

        // Check if copy trading is enabled and execute if needed
        if let Some(settings) = copy_trade_settings.as_ref().and_then(|s| s.first()) {
            if settings.is_enabled && Self::should_copy_trade(&client_message, settings).await? {
                // Execute copy trade
                let result =
                    Self::execute_copy_trade(rpc_client, server_keypair, &client_message, settings)
                        .await;

                // Update wallet manager on successful trade
                if let Ok(()) = result {
                    let mut wallet_manager = server_wallet_manager.lock().await;
                    wallet_manager
                        .handle_trade_execution(&client_message)
                        .await?;
                }
            }
        }

        Ok(())
    }

    async fn start_websocket_monitor(&mut self) -> Result<()> {
        let mut reconnect_attempts = 0;
        let mut stop_rx = self.stop_receiver.clone();

        while !*stop_rx.borrow() && reconnect_attempts < MAX_RECONNECT_ATTEMPTS {
            match self.connect_websocket().await {
                Ok(()) => {
                    reconnect_attempts = 0;
                    println!("WebSocket connection established");
                }
                Err(e) => {
                    reconnect_attempts += 1;
                    println!(
                        "WebSocket connection failed (attempt {}/{}): {}",
                        reconnect_attempts, MAX_RECONNECT_ATTEMPTS, e
                    );
                    if reconnect_attempts < MAX_RECONNECT_ATTEMPTS {
                        sleep(RECONNECT_DELAY).await;
                        continue;
                    }
                }
            }

            // Wait for stop signal or timeout
            tokio::select! {
                _ = stop_rx.changed() => break,
                _ = sleep(WEBSOCKET_TIMEOUT) => {
                    println!("WebSocket connection timeout, reconnecting...");
                    continue;
                }
            }
        }

        if reconnect_attempts >= MAX_RECONNECT_ATTEMPTS {
            Err(anyhow::anyhow!("Max reconnection attempts reached"))
        } else {
            Ok(())
        }
    }

    async fn connect_websocket(&mut self) -> Result<()> {
        let (ws_stream, _) = connect_async(&self.ws_url).await?;
        let (mut write, mut read) = ws_stream.split();

        // Subscribe to tracked wallet addresses
        if let Some(wallets) = &self.tracked_wallets {
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
                write.send(Message::Text(subscribe_msg.to_string())).await?;
                println!("Subscribed to wallet: {}", wallet.wallet_address);
            }
        }

        let message_queue = self.message_queue.clone();
        let mut stop_rx = self.stop_receiver.clone();
        let rpc_client = self.rpc_client.clone();

        tokio::spawn(async move {
            while !*stop_rx.borrow() {
                tokio::select! {
                    Some(msg) = read.next() => {
                        match msg {
                            Ok(Message::Text(text)) => {
                                match Self::process_websocket_message(&text, &rpc_client).await {
                                    Ok(Some(tx_info)) => {
                                        let _ = message_queue.send(tx_info);
                                    }
                                    Ok(None) => continue,
                                    Err(e) => println!("Error processing message: {}", e),
                                }
                            }
                            Err(e) => {
                                println!("WebSocket error: {}", e);
                                break;
                            }
                            _ => continue,
                        }
                    }
                    _ = stop_rx.changed() => {
                        break;
                    }
                }
            }
            println!("WebSocket monitor stopped");
        });

        Ok(())
    }

    async fn process_websocket_message(
        text: &str,
        rpc_client: &Arc<RpcClient>,
    ) -> Result<Option<ClientTxInfo>> {
        let value: Value = serde_json::from_str(text)?;

        // Parse message context
        let tx_context = match Self::parse_transaction_context(&value) {
            Ok(Some(context)) => context,
            Ok(None) => return Ok(None),
            Err(e) => {
                println!("Error parsing transaction context: {}", e);
                return Ok(None);
            }
        };

        // Check for Pump.fun transactions
        if !tx_context
            .logs
            .iter()
            .any(|log| log.contains(&PUMP_FUN_PROGRAM_ID.to_string()))
        {
            return Ok(None);
        }

        // Get transaction details
        let transaction_data = rpc_client.get_transaction(
            &Signature::from_str(&tx_context.signature)?,
            UiTransactionEncoding::JsonParsed,
        )?;

        let transaction_value = serde_json::to_value(&transaction_data)?;

        // Create client transaction info
        let client_tx_info = Self::create_client_tx_info(&transaction_value).await?;
        Ok(Some(client_tx_info))
    }

    fn parse_transaction_context(value: &Value) -> Result<Option<TransactionContext>> {
        // Check if it's a subscription confirmation
        if value.get("result").is_some() {
            return Ok(None);
        }

        let tx_value = value
            .get("params")
            .and_then(|p| p.get("result"))
            .and_then(|r| r.get("value"))
            .ok_or_else(|| anyhow::anyhow!("Invalid message structure"))?;

        let signature = tx_value
            .get("signature")
            .and_then(|s| s.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing signature"))?
            .to_string();

        let logs = tx_value
            .get("logs")
            .and_then(|l| l.as_array())
            .ok_or_else(|| anyhow::anyhow!("Missing logs"))?
            .iter()
            .filter_map(|l| l.as_str().map(String::from))
            .collect();

        Ok(Some(TransactionContext {
            signature,
            logs,
            value: tx_value.clone(),
        }))
    }

    async fn should_copy_trade(
        tx_info: &ClientTxInfo,
        settings: &CopyTradeSettings,
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
                if settings.max_open_positions <= 0 {
                    println!("Maximum open positions reached");
                    return Ok(false);
                }

                if !settings.allow_additional_buys {
                    println!("Additional buys not allowed");
                    return Ok(false);
                }
            }
            TransactionType::Sell => {
                // Implement any sell-specific validation
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
                let token_balance =
                    get_token_balance(rpc_client, &Pubkey::from_str(&tx_info.token_address)?)
                        .await?;

                let request = SellRequest {
                    token_address: tx_info.token_address.clone(),
                    token_quantity: token_balance,
                    slippage_tolerance: settings.max_slippage,
                };

                let response = process_sell_request(rpc_client, server_keypair, request).await?;
                if response.success {
                    println!("Copy trade sell executed: {}", response.signature);
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

    async fn create_client_tx_info(transaction_data: &Value) -> Result<ClientTxInfo> {
        let meta = transaction_data
            .get("result")
            .and_then(|r| r.get("meta"))
            .ok_or_else(|| anyhow::anyhow!("Missing transaction meta"))?;

        let log_messages = meta
            .get("logMessages")
            .and_then(|l| l.as_array())
            .ok_or_else(|| anyhow::anyhow!("Missing log messages"))?;

        let transaction_type = if log_messages
            .iter()
            .any(|msg| msg.as_str().unwrap_or("").contains("Instruction: Buy"))
        {
            TransactionType::Buy
        } else {
            TransactionType::Sell
        };

        let pre_balances = meta
            .get("preTokenBalances")
            .and_then(|b| b.as_array())
            .ok_or_else(|| anyhow::anyhow!("Missing pre balances"))?;

        let post_balances = meta
            .get("postTokenBalances")
            .and_then(|b| b.as_array())
            .ok_or_else(|| anyhow::anyhow!("Missing post balances"))?;

        let pre_amount = if pre_balances.is_empty() {
            0.0
        } else {
            pre_balances[0]
                .get("uiTokenAmount")
                .and_then(|a| a.get("uiAmountString"))
                .and_then(|s| s.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0)
        };

        let post_amount = if post_balances.is_empty() {
            0.0
        } else {
            post_balances[0]
                .get("uiTokenAmount")
                .and_then(|a| a.get("uiAmountString"))
                .and_then(|s| s.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0)
        };

        let amount_token = (post_amount - pre_amount).abs();

        let pre_sol = meta
            .get("preBalances")
            .and_then(|b| b.as_array())
            .and_then(|b| b.get(0))
            .and_then(|b| b.as_u64())
            .ok_or_else(|| anyhow::anyhow!("Missing pre SOL balance"))?;

        let post_sol = meta
            .get("postBalances")
            .and_then(|b| b.as_array())
            .and_then(|b| b.get(0))
            .and_then(|b| b.as_u64())
            .ok_or_else(|| anyhow::anyhow!("Missing post SOL balance"))?;

        let sol_amount = ((post_sol as i64 - pre_sol as i64).abs() as f64) / 1e9;

        let price_per_token = if amount_token > 0.0 {
            sol_amount / amount_token
        } else {
            0.0
        };

        let token_mint = post_balances[0]
            .get("mint")
            .and_then(|m| m.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing mint address"))?;

        let token_info = Self::get_token_info(token_mint).await?;

        Ok(ClientTxInfo {
            signature: transaction_data
                .get("result")
                .and_then(|r| r.get("transaction"))
                .and_then(|t| t.get("signatures"))
                .and_then(|s| s.as_array())
                .and_then(|s| s.get(0))
                .and_then(|s| s.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing signature"))?
                .to_string(),
            token_address: token_mint.to_string(),
            token_name: token_info.name,
            token_symbol: token_info.symbol,
            transaction_type,
            amount_token,
            amount_sol: sol_amount,
            price_per_token,
            token_image_uri: token_info.metadata_uri.unwrap_or_default(),
            market_cap: token_info.market_cap,
            usd_market_cap: 0.0,
            timestamp: transaction_data
                .get("result")
                .and_then(|r| r.get("blockTime"))
                .and_then(|t| t.as_i64())
                .unwrap_or(0),
            seller: transaction_data
                .get("result")
                .and_then(|r| r.get("transaction"))
                .and_then(|t| t.get("message"))
                .and_then(|m| m.get("accountKeys"))
                .and_then(|a| a.as_array())
                .and_then(|a| a.get(0))
                .and_then(|a| a.as_str())
                .unwrap_or("")
                .to_string(),
            buyer: transaction_data
                .get("result")
                .and_then(|r| r.get("transaction"))
                .and_then(|t| t.get("message"))
                .and_then(|m| m.get("accountKeys"))
                .and_then(|a| a.as_array())
                .and_then(|a| a.get(1))
                .and_then(|a| a.as_str())
                .unwrap_or("")
                .to_string(),
        })
    }

    async fn get_token_info(mint_address: &str) -> Result<TokenInfo> {
        let url = format!("https://frontend-api.pump.fun/coins/{}", mint_address);
        println!("Fetching token info from: {}", url);

        let mut response = surf::get(&url)
            .header(
                "User-Agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
            )
            .header("Accept", "*/*")
            .header("Accept-Language", "en-US,en;q=0.5")
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        if response.status() != 200 {
            return Err(anyhow::anyhow!(
                "Failed to get token info: {}",
                response.status()
            ));
        }

        let pump_fun_data: Value = response.body_json().await.map_err(|e| anyhow::anyhow!(e))?;

        Ok(TokenInfo {
            address: mint_address.to_string(),
            symbol: pump_fun_data["symbol"]
                .as_str()
                .unwrap_or("Unknown")
                .to_string(),
            name: pump_fun_data["name"]
                .as_str()
                .unwrap_or("Unknown")
                .to_string(),
            balance: "0".to_string(),
            metadata_uri: Some(
                pump_fun_data["image_uri"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
            ),
            decimals: 9,
            market_cap: pump_fun_data["market_cap"].as_f64().unwrap_or(0.0),
        })
    }
}
