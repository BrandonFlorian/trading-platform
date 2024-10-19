use crate::event_system::EventSystem;
use crate::server_wallet_manager::TokenInfo;
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature, Signer};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use trading_common::constants::PUMP_FUN_PROGRAM_ID;
use trading_common::database::SupabaseClient;
use trading_common::error::AppError;
use trading_common::models::TrackedWalletNotification;
use trading_common::models::{
    ClientTxInfo, CopyTradeSettings, TrackedWallet, TransactionLog, TransactionType,
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
}

impl WalletMonitor {
    pub async fn new(
        rpc_client: Arc<RpcClient>,
        ws_url: String,
        supabase_client: SupabaseClient,
        server_keypair: Keypair,
        tx_sender: mpsc::Sender<TransactionLog>,
        event_system: Arc<EventSystem>,
    ) -> Result<Self> {
        let user_id: String = server_keypair.pubkey().to_string();

        // Check if user exists, create if not
        if !supabase_client.user_exists(&user_id).await? {
            supabase_client.create_user(&user_id).await?;
        }

        let tracked_wallets = Self::fetch_tracked_wallets(&supabase_client).await?;
        println!("tracked_wallets: {:?}", tracked_wallets);
        let copy_trade_settings = Self::fetch_copy_trade_settings(&supabase_client).await?;
        println!("copy_trade_settings: {:?}", copy_trade_settings);

        Ok(Self {
            rpc_client,
            ws_url,
            supabase_client,
            server_keypair,
            tracked_wallets: Some(tracked_wallets),
            copy_trade_settings: Some(copy_trade_settings),
            tx_sender,
            event_system,
        })
    }

    async fn fetch_tracked_wallets(
        supabase_client: &SupabaseClient,
    ) -> Result<Vec<TrackedWallet>, AppError> {
        supabase_client.get_tracked_wallets().await
    }

    async fn fetch_copy_trade_settings(
        supabase_client: &SupabaseClient,
    ) -> Result<Vec<CopyTradeSettings>, AppError> {
        supabase_client.get_copy_trade_settings().await
    }

    pub async fn start(&mut self) -> Result<(), anyhow::Error> {
        let tracked_wallet_addresses: Vec<String> = if let Some(wallets) = &self.tracked_wallets {
            wallets.iter().map(|w| w.wallet_address.clone()).collect()
        } else {
            println!("No tracked wallets found");
            return Err(anyhow::anyhow!("No tracked wallets found"));
        };

        if tracked_wallet_addresses.is_empty() {
            println!("No tracked wallet addresses to monitor");
            return Err(anyhow::anyhow!("No tracked wallet addresses to monitor"));
        }

        println!("tracked_wallet_addresses: {:?}", tracked_wallet_addresses);

        // Connect to Solana websocket
        let (ws_stream, _) = connect_async(self.get_ws_url()).await?;
        let (mut write, mut read) = ws_stream.split();
        println!("Connected to Solana websocket");
        // Subscribe to each wallet address separately
        for (index, address) in tracked_wallet_addresses.iter().enumerate() {
            let subscribe_msg = json!({
                "jsonrpc": "2.0",
                "id": index + 1,
                "method": "logsSubscribe",
                "params": [
                    {"mentions": [address]},
                    {"commitment": "confirmed"}
                ]
            });
            write.send(Message::Text(subscribe_msg.to_string())).await?;
            println!("Subscribed to address: {}", address);
        }

        // Process incoming messages
        while let Some(msg) = read.next().await {
            let msg = msg?;
            println!("Received message: {:?}", msg);
            if let Ok(value) = serde_json::from_str::<Value>(&msg.to_string()) {
                self.process_message(value).await?;
            }
        }
        Ok(())
    }

    async fn fetch_wallets_and_settings(&mut self) -> Result<()> {
        let public_key = self.server_keypair.pubkey();
        self.tracked_wallets = Some(
            self.supabase_client
                .get_tracked_wallets()
                .await
                .context("Failed to fetch tracked wallets")?,
        );
        self.copy_trade_settings = Some(
            self.supabase_client
                .get_copy_trade_settings()
                .await
                .context("Failed to fetch copy trade settings")?,
        );
        Ok(())
    }

    async fn process_message(&self, message: Value) -> Result<()> {
        println!("Processing message: {:?}", message);

        // Check if the message is a subscription confirmation
        if let Some(result) = message.get("result") {
            println!("Subscription confirmed: {:?}", result);
            return Ok(());
        }

        // Check if the message contains transaction data
        if let Some(params) = message.get("params") {
            if let Some(result) = params.get("result") {
                if let Some(value) = result.get("value") {
                    if let Some(signature) = value.get("signature") {
                        println!("Transaction signature: {}", signature);
                        // Process the transaction
                        self.process_transaction(value).await?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn process_transaction(&self, transaction_data: &Value) -> Result<(), anyhow::Error> {
        println!("Processing transaction: {:?}", transaction_data);

        let signature = transaction_data["signature"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing signature"))?;
        println!("Transaction signature: {}", signature);

        let logs = transaction_data["logs"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Missing logs"))?;
        println!("Transaction logs: {:?}", logs);

        // Check if it's a Pump.fun transaction
        if !logs.iter().any(|log| {
            log.as_str()
                .unwrap_or("")
                .contains(&PUMP_FUN_PROGRAM_ID.to_string())
        }) {
            println!("Not a Pump.fun transaction");
            return Ok(());
        }
        println!("Is a Pump.fun transaction");

        let transaction_type = if logs
            .iter()
            .any(|log| log.as_str().unwrap_or("").contains("Instruction: Buy"))
        {
            println!("Is a buy transaction");
            TransactionType::Buy
        } else if logs
            .iter()
            .any(|log| log.as_str().unwrap_or("").contains("Instruction: Sell"))
        {
            println!("Is a sell transaction");
            TransactionType::Sell
        } else {
            println!("Unknown transaction type");
            return Ok(());
        };

        println!("Transaction type: {:?}", transaction_type);

        let post_token_balances = transaction_data["meta"]["postTokenBalances"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Missing postTokenBalances"))?;

        println!("Post token balances: {:?}", post_token_balances);

        if post_token_balances.is_empty() {
            return Err(anyhow::anyhow!("No token balances found"));
        }

        let token_mint = post_token_balances[0]["mint"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing mint address"))?;
        let token_amount = post_token_balances[0]["uiTokenAmount"]["uiAmount"]
            .as_f64()
            .ok_or_else(|| anyhow::anyhow!("Missing token amount"))?;

        println!("Token mint: {}", token_mint);
        println!("Token amount: {}", token_amount);

        // Fetch token info
        let token_info = self.get_token_info(token_mint).await?;

        println!("Token info: {:?}", token_info);

        // Calculate SOL amount
        let sol_amount = self.calculate_sol_amount(transaction_data, &transaction_type)?;

        println!("SOL amount: {}", sol_amount);

        let client_tx_info = ClientTxInfo {
            signature: signature.to_string(),
            token_address: token_mint.to_string(),
            token_name: token_info.name,
            token_symbol: token_info.symbol,
            transaction_type,
            amount_token: token_amount,
            amount_sol: sol_amount,
            price_per_token: if token_amount > 0.0 {
                sol_amount / token_amount
            } else {
                0.0
            },
            token_image_uri: token_info.metadata_uri.unwrap_or_default(),
            market_cap: 0.0,
            usd_market_cap: 0.0,
            timestamp: transaction_data["blockTime"].as_i64().unwrap_or(0),
            seller: transaction_data["transaction"]["message"]["accountKeys"][0]
                .as_str()
                .unwrap_or("")
                .to_string(),
            buyer: transaction_data["transaction"]["message"]["accountKeys"][1]
                .as_str()
                .unwrap_or("")
                .to_string(),
        };

        println!("Client transaction info: {:?}", client_tx_info);

        self.send_tracked_wallet_trade(&client_tx_info).await?;

        if self.should_execute_copy_trade(&client_tx_info).await? {
            println!("Should execute copy trade");
            self.execute_copy_trade(client_tx_info).await?;
        }

        Ok(())
    }

    async fn should_execute_copy_trade(&self, tx_info: &ClientTxInfo) -> Result<bool> {
        println!("Should execute copy trade: {:?}", tx_info);
        // ...
        Ok(false)
    }

    async fn handle_buy(&self, tx_info: ClientTxInfo) -> Result<()> {
        println!("Handling buy: {:?}", tx_info);
        self.send_tracked_wallet_trade(&tx_info).await?;
        if self.should_execute_copy_trade(&tx_info).await? {
            self.execute_copy_trade(tx_info).await?;
        }
        Ok(())
    }

    async fn handle_sell(&self, tx_info: ClientTxInfo) -> Result<()> {
        println!("Handling sell: {:?}", tx_info);
        self.send_tracked_wallet_trade(&tx_info).await?;
        if self.should_execute_copy_trade(&tx_info).await? {
            self.execute_copy_trade(tx_info).await?;
        }
        Ok(())
    }

    async fn send_tracked_wallet_trade(&self, tx_info: &ClientTxInfo) -> Result<()> {
        let notification = TrackedWalletNotification {
            type_: "tracked_wallet_trade".to_string(),
            data: tx_info.clone(),
        };
        self.event_system
            .handle_tracked_wallet_trade(notification)
            .await;
        Ok(())
    }
    fn extract_signature(&self, message: &Value) -> Result<Signature> {
        let signature = message
            .pointer("/params/result/value/signature")
            .or_else(|| message.pointer("/result/value/signature"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Failed to extract signature from message"))?;

        Signature::from_str(signature).context("Failed to parse signature")
    }

    async fn get_transaction_info(&self, signature: &Signature) -> Result<ClientTxInfo> {
        // Implement logic to fetch and parse transaction info
        // This will involve calling Solana RPC methods and parsing the result
        unimplemented!("Implement get_transaction_info")
    }

    fn determine_transaction_type(&self, tx_info: &ClientTxInfo) -> TransactionType {
        // Implement logic to determine transaction type based on the transaction info
        unimplemented!("Implement determine_transaction_type")
    }

    async fn handle_transfer(&self, tx_info: ClientTxInfo) -> Result<()> {
        println!("Handling transfer: {:?}", tx_info);
        // Implement transfer handling logic
        Ok(())
    }

    async fn execute_copy_trade(&self, tx_info: ClientTxInfo) -> Result<()> {
        if let Ok(wallet_pubkey) = Pubkey::from_str(&tx_info.seller) {
            if let Some(tracked_wallets) = &self.tracked_wallets {
                if let Some(tracked_wallet) = tracked_wallets
                    .iter()
                    .find(|w| Pubkey::from_str(&w.wallet_address).ok() == Some(wallet_pubkey))
                {
                    if let Some(wallet_id) = tracked_wallet.id {
                        if let Some(copy_trade_settings) = &self.copy_trade_settings {
                            if let Some(settings) = copy_trade_settings
                                .iter()
                                .find(|s| s.tracked_wallet_id == wallet_id)
                            {
                                if settings.is_enabled {
                                    // Implement copy trade logic
                                    println!("Executing copy trade for: {:?}", tx_info);
                                    // Check allowed tokens, maximum open positions, etc.
                                    // Execute the trade
                                    // Log the transaction
                                    // Send notification
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn get_ws_url(&self) -> &str {
        &self.ws_url
    }

    async fn get_token_info(&self, mint_address: &str) -> Result<TokenInfo> {
        unimplemented!("Implement get_token_info")
    }

    async fn fetch_data(&self, url: &str) -> Result<String> {
        unimplemented!("Implement fetch_data")
    }

    fn calculate_sol_amount(
        &self,
        transaction_data: &Value,
        transaction_type: &TransactionType,
    ) -> Result<f64, anyhow::Error> {
        let pre_balances = transaction_data["meta"]["preBalances"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Missing preBalances"))?;
        let post_balances = transaction_data["meta"]["postBalances"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Missing postBalances"))?;

        if pre_balances.is_empty() || post_balances.is_empty() {
            return Err(anyhow::anyhow!("Invalid balance data"));
        }

        let pre_balance = pre_balances[0]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("Invalid preBalance"))?;
        let post_balance = post_balances[0]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("Invalid postBalance"))?;

        let sol_amount = match transaction_type {
            TransactionType::Buy => (pre_balance - post_balance) as f64 / 1e9,
            TransactionType::Sell => (post_balance - pre_balance) as f64 / 1e9,
            TransactionType::Transfer => (pre_balance.abs_diff(post_balance)) as f64 / 1e9,
            TransactionType::Unknown => {
                return Err(anyhow::anyhow!(
                    "Cannot calculate SOL amount for unknown transaction type"
                ));
            }
        };

        Ok(sol_amount)
    }
}
