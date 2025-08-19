use crate::{errors::Result, types::*};
use hyperliquid_rust_sdk::{
    BaseUrl, ExchangeClient, InfoClient, MarketOrderParams, MarketCloseParams,
    ClientOrderRequest, ClientOrder, ClientLimit, ClientCancelRequestCloid, 
    ClientCancelRequest, ClientTrigger, ExchangeResponseStatus, ExchangeDataStatus,
};
use ethers::signers::{LocalWallet, Signer};
use ethers::types::H160;
use std::str::FromStr;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use uuid::Uuid;

pub struct HyperliquidClient {
    exchange_client: ExchangeClient,
    info_client: InfoClient,
    wallet: LocalWallet,
}

impl HyperliquidClient {
    pub async fn new(private_key: &str, testnet: bool) -> Result<Self> {
        let base_url = if testnet {
            BaseUrl::Testnet
        } else {
            BaseUrl::Mainnet
        };
        
        // Parse the private key into a wallet
        let wallet = LocalWallet::from_str(private_key)
            .map_err(|e| crate::HyperliquidError::AuthError(format!("Invalid private key: {}", e)))?;
        
        let exchange_client = ExchangeClient::new(None, wallet.clone(), Some(base_url), None, None)
            .await
            .map_err(|e| crate::HyperliquidError::SdkError(e.to_string()))?;
            
        let info_client = InfoClient::new(None, Some(base_url))
            .await
            .map_err(|e| crate::HyperliquidError::SdkError(e.to_string()))?;
        
        Ok(Self {
            exchange_client,
            info_client,
            wallet,
        })
    }
    
    pub async fn place_order(&self, request: SimpleOrderRequest) -> Result<OrderResponse> {
        let is_buy = matches!(request.side, OrderSide::Buy);
        
        // Validate asset exists in universe
        if self.get_asset_index(&request.asset).await?.is_none() {
            return Err(crate::HyperliquidError::InvalidOrder(
                format!("Asset '{}' not found in Hyperliquid universe", request.asset)
            ));
        }
        
        // Generate a client order ID if none provided
        let cloid = request.client_order_id.map(|id| 
            Uuid::parse_str(&id).unwrap_or_else(|_| Uuid::new_v4())
        ).unwrap_or_else(Uuid::new_v4);

        let response = match request.order_type {
            // Handle market orders
            OrderType::Trigger { trigger } if trigger.is_market => {
                let market_params = MarketOrderParams {
                    asset: &request.asset,
                    is_buy,
                    sz: request.size.to_f64().unwrap_or(0.0),
                    px: None, // Market order, no price limit
                    slippage: request.slippage_tolerance.map(|s| s.to_f64().unwrap_or(0.01)),
                    cloid: Some(cloid),
                    wallet: None,
                };

                if request.reduce_only {
                    // Use market close for reduce-only orders
                    let close_params = MarketCloseParams {
                        asset: &request.asset,
                        sz: Some(request.size.to_f64().unwrap_or(0.0)),
                        px: None,
                        slippage: request.slippage_tolerance.map(|s| s.to_f64().unwrap_or(0.01)),
                        cloid: Some(cloid),
                        wallet: None,
                    };
                    
                    self.exchange_client.market_close(close_params).await
                        .map_err(|e| crate::HyperliquidError::SdkError(e.to_string()))?
                } else {
                    self.exchange_client.market_open(market_params).await
                        .map_err(|e| crate::HyperliquidError::SdkError(e.to_string()))?
                }
            },
            
            // Handle trigger orders (stop loss/take profit)
            OrderType::Trigger { trigger } if !trigger.is_market => {
                let price = request.price
                    .ok_or_else(|| crate::HyperliquidError::InvalidOrder("Trigger order requires price".to_string()))?;
                
                let order_request = ClientOrderRequest {
                    asset: request.asset.clone(),
                    is_buy,
                    reduce_only: request.reduce_only,
                    limit_px: price.to_f64().unwrap_or(0.0),
                    sz: request.size.to_f64().unwrap_or(0.0),
                    cloid: Some(cloid),
                    order_type: ClientOrder::Trigger(ClientTrigger {
                        trigger_px: trigger.trigger_px.parse().unwrap_or(0.0),
                        is_market: trigger.is_market,
                        tpsl: match trigger.tpsl {
                            TpSl::Tp => "tp".to_string(),
                            TpSl::Sl => "sl".to_string(),
                        },
                    }),
                };

                self.exchange_client.order(order_request, None).await
                    .map_err(|e| crate::HyperliquidError::SdkError(e.to_string()))?
            },
            
            // Handle limit orders
            OrderType::Limit { limit } => {
                let price = request.price
                    .ok_or_else(|| crate::HyperliquidError::InvalidOrder("Limit order requires price".to_string()))?;
                
                let order_request = ClientOrderRequest {
                    asset: request.asset.clone(),
                    is_buy,
                    reduce_only: request.reduce_only,
                    limit_px: price.to_f64().unwrap_or(0.0),
                    sz: request.size.to_f64().unwrap_or(0.0),
                    cloid: Some(cloid),
                    order_type: ClientOrder::Limit(ClientLimit {
                        tif: match limit.tif {
                            TimeInForce::Gtc => "Gtc".to_string(),
                            TimeInForce::Ioc => "Ioc".to_string(),
                            TimeInForce::Alo => "Alo".to_string(),
                        },
                    }),
                };

                self.exchange_client.order(order_request, None).await
                    .map_err(|e| crate::HyperliquidError::SdkError(e.to_string()))?
            },
            
            _ => {
                return Err(crate::HyperliquidError::InvalidOrder(
                    "Unsupported order type".to_string()
                ));
            }
        };

        // Handle the response
        match response {
            ExchangeResponseStatus::Ok(exchange_response) => {
                if let Some(data) = exchange_response.data {
                    if let Some(status) = data.statuses.first() {
                        match status {
                            ExchangeDataStatus::Filled(order) => {
                                Ok(OrderResponse {
                                    order_id: order.oid.to_string(),
                                    client_order_id: Some(cloid.to_string()),
                                    status: "filled".to_string(),
                                    filled_size: Decimal::from_str(&order.total_sz).unwrap_or(Decimal::ZERO),
                                    remaining_size: Decimal::ZERO,
                                    average_fill_price: Some(Decimal::from_str(&order.avg_px).unwrap_or(Decimal::ZERO)),
                                })
                            },
                            ExchangeDataStatus::Resting(order) => {
                                Ok(OrderResponse {
                                    order_id: order.oid.to_string(),
                                    client_order_id: Some(cloid.to_string()),
                                    status: "resting".to_string(),
                                    filled_size: Decimal::ZERO,
                                    remaining_size: Decimal::ZERO, // Resting order doesn't have size info available
                                    average_fill_price: None,
                                })
                            },
                            _ => {
                                Err(crate::HyperliquidError::ApiError(format!("Unexpected order status: {:?}", status)))
                            }
                        }
                    } else {
                        Err(crate::HyperliquidError::ApiError("No order status in response".to_string()))
                    }
                } else {
                    Err(crate::HyperliquidError::ApiError("No data in exchange response".to_string()))
                }
            },
            ExchangeResponseStatus::Err(e) => {
                Err(crate::HyperliquidError::ApiError(format!("Exchange error: {}", e)))
            }
        }
    }
    
    pub async fn get_positions(&self, address: &str) -> Result<Vec<Position>> {
        let h160_address = address.parse::<H160>()
            .map_err(|_| crate::HyperliquidError::InvalidOrder("Invalid address format".to_string()))?;
        
        let user_state = self.info_client.user_state(h160_address).await
            .map_err(|e| crate::HyperliquidError::SdkError(e.to_string()))?;
        
        let mut positions = Vec::new();
        
        // Convert SDK positions to Position type
        for asset_position in user_state.asset_positions {
            if let Ok(size) = asset_position.position.szi.parse::<f64>() {
                if size.abs() > 0.0 { // Only include non-zero positions
                    let position = Position {
                        asset: asset_position.position.coin.clone(),
                        side: if size > 0.0 { PositionSide::Long } else { PositionSide::Short },
                        size: Decimal::from_f64_retain(size.abs()).unwrap_or(Decimal::ZERO),
                        entry_price: asset_position.position.entry_px
                            .as_ref()
                            .and_then(|s| Decimal::from_str(s).ok())
                            .unwrap_or(Decimal::ZERO),
                        mark_price: Decimal::ZERO, // SDK doesn't seem to have mark_px field
                        unrealized_pnl: Decimal::from_str(&asset_position.position.unrealized_pnl).unwrap_or(Decimal::ZERO),
                        margin: Decimal::from_str(&asset_position.position.margin_used).unwrap_or(Decimal::ZERO),
                        leverage: {
                            let hyperliquid_rust_sdk::Leverage { value, .. } = &asset_position.position.leverage;
                            *value as u8
                        },
                    };
                    positions.push(position);
                }
            }
        }
        
        Ok(positions)
    }
    
    pub async fn cancel_order(&self, order_id: &str) -> Result<()> {
        if let Ok(_cloid) = Uuid::parse_str(order_id) {
            return Err(crate::HyperliquidError::InvalidOrder(
                "For client order ID cancellation, use cancel_order_by_cloid with asset parameter".to_string()
            ));
        }
        
        let oid: u64 = order_id.parse()
            .map_err(|_| crate::HyperliquidError::InvalidOrder("Invalid order ID format".to_string()))?;
        
        let wallet_address = self.wallet.address();
        let open_orders = self.info_client.open_orders(wallet_address).await
            .map_err(|e| crate::HyperliquidError::SdkError(e.to_string()))?;
        
        for order in open_orders {
            if order.oid == oid {
                let cancel_request = ClientCancelRequest {
                    asset: order.coin.clone(),
                    oid,
                };
                
                let response = self.exchange_client.cancel(cancel_request, None).await
                    .map_err(|e| crate::HyperliquidError::SdkError(e.to_string()))?;
                
                return match response {
                    ExchangeResponseStatus::Ok(_) => Ok(()),
                    ExchangeResponseStatus::Err(e) => Err(crate::HyperliquidError::ApiError(
                        format!("Cancel failed: {}", e)
                    )),
                };
            }
        }
        
        Err(crate::HyperliquidError::InvalidOrder(
            "Order with ID not found in open orders".to_string()
        ))
    }
    
    pub async fn cancel_order_by_cloid(&self, asset: &str, cloid: &str) -> Result<()> {
        let uuid_cloid = Uuid::parse_str(cloid)
            .map_err(|_| crate::HyperliquidError::InvalidOrder("Invalid client order ID format".to_string()))?;
        
        let cancel_request = ClientCancelRequestCloid {
            asset: asset.to_string(),
            cloid: uuid_cloid,
        };
        
        let response = self.exchange_client.cancel_by_cloid(cancel_request, None).await
            .map_err(|e| crate::HyperliquidError::SdkError(e.to_string()))?;
        
        match response {
            ExchangeResponseStatus::Ok(_) => Ok(()),
            ExchangeResponseStatus::Err(e) => {
                Err(crate::HyperliquidError::ApiError(format!("Cancel failed: {}", e)))
            }
        }
    }
    
    pub async fn get_account_info(&self, address: &str) -> Result<Account> {
        let h160_address = address.parse::<H160>()
            .map_err(|_| crate::HyperliquidError::InvalidOrder("Invalid address format".to_string()))?;
        
        let user_state = self.info_client.user_state(h160_address).await
            .map_err(|e| crate::HyperliquidError::SdkError(e.to_string()))?;
        
        // Get positions (reuse the logic from get_positions)
        let positions = self.get_positions(address).await?;
        
        // Extract account balance information
        let account_value = Decimal::from_str(&user_state.cross_margin_summary.account_value).unwrap_or(Decimal::ZERO);
        let margin_used = Decimal::from_str(&user_state.cross_margin_summary.total_margin_used).unwrap_or(Decimal::ZERO);
        
        let account = Account {
            address: address.to_string(),
            equity: account_value,
            margin_used,
            available_margin: account_value - margin_used,
            positions,
        };
        
        Ok(account)
    }
    
    pub fn get_wallet_address(&self) -> String {
        format!("0x{:x}", self.wallet.address())
    }
    
    pub async fn get_universe(&self) -> Result<Vec<String>> {
        // Get the current universe (list of available assets)
        let meta = self.info_client.meta().await
            .map_err(|e| crate::HyperliquidError::SdkError(e.to_string()))?;
        
        let assets: Vec<String> = meta.universe.iter()
            .map(|asset| asset.name.clone())
            .collect();
        
        Ok(assets)
    }
    
    pub async fn get_asset_index(&self, asset_name: &str) -> Result<Option<u32>> {
        let meta = self.info_client.meta().await
            .map_err(|e| crate::HyperliquidError::SdkError(e.to_string()))?;
        
        for (index, asset) in meta.universe.iter().enumerate() {
            if asset.name == asset_name {
                return Ok(Some(index as u32));
            }
        }
        
        Ok(None)
    }
    
    pub async fn get_open_orders(&self, address: Option<&str>) -> Result<Vec<OpenOrder>> {
        let wallet_address = if let Some(addr) = address {
            addr.parse::<H160>()
                .map_err(|_| crate::HyperliquidError::InvalidOrder("Invalid address format".to_string()))?
        } else {
            self.wallet.address()
        };
        
        let open_orders = self.info_client.open_orders(wallet_address).await
            .map_err(|e| crate::HyperliquidError::SdkError(e.to_string()))?;
        
        let mut orders = Vec::new();
        for order in open_orders {
            orders.push(OpenOrder {
                order_id: order.oid.to_string(),
                asset: order.coin.clone(),
                side: if order.side == "B" { OrderSide::Buy } else { OrderSide::Sell },
                size: Decimal::from_str(&order.sz).unwrap_or(Decimal::ZERO),
                price: Decimal::from_str(&order.limit_px).unwrap_or(Decimal::ZERO),
                timestamp: order.timestamp,
                reduce_only: false, // Not available in the SDK response
            });
        }
        
        Ok(orders)
    }
    
    pub async fn place_stop_loss(&self, request: StopLossRequest) -> Result<OrderResponse> {
        // Validate asset exists in universe
        if self.get_asset_index(&request.asset).await?.is_none() {
            return Err(crate::HyperliquidError::InvalidOrder(
                format!("Asset '{}' not found in Hyperliquid universe", request.asset)
            ));
        }
        
        // Get current position to determine size and side
        let wallet_address = self.get_wallet_address();
        let positions = self.get_positions(&wallet_address).await?;
        
        let position = positions.iter()
            .find(|p| p.asset == request.asset)
            .ok_or_else(|| crate::HyperliquidError::InvalidOrder(
                format!("No open position found for asset '{}'", request.asset)
            ))?;
        
        // Determine position size if not specified
        let size = request.size.unwrap_or(position.size);
        
        // Determine stop loss side based on current position:
        // Long position -> Stop loss is a sell (to close long)
        // Short position -> Stop loss is a buy (to close short)
        let stop_loss_side = match position.side {
            PositionSide::Long => OrderSide::Sell,
            PositionSide::Short => OrderSide::Buy,
        };
        
        // Generate a client order ID if none provided
        let cloid = request.client_order_id.map(|id| 
            Uuid::parse_str(&id).unwrap_or_else(|_| Uuid::new_v4())
        ).unwrap_or_else(Uuid::new_v4);
        
        // Create stop loss order - this is a trigger order with stop loss type
        let order_request = SimpleOrderRequest {
            asset: request.asset.clone(),
            side: stop_loss_side,
            order_type: OrderType::Trigger {
                trigger: TriggerOrderType {
                    is_market: false, // Stop loss is a trigger order, not immediate market order
                    trigger_px: request.trigger_price.to_string(),
                    tpsl: TpSl::Sl, // Stop Loss type
                },
            },
            size,
            price: Some(request.trigger_price), // Set the trigger price as the order price
            reduce_only: true, // Stop loss is always reduce-only
            client_order_id: Some(cloid.to_string()),
            slippage_tolerance: None, // No slippage for trigger orders
        };
        
        self.place_order(order_request).await
    }
    
    pub async fn modify_stop_loss(&self, request: StopLossModifyRequest) -> Result<OrderResponse> {
        // First cancel the existing stop loss order
        self.cancel_order(&request.order_id).await?;
        
        // Get the current position to determine size
        let wallet_address = self.get_wallet_address();
        let positions = self.get_positions(&wallet_address).await?;
        
        let position = positions.iter()
            .find(|p| p.asset == request.asset)
            .ok_or_else(|| crate::HyperliquidError::InvalidOrder(
                format!("No open position found for asset '{}'", request.asset)
            ))?;
        
        // Place new stop loss with updated trigger price
        let stop_loss_request = StopLossRequest {
            asset: request.asset,
            trigger_price: request.new_trigger_price,
            size: Some(position.size),
            client_order_id: None,
        };
        
        self.place_stop_loss(stop_loss_request).await
    }
    
    pub async fn cancel_stop_loss(&self, _asset: &str, order_id: &str) -> Result<()> {
        // Cancel the stop loss order (same as canceling any order)
        self.cancel_order(order_id).await
    }
    
    pub async fn get_stop_losses(&self, asset: Option<&str>) -> Result<Vec<OpenOrder>> {
        let open_orders = self.get_open_orders(None).await?;
        
        // Get current positions to determine expected stop loss sides
        let wallet_address = self.get_wallet_address();
        let positions = self.get_positions(&wallet_address).await?;
        
        let stop_losses: Vec<OpenOrder> = open_orders.into_iter()
            .filter(|order| {
                // Filter by asset if specified
                if let Some(a) = asset {
                    if order.asset != a {
                        return false;
                    }
                }
                
                // Find the position for this asset
                if let Some(position) = positions.iter().find(|p| p.asset == order.asset) {
                    // Stop loss orders have opposite side to position:
                    // - Long position -> Stop loss is a sell order
                    // - Short position -> Stop loss is a buy order
                    // Note: We can't rely on reduce_only flag as SDK doesn't provide it
                    let expected_stop_loss_side = match position.side {
                        PositionSide::Long => OrderSide::Sell,
                        PositionSide::Short => OrderSide::Buy,
                    };
                    
                    order.side == expected_stop_loss_side
                } else {
                    // No position found for this asset, so it's not a stop loss for our current positions
                    false
                }
            })
            .collect();
        
        Ok(stop_losses)
    }
}