use hyperliquid_common::{
    generated::wallet::{
        wallet_service_server::WalletService,
        GetWalletAddressRequest, GetWalletAddressResponse,
        PlaceOrderRequest, PlaceOrderResponse,
        CancelOrderRequest, CancelOrderResponse,
        GetPositionsRequest, GetPositionsResponse,
        GetAccountInfoRequest, GetAccountInfoResponse,
        GetUniverseRequest, GetUniverseResponse,
        OrderInfo, Position as ProtoPosition, Account as ProtoAccount,
    },
    HyperliquidClient, SimpleOrderRequest, OrderSide, OrderType, TimeInForce,
    LimitOrderType, TriggerOrderType, TpSl,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::{str::FromStr, sync::Arc};
use tonic::{Request, Response, Status};

pub struct HyperliquidWalletService {
    client: Arc<HyperliquidClient>,
}

impl HyperliquidWalletService {
    pub fn new(client: Arc<HyperliquidClient>) -> Self {
        Self { client }
    }
}

#[tonic::async_trait]
impl WalletService for HyperliquidWalletService {
    async fn get_wallet_address(
        &self,
        _request: Request<GetWalletAddressRequest>,
    ) -> Result<Response<GetWalletAddressResponse>, Status> {
        let address = self.client.get_wallet_address();
        
        Ok(Response::new(GetWalletAddressResponse { address }))
    }

    async fn place_order(
        &self,
        request: Request<PlaceOrderRequest>,
    ) -> Result<Response<PlaceOrderResponse>, Status> {
        let req = request.into_inner();
        
        // Convert side
        let side = match req.side.to_lowercase().as_str() {
            "buy" | "long" => OrderSide::Buy,
            "sell" | "short" => OrderSide::Sell,
            _ => return Ok(Response::new(PlaceOrderResponse {
                success: false,
                order: None,
                error: Some("Invalid side, must be 'buy', 'sell', 'long', or 'short'".to_string()),
            })),
        };

        // Convert order type
        let order_type = match req.order_type.to_lowercase().as_str() {
            "market" => OrderType::Trigger {
                trigger: TriggerOrderType {
                    is_market: true,
                    trigger_px: "0".to_string(),
                    tpsl: TpSl::Tp,
                },
            },
            "limit" => {
                let tif = match req.time_in_force.as_deref() {
                    Some("IOC") => TimeInForce::Ioc,
                    Some("ALO") => TimeInForce::Alo,
                    _ => TimeInForce::Gtc,
                };
                OrderType::Limit {
                    limit: LimitOrderType { tif },
                }
            },
            _ => return Ok(Response::new(PlaceOrderResponse {
                success: false,
                order: None,
                error: Some("Invalid order type, must be 'market' or 'limit'".to_string()),
            })),
        };

        let order_request = SimpleOrderRequest {
            asset: req.asset,
            side,
            order_type,
            size: Decimal::from_f64_retain(req.size).unwrap_or(Decimal::ZERO),
            price: req.price.map(|p| Decimal::from_f64_retain(p).unwrap_or(Decimal::ZERO)),
            reduce_only: req.reduce_only.unwrap_or(false),
            client_order_id: req.client_order_id,
            slippage_tolerance: req.slippage_tolerance.map(|s| Decimal::from_f64_retain(s).unwrap_or(Decimal::ZERO)),
        };

        match self.client.place_order(order_request).await {
            Ok(response) => {
                let order_info = OrderInfo {
                    order_id: response.order_id,
                    client_order_id: response.client_order_id,
                    status: response.status,
                    filled_size: response.filled_size.to_f64().unwrap_or(0.0),
                    remaining_size: response.remaining_size.to_f64().unwrap_or(0.0),
                    average_fill_price: response.average_fill_price.map(|p| p.to_f64().unwrap_or(0.0)),
                };

                Ok(Response::new(PlaceOrderResponse {
                    success: true,
                    order: Some(order_info),
                    error: None,
                }))
            },
            Err(e) => Ok(Response::new(PlaceOrderResponse {
                success: false,
                order: None,
                error: Some(e.to_string()),
            })),
        }
    }

    async fn cancel_order(
        &self,
        request: Request<CancelOrderRequest>,
    ) -> Result<Response<CancelOrderResponse>, Status> {
        let req = request.into_inner();
        
        let result = if let Some(asset) = req.asset {
            // Try cancel by client order ID
            self.client.cancel_order_by_cloid(&asset, &req.order_id).await
        } else {
            // Try cancel by order ID
            self.client.cancel_order(&req.order_id).await
        };

        match result {
            Ok(()) => Ok(Response::new(CancelOrderResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(CancelOrderResponse {
                success: false,
                error: Some(e.to_string()),
            })),
        }
    }

    async fn get_positions(
        &self,
        request: Request<GetPositionsRequest>,
    ) -> Result<Response<GetPositionsResponse>, Status> {
        let req = request.into_inner();
        let address = req.address.unwrap_or_else(|| self.client.get_wallet_address());

        match self.client.get_positions(&address).await {
            Ok(positions) => {
                let proto_positions: Vec<ProtoPosition> = positions
                    .into_iter()
                    .map(|pos| ProtoPosition {
                        asset: pos.asset,
                        side: match pos.side {
                            hyperliquid_common::PositionSide::Long => "long".to_string(),
                            hyperliquid_common::PositionSide::Short => "short".to_string(),
                        },
                        size: pos.size.to_f64().unwrap_or(0.0),
                        entry_price: pos.entry_price.to_f64().unwrap_or(0.0),
                        mark_price: pos.mark_price.to_f64().unwrap_or(0.0),
                        unrealized_pnl: pos.unrealized_pnl.to_f64().unwrap_or(0.0),
                        margin: pos.margin.to_f64().unwrap_or(0.0),
                        leverage: pos.leverage as u32,
                    })
                    .collect();

                Ok(Response::new(GetPositionsResponse {
                    success: true,
                    positions: proto_positions,
                    error: None,
                }))
            },
            Err(e) => Ok(Response::new(GetPositionsResponse {
                success: false,
                positions: vec![],
                error: Some(e.to_string()),
            })),
        }
    }

    async fn get_account_info(
        &self,
        request: Request<GetAccountInfoRequest>,
    ) -> Result<Response<GetAccountInfoResponse>, Status> {
        let req = request.into_inner();
        let address = req.address.unwrap_or_else(|| self.client.get_wallet_address());

        match self.client.get_account_info(&address).await {
            Ok(account) => {
                let proto_positions: Vec<ProtoPosition> = account.positions
                    .into_iter()
                    .map(|pos| ProtoPosition {
                        asset: pos.asset,
                        side: match pos.side {
                            hyperliquid_common::PositionSide::Long => "long".to_string(),
                            hyperliquid_common::PositionSide::Short => "short".to_string(),
                        },
                        size: pos.size.to_f64().unwrap_or(0.0),
                        entry_price: pos.entry_price.to_f64().unwrap_or(0.0),
                        mark_price: pos.mark_price.to_f64().unwrap_or(0.0),
                        unrealized_pnl: pos.unrealized_pnl.to_f64().unwrap_or(0.0),
                        margin: pos.margin.to_f64().unwrap_or(0.0),
                        leverage: pos.leverage as u32,
                    })
                    .collect();

                let proto_account = ProtoAccount {
                    address: account.address,
                    equity: account.equity.to_f64().unwrap_or(0.0),
                    margin_used: account.margin_used.to_f64().unwrap_or(0.0),
                    available_margin: account.available_margin.to_f64().unwrap_or(0.0),
                    positions: proto_positions,
                };

                Ok(Response::new(GetAccountInfoResponse {
                    success: true,
                    account: Some(proto_account),
                    error: None,
                }))
            },
            Err(e) => Ok(Response::new(GetAccountInfoResponse {
                success: false,
                account: None,
                error: Some(e.to_string()),
            })),
        }
    }

    async fn get_universe(
        &self,
        _request: Request<GetUniverseRequest>,
    ) -> Result<Response<GetUniverseResponse>, Status> {
        match self.client.get_universe().await {
            Ok(assets) => Ok(Response::new(GetUniverseResponse {
                success: true,
                assets,
                error: None,
            })),
            Err(e) => Ok(Response::new(GetUniverseResponse {
                success: false,
                assets: vec![],
                error: Some(e.to_string()),
            })),
        }
    }
}