use trading_common::dex::DexType;
use trading_common::error::AppError;
use trading_common::models::PriceUpdate;

use super::{PoolMonitorState, VaultPriceUpdate};

/// Handles price calculations from vault balance data
pub struct PriceCalculator;

impl PriceCalculator {
    /// Convert vault price update to standard price update format
    pub fn convert_to_price_update(
        vault_update: VaultPriceUpdate,
        pool_state: &PoolMonitorState,
        sol_price_usd: f64,
    ) -> Result<PriceUpdate, AppError> {
        // Calculate market cap if we have token supply data
        let market_cap = 0.0; // Placeholder - would need to implement token supply fetching

        let price_update = PriceUpdate {
            token_address: vault_update.token_address,
            price_sol: vault_update.price_sol,
            price_usd: vault_update.price_usd,
            market_cap,
            timestamp: vault_update.timestamp,
            dex_type: DexType::Raydium,
            liquidity: Some(vault_update.liquidity_sol),
            liquidity_usd: Some(vault_update.liquidity_sol * sol_price_usd),
            pool_address: Some(pool_state.pool_address.to_string()),
            volume_24h: None, // Would need historical data
            volume_6h: None,
            volume_1h: None,
            volume_5m: None,
        };

        Ok(price_update)
    }

    /// Calculate price from raw vault balances
    pub fn calculate_price_from_raw_balances(
        base_balance: u64,
        quote_balance: u64,
        base_decimals: u8,
        quote_decimals: u8,
    ) -> Result<f64, AppError> {
        if base_balance == 0 {
            return Ok(0.0);
        }

        // Convert to decimal-adjusted amounts
        let base_amount = base_balance as f64 / 10f64.powi(base_decimals as i32);
        let quote_amount = quote_balance as f64 / 10f64.powi(quote_decimals as i32);

        // Price = quote_amount / base_amount (SOL per token)
        let price = quote_amount / base_amount;

        Ok(price)
    }

    /// Calculate liquidity in SOL
    pub fn calculate_liquidity_sol(quote_balance: u64, quote_decimals: u8) -> f64 {
        let quote_amount = quote_balance as f64 / 10f64.powi(quote_decimals as i32);
        // Total liquidity is approximately 2x the quote side
        quote_amount * 2.0
    }

    /// Calculate price impact for a given trade size
    pub fn calculate_price_impact(
        base_balance: u64,
        quote_balance: u64,
        trade_amount_sol: f64,
        base_decimals: u8,
        quote_decimals: u8,
    ) -> Result<f64, AppError> {
        let current_price = Self::calculate_price_from_raw_balances(
            base_balance,
            quote_balance,
            base_decimals,
            quote_decimals,
        )?;

        let quote_amount = quote_balance as f64 / 10f64.powi(quote_decimals as i32);

        // Simplified constant product formula impact calculation
        let new_quote_balance = quote_amount + trade_amount_sol;
        let new_base_balance = (base_balance as f64 * quote_amount) / new_quote_balance;

        let new_price = Self::calculate_price_from_raw_balances(
            new_base_balance as u64,
            (new_quote_balance * 10f64.powi(quote_decimals as i32)) as u64,
            base_decimals,
            quote_decimals,
        )?;

        let price_impact = ((new_price - current_price) / current_price).abs();
        Ok(price_impact)
    }

    /// Calculate market cap (requires token supply data)
    async fn calculate_market_cap(
        _price_sol: f64,
        _sol_price_usd: f64,
        _token_address: &str,
    ) -> Result<f64, AppError> {
        // This would require fetching token supply from the mint account
        // For now, returning 0.0 as placeholder
        // In production, you'd:
        // 1. Cache token supply data
        // 2. Fetch from mint account if not cached
        // 3. Calculate: supply * price_sol * sol_price_usd

        Ok(0.0)
    }

    /// Validate price data for sanity checks
    pub fn validate_price_data(
        price_sol: f64,
        base_balance: u64,
        quote_balance: u64,
    ) -> Result<(), AppError> {
        // Check for reasonable price bounds
        if price_sol < 0.0 {
            return Err(AppError::InvalidPrice("Negative price".to_string()));
        }

        if price_sol > 1000.0 {
            return Err(AppError::InvalidPrice(
                "Unreasonably high price".to_string(),
            ));
        }

        // Check for reasonable liquidity
        if base_balance == 0 || quote_balance == 0 {
            return Err(AppError::InvalidPrice("Zero balance detected".to_string()));
        }

        Ok(())
    }

    /// Calculate volume-weighted average price (VWAP) from multiple updates
    pub fn calculate_vwap(price_updates: &[VaultPriceUpdate]) -> Option<f64> {
        if price_updates.is_empty() {
            return None;
        }

        let total_volume: f64 = price_updates.iter().map(|u| u.liquidity_sol).sum();

        if total_volume == 0.0 {
            return None;
        }

        let weighted_sum: f64 = price_updates
            .iter()
            .map(|u| u.price_sol * u.liquidity_sol)
            .sum();

        Some(weighted_sum / total_volume)
    }

    /// Calculate price change percentage
    pub fn calculate_price_change(old_price: f64, new_price: f64) -> f64 {
        if old_price == 0.0 {
            return 0.0;
        }

        ((new_price - old_price) / old_price) * 100.0
    }

    /// Get optimal trade size for minimal slippage
    pub fn get_optimal_trade_size(
        _base_balance: u64,
        quote_balance: u64,
        max_slippage_percent: f64,
        _base_decimals: u8,
        quote_decimals: u8,
    ) -> Result<f64, AppError> {
        let quote_amount = quote_balance as f64 / 10f64.powi(quote_decimals as i32);

        // Simple approximation: trade size that causes max_slippage_percent impact
        // This is a rough calculation and would need refinement for production
        let optimal_size = quote_amount * (max_slippage_percent / 100.0) * 0.5;

        Ok(optimal_size)
    }
}
