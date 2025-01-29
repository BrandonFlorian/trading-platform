use chrono::Utc;
use std::collections::HashMap;
use trading_common::models::PriceUpdate;

pub struct PriceCache {
    prices: HashMap<String, PriceUpdate>,
}

impl PriceCache {
    pub fn new() -> Self {
        Self {
            prices: HashMap::new(),
        }
    }

    pub fn update(&mut self, price_update: PriceUpdate) {
        self.prices
            .insert(price_update.token_address.clone(), price_update);
    }

    pub fn get_price(&self, token_address: &str) -> Option<PriceUpdate> {
        self.prices.get(token_address).cloned()
    }

    pub fn get_all_prices(&self) -> Vec<PriceUpdate> {
        self.prices.values().cloned().collect()
    }
}

impl Default for PriceCache {
    fn default() -> Self {
        Self::new()
    }
}
