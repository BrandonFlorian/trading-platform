use std::collections::HashMap;
use once_cell::sync::Lazy;

// Common perpetual assets and their indices
// This should be updated based on the actual Hyperliquid universe
pub static ASSET_INDICES: Lazy<HashMap<&'static str, u32>> = Lazy::new(|| {
    let mut m = HashMap::new();
    // Major perpetuals - placeholders for actual indices
    m.insert("BTC", 0);
    m.insert("ETH", 1);
    m.insert("SOL", 2);
    m.insert("MATIC", 3);
    m.insert("ARB", 4);
    m.insert("OP", 5);
    m.insert("AVAX", 6);
    m.insert("BNB", 7);
    m.insert("DOGE", 8);
    m.insert("LINK", 9);
    m.insert("ATOM", 10);
    m.insert("DOT", 11);
    m.insert("UNI", 12);
    m.insert("CRV", 13);
    m.insert("LDO", 14);
    m.insert("SUI", 15);
    m.insert("APT", 16);
    m.insert("INJ", 17);
    m.insert("BLUR", 18);
    m.insert("XRP", 19);
    m.insert("AAVE", 20);
    m.insert("COMP", 21);
    m.insert("MKR", 22);
    m.insert("WLD", 23);
    m.insert("SEI", 24);
    m.insert("TIA", 25);
    // Add more as needed
    m
});

pub fn get_asset_index(symbol: &str) -> Option<u32> {
    ASSET_INDICES.get(symbol.to_uppercase().as_str()).copied()
}

pub fn is_spot_asset(symbol: &str) -> bool {
    // Spot assets have indices starting at 10000
    symbol.contains('/') || symbol.contains('-')
}

pub fn get_spot_asset_index(symbol: &str) -> Option<u32> {
    // For spot assets, need the actual spot universe indices
    // This is a placeholder implementation
    if is_spot_asset(symbol) {
        // Extract base asset and find its index
        let parts: Vec<&str> = symbol.split('/').collect();
        if parts.len() == 2 {
            // Return 10000 + index for spot assets
            // This needs to be implemented based on actual Hyperliquid spot universe
            Some(10000)
        } else {
            None
        }
    } else {
        None
    }
}