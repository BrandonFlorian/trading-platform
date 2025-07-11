use std::str::FromStr;
use solana_sdk::pubkey::Pubkey;
use solana_client::rpc_client::RpcClient;
use trading_common::pumpdotfun::utils::derive_creator_vault;

#[tokio::main]
async fn main() {
    let rpc_client = RpcClient::new("https://mainnet.helius-rpc.com/?api-key=6996d675-4ed3-4e1d-b06e-8343e7f3a96f".to_string());
    let mint = Pubkey::from_str("BB8BURYXDuiNiRDAj4i9JsT87ZGMddR4AhAbcrmrpump").unwrap();
    
    println!("Testing the fixed derive_creator_vault function...");
    
    match derive_creator_vault(&rpc_client, &mint).await {
        Ok(creator_vault) => {
            println!("Derived creator vault: {}", creator_vault);
            
            // Check if this matches one of the expected addresses
            let expected_addresses = [
                "5g3obkuTJCqMLqsy9wBjDGo1r9ts5CCeHUjPmcrdNUjh", // User's test data
                "9iT8LhVvgJAy3VwuoFQmH1hbg5v68k2wHukDacEfXrKH", // This should be the creator address
                "3gMS5nByBi6eXgJqbvSk8o22tJvTxbzcDDb4bCvoDPaF", // Hardcoded value from audit
            ];
            
            for addr in expected_addresses {
                let expected = Pubkey::from_str(addr).unwrap();
                if creator_vault == expected {
                    println!("*** MATCH FOUND: {} ***", addr);
                    break;
                }
            }
        }
        Err(e) => {
            println!("Error: {}", e);
        }
    }
}