use std::str::FromStr;
use solana_sdk::pubkey::Pubkey;
use solana_client::rpc_client::RpcClient;
use trading_common::pumpdotfun::utils::{derive_creator_vault, get_bonding_curve_data};

#[tokio::main]
async fn main() {
    let rpc_client = RpcClient::new("https://mainnet.helius-rpc.com/?api-key=6996d675-4ed3-4e1d-b06e-8343e7f3a96f".to_string());
    
    // Test all tokens from user's data
    let test_data = vec![
        ("BB8BURYXDuiNiRDAj4i9JsT87ZGMddR4AhAbcrmrpump", "5g3obkuTJCqMLqsy9wBjDGo1r9ts5CCeHUjPmcrdNUjh"),
        ("7qxRENHE78DaPADwp61HxR69HoYnw9JvrzLg1q7Ypump", "DUbdj6h8if8vzxjmQuHBUU2dwNMndwahQqyKrXBRAFsX"),
        ("2j6cFVu7dgeNXmHrkyyvewapCDvAKYBfvdNETAqfpump", "D4jJYzUZHwsHj3X8p4wcZpbC3YroaKRbrLvBk1TSP5GG"),
    ];
    
    for (token_addr, expected_vault) in test_data {
        let mint = Pubkey::from_str(token_addr).unwrap();
        let expected = Pubkey::from_str(expected_vault).unwrap();
        
        println!("=== Testing token: {} ===", token_addr);
        
        // Get bonding curve data
        match get_bonding_curve_data(&rpc_client, &mint).await {
            Ok(bonding_curve_data) => {
                println!("Creator from bonding curve: {}", bonding_curve_data.creator);
                
                // Test our current implementation
                match derive_creator_vault(&rpc_client, &mint).await {
                    Ok(derived_vault) => {
                        println!("Our derived vault: {}", derived_vault);
                        println!("Expected vault: {}", expected_vault);
                        
                        if derived_vault == expected {
                            println!("✅ MATCH: Our implementation works!");
                        } else {
                            println!("❌ NO MATCH: Our implementation is wrong");
                            
                            // Check if the expected vault is the creator
                            if bonding_curve_data.creator == expected {
                                println!("🔍 Expected vault IS the creator address");
                            } else {
                                println!("🔍 Expected vault is NOT the creator address");
                            }
                        }
                    }
                    Err(e) => {
                        println!("Error deriving vault: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("Error getting bonding curve data: {}", e);
            }
        }
        
        println!();
    }
}