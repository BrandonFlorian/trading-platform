use solana_client::rpc_client::RpcClient;
use solana_program::program_pack::Pack;
use solana_sdk::pubkey::Pubkey;
use spl_token::state::Account as TokenAccount;
use std::str::FromStr;
use trading_common::pumpdotfun::utils::get_bonding_curve_data;

#[tokio::main]
async fn main() {
    let rpc_client = RpcClient::new(
        "https://mainnet.helius-rpc.com/?api-key=6996d675-4ed3-4e1d-b06e-8343e7f3a96f".to_string(),
    );

    // Test data from user
    let test_data = vec![
        (
            "BB8BURYXDuiNiRDAj4i9JsT87ZGMddR4AhAbcrmrpump",
            "5g3obkuTJCqMLqsy9wBjDGo1r9ts5CCeHUjPmcrdNUjh",
        ),
        (
            "7qxRENHE78DaPADwp61HxR69HoYnw9JvrzLg1q7Ypump",
            "DUbdj6h8if8vzxjmQuHBUU2dwNMndwahQqyKrXBRAFsX",
        ),
        (
            "2j6cFVu7dgeNXmHrkyyvewapCDvAKYBfvdNETAqfpump",
            "D4jJYzUZHwsHj3X8p4wcZpbC3YroaKRbrLvBk1TSP5GG",
        ),
    ];

    println!("=== INVESTIGATING ACTUAL VAULT ADDRESSES ===\n");

    for (token_addr, vault_addr) in test_data {
        let mint = Pubkey::from_str(token_addr).unwrap();
        let vault = Pubkey::from_str(vault_addr).unwrap();

        println!("Token: {}", token_addr);
        println!("Vault: {}", vault_addr);

        // Check what the vault account actually is
        match rpc_client.get_account(&vault) {
            Ok(account) => {
                println!("Vault account found!");
                println!("  Owner: {}", account.owner);
                println!("  Lamports: {}", account.lamports);
                println!("  Data length: {}", account.data.len());

                // Check if it's a token account
                if account.owner == spl_token::id() && account.data.len() == 165 {
                    println!("  This is a TOKEN ACCOUNT!");

                    // Parse token account data
                    if let Ok(token_account) = TokenAccount::unpack(&account.data) {
                        println!("  Token account mint: {}", token_account.mint);
                        println!("  Token account owner: {}", token_account.owner);
                        println!("  Token account amount: {}", token_account.amount);

                        // Check if this is the creator's token account for this mint
                        if token_account.mint == mint {
                            println!(
                                "  *** THIS IS THE CREATOR'S TOKEN ACCOUNT FOR THIS MINT! ***"
                            );

                            // Now find the creator and see if this is their associated token account
                            match get_bonding_curve_data(&rpc_client, &mint).await {
                                Ok(bonding_curve_data) => {
                                    println!("  Creator: {}", bonding_curve_data.creator);

                                    if token_account.owner == bonding_curve_data.creator {
                                        println!("  *** CONFIRMED: This is the creator's token account! ***");

                                        // Check if it's the associated token account
                                        let ata = spl_associated_token_account::get_associated_token_address(&bonding_curve_data.creator, &mint);
                                        if ata == vault {
                                            println!(
                                                "  *** AND IT'S THE ASSOCIATED TOKEN ACCOUNT! ***"
                                            );
                                        } else {
                                            println!("  But it's NOT the associated token account");
                                            println!("  Expected ATA: {}", ata);
                                        }
                                    }
                                }
                                Err(e) => {
                                    println!("  Error getting bonding curve data: {}", e);
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                println!("Error getting vault account: {}", e);
            }
        }

        println!("\n{}\n", "-".repeat(80));
    }
}
