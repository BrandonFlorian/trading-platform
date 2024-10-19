use solana_sdk::{instruction::Instruction, signer::Signer, transaction::Transaction};

pub async fn create_buy_transaction(
    rpc_client: &RpcClient,
    payer: &dyn Signer,
    token_address: &Pubkey,
    amount_sol: f64,
    slippage: f64,
) -> Result<Transaction, Box<dyn std::error::Error>> {
}

pub async fn create_sell_transaction(
    rpc_client: &RpcClient,
    payer: &dyn Signer,
    token_address: &Pubkey,
    amount_tokens: f64,
    slippage: f64,
) -> Result<Transaction, Box<dyn std::error::Error>> {
}

pub async fn send_and_confirm_transaction(
    rpc_client: &RpcClient,
    transaction: Transaction,
) -> Result<String, Box<dyn std::error::Error>> {
}
