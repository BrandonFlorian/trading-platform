use solana_sdk::program_pack::Pack;

use {
    crate::error::AppError,
    solana_client::rpc_client::RpcClient,
    solana_sdk::{
        compute_budget::ComputeBudgetInstruction,
        instruction::Instruction,
        message::Message,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        system_instruction,
        transaction::Transaction,
    },
    spl_token::state::Account as TokenAccount,
};

use super::{constants::*, types::*, utils::*};
use crate::models::{BuyRequest, BuyResponse};

pub async fn buy(
    rpc_client: &RpcClient,
    secret_keypair: &Keypair,
    pair_address: &str,
    amount_in_sol: f64,
) -> Result<String, AppError> {
    let pool_keys = fetch_pool_keys(rpc_client, pair_address).await?;

    // Convert SOL amount to lamports
    let amount_in = (amount_in_sol * LAMPORTS_PER_SOL as f64) as u64;

    let mint = if pool_keys.base_mint == WSOL {
        pool_keys.quote_mint
    } else {
        pool_keys.base_mint
    };

    let (token_account, token_account_ix) =
        get_token_account(rpc_client, &secret_keypair.pubkey(), &mint).await?;

    // Create WSOL account for the swap
    let wsol_keypair = Keypair::new();
    let wsol_account = wsol_keypair.pubkey();

    let rent = rpc_client.get_minimum_balance_for_rent_exemption(TokenAccount::LEN)?;
    let total_rent = rent + amount_in;

    let mut instructions = vec![
        ComputeBudgetInstruction::set_compute_unit_limit(UNIT_BUDGET),
        ComputeBudgetInstruction::set_compute_unit_price(UNIT_PRICE),
        system_instruction::create_account(
            &secret_keypair.pubkey(),
            &wsol_account,
            total_rent,
            TokenAccount::LEN as u64,
            &spl_token::id(),
        ),
        spl_token::instruction::initialize_account(
            &spl_token::id(),
            &wsol_account,
            &WSOL,
            &secret_keypair.pubkey(),
        )?,
    ];

    if let Some(create_token_account) = token_account_ix {
        instructions.push(create_token_account);
    }

    let swap_ix = make_swap_instruction(
        amount_in,
        wsol_account,
        token_account,
        &pool_keys,
        secret_keypair,
    )?;
    instructions.push(swap_ix);

    instructions.push(spl_token::instruction::close_account(
        &spl_token::id(),
        &wsol_account,
        &secret_keypair.pubkey(),
        &secret_keypair.pubkey(),
        &[],
    )?);

    let recent_blockhash = rpc_client.get_latest_blockhash()?;
    let message = Message::new(&instructions, Some(&secret_keypair.pubkey()));

    // Create signers vector with correct types
    let signers: Vec<&dyn Signer> = vec![secret_keypair, &wsol_keypair];
    let transaction = Transaction::new(&signers, message, recent_blockhash);

    let signature = rpc_client.send_and_confirm_transaction_with_spinner(&transaction)?;
    Ok(signature.to_string())
}

pub async fn process_buy_request(
    rpc_client: &RpcClient,
    server_keypair: &Keypair,
    request: &BuyRequest,
) -> Result<BuyResponse, AppError> {
    let signature = buy(
        rpc_client,
        server_keypair,
        &request.token_address,
        request.sol_quantity,
    )
    .await?;

    Ok(BuyResponse {
        success: true,
        signature: signature.clone(),
        solscan_tx_url: format!("https://solscan.io/tx/{}", signature),
        token_quantity: 0.0, // need to calculate this from the transaction receipt
        sol_spent: request.sol_quantity,
        error: None,
    })
}
