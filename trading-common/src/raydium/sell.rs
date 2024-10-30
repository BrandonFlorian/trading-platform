use std::str::FromStr;

use crate::models::{SellRequest, SellResponse};

use {
    crate::error::AppError,
    solana_client::rpc_client::RpcClient,
    solana_sdk::{
        compute_budget::ComputeBudgetInstruction, message::Message, pubkey::Pubkey,
        signature::Keypair, signer::Signer, transaction::Transaction,
    },
};

use super::{constants::*, types::*, utils::*};

pub async fn sell(
    rpc_client: &RpcClient,
    secret_keypair: &impl Signer,
    pair_address: &str,
    amount_in_tokens: u64,
) -> Result<String, AppError> {
    let pool_keys = fetch_pool_keys(rpc_client, pair_address).await?;

    // Determine which token we're selling (base or quote)
    let token_mint = if pool_keys.base_mint == WSOL {
        pool_keys.quote_mint
    } else {
        pool_keys.base_mint
    };

    // Get token account for the input token (the token we're selling)
    let token_account = rpc_client
        .get_token_accounts_by_owner(
            &secret_keypair.pubkey(),
            solana_client::rpc_request::TokenAccountsFilter::Mint(token_mint),
        )?
        .iter()
        .next()
        .ok_or_else(|| AppError::BadRequest("No token account found".to_string()))?
        .pubkey
        .clone();

    // Verify token balance
    let token_balance =
        get_token_balance(rpc_client, &Pubkey::from_str(&token_account).unwrap()).await?;
    if token_balance < amount_in_tokens as f64 {
        return Err(AppError::BadRequest(
            "Insufficient token balance".to_string(),
        ));
    }

    // Create WSOL account for receiving SOL
    let (wsol_account, wsol_account_ix) =
        get_token_account(rpc_client, &secret_keypair.pubkey(), &WSOL).await?;

    let mut instructions = vec![
        // Set compute unit limit
        ComputeBudgetInstruction::set_compute_unit_limit(UNIT_BUDGET),
        // Set compute unit price
        ComputeBudgetInstruction::set_compute_unit_price(UNIT_PRICE),
    ];

    // Add WSOL account creation instruction if needed
    if let Some(create_wsol_account) = wsol_account_ix {
        instructions.push(create_wsol_account);
    }

    // Add swap instruction
    let swap_ix = make_swap_instruction(
        amount_in_tokens,
        Pubkey::from_str(&token_account).unwrap(),
        wsol_account,
        &pool_keys,
        secret_keypair,
    )?;
    instructions.push(swap_ix);

    // Add close WSOL account instruction to convert back to native SOL
    instructions.push(spl_token::instruction::close_account(
        &spl_token::id(),
        &wsol_account,
        &secret_keypair.pubkey(),
        &secret_keypair.pubkey(),
        &[],
    )?);

    // Create and send transaction
    let recent_blockhash = rpc_client.get_latest_blockhash()?;
    let message = Message::new(&instructions, Some(&secret_keypair.pubkey()));
    let transaction = Transaction::new(&[secret_keypair], message, recent_blockhash);

    let signature = rpc_client.send_and_confirm_transaction_with_spinner(&transaction)?;

    Ok(signature.to_string())
}

pub async fn process_sell_request(
    rpc_client: &RpcClient,
    server_keypair: &Keypair,
    request: &SellRequest,
) -> Result<SellResponse, AppError> {
    // Convert token amount to raw amount (considering decimals)
    let token_amount = (request.token_quantity * 1_000_000.0) as u64; // Assuming 6 decimals

    let signature = sell(
        rpc_client,
        server_keypair,
        &request.token_address,
        token_amount,
    )
    .await?;

    // Calculate expected SOL output
    let pool_keys = fetch_pool_keys(rpc_client, &request.token_address).await?;
    let (virtual_token_reserves, virtual_sol_reserves) =
        get_pool_reserves(rpc_client, &pool_keys).await?;

    let sol_received = (token_amount as f64 * virtual_sol_reserves as f64)
        / (virtual_token_reserves as f64 * LAMPORTS_PER_SOL as f64);

    Ok(SellResponse {
        success: true,
        signature: signature.clone(),
        token_quantity: request.token_quantity,
        sol_received,
        solscan_tx_url: format!("https://solscan.io/tx/{}", signature),
        error: None,
    })
}
