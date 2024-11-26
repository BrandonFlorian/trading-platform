use crate::TransactionType;
use anyhow::Result;
use solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta;

pub fn extract_transaction_details(
    transaction: &EncodedConfirmedTransactionWithStatusMeta,
) -> Result<(TransactionType, String, f64, f64, f64)> {
    let meta = transaction
        .transaction
        .meta
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No transaction metadata"))?;

    // Get transaction type from logs
    let empty_logs = Vec::new();
    let logs = meta.log_messages.as_ref().unwrap_or(&empty_logs);

    let transaction_type = if logs.iter().any(|log| log.contains("Instruction: Buy")) {
        TransactionType::Buy
    } else if logs.iter().any(|log| log.contains("Instruction: Sell")) {
        TransactionType::Sell
    } else {
        TransactionType::Unknown
    };

    // Extract amounts
    let empty_token_balances = Vec::new();
    let pre_balances = meta
        .pre_token_balances
        .as_ref()
        .unwrap_or(&empty_token_balances);
    let post_balances = meta
        .post_token_balances
        .as_ref()
        .unwrap_or(&empty_token_balances);

    let token_address = post_balances
        .first()
        .map(|b| b.mint.clone())
        .ok_or_else(|| anyhow::anyhow!("No token balance information"))?;

    let pre_amount = pre_balances
        .first()
        .and_then(|b| b.ui_token_amount.ui_amount)
        .unwrap_or(0.0);
    let post_amount = post_balances
        .first()
        .and_then(|b| b.ui_token_amount.ui_amount)
        .unwrap_or(0.0);
    let amount_token = (post_amount - pre_amount).abs();

    let pre_sol = meta.pre_balances.first().copied().unwrap_or(0);
    let post_sol = meta.post_balances.first().copied().unwrap_or(0);
    let amount_sol = ((post_sol as i64 - pre_sol as i64).abs() as f64) / 1e9;

    let price_per_token = if amount_token > 0.0 {
        amount_sol / amount_token
    } else {
        0.0
    };

    Ok((
        transaction_type,
        token_address,
        amount_token,
        amount_sol,
        price_per_token,
    ))
}

pub fn extract_accounts(
    transaction: &EncodedConfirmedTransactionWithStatusMeta,
    transaction_type: &TransactionType,
) -> Result<(String, String)> {
    match &transaction.transaction.transaction {
        solana_transaction_status::EncodedTransaction::Json(tx) => {
            let account_keys = crate::data::get_account_keys_from_message(&tx.message);

            let (seller, buyer) = match transaction_type {
                TransactionType::Sell => (
                    account_keys.first().cloned().unwrap_or_default(),
                    account_keys.get(1).cloned().unwrap_or_default(),
                ),
                TransactionType::Buy => (
                    account_keys.get(1).cloned().unwrap_or_default(),
                    account_keys.first().cloned().unwrap_or_default(),
                ),
                _ => (String::new(), String::new()),
            };

            Ok((seller, buyer))
        }
        _ => Ok((String::new(), String::new())),
    }
}
