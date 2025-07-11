use crate::models::CopyTradeSettings;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use validator::ValidationError;

/// Validates Solana address format and structure
pub fn validate_solana_address(address: &str) -> Result<(), ValidationError> {
    match Pubkey::from_str(address) {
        Ok(_) => Ok(()),
        Err(_) => Err(ValidationError::new("invalid_solana_address")),
    }
}

/// Validates that SOL amount can be safely used in calculations
pub fn validate_sol_amount_safe(amount: f64) -> Result<(), ValidationError> {
    if amount <= 0.0 {
        return Err(ValidationError::new("amount_must_be_positive"));
    }

    if amount < 0.0001 {
        return Err(ValidationError::new("amount_too_small"));
    }

    if amount > 1000.0 {
        return Err(ValidationError::new("amount_too_large"));
    }

    if !amount.is_finite() {
        return Err(ValidationError::new("amount_not_finite"));
    }

    Ok(())
}

/// Validates slippage percentage is in valid range (0.0 to 1.0)
pub fn validate_slippage_percentage(slippage: f64) -> Result<(), ValidationError> {
    if slippage < 0.0 || slippage > 1.0 {
        return Err(ValidationError::new("slippage_out_of_range"));
    }

    if !slippage.is_finite() {
        return Err(ValidationError::new("slippage_not_finite"));
    }

    Ok(())
}

/// Validates token quantity for sell operations
pub fn validate_token_quantity(quantity: f64) -> Result<(), ValidationError> {
    if quantity <= 0.0 {
        return Err(ValidationError::new("quantity_must_be_positive"));
    }

    if quantity < 0.000001 {
        return Err(ValidationError::new("quantity_too_small"));
    }

    if !quantity.is_finite() {
        return Err(ValidationError::new("quantity_not_finite"));
    }

    Ok(())
}

/// Validates list of token addresses
pub fn validate_token_addresses_list(
    addresses: &Option<Vec<String>>,
) -> Result<(), ValidationError> {
    if let Some(addr_list) = addresses {
        if addr_list.len() > 50 {
            return Err(ValidationError::new("too_many_tokens"));
        }

        for address in addr_list {
            validate_solana_address(address)?;
        }
    }
    Ok(())
}

/// Validates that division won't result in zero or infinity
pub fn validate_safe_division(numerator: f64, denominator: f64) -> Result<f64, ValidationError> {
    if denominator == 0.0 || !denominator.is_finite() {
        return Err(ValidationError::new("division_by_zero"));
    }

    let result = numerator / denominator;

    if !result.is_finite() {
        return Err(ValidationError::new("division_result_not_finite"));
    }

    Ok(result)
}

/// Validates min SOL balance requirements
pub fn validate_min_sol_balance(balance: f64) -> Result<(), ValidationError> {
    if balance < 0.001 {
        return Err(ValidationError::new("min_balance_too_low"));
    }

    if balance > 10.0 {
        return Err(ValidationError::new("min_balance_too_high"));
    }

    if !balance.is_finite() {
        return Err(ValidationError::new("balance_not_finite"));
    }

    Ok(())
}

/// Validates max open positions
pub fn validate_max_positions(positions: i32) -> Result<(), ValidationError> {
    if positions < 1 {
        return Err(ValidationError::new("positions_too_low"));
    }

    if positions > 10 {
        return Err(ValidationError::new("positions_too_high"));
    }

    Ok(())
}

/// Business rule validation for copy trade settings
pub fn validate_copy_trade_business_rules(
    settings: &CopyTradeSettings,
) -> Result<(), ValidationError> {
    // Trade amount should be reasonable relative to min balance
    if settings.trade_amount_sol < settings.min_sol_balance {
        return Err(ValidationError::new("trade_amount_less_than_min_balance"));
    }

    // If using allowed tokens list, it should not be empty
    if settings.use_allowed_tokens_list {
        if let Some(tokens) = &settings.allowed_tokens {
            if tokens.is_empty() {
                return Err(ValidationError::new("allowed_tokens_list_empty"));
            }
        } else {
            return Err(ValidationError::new("allowed_tokens_list_required"));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sol_amount_validation() {
        // Valid amounts
        assert!(validate_sol_amount_safe(0.1).is_ok());
        assert!(validate_sol_amount_safe(10.0).is_ok());
        assert!(validate_sol_amount_safe(0.0001).is_ok()); // Minimum valid
        assert!(validate_sol_amount_safe(1000.0).is_ok()); // Maximum valid

        // Invalid amounts
        assert!(validate_sol_amount_safe(-1.0).is_err());
        assert!(validate_sol_amount_safe(0.0).is_err());
        assert!(validate_sol_amount_safe(0.00001).is_err()); // Too small
        assert!(validate_sol_amount_safe(1001.0).is_err()); // Too large
        assert!(validate_sol_amount_safe(f64::INFINITY).is_err());
        assert!(validate_sol_amount_safe(f64::NAN).is_err());
    }

    #[test]
    fn test_slippage_validation() {
        // Valid slippage (0% to 100%)
        assert!(validate_slippage_percentage(0.0).is_ok()); // 0%
        assert!(validate_slippage_percentage(0.05).is_ok()); // 5%
        assert!(validate_slippage_percentage(0.5).is_ok()); // 50%
        assert!(validate_slippage_percentage(1.0).is_ok()); // 100%

        // Invalid slippage
        assert!(validate_slippage_percentage(1.5).is_err()); // 150% - invalid
        assert!(validate_slippage_percentage(-0.1).is_err()); // Negative - invalid
        assert!(validate_slippage_percentage(f64::INFINITY).is_err());
        assert!(validate_slippage_percentage(f64::NAN).is_err());
    }

    #[test]
    fn test_token_quantity_validation() {
        // Valid quantities
        assert!(validate_token_quantity(1.0).is_ok());
        assert!(validate_token_quantity(0.000001).is_ok()); // Minimum valid

        // Invalid quantities
        assert!(validate_token_quantity(0.0).is_err());
        assert!(validate_token_quantity(-1.0).is_err());
        assert!(validate_token_quantity(0.0000001).is_err()); // Too small
        assert!(validate_token_quantity(f64::INFINITY).is_err());
        assert!(validate_token_quantity(f64::NAN).is_err());
    }

    #[test]
    fn test_solana_address_validation() {
        // Valid Solana addresses
        assert!(validate_solana_address("11111111111111111111111111111112").is_ok()); // System program
        assert!(validate_solana_address("So11111111111111111111111111111111111111112").is_ok()); // SOL mint

        // Invalid addresses
        assert!(validate_solana_address("invalid").is_err());
        assert!(validate_solana_address("").is_err());
        assert!(validate_solana_address("123").is_err());
        assert!(validate_solana_address("gggggggggggggggggggggggggggggggggggggggggggg").is_err());
        // Invalid base58
    }

    #[test]
    fn test_safe_division() {
        // Valid divisions
        assert_eq!(validate_safe_division(10.0, 2.0).unwrap(), 5.0);
        assert_eq!(validate_safe_division(1.0, 3.0).unwrap(), 1.0 / 3.0);

        // Invalid divisions
        assert!(validate_safe_division(10.0, 0.0).is_err()); // Division by zero
        assert!(validate_safe_division(f64::INFINITY, 1.0).is_err()); // Infinite numerator
        assert!(validate_safe_division(1.0, f64::NAN).is_err()); // NaN denominator
    }

    #[test]
    fn test_position_limits() {
        // Valid positions
        assert!(validate_max_positions(1).is_ok());
        assert!(validate_max_positions(5).is_ok());
        assert!(validate_max_positions(10).is_ok());

        // Invalid positions
        assert!(validate_max_positions(0).is_err());
        assert!(validate_max_positions(11).is_err());
        assert!(validate_max_positions(-1).is_err());
    }
}
