use postgrest::Postgrest;
use serde_json::json;
use uuid::Uuid;

use crate::{
    error::AppError,
    models::{CopyTradeSettings, TrackedWallet, TransactionLog, User},
};
use anyhow::{Context, Result};

#[derive(Clone)]
pub struct SupabaseClient {
    client: Postgrest,
    user_id: String,
}

impl SupabaseClient {
    pub fn new(url: &str, _api_key: &str, service_role_key: &str, user_id: &str) -> Self {
        println!("New Postgrest client created!");
        let client = Postgrest::new(url)
            .insert_header("apikey", service_role_key)
            .insert_header("Authorization", format!("Bearer {}", service_role_key));

        Self {
            client,
            user_id: user_id.to_string(),
        }
    }

    pub async fn user_exists(&self, user_id: &str) -> Result<bool, AppError> {
        println!("Checking if user exists: {}", user_id);

        let result = self
            .client
            .from("users")
            .select("*")
            .eq("wallet_address", user_id)
            .execute()
            .await
            .map_err(|e| AppError::PostgrestError(e.to_string()))?;

        let body = result
            .text()
            .await
            .map_err(|e| AppError::RequestError(e.to_string()))?;

        println!("User exists check response body: {}", body);

        // Parse body as JSON array and check if it's not empty
        let users: Vec<serde_json::Value> = serde_json::from_str(&body).map_err(|e| {
            AppError::JsonParseError(format!("Failed to parse user response: {}", e))
        })?;

        Ok(!users.is_empty()) // Return true only if we got results
    }

    pub async fn create_user(&self, user_id: &str) -> Result<Uuid, AppError> {
        println!("Attempting to create user with wallet address: {}", user_id);

        let insert_data = json!({"wallet_address": user_id});
        println!("Insert data: {}", insert_data);

        let resp = self
            .client
            .from("users")
            .insert(insert_data.to_string())
            .execute()
            .await
            .map_err(|e| AppError::PostgrestError(e.to_string()))?;

        let status = resp.status();
        println!("Create user response status: {}", status);

        let body = resp
            .text()
            .await
            .map_err(|e| AppError::RequestError(e.to_string()))?;
        println!("Create user response body: {}", body);

        if status != 201 && status != 200 {
            return Err(AppError::DatabaseError(format!(
                "Failed to create user. Status: {}, Body: {}",
                status, body
            )));
        }

        let inserted: Vec<User> = serde_json::from_str(&body).map_err(|e| {
            AppError::JsonParseError(format!("Failed to parse user response: {}", e))
        })?;

        let first_user = inserted
            .first()
            .ok_or_else(|| AppError::DatabaseError("No user was inserted".to_string()))?;

        first_user
            .id
            .ok_or_else(|| AppError::DatabaseError("Inserted user has no ID".to_string()))
    }

    pub async fn get_tracked_wallets(&self) -> Result<Vec<TrackedWallet>, AppError> {
        let resp = self
            .client
            .from("tracked_wallets")
            .select("*")
            .eq("user_id", &self.user_id)
            .execute()
            .await
            .map_err(|e| AppError::PostgrestError(e.to_string()))?;

        let body = resp
            .text()
            .await
            .map_err(|e| AppError::RequestError(e.to_string()))?;

        let wallets: Vec<TrackedWallet> = serde_json::from_str(&body)
            .map_err(|e| AppError::JsonParseError(format!("Failed to parse wallets: {}", e)))?;

        if wallets.is_empty() {
            println!("No wallets found");
        } else {
            println!("Found wallets: {:?}", wallets);
        }

        Ok(wallets)
    }

    pub async fn add_tracked_wallet(&self, mut wallet: TrackedWallet) -> Result<Uuid, AppError> {
        wallet.user_id = Some(self.user_id.clone());

        let insert_data = serde_json::json!({
            "user_id": wallet.user_id,
            "wallet_address": wallet.wallet_address,
            "is_active": wallet.is_active
        });

        let resp = self
            .client
            .from("tracked_wallets")
            .insert(insert_data.to_string())
            .execute()
            .await
            .map_err(|e| AppError::PostgrestError(e.to_string()))?;

        let body = resp
            .text()
            .await
            .map_err(|e| AppError::RequestError(e.to_string()))?;

        let inserted: Vec<TrackedWallet> =
            serde_json::from_str(&body).map_err(|e| AppError::JsonParseError(e.to_string()))?;

        let first_wallet = inserted
            .get(0)
            .ok_or_else(|| AppError::DatabaseError("No wallet was inserted".to_string()))?;

        first_wallet
            .id
            .ok_or_else(|| AppError::DatabaseError("Inserted wallet has no ID".to_string()))
    }

    pub async fn archive_tracked_wallet(&self, wallet_address: &str) -> Result<String, AppError> {
        let resp = self
            .client
            .from("tracked_wallets")
            .update(json!({"is_active": false}).to_string())
            .eq("user_id", &self.user_id)
            .eq("wallet_address", wallet_address)
            .execute()
            .await
            .map_err(|e| AppError::PostgrestError(e.to_string()))?;

        let body = resp
            .text()
            .await
            .map_err(|e| AppError::RequestError(e.to_string()))?;

        let updated: Vec<TrackedWallet> = serde_json::from_str(&body)?;

        updated
            .first()
            .ok_or_else(|| AppError::DatabaseError("No wallet was updated".to_string()))
            .map(|wallet| format!("Archived wallet: {}", wallet.wallet_address))
    }

    pub async fn unarchive_tracked_wallet(&self, wallet_address: &str) -> Result<String, AppError> {
        let resp = self
            .client
            .from("tracked_wallets")
            .update(json!({"is_active": true}).to_string())
            .eq("user_id", &self.user_id)
            .eq("wallet_address", wallet_address)
            .execute()
            .await
            .map_err(|e| AppError::PostgrestError(e.to_string()))?;

        let body = resp
            .text()
            .await
            .map_err(|e| AppError::RequestError(e.to_string()))?;

        let updated: Vec<TrackedWallet> = serde_json::from_str(&body)?;

        updated
            .get(0)
            .ok_or_else(|| AppError::DatabaseError("No wallet was updated".to_string()))
            .map(|wallet| format!("Unarchived wallet: {}", wallet.wallet_address))
    }

    pub async fn delete_tracked_wallet(&self, wallet_address: &str) -> Result<String, AppError> {
        let resp = self
            .client
            .from("tracked_wallets")
            .delete()
            .eq("user_id", &self.user_id)
            .eq("wallet_address", wallet_address)
            .execute()
            .await
            .map_err(|e| AppError::PostgrestError(e.to_string()))?;

        if resp.status().is_success() {
            let body = resp
                .text()
                .await
                .map_err(|e| AppError::RequestError(e.to_string()))?;

            let deleted_items: Vec<serde_json::Value> =
                serde_json::from_str(&body).map_err(|e| AppError::JsonParseError(e.to_string()))?;

            if deleted_items.is_empty() {
                Err(AppError::DatabaseError(
                    "No wallet found to delete".to_string(),
                ))
            } else {
                Ok(format!(
                    "{} tracked wallet(s) deleted successfully",
                    deleted_items.len()
                ))
            }
        } else {
            Err(AppError::DatabaseError(format!(
                "Failed to delete tracked wallet. Status: {}",
                resp.status()
            )))
        }
    }

    pub async fn update_tracked_wallet(&self, mut wallet: TrackedWallet) -> Result<Uuid, AppError> {
        wallet.user_id = Some(self.user_id.clone());

        let wallet_id = wallet
            .id
            .ok_or_else(|| AppError::BadRequest("Wallet ID is required for update".to_string()))?;

        let resp = self
            .client
            .from("tracked_wallets")
            .update(
                json!({
                    "id": wallet_id,
                    "user_id": wallet.user_id,
                    "wallet_address": wallet.wallet_address,
                    "is_active": wallet.is_active
                })
                .to_string(),
            )
            .eq("user_id", &self.user_id)
            .eq("id", wallet_id.to_string())
            .execute()
            .await
            .map_err(|e| AppError::PostgrestError(e.to_string()))?;

        let body = resp
            .text()
            .await
            .map_err(|e| AppError::RequestError(e.to_string()))?;

        let updated: Vec<TrackedWallet> = serde_json::from_str(&body)?;

        updated
            .get(0)
            .and_then(|w| w.id)
            .ok_or_else(|| AppError::DatabaseError("Failed to update wallet".to_string()))
    }

    pub async fn get_copy_trade_settings(&self) -> Result<Vec<CopyTradeSettings>, AppError> {
        let resp = self
            .client
            .from("copy_trade_settings")
            .select("*")
            .eq("user_id", &self.user_id)
            .execute()
            .await
            .map_err(|e| AppError::PostgrestError(e.to_string()))?;

        let body = resp
            .text()
            .await
            .map_err(|e| AppError::RequestError(e.to_string()))?;

        println!("Raw copy trade settings response: {}", body);

        let settings: Vec<CopyTradeSettings> = serde_json::from_str(&body).map_err(|e| {
            AppError::JsonParseError(format!(
                "Failed to parse copy trade settings: {}. Raw response: {}",
                e, body
            ))
        })?;

        Ok(settings)
    }

    pub async fn create_copy_trade_settings(
        &self,
        settings: CopyTradeSettings,
    ) -> Result<Uuid, AppError> {
        let resp = self
            .client
            .from("copy_trade_settings")
            .insert(
                json!({
                    "user_id": self.user_id,
                    "tracked_wallet_id": settings.tracked_wallet_id,
                    "is_enabled": settings.is_enabled,
                    "trade_amount_sol": settings.trade_amount_sol,
                    "max_slippage": settings.max_slippage,
                    "max_open_positions": settings.max_open_positions,
                    "allowed_tokens": settings.allowed_tokens,
                    "use_allowed_tokens_list": settings.use_allowed_tokens_list,
                    "allow_additional_buys": settings.allow_additional_buys,
                    "match_sell_percentage": settings.match_sell_percentage,
                    "min_sol_balance": settings.min_sol_balance
                })
                .to_string(),
            )
            .execute()
            .await
            .map_err(|e| AppError::PostgrestError(e.to_string()))?;

        let body = resp
            .text()
            .await
            .map_err(|e| AppError::RequestError(e.to_string()))?;

        let inserted: Vec<CopyTradeSettings> = serde_json::from_str(&body)?;

        inserted.get(0).and_then(|s| s.id).ok_or_else(|| {
            AppError::DatabaseError("Failed to create copy trade settings".to_string())
        })
    }

    pub async fn update_copy_trade_settings(
        &self,
        settings: CopyTradeSettings,
    ) -> Result<Uuid, AppError> {
        let resp = self
            .client
            .from("copy_trade_settings")
            .update(
                json!({
                    "is_enabled": settings.is_enabled,
                    "trade_amount_sol": settings.trade_amount_sol,
                    "max_slippage": settings.max_slippage,
                    "max_open_positions": settings.max_open_positions,
                    "allowed_tokens": settings.allowed_tokens,
                    "use_allowed_tokens_list": settings.use_allowed_tokens_list,
                    "allow_additional_buys": settings.allow_additional_buys,
                    "match_sell_percentage": settings.match_sell_percentage,
                    "min_sol_balance": settings.min_sol_balance
                })
                .to_string(),
            )
            .eq("user_id", &self.user_id)
            .eq("tracked_wallet_id", settings.tracked_wallet_id.to_string())
            .execute()
            .await
            .map_err(|e| AppError::PostgrestError(e.to_string()))?;

        let body = resp
            .text()
            .await
            .map_err(|e| AppError::RequestError(e.to_string()))?;

        let updated: Vec<CopyTradeSettings> = serde_json::from_str(&body)?;

        updated.get(0).and_then(|s| s.id).ok_or_else(|| {
            AppError::DatabaseError("Failed to update copy trade settings".to_string())
        })
    }

    pub async fn delete_copy_trade_settings(
        &self,
        tracked_wallet_id: Uuid,
    ) -> Result<String, AppError> {
        let resp = self
            .client
            .from("copy_trade_settings")
            .delete()
            .eq("user_id", &self.user_id)
            .eq("tracked_wallet_id", tracked_wallet_id.to_string())
            .execute()
            .await
            .map_err(|e| AppError::PostgrestError(e.to_string()))?;

        if resp.status() == 204 {
            Ok("Copy trade settings deleted successfully".to_string())
        } else {
            Err(AppError::DatabaseError(
                "Failed to delete copy trade settings".to_string(),
            ))
        }
    }

    pub async fn get_transaction_history(&self) -> Result<Vec<TransactionLog>, AppError> {
        let resp = self
            .client
            .from("transactions")
            .select("*")
            .eq("user_id", &self.user_id)
            .execute()
            .await
            .map_err(|e| AppError::PostgrestError(e.to_string()))?;

        let body = resp
            .text()
            .await
            .map_err(|e| AppError::RequestError(e.to_string()))?;

        let transactions: Vec<TransactionLog> = serde_json::from_str(&body).map_err(|e| {
            AppError::JsonParseError(format!("Failed to parse transactions: {}", e))
        })?;

        Ok(transactions)
    }

    pub async fn log_transaction(&self, transaction: TransactionLog) -> Result<Uuid> {
        let resp = self
            .client
            .from("transactions")
            .insert(
                json!({
                    "id": transaction.id,
                    "user_id": self.user_id,
                    "tracked_wallet_id": transaction.tracked_wallet_id,
                    "transaction_type": transaction.transaction_type,
                    "signature": transaction.signature
                })
                .to_string(),
            )
            .execute()
            .await
            .context("Failed to log transaction")?;

        let body = resp.text().await.context("Failed to log transaction")?;

        let inserted: Vec<TransactionLog> =
            serde_json::from_str(&body).context("Failed to parse inserted transaction")?;

        inserted
            .first()
            .map(|t| t.id)
            .ok_or_else(|| anyhow::anyhow!("Failed to get ID of inserted transaction"))
    }

    // Helper function to verify table schema matches our struct
    pub async fn verify_copy_trade_settings_schema(&self) -> Result<(), AppError> {
        let resp = self
            .client
            .from("copy_trade_settings")
            .select("*")
            .limit(0)
            .execute()
            .await
            .map_err(|e| AppError::PostgrestError(e.to_string()))?;

        let schema = resp
            .text()
            .await
            .map_err(|e| AppError::RequestError(e.to_string()))?;

        println!("Copy trade settings schema: {}", schema);

        Ok(())
    }
}
