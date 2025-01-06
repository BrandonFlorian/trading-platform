use std::sync::Arc;

use postgrest::Postgrest;
use serde_json::json;
use uuid::Uuid;

use crate::{
    error::AppError,
    event_system::EventSystem,
    models::{CopyTradeSettings, TrackedWallet, TransactionLog, User},
};
use anyhow::{Context, Result};
use chrono::Utc;
use std::time::Instant;
#[derive(Clone)]
pub struct SupabaseClient {
    client: Postgrest,
    user_id: String,
    event_system: Arc<EventSystem>,
}

impl SupabaseClient {
    pub fn new(
        url: &str,
        _api_key: &str,
        service_role_key: &str,
        user_id: &str,
        event_system: Arc<EventSystem>,
    ) -> Self {
        println!("New Postgrest client created!");
        let client = Postgrest::new(url)
            .insert_header("apikey", service_role_key)
            .insert_header("Authorization", format!("Bearer {}", service_role_key));

        Self {
            client,
            user_id: user_id.to_string(),
            event_system,
        }
    }

    pub async fn user_exists(&self, user_id: &str) -> Result<bool, AppError> {
        let start_time = Instant::now();

        let operation_result = async {
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

            let users: Vec<serde_json::Value> = serde_json::from_str(&body).map_err(|e| {
                AppError::JsonParseError(format!("Failed to parse user response: {}", e))
            })?;

            Ok(!users.is_empty())
        }
        .await;

        // Emit database operation event
        self.event_system.emit_db_event(
            "select",
            "users",
            start_time,
            operation_result
                .as_ref()
                .err()
                .map(|e: &AppError| e.to_string()),
        );

        // If there was an error, emit error event with context
        if let Err(ref e) = operation_result {
            self.event_system.emit_error(
                "database_error",
                &e.to_string(),
                json!({
                    "operation": "select",
                    "table": "users",
                    "user_id": user_id,
                    "query_type": "exists_check"
                }),
            );
        }

        operation_result
    }

    pub async fn create_user(&self, user_id: &str) -> Result<Uuid, AppError> {
        let start_time = Instant::now();

        let operation_result = async {
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
        .await;

        // Emit database operation event
        self.event_system.emit_db_event(
            "insert",
            "users",
            start_time,
            operation_result.as_ref().err().map(|e| e.to_string()),
        );

        // If there was an error, emit error event with context
        if let Err(ref e) = operation_result {
            self.event_system.emit_error(
                "database_error",
                &e.to_string(),
                json!({
                    "operation": "insert",
                    "table": "users",
                    "user_id": user_id,
                    "created_at": Utc::now()
                }),
            );
        }

        operation_result
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
        let start_time = Instant::now();

        let operation_result = async {
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
                .first()
                .ok_or_else(|| AppError::DatabaseError("No wallet was inserted".to_string()))?;

            first_wallet
                .id
                .ok_or_else(|| AppError::DatabaseError("Inserted wallet has no ID".to_string()))
        }
        .await;

        // Emit database operation event
        self.event_system.emit_db_event(
            "insert",
            "tracked_wallets",
            start_time,
            operation_result.as_ref().err().map(|e| e.to_string()),
        );

        // If there was an error, emit error event with context
        if let Err(ref e) = operation_result {
            self.event_system.emit_error(
                "database_error",
                &e.to_string(),
                json!({
                    "operation": "insert",
                    "table": "tracked_wallets",
                    "user_id": self.user_id,
                    "wallet_address": wallet.wallet_address,
                    "is_active": wallet.is_active,
                    "created_at": Utc::now()
                }),
            );
        }

        operation_result
    }

    pub async fn archive_tracked_wallet(&self, wallet_address: &str) -> Result<String, AppError> {
        let start_time = Instant::now();

        let operation_result = async {
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
        .await;

        self.event_system.emit_db_event(
            "update",
            "tracked_wallets",
            start_time,
            operation_result.as_ref().err().map(|e| e.to_string()),
        );

        if let Err(ref e) = operation_result {
            self.event_system.emit_error(
                "database_error",
                &e.to_string(),
                json!({
                    "operation": "update",
                    "table": "tracked_wallets",
                    "user_id": self.user_id,
                    "wallet_address": wallet_address,
                    "is_active": false
                }),
            );
        }

        operation_result
    }

    pub async fn unarchive_tracked_wallet(&self, wallet_address: &str) -> Result<String, AppError> {
        let start_time = Instant::now();

        let operation_result = async {
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
                .first()
                .ok_or_else(|| AppError::DatabaseError("No wallet was updated".to_string()))
                .map(|wallet| format!("Unarchived wallet: {}", wallet.wallet_address))
        }
        .await;

        self.event_system.emit_db_event(
            "update",
            "tracked_wallets",
            start_time,
            operation_result.as_ref().err().map(|e| e.to_string()),
        );

        if let Err(ref e) = operation_result {
            self.event_system.emit_error(
                "database_error",
                &e.to_string(),
                json!({
                    "operation": "update",
                    "table": "tracked_wallets",
                    "user_id": self.user_id,
                    "wallet_address": wallet_address,
                    "is_active": true
                }),
            );
        }

        operation_result
    }

    pub async fn delete_tracked_wallet(&self, wallet_address: &str) -> Result<String, AppError> {
        let start_time = Instant::now();

        let operation_result = async {
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

                let deleted_items: Vec<serde_json::Value> = serde_json::from_str(&body)
                    .map_err(|e| AppError::JsonParseError(e.to_string()))?;

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
        .await;

        self.event_system.emit_db_event(
            "delete",
            "tracked_wallets",
            start_time,
            operation_result.as_ref().err().map(|e| e.to_string()),
        );

        if let Err(ref e) = operation_result {
            self.event_system.emit_error(
                "database_error",
                &e.to_string(),
                json!({
                    "operation": "delete",
                    "table": "tracked_wallets",
                    "user_id": self.user_id,
                    "wallet_address": wallet_address
                }),
            );
        }

        operation_result
    }

    pub async fn update_tracked_wallet(&self, mut wallet: TrackedWallet) -> Result<Uuid, AppError> {
        let start_time = Instant::now();

        let operation_result = async {
            wallet.user_id = Some(self.user_id.clone());

            let wallet_id = wallet.id.ok_or_else(|| {
                AppError::BadRequest("Wallet ID is required for update".to_string())
            })?;

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
                .first()
                .and_then(|w| w.id)
                .ok_or_else(|| AppError::DatabaseError("Failed to update wallet".to_string()))
        }
        .await;

        self.event_system.emit_db_event(
            "update",
            "tracked_wallets",
            start_time,
            operation_result.as_ref().err().map(|e| e.to_string()),
        );

        if let Err(ref e) = operation_result {
            self.event_system.emit_error(
                "database_error",
                &e.to_string(),
                json!({
                    "operation": "update",
                    "table": "tracked_wallets",
                    "user_id": self.user_id,
                    "wallet_address": wallet.wallet_address,
                    "is_active": wallet.is_active
                }),
            );
        }

        operation_result
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
        let start_time = Instant::now();

        let operation_result = async {
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

            inserted.first().and_then(|s| s.id).ok_or_else(|| {
                AppError::DatabaseError("Failed to create copy trade settings".to_string())
            })
        }
        .await;

        self.event_system.emit_db_event(
            "insert",
            "copy_trade_settings",
            start_time,
            operation_result.as_ref().err().map(|e| e.to_string()),
        );

        if let Err(ref e) = operation_result {
            self.event_system.emit_error(
                "database_error",
                &e.to_string(),
                json!({
                    "operation": "insert",
                    "table": "copy_trade_settings",
                    "user_id": self.user_id,
                    "tracked_wallet_id": settings.tracked_wallet_id,
                    "is_enabled": settings.is_enabled
                }),
            );
        }

        operation_result
    }

    pub async fn update_copy_trade_settings(
        &self,
        settings: CopyTradeSettings,
    ) -> Result<Uuid, AppError> {
        let start_time = Instant::now();

        let operation_result = async {
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

            updated.first().and_then(|s| s.id).ok_or_else(|| {
                AppError::DatabaseError("Failed to update copy trade settings".to_string())
            })
        }
        .await;

        self.event_system.emit_db_event(
            "update",
            "copy_trade_settings",
            start_time,
            operation_result.as_ref().err().map(|e| e.to_string()),
        );

        if let Err(ref e) = operation_result {
            self.event_system.emit_error(
                "database_error",
                &e.to_string(),
                json!({
                    "operation": "update",
                    "table": "copy_trade_settings",
                    "user_id": self.user_id,
                    "tracked_wallet_id": settings.tracked_wallet_id,
                    "is_enabled": settings.is_enabled,
                    "max_open_positions": settings.max_open_positions,
                    "trade_amount_sol": settings.trade_amount_sol,
                    "updated_at": Utc::now()
                }),
            );
        }

        operation_result
    }

    pub async fn delete_copy_trade_settings(
        &self,
        tracked_wallet_id: Uuid,
    ) -> Result<String, AppError> {
        let start_time = Instant::now();

        let operation_result = async {
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
        .await;

        self.event_system.emit_db_event(
            "delete",
            "copy_trade_settings",
            start_time,
            operation_result.as_ref().err().map(|e| e.to_string()),
        );

        if let Err(ref e) = operation_result {
            self.event_system.emit_error(
                "database_error",
                &e.to_string(),
                json!({
                    "operation": "delete",
                    "table": "copy_trade_settings",
                    "user_id": self.user_id,
                    "tracked_wallet_id": tracked_wallet_id
                }),
            );
        }

        operation_result
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

    pub async fn log_transaction(&self, transaction: TransactionLog) -> Result<Uuid, AppError> {
        let start_time = Instant::now();

        let operation_result = async {
            let resp = self
                .client
                .from("transactions")
                .insert(
                    json!({
                        "id": transaction.id,
                        "user_id": self.user_id,
                        "tracked_wallet_id": transaction.tracked_wallet_id,
                        "signature": transaction.signature,
                        "transaction_type": transaction.transaction_type,
                        "token_address": transaction.token_address,
                        "amount": transaction.amount,
                        "price_sol": transaction.price_sol,
                        "timestamp": transaction.timestamp
                    })
                    .to_string(),
                )
                .execute()
                .await
                .map_err(|e| {
                    AppError::PostgrestError(format!("Failed to log transaction: {}", e))
                })?;

            if resp.status() != 201 {
                return Err(AppError::DatabaseError(format!(
                    "Failed to insert transaction. Status: {}",
                    resp.status()
                )));
            }

            let body = resp
                .text()
                .await
                .map_err(|e| AppError::RequestError(e.to_string()))?;

            let inserted: Vec<TransactionLog> = serde_json::from_str(&body).map_err(|e| {
                AppError::JsonParseError(format!("Failed to parse response: {}", e))
            })?;

            inserted
                .first()
                .map(|t| t.id)
                .ok_or_else(|| AppError::DatabaseError("No transaction ID returned".to_string()))
        }
        .await;

        // Emit database operation event
        self.event_system.emit_db_event(
            "insert",
            "transactions",
            start_time,
            operation_result.as_ref().err().map(|e| e.to_string()),
        );

        // If there was an error, emit error event with context
        if let Err(ref e) = operation_result {
            self.event_system.emit_error(
                "database_error",
                &e.to_string(),
                json!({
                    "operation": "insert",
                    "table": "transactions",
                    "user_id": self.user_id,
                    "transaction_id": transaction.id
                }),
            );
        }

        operation_result
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
