use async_trait::async_trait;
use axum::{
    extract::{FromRequest, Request},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::de::DeserializeOwned;
use validator::Validate;

use crate::error::AppError;

/// A validated JSON extractor that automatically validates request bodies
/// using the validator crate
#[derive(Debug)]
pub struct ValidatedJson<T>(pub T);

impl<T, S> FromRequest<S> for ValidatedJson<T>
where
    T: DeserializeOwned + Validate + Send,
    S: Send + Sync,
{
    type Rejection = ValidationError;

    fn from_request(
        req: Request,
        state: &S,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> + Send {
        async move {
            // Extract JSON first
            let Json(data) = Json::<T>::from_request(req, state)
                .await
                .map_err(|_| ValidationError::JsonParse)?;

            // Validate the data
            data.validate()
                .map_err(|e| ValidationError::Validation(e))?;

            Ok(ValidatedJson(data))
        }
    }
}

/// Custom validation error type for middleware
#[derive(Debug)]
pub enum ValidationError {
    JsonParse,
    Validation(validator::ValidationErrors),
}

impl IntoResponse for ValidationError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ValidationError::JsonParse => {
                (StatusCode::BAD_REQUEST, "Invalid JSON format".to_string())
            }
            ValidationError::Validation(errors) => {
                let error_messages: Vec<String> = errors
                    .field_errors()
                    .iter()
                    .flat_map(|(field, errors)| {
                        errors.iter().map(move |error| {
                            let code = error.code.as_ref();
                            format!("{}: {}", field, code)
                        })
                    })
                    .collect();

                (
                    StatusCode::BAD_REQUEST,
                    format!("Validation failed: {}", error_messages.join(", ")),
                )
            }
        };

        let error_json = serde_json::json!({
            "error": message,
            "status": "validation_failed"
        });

        (status, Json(error_json)).into_response()
    }
}

/// Helper function to validate allowed tokens list separately
/// since we couldn't add it directly to the struct validation
pub fn validate_allowed_tokens_if_used(
    settings: &crate::models::CopyTradeSettings,
) -> Result<(), AppError> {
    if let Err(e) = crate::validation::validate_token_addresses_list(&settings.allowed_tokens) {
        return Err(AppError::BadRequest(format!(
            "allowed_tokens validation failed: {}",
            e.code
        )));
    }

    if let Err(e) = crate::validation::validate_copy_trade_business_rules(settings) {
        return Err(AppError::BadRequest(format!(
            "business rules validation failed: {}",
            e.code
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{BuyRequest, SellRequest};
    use axum::{
        body::Body,
        extract::State,
        http::{Method, Request},
        routing::post,
        Router,
    };
    use serde_json::json;
    use tower::ServiceExt;

    #[derive(Clone)]
    struct TestState;

    async fn test_buy_handler(
        _state: State<TestState>,
        ValidatedJson(request): ValidatedJson<BuyRequest>,
    ) -> Json<serde_json::Value> {
        Json(json!({
            "success": true,
            "token_address": request.token_address,
            "sol_quantity": request.sol_quantity
        }))
    }

    async fn test_sell_handler(
        _state: State<TestState>,
        ValidatedJson(request): ValidatedJson<SellRequest>,
    ) -> Json<serde_json::Value> {
        Json(json!({
            "success": true,
            "token_address": request.token_address,
            "token_quantity": request.token_quantity
        }))
    }

    fn create_test_app() -> Router {
        Router::new()
            .route("/buy", post(test_buy_handler))
            .route("/sell", post(test_sell_handler))
            .with_state(TestState)
    }

    #[tokio::test]
    async fn test_valid_buy_request() {
        let app = create_test_app();

        let valid_request = json!({
            "token_address": "So11111111111111111111111111111111111111112",
            "sol_quantity": 0.1,
            "slippage_tolerance": 0.05
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/buy")
            .header("content-type", "application/json")
            .body(Body::from(valid_request.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_invalid_buy_request_amount() {
        let app = create_test_app();

        let invalid_request = json!({
            "token_address": "So11111111111111111111111111111111111111112",
            "sol_quantity": -0.1, // Invalid: negative amount
            "slippage_tolerance": 0.05
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/buy")
            .header("content-type", "application/json")
            .body(Body::from(invalid_request.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_invalid_buy_request_address() {
        let app = create_test_app();

        let invalid_request = json!({
            "token_address": "invalid_address",
            "sol_quantity": 0.1,
            "slippage_tolerance": 0.05
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/buy")
            .header("content-type", "application/json")
            .body(Body::from(invalid_request.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_invalid_slippage_range() {
        let app = create_test_app();

        let invalid_request = json!({
            "token_address": "So11111111111111111111111111111111111111112",
            "sol_quantity": 0.1,
            "slippage_tolerance": 1.5 // Invalid: > 100%
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/buy")
            .header("content-type", "application/json")
            .body(Body::from(invalid_request.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
