use crate::error::AppError;
use reqwest::Client;
use std::time::Duration;

use super::{constants::*, QuoteRequest, QuoteResponse, SwapRequest, SwapResponse};

pub struct JupiterApi {
    client: Client,
}

impl Default for JupiterApi {
    fn default() -> Self {
        Self::new()
    }
}

impl JupiterApi {
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("Failed to build HTTP client"),
        }
    }

    pub async fn get_quote(&self, request: &QuoteRequest) -> Result<QuoteResponse, AppError> {
        let response = self
            .client
            .get(JUPITER_QUOTE_API_URL)
            .query(&request)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| AppError::RequestError(format!("Failed to fetch quote: {}", e)))?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(AppError::RequestError(format!(
                "Quote request failed: {}",
                error_text
            )));
        }

        response
            .json::<QuoteResponse>()
            .await
            .map_err(|e| AppError::JsonParseError(format!("Failed to parse quote response: {}", e)))
    }

    pub async fn get_swap_transaction(
        &self,
        request: &SwapRequest,
    ) -> Result<SwapResponse, AppError> {
        let response = self
            .client
            .post(JUPITER_SWAP_API_URL)
            .json(&request)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| AppError::RequestError(format!("Failed to fetch swap: {}", e)))?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(AppError::RequestError(format!(
                "Swap request failed: {}",
                error_text
            )));
        }

        response
            .json::<SwapResponse>()
            .await
            .map_err(|e| AppError::JsonParseError(format!("Failed to parse swap response: {}", e)))
    }
}
