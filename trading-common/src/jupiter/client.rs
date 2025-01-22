use super::types::*;
use crate::error::AppError;
use reqwest::{Client, Url};
use std::time::Duration;

const JUPITER_API_URL: &str = "https://quote-api.jup.ag/v6";
const TIMEOUT_SECS: u64 = 10;

pub struct JupiterClient {
    client: Client,
    base_url: String,
}

impl Default for JupiterClient {
    fn default() -> Self {
        Self::new(JUPITER_API_URL)
    }
}

impl JupiterClient {
    pub fn new(base_url: &str) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(TIMEOUT_SECS))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            base_url: base_url.to_string(),
        }
    }

    pub async fn get_quote(
        &self,
        request: &JupiterQuoteRequest,
    ) -> Result<JupiterQuoteResponse, AppError> {
        let url = format!("{}/quote", self.base_url);
        println!("Sending quote request to {}: {:?}", url, request);

        let response = self
            .client
            .get(&url)
            .query(&request)
            .send()
            .await
            .map_err(|e| AppError::RequestError(format!("Jupiter quote request failed: {}", e)))?;

        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| AppError::RequestError(format!("Failed to get response text: {}", e)))?;

        if !status.is_success() {
            return Err(AppError::RequestError(format!(
                "Jupiter quote failed with status {}: {}",
                status, text
            )));
        }

        println!("Quote response: {}", text);

        serde_json::from_str(&text).map_err(|e| {
            AppError::JsonParseError(format!("Failed to parse Jupiter quote response: {}", e))
        })
    }

    pub async fn get_swap_transaction(
        &self,
        request: &JupiterSwapRequest,
    ) -> Result<JupiterSwapResponse, AppError> {
        let url = format!("{}/swap", self.base_url);
        println!("Sending swap request to {}: {:?}", url, request);

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| AppError::RequestError(format!("Jupiter swap request failed: {}", e)))?;

        if !response.status().is_success() {
            let error = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".into());
            return Err(AppError::RequestError(format!(
                "Jupiter swap failed: {}",
                error
            )));
        }

        let text = response.text().await.map_err(|e| {
            AppError::RequestError(format!("Failed to get swap response text: {}", e))
        })?;

        println!("Swap response: {}", text);

        serde_json::from_str(&text).map_err(|e| {
            AppError::JsonParseError(format!("Failed to parse Jupiter swap response: {}", e))
        })
    }

    pub async fn get_swap_instructions(
        &self,
        request: &JupiterSwapRequest,
    ) -> Result<SwapInstructionsResponse, AppError> {
        let url = format!("{}/swap-instructions", self.base_url);
        println!(
            "Sending swap instructions request to {}: {:?}",
            url, request
        );

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                AppError::RequestError(format!("Jupiter swap instructions request failed: {}", e))
            })?;

        if !response.status().is_success() {
            let error = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".into());
            return Err(AppError::RequestError(format!(
                "Jupiter swap instructions failed: {}",
                error
            )));
        }

        let text = response.text().await.map_err(|e| {
            AppError::RequestError(format!(
                "Failed to get swap instructions response text: {}",
                e
            ))
        })?;

        println!("Swap instructions response: {}", text);

        serde_json::from_str(&text).map_err(|e| {
            AppError::JsonParseError(format!(
                "Failed to parse Jupiter swap instructions response: {}",
                e
            ))
        })
    }
}
