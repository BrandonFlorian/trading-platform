use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SwapInfo {
    #[serde(rename = "ammKey")]
    pub amm_key: String,
    pub label: String,
    pub input_mint: String,
    pub output_mint: String,
    pub in_amount: String,
    pub out_amount: String,
    pub fee_amount: String,
    pub fee_mint: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RoutePlan {
    #[serde(rename = "swapInfo")]
    pub swap_info: SwapInfo,
    pub percent: u8,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JupiterQuoteRequest {
    pub input_mint: String,
    pub output_mint: String,
    pub amount: String,
    pub slippage_bps: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform_fee_bps: Option<u8>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JupiterQuoteResponse {
    pub input_mint: String,
    pub output_mint: String,
    pub in_amount: String,
    pub out_amount: String,
    #[serde(rename = "otherAmountThreshold")]
    pub other_amount_threshold: String,
    pub swap_mode: String,
    pub slippage_bps: u16,
    pub platform_fee: Option<PlatformFee>,
    pub price_impact_pct: String,
    pub route_plan: Vec<RoutePlan>,
    pub score_report: Option<Value>,
    #[serde(rename = "contextSlot")]
    pub context_slot: u64,
    #[serde(rename = "timeTaken")]
    pub time_taken: f64,
    #[serde(rename = "swapUsdValue")]
    pub swap_usd_value: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PlatformFee {
    pub amount: String,
    pub fee_bps: u8,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JupiterTransactionConfig {
    pub wrap_and_unwrap_sol: bool,
    pub compute_unit_price_micro_lamports: Option<u64>,
    pub compute_unit_limit: Option<u64>,
    pub prioritization_fee: Option<u64>,
    pub use_shared_accounts: bool,
    pub min_exchange_rate: Option<f64>,
    pub use_alt: bool,
    pub dynamic_compute_unit_limit: bool,
    pub blockhash_slots_to_expiry: Option<u8>,
}

impl Default for JupiterTransactionConfig {
    fn default() -> Self {
        Self {
            wrap_and_unwrap_sol: true,
            compute_unit_price_micro_lamports: None,
            compute_unit_limit: None,
            prioritization_fee: None,
            use_shared_accounts: true,
            min_exchange_rate: None,
            use_alt: true,
            dynamic_compute_unit_limit: true,
            blockhash_slots_to_expiry: Some(20),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JupiterSwapRequest {
    pub user_public_key: String,
    pub quote_response: JupiterQuoteResponse,
    #[serde(flatten)]
    pub config: JupiterTransactionConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JupiterSwapResponse {
    pub swap_transaction: String,
    pub last_valid_block_height: u64,
    pub simulation_slot: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone)]
#[serde(rename_all = "camelCase")]
pub enum PriorityLevel {
    Medium,
    High,
    VeryHigh,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone)]
#[serde(rename_all = "camelCase")]
pub enum PrioritizationFeeLamports {
    #[serde(rename_all = "camelCase")]
    PriorityLevelWithMaxLamports {
        priority_level: PriorityLevel,
        max_lamports: u64,
        #[serde(default)]
        global: bool,
    },
    #[serde(rename_all = "camelCase")]
    Lamports(u64),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EncodedAccount {
    pub pubkey: String,
    pub is_signer: bool,
    pub is_writable: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EncodedInstruction {
    pub program_id: String,
    pub accounts: Vec<EncodedAccount>,
    pub data: String, // Base64 encoded data
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SwapInstructionsResponse {
    pub token_ledger_instruction: Option<EncodedInstruction>,
    pub compute_budget_instructions: Vec<EncodedInstruction>,
    pub setup_instructions: Vec<EncodedInstruction>,
    pub swap_instruction: EncodedInstruction,
    pub cleanup_instruction: Option<EncodedInstruction>,
    pub other_instructions: Vec<EncodedInstruction>,
    pub address_lookup_table_addresses: Vec<String>,
    pub prioritization_fee_lamports: u64,
    pub compute_unit_limit: u32,
    pub prioritization_type: Option<PrioritizationType>,
    pub simulation_slot: Option<u64>,
    pub dynamic_slippage_report: Option<DynamicSlippageReport>,
    pub simulation_error: Option<UiSimulationError>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UiSimulationError {
    pub error_code: String,
    pub error: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PrioritizationType {
    pub compute_budget: ComputeBudget,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ComputeBudget {
    pub micro_lamports: u64,
    pub estimated_micro_lamports: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DynamicSlippageReport {
    pub slippage_bps: u16,
    pub other_amount: Option<u64>,
    pub simulated_incurred_slippage_bps: Option<i16>,
    pub amplification_ratio: Option<f64>,
}
