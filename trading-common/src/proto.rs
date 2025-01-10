include!("generated/wallet.rs");

// Re-export the wallet module
pub mod wallet {
    pub use super::*;
}
