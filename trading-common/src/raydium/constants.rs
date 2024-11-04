use solana_sdk::pubkey::Pubkey;

pub const RAY_V4_PROGRAM_ID: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
pub const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub const WSOL: &str = "So11111111111111111111111111111111111111112";
pub const COMPUTE_BUDGET_PRICE: u64 = 1_000_000;
pub const COMPUTE_BUDGET_UNITS: u32 = 300_000;

pub const RAY_V4: Pubkey = solana_sdk::pubkey!("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8");
pub const RAY_AUTHORITY_V4: Pubkey =
    solana_sdk::pubkey!("5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1");
pub const OPEN_BOOK_PROGRAM: Pubkey =
    solana_sdk::pubkey!("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX");
pub const SOL: &str = "So11111111111111111111111111111111111111112";

pub const SOL_DECIMALS: u8 = 9;
pub const LAMPORTS_PER_SOL: u64 = 1_000_000_000;

pub const ACCOUNT_FLAGS_LAYOUT_SIZE: usize = 8;
pub const TOKEN_ACCOUNT_LAYOUT_SIZE: usize = 165;
pub const MARKET_STATE_LAYOUT_V3_SIZE: usize = 388;
pub const LIQUIDITY_STATE_LAYOUT_V4_SIZE: usize = 752;
