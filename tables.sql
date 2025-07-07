-- Drop existing tables if they exist
DROP TABLE IF EXISTS watchlist_tokens CASCADE;
DROP TABLE IF EXISTS watchlists CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS allowed_tokens CASCADE;
DROP TABLE IF EXISTS copy_trade_settings CASCADE;
DROP TABLE IF EXISTS tracked_wallets CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- Drop existing types if they exist
DROP TYPE IF EXISTS transaction_type_enum CASCADE;
DROP TYPE IF EXISTS transaction_status_enum CASCADE;

-- Create enums for better type safety
CREATE TYPE transaction_type_enum AS ENUM ('buy', 'sell');
CREATE TYPE transaction_status_enum AS ENUM ('pending', 'completed', 'failed');

-- Users table
CREATE TABLE users (
  id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  wallet_address TEXT UNIQUE NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tracked wallets table
CREATE TABLE tracked_wallets (
  id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  user_id UUID REFERENCES users(id) ON DELETE CASCADE,
  wallet_address TEXT NOT NULL,
  is_active BOOLEAN DEFAULT true,
  deleted_at TIMESTAMP WITH TIME ZONE,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(user_id, wallet_address)
);

-- Copy trade settings table
CREATE TABLE copy_trade_settings (
  id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  user_id UUID REFERENCES users(id) ON DELETE CASCADE,
  tracked_wallet_id UUID REFERENCES tracked_wallets(id) ON DELETE CASCADE,
  is_enabled BOOLEAN DEFAULT false,
  trade_amount_sol DECIMAL(18, 9) NOT NULL,
  max_slippage DECIMAL(5, 2) DEFAULT 1.00,
  max_open_positions INT DEFAULT 1,
  allow_additional_buys BOOLEAN DEFAULT false,
  match_sell_percentage BOOLEAN DEFAULT false,
  allowed_tokens TEXT[],
  use_allowed_tokens_list BOOLEAN DEFAULT false,
  min_sol_balance DECIMAL(18, 9) DEFAULT 0.01,
  deleted_at TIMESTAMP WITH TIME ZONE,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(user_id, tracked_wallet_id),
  CONSTRAINT chk_positive_trade_amount CHECK (trade_amount_sol > 0),
  CONSTRAINT chk_slippage_range CHECK (max_slippage >= 0 AND max_slippage <= 100),
  CONSTRAINT chk_positive_max_positions CHECK (max_open_positions > 0),
  CONSTRAINT chk_positive_min_balance CHECK (min_sol_balance >= 0)
);

-- Allowed tokens table
CREATE TABLE allowed_tokens (
  id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  user_id UUID REFERENCES users(id) ON DELETE CASCADE,
  token_address TEXT NOT NULL,
  is_tradable BOOLEAN DEFAULT true,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(user_id, token_address)
);

-- Transactions table
CREATE TABLE transactions (
  id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  user_id UUID REFERENCES users(id) ON DELETE CASCADE,
  tracked_wallet_id UUID REFERENCES tracked_wallets(id) ON DELETE SET NULL,
  signature TEXT NOT NULL UNIQUE,
  transaction_type transaction_type_enum NOT NULL,
  status transaction_status_enum DEFAULT 'completed',
  token_address TEXT NOT NULL,
  amount DECIMAL(18, 9) NOT NULL,
  price_sol DECIMAL(18, 9) NOT NULL,
  dex_name TEXT,
  slippage_used DECIMAL(5, 2),
  gas_fee DECIMAL(18, 9),
  timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT chk_positive_amount CHECK (amount > 0),
  CONSTRAINT chk_positive_price CHECK (price_sol > 0),
  CONSTRAINT chk_valid_slippage CHECK (slippage_used IS NULL OR (slippage_used >= 0 AND slippage_used <= 100)),
  CONSTRAINT chk_positive_gas_fee CHECK (gas_fee IS NULL OR gas_fee >= 0)
);

-- Watchlists table
CREATE TABLE watchlists (
  id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  user_id UUID REFERENCES users(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  description TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(user_id, name)
);

-- Watchlist tokens table
CREATE TABLE watchlist_tokens (
  id UUID DEFAULT uuid_generate_v4() PRIMARY KEY, 
  watchlist_id UUID REFERENCES watchlists(id) ON DELETE CASCADE,
  token_address TEXT NOT NULL,
  added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(watchlist_id, token_address)
);

-- Create indexes for performance
-- Users indexes
CREATE INDEX idx_users_wallet_address ON users(wallet_address);

-- Tracked wallets indexes
CREATE INDEX idx_tracked_wallets_user_id ON tracked_wallets(user_id);
CREATE INDEX idx_tracked_wallets_active ON tracked_wallets(is_active) WHERE is_active = true;
CREATE INDEX idx_tracked_wallets_user_active ON tracked_wallets(user_id, is_active);
CREATE INDEX idx_tracked_wallets_wallet_address ON tracked_wallets(wallet_address);

-- Copy trade settings indexes
CREATE INDEX idx_copy_trade_settings_user_id ON copy_trade_settings(user_id);
CREATE INDEX idx_copy_trade_settings_enabled ON copy_trade_settings(is_enabled) WHERE is_enabled = true;
CREATE INDEX idx_copy_trade_settings_user_enabled ON copy_trade_settings(user_id, is_enabled);
CREATE INDEX idx_copy_trade_settings_tracked_wallet ON copy_trade_settings(tracked_wallet_id);

-- Allowed tokens indexes
CREATE INDEX idx_allowed_tokens_user_id ON allowed_tokens(user_id);
CREATE INDEX idx_allowed_tokens_token_address ON allowed_tokens(token_address);
CREATE INDEX idx_allowed_tokens_user_token ON allowed_tokens(user_id, token_address);

-- Transactions indexes
CREATE INDEX idx_transactions_user_id ON transactions(user_id);
CREATE INDEX idx_transactions_timestamp ON transactions(timestamp DESC);
CREATE INDEX idx_transactions_token_address ON transactions(token_address);
CREATE INDEX idx_transactions_signature ON transactions(signature);
CREATE INDEX idx_transactions_user_timestamp ON transactions(user_id, timestamp DESC);
CREATE INDEX idx_transactions_status ON transactions(status);
CREATE INDEX idx_transactions_type ON transactions(transaction_type);
CREATE INDEX idx_transactions_tracked_wallet ON transactions(tracked_wallet_id);

-- Watchlists indexes
CREATE INDEX idx_watchlists_user_id ON watchlists(user_id);
CREATE INDEX idx_watchlists_name ON watchlists(name);

-- Watchlist tokens indexes
CREATE INDEX idx_watchlist_tokens_watchlist_id ON watchlist_tokens(watchlist_id);
CREATE INDEX idx_watchlist_tokens_token_address ON watchlist_tokens(token_address);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER
SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updated_at columns
CREATE TRIGGER update_users_updated_at 
    BEFORE UPDATE ON users 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_tracked_wallets_updated_at 
    BEFORE UPDATE ON tracked_wallets 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_copy_trade_settings_updated_at 
    BEFORE UPDATE ON copy_trade_settings 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_allowed_tokens_updated_at 
    BEFORE UPDATE ON allowed_tokens 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_watchlists_updated_at 
    BEFORE UPDATE ON watchlists 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Add comments for documentation
COMMENT ON TABLE users IS 'User accounts identified by wallet addresses';
COMMENT ON TABLE tracked_wallets IS 'Wallets being monitored for copy trading';
COMMENT ON TABLE copy_trade_settings IS 'Configuration settings for copy trading behavior';
COMMENT ON TABLE allowed_tokens IS 'Whitelist of tokens allowed for trading per user';
COMMENT ON TABLE transactions IS 'Record of all trading transactions';
COMMENT ON TABLE watchlists IS 'User-defined token watchlists';
COMMENT ON TABLE watchlist_tokens IS 'Tokens within each watchlist';

COMMENT ON COLUMN transactions.signature IS 'Unique Solana transaction signature';
COMMENT ON COLUMN transactions.dex_name IS 'Name of the DEX used (pump.fun, Raydium, etc.)';
COMMENT ON COLUMN transactions.slippage_used IS 'Actual slippage percentage used in transaction';
COMMENT ON COLUMN transactions.gas_fee IS 'Transaction fee paid in SOL';
COMMENT ON COLUMN copy_trade_settings.deleted_at IS 'Soft delete timestamp';
COMMENT ON COLUMN tracked_wallets.deleted_at IS 'Soft delete timestamp';