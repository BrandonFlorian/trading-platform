DROP TABLE users cascade;
DROP TABLE tracked_wallets cascade;
DROP TABLE copy_trade_settings cascade;
DROP TABLE transactions cascade;


CREATE TABLE users (
  id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  wallet_address TEXT UNIQUE NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE tracked_wallets (
  id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  user_id TEXT REFERENCES users(wallet_address),
  wallet_address TEXT NOT NULL,
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(user_id, wallet_address)
);


CREATE TABLE copy_trade_settings (
  id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  user_id TEXT REFERENCES users(wallet_address),
  tracked_wallet_id UUID REFERENCES tracked_wallets(id),
  is_enabled BOOLEAN DEFAULT false,
  trade_amount_sol DECIMAL(18, 9) NOT NULL,
  max_slippage DECIMAL(5, 2) DEFAULT 1.00,
  max_open_positions INT DEFAULT 1,
  allow_additional_buys BOOLEAN DEFAULT false,
  match_sell_percentage BOOLEAN DEFAULT false,
  allowed_tokens TEXT[],
  use_allowed_tokens_list BOOLEAN DEFAULT false,
  min_sol_balance DECIMAL(18, 9) DEFAULT 0.01,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(user_id, tracked_wallet_id)
);

CREATE TABLE allowed_tokens (
  id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  user_id TEXT REFERENCES users(wallet_address),
  token_address TEXT NOT NULL,
  is_tradable BOOLEAN DEFAULT true,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(user_id, token_address)
);


CREATE TABLE transactions (
  id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  user_id TEXT REFERENCES users(wallet_address),
  tracked_wallet_id UUID REFERENCES tracked_wallets(id),
  signature TEXT NOT NULL,
  transaction_type TEXT NOT NULL,
  token_address TEXT NOT NULL,
  amount DECIMAL(18, 9) NOT NULL,
  price_sol DECIMAL(18, 9) NOT NULL,
  timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);