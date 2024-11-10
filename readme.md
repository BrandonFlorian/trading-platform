# Solana DEX Trading Bot & API

NOTE: This project is currently under development and not all functionality is available. Do not use this in production yet. Please check back soon!

This project is a Solana-based trading bot and API system, designed to monitor specific wallets on the Solana blockchain and execute trades based on user-defined settings.

The platform is a rebuild of my first attempt at a trading bot, which was built in Python. This version is being built with Rust and is designed to be more efficient and easier to maintain.

The front end is a Next.js app that can be found [here](https://github.com/BrandonFlorian/solana-tools-client). It was originally built for the python version that can be found [here](https://github.com/BrandonFlorian/solana-tools-server) but will work for both. Python version is currently a private repo and you will need to contact me for access.

The front end has not been updated in a while, and is next on my todo list. If not working, please use the API directly until then and add your settings that way.

## Project Structure

The project is organized as a Rust workspace with three main components:

1. `trading-common`: Core functionality and shared components

   - Models and types
   - Database interactions (Supabase)
   - Event system
   - Server wallet management
   - Copy trade logic
   - Transaction processing utilities

2. `trading-api`: REST API server

   - CRUD operations for tracked wallets and settings
   - Trade execution endpoints
   - Integration with both pump.fun and Raydium DEXs
   - Real-time wallet status updates

3. `trading-bot`: Core trading engine
   - WebSocket-based wallet monitoring
   - Automated copy trading execution
   - Real-time balance tracking
   - Event-driven architecture

## Features

- Real-time wallet monitoring via WebSocket
- Copy trading with customizable settings:
  - Maximum open positions
  - Trade amount per position
  - Slippage tolerance
  - Allowed tokens list
  - Additional buy settings
- Support for multiple DEXs:
  - pump.fun
  - Raydium
- Automatic token balance tracking
- Graceful shutdown handling
- Robust error handling and retry logic
- Event-based system for real-time updates

## Prerequisites

- Rust (latest stable version)
- Cargo (comes with Rust)
- Solana CLI tools
- Supabase account and project
- Secret key for the server wallet (this should be a fresh wallet, not used for anything else)
- Helius RPC node access (or equivalent Solana RPC provider)

## Setup

1. Clone the repository:

```
git clone https://github.com/yourusername/solana-trading-project.git
cd solana-trading-project
```

2. Set up environment variables:

Create a `.env` file in the root directory and add the following (replace with your actual values):

```
#SOLANA
SOLANA_RPC_HTTP_URL=
SOLANA_RPC_WS_URL=

#SUPABASE
SUPABASE_URL=

SUPABASE_API_KEY=
SUPABASE_SERVICE_ROLE_KEY=
SUPABASE_ANON_PUBLIC_KEY=


#WALLET
SERVER_WALLET_SECRET_KEY=
TRACKED_WALLET_ID=

#PORTS
WS_PORT=
API_PORT=

```

3. Build the project:

```
cargo build
```

## Running the Project

## API Endpoints

The trading API provides the following endpoints:

### Tracked Wallets

- `GET /tracked_wallets`: Get all tracked wallets
- `POST /tracked_wallets`: Add a new tracked wallet
- `PUT /tracked_wallets/archive/:wallet_address`: Archive a tracked wallet
- `PUT /tracked_wallets/unarchive/:wallet_address`: Unarchive a tracked wallet
- `DELETE /tracked_wallets/:wallet_address`: Delete a tracked wallet
- `PUT /tracked_wallets/update`: Update a tracked wallet

### Copy Trade Settings

- `GET /copy_trade_settings`: Get all copy trade settings
- `POST /copy_trade_settings`: Create new copy trade settings
- `PUT /copy_trade_settings`: Update copy trade settings
- `DELETE /copy_trade_settings/:tracked_wallet_id`: Delete copy trade settings for a specific tracked wallet

### Trade Execution

- `POST /pump_fun/buy`: Execute buy on pump.fun
- `POST /pump_fun/sell`: Execute sell on pump.fun
- `POST /raydium/buy`: Execute buy on Raydium
- `POST /raydium/sell`: Execute sell on Raydium

### Transaction History

- `GET /transaction_history`: Get transaction history

All endpoints require the database to be set up. Please see the `tables.sql` file for the schema.

For detailed information on request and response formats for each endpoint, please refer to the API documentation.

### Trading Bot

To run the trading bot:

```
cargo run --bin trading-bot
```

The bot will check the database if your wallet exists and if it is following any tracked wallets.

If it is following any tracked wallets, it will connect to the RPC websocket and start monitoring the wallet and execute trades based on the settings in the database.

Currently only support pump.fun copy trading but will support all the major DEXs shortly.

## Configuration

All configuration is done through environment variables. Please see the `.env.example` file for more information.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
