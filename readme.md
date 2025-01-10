# Solana DEX Trading Bot & API

NOTE: This project is currently under development and not all functionality is available. Do not use this in production yet. Please check back soon!

This project is a Solana-based trading bot and API system, designed to monitor specific wallets on the Solana blockchain and execute trades based on user-defined settings.

## Architecture Overview

The project is organized as a Rust workspace with four main components:

1. `trading-common`: Core functionality and shared components

   - Shared models and types
   - Database interactions (Supabase)
   - Event system for real-time updates
   - gRPC protocol definitions
   - DEX integration utilities (pump.fun, Raydium)
   - Transaction processing and validation

2. `trading-api`: REST API server

   - CRUD operations for tracked wallets and settings
   - Trade execution endpoints
   - Multi-DEX integration endpoints
   - WebSocket server for real-time updates
   - Redis-based event broadcasting

3. `trading-bot`: Core trading engine

   - WebSocket-based wallet monitoring
   - Automated copy trading execution
   - Real-time balance tracking
   - Event-driven architecture
   - Redis subscription for settings updates

4. `trading-wallet`: Wallet management service
   - gRPC-based wallet operations
   - Centralized wallet state management
   - Token balance tracking
   - Transaction execution
   - Real-time balance updates

## Key Features

### Copy Trading

- Real-time wallet monitoring via WebSocket
- Customizable trading parameters:
  - Maximum open positions
  - Per-position trade amount
  - Slippage tolerance
  - Allowed tokens whitelist
  - Additional buy settings
  - Minimum balance requirements

### Multi-DEX Support

- pump.fun integration
- Raydium integration
- Remaining DEXs to be added soon
- Unified trading interface
- DEX-specific optimizations

### System Features

- Event-driven architecture
- Real-time updates via WebSocket
- gRPC-based wallet management
- Redis-based settings propagation
- Automatic token balance tracking
- Graceful shutdown handling
- Comprehensive error handling and retry logic

## Prerequisites

- Rust (latest stable version)
- Cargo
- Solana CLI tools
- Redis server
- Supabase account and project
- Dedicated server wallet (fresh Solana wallet)
- Helius RPC node (or equivalent Solana RPC provider)
- Protocol Buffers compiler (protoc)
- docker and docker-compose
- protobuf

## Protocol Buffers (gRPC) Setup

The project uses gRPC for communication between services and the wallet manager. The protocol definitions are located in:

```bash
trading-common/proto/wallet.proto
```

When you build the project:

1. The build script (`trading-common/build.rs`) automatically compiles the proto definitions

2. Generated code is placed in `trading-common/src/generated/wallet.rs`

3. This generated code is then available to all services through `trading-common`

### Proto Recompilation

If you modify the `wallet.proto` file, the code will automatically regenerate during the next build. You can also force regeneration with:

```bash
cargo clean -p trading-common
cargo build
```

## Setup

1. Clone the repository:

```bash
git clone https://github.com/yourusername/solana-trading-project.git
cd solana-trading-project
```

2. Set up environment variables:

```bash
cp .env.example .env
```

3. Build the project:

```bash
cargo build --workspace
```

## Running the Project

1. Start the redis server:

```bash
docker-compose up -d redis
```

Verify that the redis server is running:

```bash
docker-compose ps
```

2. Start the wallet service:

```bash
cargo run --bin trading-wallet
```

3. Then start the API server:

```bash
cargo run --bin trading-api
```

4. Finally start the trading bot:

```bash
cargo run --bin trading-bot
```

## API Endpoints

The trading API provides the following endpoints:

### Wallet

- `GET /wallet/info`: Get wallet info

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

All endpoints require the database to be set up. Please see the `tables.sql` file for the schema and you can use the rest-client.http file to test the endpoints.

## Configuration

All configuration is done through environment variables. Please see the `.env.example` file for more information.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
