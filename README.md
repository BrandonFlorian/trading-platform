# Trading Platform

NOTE: This is a WIP/R&D repo, and not really ready for production. I am working on a more professional version of it in a private repo, and am not likely to maintain this one as often. There are many areas for improvement and optimizations, but I am sure it is still valuable info for other developers :) 

A multi-chain cryptocurrency trading platform supporting Solana DEXs and Hyperliquid perpetual futures.

## Architecture

This platform consists of multiple microservices organized into Solana and Hyperliquid ecosystems:

### Solana Services

#### Core Trading Services
- **trading-common**: Shared library with models, database client, DEX integrations (pump.fun, Raydium, Jupiter), Redis pool, WebSocket server, and gRPC protocol definitions
- **trading-api**: REST API server with CRUD operations and trade execution endpoints (port 3000)
- **trading-bot**: Core trading engine with WebSocket wallet monitoring and copy trading functionality (port 3001)
- **trading-wallet**: gRPC wallet management service for centralized wallet operations (port 50051)

#### Price Feed Services
- **trading-price-feed**: Real-time price monitoring for any Solana token using Raydium DEX vault subscriptions (port 3005)
  - Zero-RPC polling approach via WebSocket subscriptions to vault account changes
  - Automatic pool discovery for any token address
  - Multi-layer caching (in-memory + Redis) with health monitoring
  - Endpoints: `/health`, `/status`, `/ws` for real-time updates
  - Publishes price updates to Redis and WebSocket clients

- **trading-sol-price-feed**: Dedicated SOL/USD price monitoring with dual data sources (port 3006)
  - Primary: Pyth Network oracle (`7UVimffxr9ow1uXYxsr4LHAcV58mLzhmwaeKvJ1pjLiE`) with confidence intervals
  - Fallback: Raydium USDC/SOL pool (`58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2`)
  - 1-second polling with automatic failover between sources
  - Endpoints: `/price` (JSON), `/ws` for real-time SOL price streaming

### Hyperliquid Services

#### Core Trading Services
- **hyperliquid-common**: Shared types, error handling, and SDK wrapper for Hyperliquid API integration
- **hyperliquid-api**: REST API for Hyperliquid perpetual futures trading (port 3100)
  - Market orders (long/short with slippage protection)
  - Limit orders (GTC, IOC, ALO time-in-force options)
  - Stop loss creation, modification, and cancellation
  - Position and account management
  - Order cancellation by order ID
  - Asset universe querying
- **hyperliquid-wallet**: gRPC service for secure Hyperliquid wallet operations (port 50052)
  - Private key management and transaction signing
  - All trading operations routed through secure gRPC interface

## Service Dependencies

Services should be started in this order due to dependencies:

1. **Infrastructure**: `docker-compose up -d redis`
2. **Wallet Services**: 
   - `cargo run --bin trading-wallet`
   - `cargo run --bin hyperliquid-wallet`
3. **API Servers**: 
   - `cargo run --bin trading-api`
   - `cargo run --bin hyperliquid-api`
4. **Trading Bots**: `cargo run --bin trading-bot`
5. **Price Feeds** (optional):
   - `cargo run --bin trading-price-feed`
   - `cargo run --bin trading-sol-price-feed`

## Getting Started

### Prerequisites
- Rust 1.70+ with Cargo
- Docker and Docker Compose (for Redis)
- Solana RPC access (Helius, QuickNode, etc.)
- Hyperliquid account with API access

### Setup
1. Copy `.env.example` to `.env` and configure:
   ```bash
   # Solana Configuration
   SOLANA_RPC_HTTP_URL="your-solana-rpc-endpoint"
   SOLANA_RPC_WS_URL="your-solana-websocket-endpoint"
   
   # Hyperliquid Configuration  
   HYPERLIQUID_PRIVATE_KEY="your-hyperliquid-private-key"
   HYPERLIQUID_TESTNET=false  # Set to true for testnet
   
   # Database (Supabase)
   SUPABASE_URL="your-supabase-url"
   SUPABASE_SERVICE_ROLE_KEY="your-service-role-key"
   
   # Redis
   REDIS_URL=redis://localhost:6379
   
   # Wallet Management
   SERVER_WALLET_SECRET_KEY="your-solana-wallet-private-key"
   ```

2. Start Redis: `docker-compose up -d redis`
3. Build the workspace: `cargo build --workspace`
4. Start services in dependency order (see above)

## Development

### Build Commands
```bash
# Build entire workspace
cargo build --workspace

# Build specific services
cargo build --bin trading-api
cargo build --bin hyperliquid-api
cargo build --bin trading-bot

# Release builds
cargo build --workspace --release
```

### Development Tools
```bash
# Watch and rebuild on changes
cargo watch -x "build --workspace"

# Run tests
cargo test --workspace

# Format code
cargo fmt --all

# Run linter
cargo clippy --workspace --all-targets

# Check for compilation errors
cargo check --workspace
```

### Protocol Buffers
- Definitions in `trading-common/proto/wallet.proto` and `hyperliquid-common/proto/wallet.proto`
- Auto-generated during build via build scripts
- Force recompilation: `cargo clean -p trading-common && cargo build`

## API Documentation

### Solana Trading
See `rest-client.http` for complete Solana API examples including:
- Wallet management: `/api/wallets/*`
- Copy trade settings: `/api/copy-trade-settings/*`
- Trade execution: `/api/trade/pump`, `/api/trade/raydium`, `/api/trade/jupiter`
- Transaction history: `/api/transactions/*`
- Watchlist management: `/api/watchlist/*`
- Token metadata: `/api/token/*`

### Hyperliquid Trading
See `hyperliquid-rest-client.http` for complete Hyperliquid API examples including:
- Market orders: `POST /api/trade/market`
- Limit orders: `POST /api/trade/limit`
- Position management: `GET /api/positions`
- Account info: `GET /api/account`
- Order cancellation: `POST /api/orders/{order_id}/cancel`
- Stop loss management: `/api/stop-loss/*`
- Asset universe: `GET /api/universe`

### Price Feed APIs
- **General tokens**: `ws://localhost:3005/ws` for real-time price updates
- **SOL price**: `GET http://localhost:3006/price` or `ws://localhost:3006/ws`

## Key Features

### Solana Integration
- Support for pump.fun, Raydium, and Jupiter DEX protocols
- Copy trading with configurable parameters
- Wallet-based authentication and multi-wallet support
- Real-time price feeds with automatic pool discovery
- Transaction history and watchlist management

### Hyperliquid Integration
- Perpetual futures trading (long/short positions)
- Advanced order types (market, limit, stop loss)
- Real-time position and account monitoring
- Risk management with stop loss automation
- Secure wallet operations via gRPC

### Infrastructure
- Redis-based event broadcasting and caching
- WebSocket real-time updates for frontend integration
- Microservice architecture with service discovery
- Comprehensive error handling and logging
- Docker support for easy deployment
