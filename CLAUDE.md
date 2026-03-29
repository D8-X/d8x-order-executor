# D8X Order Executor

## Overview

A TypeScript-based blockchain order execution system for the D8X perpetuals trading protocol. The system consists of two services that communicate via Redis pub/sub:

- **Sentinel**: Listens to blockchain events and broker WebSocket, streams to Redis
- **Executor**: Processes events, manages order state, executes orders on-chain

## Tech Stack

- TypeScript 5.0 / Node.js 20+
- Ethers.js v6 (blockchain)
- Redis (pub/sub messaging)
- Docker (containerization)
- Prometheus + AlertManager (monitoring)

## Project Structure

```
src/
├── sentinel/                    # Event listening service
│   ├── blockchainListener.ts    # Blockchain event listener (WS/HTTP modes)
│   ├── brokerListener.ts        # Broker WebSocket listener
│   └── main.ts                  # Entry point
├── executor/                    # Order execution service
│   ├── executor.ts              # Core execution engine (bot wallets, gas mgmt)
│   ├── distributor.ts           # State machine, order orchestration
│   ├── metrics.ts               # Prometheus metrics endpoint
│   ├── reverts.ts               # Error handling, Slack alerts
│   └── main.ts                  # Entry point
├── multiUrlJsonRpcProvider.ts   # HTTP RPC with fallback
├── multiUrlWebsocketProvider.ts # WebSocket RPC with fallback
├── types.ts                     # TypeScript interfaces
├── utils.ts                     # Utilities (config loading, wallet derivation)
└── config/
    └── sample.config.json       # Multi-chain configuration template
```

## Build Commands

```bash
yarn install          # Install dependencies (runs patch-package after)
yarn build            # Compile TypeScript to dist/
yarn start-sentinel   # Run sentinel service
yarn start-executor   # Run executor service
```

## Configuration

### Environment Variables (see sample.env)

| Variable | Purpose |
|----------|---------|
| `SDK_CONFIG` | Chain identifier (e.g., `base_sepolia`) |
| `SEED_PHRASE` | Mnemonic for HD wallet derivation |
| `REDIS_HOST` | Redis hostname |
| `REDIS_PORT` | Redis port (default: 6379) |
| `REDIS_PASSWORD` | Redis authentication |
| `EXECUTOR_CONFIG` | Path to config JSON |
| `SLACK_WEBHOOK_URL` | Slack alerts endpoint (optional) |

### Config File (sample.config.json)

Array of chain configs with:
- `sdkConfig`: Network name
- `bots`: Number of bot wallets
- `rpcExec/rpcWatch/rpcListenHttp/rpcListenWs`: RPC endpoints
- `brokerWS`: Broker WebSocket URLs
- Gas settings: `maxGasPriceGWei`, `gasLimit`
- Timing: `waitForBlockSeconds`, `executeIntervalSecondsMin/Max`

## Docker Setup

The system uses Docker Compose (`docker-compose.yml`) with:

1. **cache** (Redis): Message broker
2. **sentinel-{chain}**: Event listener per chain
3. **executor-{chain}**: Order executor per chain
4. **prometheus**: Metrics collection (port 9090)
5. **alertmanager**: Alert routing to Slack (port 9093)

Pre-built images from `ghcr.io/d8-x/tsnode-exec-{sentinel,executor}:dev`

### Running with Docker

```bash
# Login to GitHub Container Registry
echo $GITHUB_TOKEN | docker login ghcr.io -u $GUSER --password-stdin

# Start all services
docker compose up -d
```

### Building Images Locally

```bash
# From repo root
docker build -f src/sentinel/Dockerfile -t sentinel .
docker build -f src/executor/Dockerfile -t executor .
```

## Architecture Flow

1. **Sentinel** detects blockchain events (Trade, Liquidate, OrderCreated, etc.)
2. Events published to Redis channels
3. **Distributor** receives events, updates internal state (orders, positions, prices)
4. Distributor checks order executability (collateral, dependencies, price freshness)
5. **Executor** processes queue with bot wallets, submits transactions
6. Results tracked via Prometheus metrics, failures alert to Slack

## Key Implementation Details

### Multi-RPC Fallback
- `MultiUrlJsonRpcProvider`: HTTP with automatic URL rotation on failure
- `MultiUrlWebsocketProvider`: WebSocket with connection failover
- Sentinel has dual-mode: WebSocket events → HTTP polling fallback

### Wallet Management
- Index 0: Treasury (funds bot wallets)
- Index 1+: Bot wallets for parallel order execution
- HD derivation path: `m/44'/60'/0'/0/{index}`

### Monitoring
- Metrics at `http://executor:9001/metrics`
- Counters: `execute_order_errors{type}`, `execute_order_confirmations`
- Alert rules in `prometheus_configs/executor.rules.yaml`

## Supported Chains

x1, xlayer, arbitrumSepolia, europaTestnet, bartio, bera, base, base_sepolia (configured in sample.config.json)
