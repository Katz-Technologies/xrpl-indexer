# XRPL Indexer Platform

High-performance platform for real-time indexing and analysis of XRP Ledger (XRPL) transactions using ClickHouse for data storage and Redis for caching and queue management in production.

## 📋 Table of Contents

- [Description](#description)
- [Technical Design](#technical-design)
- [System Architecture](#system-architecture)
- [Code Organization](#code-organization)
- [Libraries and Frameworks](#libraries-and-frameworks)
- [XRPL Integration Points](#xrpl-integration-points)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [API](#api)
- [Database](#database)
- [Development](#development)
- [License](#license)

## 🎯 Description

XRPL Indexer Platform is a scalable system for indexing, storing, and analyzing XRP Ledger transactions. The platform provides:

- **Real-time Indexing**: Automatic processing of new ledgers and transactions via WebSocket connection to XRPL
- **Data Storage**: Optimized storage in ClickHouse for fast analytical queries
- **Money Flow Processing**: Extraction and normalization of money flows from Payment, DEX, Swap, and other transactions
- **REST API**: HTTP endpoints for data access
- **WebSocket API**: Socket.IO for real-time updates
- **Backfilling**: Tools for populating historical data
- **Orchestrator**: Parallel processing of large ledger ranges

## 🏗️ Technical Design

### Overall Concept

The platform is built on a producer-consumer architecture with direct writes to ClickHouse. The main data flow:

1. **XRPL Connection**: WebSocket connection to XRPL node to receive ledger streams
2. **Transaction Processing**: Extraction of transactions from ledgers and conversion to money flows
3. **Storage**: Batching and writing to ClickHouse with performance optimization
4. **Notifications**: Sending events via Socket.IO to subscribers

### Key Design Decisions

- **Direct Processing**: Transactions are processed directly in producers without intermediate queues (Redis will be used for queues in production)
- **Batching**: Grouping records to optimize writes to ClickHouse
- **Deduplication**: Using `ReplacingMergeTree` in ClickHouse for automatic deduplication
- **Partitioning**: Monthly partitioning for query optimization
- **Gap Detection**: Automatic detection and filling of missing ledgers on reconnection

### Money Flow Processing

The system extracts money flows from transactions by analyzing balance changes in `AffectedNodes`. The following operation types are supported:

- **Transfer**: Direct transfers between addresses
- **Swap**: Currency exchange via AMM or DEX
- **DexOffer**: Creation/execution of DEX offers
- **Fee**: Transaction fees
- **Burn**: Token burning
- **Payout**: Token issuance by issuer
- **Loss**: Losses from partial executions

## 📐 System Architecture

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│                         XRPL Network                    │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │  XRPL Node 1 │  │  XRPL Node 2 │  │  XRPL Cluster │  │
│  │  (WebSocket) │  │  (WebSocket) │  │  (WebSocket)  │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬────────┘  │
└─────────┼──────────────────┼──────────────────┼─────────┘
          │                  │                  │
          └──────────────────┼──────────────────┘
                             │
                             ▼
          ┌──────────────────────────────────────┐
          │      Main Indexer Service (Go)       │
          │  ┌────────────────────────────────┐  │
          │  │   XRPL WebSocket Client        │  │
          │  │   - Stream Ledger              │  │
          │  │   - Stream Validation          │  │
          │  │   - Auto-reconnect             │  │
          │  └──────────────┬─────────────────┘  │
          │                 │                    │
          │  ┌──────────────▼─────────────────┐  │
          │  │      Producers                 │  │
          │  │  - Process Ledgers             │  │
          │  │  - Extract Transactions        │  │
          │  │  - Detect Gaps                 │  │
          │  └──────────────┬─────────────────┘  │
          │                 │                    │
          │  ┌──────────────▼─────────────────┐  │
          │  │      Consumers                 │  │
          │  │  - Extract Balance Changes     │  │
          │  │  - Build Action Groups         │  │
          │  │  - Generate Money Flows        │  │
          │  └──────────────┬─────────────────┘  │
          └─────────────────┼────────────────────┘
                            │
          ┌─────────────────┼───────────────────┐
          │                 │                   │
          ▼                 ▼                   ▼
┌─────────────────┐ ┌──────────────┐  ┌──────────────────┐
│   ClickHouse    │ │    Redis     │  │   Socket.IO Hub  │
│   (Storage)     │ │  (Production)│  │  (Real-time WS)  │
│                 │ │              │  │                  │
│ - money_flow    │ │ - Queues     │  │ - Subscriptions  │
│ - empty_ledgers │ │ - Cache      │  │ - Events         │
│ - known_tokens  │ │ - Pub/Sub    │  │ - Notifications  │
│ - subscriptions │ │              │  │                  │
└─────────────────┘ └──────────────┘  └──────────────────┘
          │
          ▼
┌─────────────────────────────────────┐
│         REST API (Echo)             │
│  - GET /new-tokens                  │
│  - POST /subscription-links         │
│  - DELETE /subscription-links       │
│  - GET /socketio/health             │
└─────────────────────────────────────┘
```

### System Components

#### 1. Main Service (`main.go`)
- Application entry point
- Initialization of all connections (XRPL, ClickHouse)
- Starting producers and monitoring
- HTTP server (Echo framework)

#### 2. XRPL Connection (`connections/xrpl.go`)
- WebSocket client for connecting to XRPL
- Automatic reconnection with exponential backoff
- Subscription to streams: Ledger, Validation, PeerStatus, Consensus
- Connection state monitoring

#### 3. Producers (`producers/`)
- **`main.go`**: Main ledger processing loop
  - Receiving ledgers from WebSocket stream
  - Gap detection
  - Starting transaction processing
- **`transaction.go`**: Transaction processing
  - Fetching transactions from ledger via RPC
  - Determining real-time vs backfill
  - New token checking (real-time only)

#### 4. Consumers (`consumers/consumers.go`)
- Extracting balance changes from `AffectedNodes`
- Building action groups (Transfer, Swap, DEX Offer, Fee, Burn, Payout, Loss)
- Generating money flow records
- Validation and normalization of amounts

#### 5. ClickHouse Integration (`connections/clickhouse.go`)
- ClickHouse connection management
- Record batching for optimization
- Automatic periodic writes (by batch size or timeout)
- Subscription and notification processing

#### 6. Socket.IO Hub (`socketio/hub.go`)
- Managing client WebSocket connections
- Socket.IO protocol implementation (Engine.IO v4)
- Sending events: `new_token_detected`, `subscription_activity`
- Support for multiple clients

#### 7. Orchestrator (`cmd/orchestrator/`)
- Parallel processing of large ledger ranges
- Worker management for backfilling
- Gap detection and filling
- Work redistribution among workers

### Data Flow

```
XRPL Ledger Stream
       │
       ▼
  [Producer] ──► Extract Ledger Index
       │
       ▼
  [RPC Call] ──► Fetch Transactions
       │
       ▼
  [Consumer] ──► Extract Balance Changes
       │
       ▼
  [Consumer] ──► Build Action Groups
       │
       ▼
  [Consumer] ──► Generate Money Flows
       │
       ├──► [ClickHouse Batch Writer] ──► ClickHouse
       │
       └──► [Socket.IO Hub] ──► WebSocket Clients
```

### Error Handling and Reliability

- **Automatic Reconnection**: On XRPL connection loss
- **Gap Detection**: Automatic detection of missing ledgers
- **Retry Logic**: Retries on RPC request errors
- **Batch Recovery**: Error handling at individual record level in batch
- **Graceful Shutdown**: Proper shutdown with all data saved

## 📁 Code Organization

```
xrpl-indexer/
├── bin/                          # Compiled binaries
│   ├── platform-server          # Main indexing service
│   ├── platform-cli             # CLI tool
│   └── platform-orchestrator   # Backfill orchestrator
│
├── clickhouse/                  # Database configuration and schemas
│   ├── configs/
│   │   └── limits.xml          # ClickHouse limits
│   └── ddl/
│       └── 001_init.sql        # SQL database schema
│
├── cmd/                         # Executable commands
│   ├── cli/                    # CLI tool
│   │   ├── backfill.go         # Ledger backfilling
│   │   ├── import_tokens.go    # Token import
│   │   └── main.go             # CLI entry point
│   └── orchestrator/           # Orchestrator
│       ├── config.go           # Configuration
│       ├── main.go             # Entry point
│       ├── orchestrator.go     # Orchestration logic
│       ├── progress.go         # Progress tracking
│       └── worker.go           # Worker management
│
├── config/                      # Application configuration
│   └── env.go                  # Environment variables
│
├── connections/                 # External service connections
│   ├── clickhouse.go           # ClickHouse connection and batching
│   ├── close.go                # Connection closing
│   ├── monitor.go              # XRPL connection monitoring
│   ├── stream.go               # XRPL stream subscription
│   ├── token_detector.go       # New token detection
│   ├── tokens.go               # Token operations
│   ├── xrpl_rpc.go             # XRPL RPC client
│   └── xrpl.go                 # XRPL WebSocket client
│
├── consumers/                   # Transaction processing
│   └── consumers.go             # Money flow extraction
│
├── controllers/                 # HTTP controllers
│   ├── new-token-controller.go # New tokens API
│   └── subscription-controller.go # Subscriptions API
│
├── indexer/                     # Indexing logic
│   ├── main.go                 # Indexing utilities
│   └── modifier.go             # Transaction modification
│
├── logger/                      # Logging
│   └── logger.go               # Zerolog configuration
│
├── models/                      # Data models
│   ├── authaccount.go          # Account model
│   ├── currency.go             # Currency model
│   ├── ledger.go               # Ledger model
│   ├── memo.go                 # Memo model
│   ├── meta.go                  # Metadata model
│   ├── path.go                 # Payment path model
│   ├── pricedata.go            # Price data model
│   ├── signer.go               # Signer model
│   ├── signerentry.go          # Signer entry model
│   ├── stream.go               # Stream model
│   └── transaction.go          # Transaction model
│
├── producers/                   # Ledger and transaction processing
│   ├── main.go                 # Main producers loop
│   └── transaction.go          # Transaction processing
│
├── responses/                   # HTTP responses
│   └── transaction_response.go # API response format
│
├── routes/                      # API routes
│   └── routes.go               # Route registration
│
├── shutdown/                    # Graceful shutdown
│   └── context.go              # Shutdown context
│
├── signals/                     # OS signal handling
│   └── notify.go                # Shutdown signals
│
├── socketio/                    # Socket.IO implementation
│   ├── events.go               # Event types
│   └── hub.go                  # Connection management hub
│
├── docker-compose.yml           # Docker Compose configuration
├── go.mod                       # Go dependencies
├── go.sum                       # Go checksums
├── main.go                      # Main service entry point
├── Makefile                     # Make commands
└── README.md                    # Documentation
```

### Organization Principles

- **Separation of Concerns**: Each package is responsible for its own domain
- **Modularity**: Components are loosely coupled and can evolve independently
- **Reusability**: Common utilities are extracted into separate packages
- **Testability**: Structure allows easy component testing

## 📚 Libraries and Frameworks

### Core Dependencies

#### Go Runtime
- **Go 1.24.0+**: Programming language and runtime

#### Web Framework
- **Echo v4.12.0** (`github.com/labstack/echo/v4`): High-performance HTTP web framework
  - Used for REST API endpoints
  - Middleware for request handling
  - JSON serialization/deserialization

#### Database
- **ClickHouse Go Driver v2.40.3** (`github.com/ClickHouse/clickhouse-go/v2`): Official ClickHouse driver
  - Native connection via TCP
  - Batch support
  - Data compression (LZ4)

#### XRPL Integration
- **xrpl-go v0.2.10** (`github.com/xrpscan/xrpl-go`): Go client for XRPL
  - WebSocket connection to XRPL nodes
  - Stream subscription (Ledger, Validation, etc.)
  - RPC client for requests

#### WebSocket
- **Gorilla WebSocket v1.5.1** (`github.com/gorilla/websocket`): WebSocket implementation
  - Used for Socket.IO hub
  - HTTP to WebSocket upgrade

#### Logging
- **Zerolog v1.33.0** (`github.com/rs/zerolog`): Structured logging
  - High performance
  - JSON log format
  - Log levels (debug, info, warn, error)

#### Decimal Arithmetic
- **Decimal v1.4.0** (`github.com/shopspring/decimal`): Precise decimal arithmetic
  - Critical for financial calculations
  - Avoiding float64 rounding errors

#### Configuration
- **Godotenv v1.5.1** (`github.com/joho/godotenv`): Loading environment variables from .env files

#### Log Rotation
- **Lumberjack v2.2.1** (`gopkg.in/natefinch/lumberjack.v2`): Log file rotation
  - Automatic rotation by size
  - Old log retention

### Indirect Dependencies

- **ch-go v0.68.0**: Low-level ClickHouse client
- **brotli v1.2.0**: Data compression
- **lz4/v4 v4.1.22**: LZ4 compression algorithm
- **OpenTelemetry v1.38.0**: Instrumentation and tracing (for ClickHouse driver)

### Infrastructure

- **Docker**: ClickHouse containerization
- **Docker Compose**: Container orchestration
- **ClickHouse Server 24.8+**: Data storage
- **Redis** (production): Queues and caching for real-time indexing

## 🔌 XRPL Integration Points

### 1. WebSocket Connection

**File**: `connections/xrpl.go`

```go
XrplClient = xrpl.NewClient(xrpl.ClientConfig{URL: URL})
```

- Connection to XRPL node via WebSocket
- Support for multiple servers for fault tolerance
- Automatic reconnection with exponential backoff

**Configuration**:
- `XRPL_WEBSOCKET_URL`: Primary WebSocket URL
- `XRPL_WEBSOCKET_FULLHISTORY_URL`: URL for full history

### 2. Stream Subscription

**File**: `connections/stream.go`

```go
connections.SubscribeStreams()
```

Subscribes to the following streams:
- **Ledger Stream**: Receiving closed ledgers in real-time
- **Validation Stream**: Ledger validation (monitoring)
- **Peer Status**: Peer status (monitoring)
- **Consensus**: Consensus information (monitoring)

### 3. RPC Requests

**File**: `connections/xrpl_rpc.go`

Used for fetching detailed information:
- **`ledger`**: Getting full ledger with transactions
- **`account_info`**: Account information
- **`tx`**: Getting transaction by hash

**Usage example**:
```go
txResponse, err := ledger.FetchTransactions()
```

### 4. Transaction Processing

**File**: `producers/transaction.go`

- Fetching transactions from ledger via RPC
- Parsing transaction metadata
- Extracting balance information from `AffectedNodes`

### 5. Money Flow Extraction

**File**: `consumers/consumers.go`

Analysis of `AffectedNodes` in transaction metadata:
- **AccountRoot**: XRP balance changes
- **RippleState**: Token (IOU) balance changes
- **Offer**: DEX offers
- **AMM**: AMM operations

### 6. New Token Detection

**File**: `connections/token_detector.go`

- Analyzing transactions for new currencies
- Checking via `known_tokens` table
- Notification via Socket.IO on detection

### 7. Connection Monitoring

**File**: `connections/monitor.go`

- Periodic WebSocket connection state checking
- Automatic reconnection on disconnect
- Connection state logging

### Transaction Types Processed by the System

1. **Payment**: Primary transaction type for transfers
2. **OfferCreate/OfferCancel**: DEX offers
3. **Payment + DEX**: Swaps via DEX
4. **AMM**: AMM operations (via AccountRoot with AMMID)

## 📦 Requirements

### System Requirements

- **Go**: version 1.24.0 or higher
- **Docker**: for running ClickHouse
- **Docker Compose**: for container orchestration
- **Linux/Unix**: Linux recommended for production
- **Windows**: supported for development

### External Dependencies

- **ClickHouse**: version 24.8 or higher
- **XRPL Node**: WebSocket connection to XRPL node
- **Redis** (production): version 6.0+ for queues and caching

### Resources

**Minimum**:
- CPU: 2 cores
- RAM: 4GB
- Disk: 50GB (depends on data volume)

**Recommended** (production):
- CPU: 4+ cores
- RAM: 8GB+
- Disk: 500GB+ SSD
- Network: Stable connection to XRPL nodes

## 🚀 Installation

### 1. Clone Repository

```bash
git clone <repository-url>
cd xrpl-indexer
```

### 2. Install Go Dependencies

```bash
go mod download
```

### 3. Environment Setup

Create a `.env` file:

```env
# Server
SERVER_HOST=0.0.0.0
SERVER_PORT=8080

# Logging
LOG_LEVEL=info
LOG_TYPE=console
LOG_FILE_ENABLED=true
LOG_FILE_PATH=logs/platform.log
LOG_FILE_MAX_SIZE_MB=100
LOG_FILE_MAX_BACKUPS=3
LOG_FILE_MAX_AGE_DAYS=7

# XRPL
XRPL_WEBSOCKET_URL=wss://s1.ripple.com/
XRPL_WEBSOCKET_FULLHISTORY_URL=wss://xrplcluster.com/

# ClickHouse
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_DATABASE=xrpl
CLICKHOUSE_USER=katz
CLICKHOUSE_PASSWORD=katz-password
CLICKHOUSE_BATCH_SIZE=5000
CLICKHOUSE_BATCH_TIMEOUT_MS=5000

# Redis (for production)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=
REDIS_DB=0

# Detailed logging (optional)
DETAILED_LOGGING_LEDGERS=98900000,98900001,98900002
```

### 4. Start ClickHouse via Docker Compose

```bash
docker-compose up -d clickhouse
```

This will start:
- ClickHouse server on ports 9000 (native) and 8123 (HTTP)
- Automatic database schema initialization

### 5. Build Project

#### Windows

```bash
go build -o .\bin\platform-server.exe .
go build -o .\bin\platform-cli.exe .\cmd\cli
go build -o .\bin\platform-orchestrator.exe .\cmd\orchestrator
```

#### Linux

```bash
go build -o ./bin/platform-server ./
go build -o ./bin/platform-cli ./cmd/cli
go build -o ./bin/platform-orchestrator ./cmd/orchestrator
```

Or use Makefile:

```bash
make build
```

### 6. Start Service

#### Windows

```bash
.\bin\platform-server.exe
```

#### Linux

```bash
./bin/platform-server
```

## ⚙️ Configuration

All settings are configured via environment variables in the `.env` file.

### Main Settings

See the [Installation](#installation) section for an example `.env` file.

### Docker Compose Settings

In `docker-compose.yml`, ClickHouse parameters are configured:
- **CPU**: 3.6 cores
- **Memory**: 6.5GB limit, 6GB reservation
- **Ports**: 9000 (native), 8123 (HTTP)
- **Volumes**: Data stored in Docker volume `clickhouse-data`

## 📖 Usage

### Starting Main Service

```bash
# Build
make build

# Start
./bin/platform-server
```

The service will:
1. Connect to XRPL node via WebSocket
2. Subscribe to ledger stream
3. Process transactions in real-time
4. Write data to ClickHouse
5. Send events via Socket.IO

### Using Orchestrator for Backfilling

The orchestrator allows parallel processing of large ledger ranges:

```bash
./bin/platform-orchestrator \
  --workers 4 \
  --from 82000000 \
  --to 85000000 \
  --servers "wss://s1.ripple.com/,wss://s2.ripple.com/,wss://xrplcluster.com/" \
  --check-interval 30s \
  --verbose
```

Parameters:
- `--workers`: Number of parallel processes (default: 2)
- `--from`: Starting ledger index
- `--to`: Ending ledger index
- `--servers`: Comma-separated list of XRPL servers
- `--check-interval`: Status check interval (default: 30s)
- `--verbose`: Verbose logging
- `--redistribute-threshold`: Threshold for work redistribution

### Running in Background

#### Linux

To run backfilling in background:

```bash
nohup ./run.sh > logs/backfill.log 2>&1 &
```

To run orchestrator in background:

```bash
./bin/platform-orchestrator --workers 2 --from 98900000 --to 99119667 \
  --servers "wss://s1.ripple.com/,wss://s2.ripple.com/" \
  --check-interval 30s --verbose > logs/orchestrator.log 2>&1 &
```

To stop orchestrator:

```bash
touch stop.orchestrator
```

## 🔌 API

### REST API

#### Get New Tokens

```http
GET /new-tokens
```

**Example**:
```bash
curl http://localhost:8080/new-tokens
```

#### Create Subscription

```http
POST /subscription-links
Content-Type: application/json

{
  "from_address": "rSubscriber...",
  "to_address": "rSubscribedTo..."
}
```

**Example**:
```bash
curl -X POST http://localhost:8080/subscription-links \
  -H "Content-Type: application/json" \
  -d '{"from_address":"rSubscriber...","to_address":"rSubscribedTo..."}'
```

#### Delete Subscription

```http
DELETE /subscription-links
Content-Type: application/json

{
  "from_address": "rSubscriber...",
  "to_address": "rSubscribedTo..."
}
```

#### Health Check

```http
GET /socketio/health
```

### Socket.IO API

Connecting to WebSocket server:

```javascript
const socket = io('http://localhost:8080/socket.io/', {
  transports: ['websocket']
});

// Subscribe to new tokens
socket.on('new_token_detected', (data) => {
  console.log('New token detected:', data);
  // {
  //   currency: "USD",
  //   issuer: "rIssuer...",
  //   ledger_index: 12345678
  // }
});

// Subscribe to subscription activity
socket.on('subscription_activity', (data) => {
  console.log('Subscription activity:', data);
  // {
  //   activities: [
  //     {
  //       subscribers: ["rSubscriber1...", "rSubscriber2..."],
  //       money_flow: {
  //         tx_hash: "...",
  //         ledger_index: 12345678,
  //         kind: "swap",
  //         from_address: "...",
  //         to_address: "...",
  //         ...
  //       }
  //     }
  //   ]
  // }
});
```

## 🗄️ Database

### ClickHouse Schema

The project uses ClickHouse version 24.8+ with an optimized data schema.

#### Main Tables

**`xrpl.money_flow`**
- Stores money flows from transactions
- Monthly partitioning (`toYYYYMM(close_time)`)
- Indexes on addresses, currencies, transaction types
- Uses `ReplacingMergeTree` for deduplication
- Sort order: `(tx_hash, ledger_index, from_address, to_address, ...)`

**`xrpl.empty_ledgers`**
- Stores information about ledgers without Payment transactions
- Used for query optimization
- Indexes on `ledger_index` and `close_time`

**`xrpl.known_tokens`**
- Stores known tokens
- Used for new token detection
- Index on `(currency, issuer)`

**`xrpl.subscription_links`**
- Stores subscription links (subscriber -> subscribed_to)
- Used for notifications via Socket.IO
- Indexes on `from_address` and `to_address`

**`xrpl.new_tokens`**
- Stores new token detection history
- Indexes on `first_seen_ledger_index`

**`xrpl.xrp_prices`**
- Stores XRP price history in USD
- Monthly partitioning

### Database Queries

Useful query examples:

```sql
-- Get money flows for an address
SELECT * FROM xrpl.money_flow
WHERE (from_address = 'r...' OR to_address = 'r...')
ORDER BY close_time DESC
LIMIT 100;

-- Statistics by transaction types for the last day
SELECT kind, count() as count
FROM xrpl.money_flow
WHERE close_time >= now() - INTERVAL 1 DAY
GROUP BY kind
ORDER BY count DESC;

-- Top addresses by received XRP volume for the week
SELECT 
  to_address,
  sum(to_amount) as total_received
FROM xrpl.money_flow
WHERE to_currency = 'XRP'
  AND close_time >= now() - INTERVAL 7 DAY
GROUP BY to_address
ORDER BY total_received DESC
LIMIT 10;

-- Find all swaps for a specific token
SELECT *
FROM xrpl.money_flow
WHERE kind = 'swap'
  AND (
    (from_currency = 'USD' AND from_issuer_address = 'rIssuer...')
    OR
    (to_currency = 'USD' AND to_issuer_address = 'rIssuer...')
  )
ORDER BY close_time DESC
LIMIT 100;
```

## 🛠️ Development

### Building Project

```bash
# Build all components
make build

# Clean
make clean
```

### Running Tests

```bash
go test ./...
```

### Code Formatting

```bash
go fmt ./...
```

### Linting

```bash
golangci-lint run
```

### Adding New Transaction Types

1. Add processing in `consumers/consumers.go` in the `ExtractBalanceChanges` function
2. Update model in `models/` if necessary
3. Update database schema in `clickhouse/ddl/001_init.sql` if necessary

### Logging

The project uses `zerolog` for structured logging. Log levels:
- `debug`: Detailed debug information
- `info`: General information (default)
- `warn`: Warnings
- `error`: Errors

For detailed logging of specific ledgers, use the environment variable:
```env
DETAILED_LOGGING_LEDGERS=98900000,98900001
```

## 📝 License

This project is distributed under the GNU General Public License v3.0. See the [LICENSE](LICENSE) file for details.

## 🔗 Useful Links

- [XRPL Documentation](https://xrpl.org/)
- [ClickHouse Documentation](https://clickhouse.com/docs)
- [Go Documentation](https://go.dev/doc/)
- [Echo Framework](https://echo.labstack.com/)
- [Socket.IO](https://socket.io/)
- [Zerolog](https://github.com/rs/zerolog)

---

**Note**: This project is under active development. API and database schema may change between versions. Redis will be used in production for queue management and caching of real-time indexing data.
