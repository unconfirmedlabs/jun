# jun

Sui event indexer and chain toolkit for Bun. Native gRPC streaming over HTTP/2 with BCS binary decoding, Postgres output, and structured logging.

```
Sui Fullnode ──gRPC/HTTP2──▶ Jun ──BCS decode──▶ Postgres / SQLite / stdout
```

## Install

```bash
bun install
```

## Quick start

### Define and run an indexer

```typescript
import { defineIndexer } from "jun";

const indexer = defineIndexer({
  network: "testnet",
  grpcUrl: "fullnode.testnet.sui.io:443",
  database: process.env.DATABASE_URL!,
  startCheckpoint: "package:0x10ad...",  // backfill from contract deploy
  events: {
    RecordPressed: {
      type: "0x10ad...::pressing::RecordPressedEvent",
      fields: {
        pressing_id: "address",
        record_id: "address",
        quantity: "u64",
        pressed_by: "address",
      },
    },
  },
});

// Live streaming + historical backfill, concurrently
await indexer.run();
```

### CLI

```bash
jun c info                   # chain info
jun c object 0x5             # query objects
jun stream                   # stream live checkpoints
jun replay --from-checkpoint 259000000 --count 1000 --output mainnet.sqlite
jun chat --live              # AI analysis with Claude
```

## Indexer API

### `defineIndexer(config)`

Creates an indexer with three modes:

```typescript
const indexer = defineIndexer({
  network: "mainnet",
  grpcUrl: "fullnode.mainnet.sui.io:443",
  database: "postgresql://user:pass@localhost/jun",
  startCheckpoint: 316756645n,     // where to start backfill
  backfillConcurrency: 10,         // parallel archive fetches (default: 10)
  archiveUrl: "https://...",       // override archive URL
  events: { /* ... */ },
});

await indexer.run();                                  // live + backfill (default)
await indexer.run({ mode: "live-only" });             // just live streaming
await indexer.run({ mode: "backfill-only" });         // backfill then exit
await indexer.run({ repairGaps: true });              // + periodic gap detection

await indexer.live();              // legacy: live only, direct writes
await indexer.backfill({ from });  // legacy: backfill only, direct writes
```

### `startCheckpoint` formats

```typescript
startCheckpoint: 316756645n                        // raw checkpoint number
startCheckpoint: "epoch:1080"                      // first checkpoint of epoch
startCheckpoint: "timestamp:2026-03-28T00:00:00Z"  // checkpoint at/after timestamp
startCheckpoint: "package:0x10ad..."               // checkpoint where package was published
```

### Field types

```typescript
fields: {
  item_id: "address",       // 32-byte Sui address → TEXT
  active: "bool",           // → BOOLEAN
  edition: "u16",           // u8/u16/u32 → INTEGER
  price: "u64",             // u64/u128/u256 → NUMERIC
  name: "string",           // → TEXT
  tags: "vector<string>",   // → JSONB
  label: "option<string>",  // → nullable TEXT
}
```

Field order must match the Move struct exactly (BCS is positional).

### How `run()` works

`run()` launches live streaming and backfill concurrently on Bun's event loop:

- **Live loop**: gRPC subscribe stream with exponential backoff reconnect
- **Backfill loop**: concurrent archive fetches in sliding windows of 1000
- **WriteBuffers**: each loop pushes events into its own buffer (live: 200ms/50 events, backfill: 1s/500 events) that flushes to Postgres with `Promise.all` across tables
- **Adaptive throttle**: monitors Postgres INSERT latency and adjusts backfill concurrency (halves on high p95, increments on sustained good latency, pauses after repeated failures)
- **Gap repair**: periodic SQL scan for missing checkpoints, fills from archive
- **Cursor tracking**: separate cursor keys (`live:network`, `backfill:network`) with watermark-based safe advancement

## YAML config

Define indexers declaratively without writing TypeScript.

```yaml
# config.yml
network: testnet
grpcUrl: fullnode.testnet.sui.io:443
database: $DATABASE_URL
startCheckpoint: "package:0x10ad..."

mode: all
repairGaps: true
serve:
  port: 8080

events:
  RecordPressed:
    type: "0x10ad...::pressing::RecordPressedEvent"
    fields:
      pressing_id: address
      release_id: address
      edition: u16
      record_id: address
      record_number: u64
      quantity: u64
      pressed_by: address
      paid_value: u64
      timestamp_ms: u64

views:
  daily_presses:
    sql: |
      SELECT date_trunc('day', sui_timestamp) AS day,
             release_id,
             count(*) AS presses
      FROM record_pressed
      GROUP BY 1, 2
    refresh: 60s

  top_pressers:
    sql: |
      SELECT pressed_by, sum(quantity) AS total
      FROM record_pressed
      GROUP BY 1
      ORDER BY 2 DESC
      LIMIT 100
    refresh: 5m
```

```bash
jun run config.yml
jun run config.yml --mode backfill-only
jun run config.yml --serve 9090          # override YAML port
jun run config.yml --no-serve            # disable YAML serve
```

- **Env var substitution**: `$VAR` and `${VAR}` in any string value
- **Runtime defaults**: `mode`, `repairGaps`, `serve` settable in YAML, CLI flags override
- **Materialized views**: pre-computed aggregations refreshed on a timer, queryable via `/query`

### Materialized views

Views are Postgres materialized views — pre-computed query results stored as physical tables. Jun creates them on startup and refreshes on a timer.

```yaml
views:
  daily_presses:
    sql: |
      SELECT date_trunc('day', sui_timestamp) AS day, count(*) AS presses
      FROM record_pressed
      GROUP BY 1
    refresh: 60s    # "30s", "5m", "1h"
```

Query them instantly via the HTTP API:

```bash
curl "http://localhost:8080/query?sql=SELECT * FROM daily_presses ORDER BY day DESC LIMIT 7"
```

## HTTP API

Enable with `serve: { port: 8080 }` in YAML config or `--serve 8080` CLI flag.

```
GET /health              → { status: "ok" }
GET /status              → cursor positions, throttle state, buffer stats, counters
GET /query?sql=...       → read-only SQL (SELECT/WITH/EXPLAIN only, row limit, timeout)
GET /metrics             → Prometheus text format
```

Query parameters for `/query`: `?sql=...&limit=1000&timeout=5000`

Prometheus metrics include: `jun_uptime_seconds`, `jun_live_cursor`, `jun_backfill_cursor`, `jun_checkpoints_processed_total`, `jun_events_flushed_total`, `jun_flushes_total`, `jun_flush_duration_seconds`, `jun_buffer_size`, `jun_backfill_concurrency`, `jun_backfill_paused`.

## SDK

Every building block is importable for custom composition.

```
jun                     defineIndexer, Indexer, IndexerConfig, EventHandler, DecodedEvent, FieldDefs
jun/grpc                createGrpcClient, GrpcClient, GrpcCheckpointResponse
jun/checkpoints         createArchiveClient, cachedGetCheckpoint, cacheGet, cachePut, cacheStats
jun/events              createProcessor, EventProcessor, EventHandler, DecodedEvent
jun/schema              buildBcsSchema, generateDDL, formatValue, formatRow
jun/pipeline            createWriteBuffer, createAdaptiveThrottle, createGapDetector
jun/output/postgres     createPostgresOutput, PostgresOutput
jun/output/sqlite       createSqliteWriter, SqliteWriter
jun/cursor              createStateManager, StateManager
jun/verify              verifyTransaction, verifyObject
jun/codegen             generateFieldDSL, mapSignatureToFieldType
jun/mcp                 createMcpServer
jun/signing             signMessage, verifyMessage, loadKeypair
jun/logger              createLogger, Logger
```

### Compose building blocks

```typescript
import { createGrpcClient } from "jun/grpc";
import { createProcessor } from "jun/events";
import { createArchiveClient } from "jun/checkpoints";

const grpc = createGrpcClient({ url: "fullnode.mainnet.sui.io:443" });
const processor = createProcessor({
  Transfer: {
    type: "0x2::coin::TransferEvent",
    fields: { amount: "u64", recipient: "address" },
  },
});

for await (const checkpoint of grpc.subscribeCheckpoints()) {
  const events = processor.process(checkpoint);
  // handle events however you want
}
```

### Custom pipeline

```typescript
import { createWriteBuffer, createAdaptiveThrottle } from "jun/pipeline";
import { createPostgresOutput } from "jun/output/postgres";
import { createStateManager } from "jun/cursor";
import { createLogger } from "jun/logger";

const log = createLogger();
const sql = new (await import("bun")).SQL(process.env.DATABASE_URL!);
const state = await createStateManager(sql);
const output = createPostgresOutput(sql, handlers);

const buffer = createWriteBuffer(output, state, {
  label: "custom",
  intervalMs: 500,
  maxEvents: 200,
  onFlush: (stats) => log.info(stats, "flushed"),
}, log);

buffer.start();
// ... push events from your own source
await buffer.stop();
```

## Logging

Structured JSON logging via [pino](https://github.com/pinojs/pino). Controlled by `LOG_LEVEL` env var.

```bash
# Default (info level)
bun run examples/sona.ts --run

# Debug logging (buffer pushes, throttle evaluations, window progress)
LOG_LEVEL=debug bun run examples/sona.ts --run

# Pretty-print for development
LOG_LEVEL=debug bun run examples/sona.ts --run | bunx pino-pretty

# Trace logging (every single push/cursor update)
LOG_LEVEL=trace bun run examples/sona.ts --run | bunx pino-pretty
```

Log components: `live`, `backfill`, `live:buffer`, `backfill:buffer`, `throttle`, `gaps`.

## Examples

### Index Sona pressing events (Postgres)

```typescript
// examples/sona.ts
import { defineIndexer } from "jun";

const indexer = defineIndexer({
  network: "testnet",
  grpcUrl: "slc1.rpc.testnet.sui.mirai.cloud:443",
  database: process.env.DATABASE_URL!,
  startCheckpoint: 316756645n,
  events: {
    RecordPressed: {
      type: "0x10ad...::pressing::RecordPressedEvent",
      fields: {
        pressing_id: "address",
        release_id: "address",
        edition: "u16",
        record_id: "address",
        record_number: "u64",
        quantity: "u64",
        pressed_by: "address",
        paid_value: "u64",
        timestamp_ms: "u64",
      },
    },
  },
});

await indexer.run();
```

```bash
DATABASE_URL=postgres://localhost/jun bun run examples/sona.ts --run
DATABASE_URL=postgres://localhost/jun bun run examples/sona.ts --run --backfill-only
DATABASE_URL=postgres://localhost/jun bun run examples/sona.ts --run --repair-gaps
LOG_LEVEL=debug DATABASE_URL=postgres://localhost/jun bun run examples/sona.ts --run | bunx pino-pretty
```

### Stream and filter events (no Postgres)

```bash
# All events from mainnet
jun stream --include events

# Filter to a specific module
jun stream --include events --filter "marketplace::ItemListedEvent"

# Write 5 minutes to SQLite for analysis
jun stream --duration 5m --output testnet.sqlite

# Pipe to jq for custom processing
jun stream --jsonl --include events | jq '.events[]?.type'
```

### Backfill historical data to SQLite

```bash
# Fetch 10K checkpoints from a specific range
jun fetch --from-checkpoint 318000000 --count 10000 --output mainnet.sqlite

# Replay an entire epoch from the archive
jun replay --from-epoch 1080 --output epoch-1080.sqlite

# Replay with cryptographic verification
jun replay --from-checkpoint 259000000 --count 100 --verify
```

### Generate field definitions from on-chain types

```bash
jun codegen 0x10ad578f5b202fd137546f2e7bc12c319dfef98871feeb429506c4b3d62bf702::pressing::RecordPressedEvent
```

### Use the gRPC client directly

```typescript
import { createGrpcClient } from "jun/grpc";

const client = createGrpcClient({ url: "fullnode.mainnet.sui.io:443" });

// Stream live checkpoints
for await (const cp of client.subscribeCheckpoints()) {
  console.log(`checkpoint ${cp.cursor}: ${cp.checkpoint.transactions.length} txs`);
}

// Fetch a single checkpoint
const cp = await client.getCheckpoint(318000000n);
```

### Fetch from the checkpoint archive

```typescript
import { createArchiveClient } from "jun/checkpoints";

const archive = createArchiveClient();
const cp = await archive.fetchCheckpoint(259000000n);
console.log(cp.checkpoint.transactions.length, "transactions");
```

### Verify a transaction trustlessly

```bash
jun verify tx 8bFJ3kN...
```

```typescript
import { verifyTransaction } from "jun/verify";

const result = await verifyTransaction("8bFJ3kN...", {
  rpcUrl: "https://fullnode.mainnet.sui.io",
  archiveUrl: "https://checkpoints.mainnet.sui.io",
  grpcUrl: "fullnode.mainnet.sui.io:443",
});
```

### AI analysis with Claude

```bash
# Chat about live chain data
jun chat --live

# Load historical checkpoints and analyze
jun chat --from-checkpoint 259063382 --count 100
```

## CLI reference

`jun c` is shorthand for `jun client`. All query commands support `--json` for machine-readable output and `--url` to override the gRPC endpoint.

### Client — query the Sui blockchain

#### Chain info

```bash
jun c info                          # chain ID, epoch, checkpoint, server version
jun c gas-price                     # current reference gas price
jun c epoch                         # current epoch (validators, stake, subsidy)
jun c epoch 1080                    # historical epoch
jun c system-state                  # full system state snapshot
jun c protocol-config               # protocol feature flags and attributes
jun c protocol-config --flags-only  # just feature flags
jun c checkpoint                    # latest checkpoint
jun c checkpoint 259000000          # specific checkpoint
jun c ping                          # gRPC latency test (native HTTP/2)
jun c ping --count 10               # more samples
```

#### Objects & accounts

```bash
jun c object 0x5                    # get object by ID
jun c obj 0x5 --bcs                 # include BCS bytes
jun c objects 0x5 0x6 0x7           # batch get multiple objects
jun c owned 0xADDR                  # list objects owned by address
jun c own 0xADDR --type 0x2::coin::Coin  # filter by type
jun c dynamic-fields 0x5            # list dynamic fields
jun c df 0x5 --limit 20             # with pagination
```

#### Balances & coins

```bash
jun c balance 0xADDR                # SUI balance (formatted)
jun c bal 0xADDR --coin-type 0x...  # specific coin type
jun c balances 0xADDR               # all coin balances
jun c coins 0xADDR                  # list individual coin objects
```

#### Transactions

```bash
jun c tx-block DIGEST               # get transaction by digest
jun c txb DIGEST --json             # raw JSON
jun c txs DIGEST1 DIGEST2           # batch get
jun c simulate BASE64_TX            # dry-run (read-only)
```

#### Move packages

```bash
jun c package 0x1                   # package modules and types
jun c function 0x2::transfer::public_transfer  # function signature
jun c package-versions 0x1          # version history
jun c coin-meta 0x2::sui::SUI      # coin metadata (name, symbol, decimals)
```

#### Name service

```bash
jun ns resolve-address adeniyi.sui  # name → address
jun ns resolve-name 0x1eb7...       # address → name
```

### Stream

Stream live checkpoints from a Sui fullnode via gRPC.

```bash
jun stream                                       # everything
jun stream --include events                      # only events
jun stream --include events --filter "marketplace::ItemListedEvent"
jun stream --duration 5m                         # stop after 5 minutes
jun stream --until-checkpoint 318200000          # stop at checkpoint
jun stream --output testnet.sqlite               # write to SQLite
jun stream --jsonl | jq '.events[]?.type'        # pipe to jq
```

### Fetch

Fetch historical checkpoints by range via gRPC. Concurrent with retry logic.

```bash
jun fetch --from-checkpoint 318000000 --count 1000
jun fetch --from-epoch 1080 --to-epoch 1082
jun fetch --from-checkpoint 318000000 --count 10000 --concurrency 32 --output backfill.sqlite
```

### Replay

Replay historical checkpoints from the [Sui checkpoint archive](https://checkpoints.mainnet.sui.io/). Every checkpoint from genesis, no pruning.

```bash
jun replay --from-checkpoint 0 --count 1000
jun replay --from-epoch 1080
jun replay --from-checkpoint 259000000 --count 100 --verify  # with cryptographic verification
jun replay --from-checkpoint 100000000 --count 10000 --concurrency 32 --output backfill.sqlite
```

With `--verify`, each checkpoint is cryptographically verified using [kei](https://github.com/unconfirmedlabs/kei) — BLS12-381 aggregate signatures checked against the validator committee. No trust in the archive CDN required.

### Verify

Cryptographically verify transactions and objects against signed checkpoints.

```bash
jun verify tx 8bFJ3kN...            # verify transaction inclusion
jun verify object 0x5               # verify object state
```

### Message

Sign and verify Sui personal messages using your local keystore.

```bash
jun msg sign "hello world"                       # sign with active keypair
jun msg sign "hello" --address 0xabc...          # specific address
jun msg verify "hello world" <signature> <addr>  # verify signature
```

### Codegen

Auto-generate field definitions from on-chain Move structs.

```bash
jun codegen 0xPACKAGE::module::EventName
```

### Config

```bash
jun config show           # view active env + cache stats
jun config set testnet    # switch environment
jun config add staging --grpc-url https://my-node.example.com
```

### Cache

Local checkpoint cache at `~/.jun/cache/checkpoints/`.

```bash
jun cache show
jun cache fill --from-epoch 1080
jun cache clear
```

### MCP

Start an [MCP](https://modelcontextprotocol.io/) server for AI assistants. Exposes Sui chain query tools and optional SQLite analysis.

```bash
jun mcp                    # chain query tools only
jun mcp mainnet.sqlite     # chain + SQLite tools
```

**Chain query tools** (always available):

| Tool | Description |
|------|-------------|
| `sui_info` | Chain overview (epoch, checkpoint, gas price, server) |
| `sui_object` | Get object by ID (type, owner, JSON content) |
| `sui_transaction` | Get transaction by digest (events, effects, balance changes) |
| `sui_balance` | Coin balance for an address (formatted with symbol) |
| `sui_owned_objects` | List objects owned by an address (with type filter) |
| `sui_dynamic_fields` | List dynamic fields of an object |
| `sui_epoch` | Epoch info (validators, stake, subsidy, timing) |
| `sui_move_function` | Move function signature (params, returns, visibility) |
| `sui_package` | Move package metadata (modules, types, linkage) |
| `sui_resolve_name` | SuiNS name ↔ address resolution |

**SQLite tools** (when database provided):

| Tool | Description |
|------|-------------|
| `query` | Run read-only SQL against checkpoint database |
| `summary` | Quick overview (row counts, time range, top events) |
| `schema` | Database DDL with row counts (resource) |

**Developer tools** (always available):

| Tool | Description |
|------|-------------|
| `jun_validate_config` | Validate a YAML indexer config, returns parsed structure or errors |
| `jun_codegen` | Generate field DSL from an on-chain Move struct type |
| `jun_generate_config` | Generate a complete YAML config from event type names (fetches fields from chain) |

**Developer resources** (always available):

| Resource | Description |
|----------|-------------|
| `jun://config-guide` | Complete YAML config reference — all fields, types, formats, examples |
| `jun://http-api` | HTTP API reference — endpoints, query safety, Prometheus metrics |

### Chat

Stream or replay checkpoints into a temporary SQLite database and chat with Claude about the data.

```bash
jun chat --live
jun chat --from-checkpoint 259063382 --count 100
jun chat --from-checkpoint 259063382 --count 100 --verify
```

### SQLite export

All data commands can write to portable SQLite files:

```bash
jun stream --duration 5m --output testnet.sqlite
jun fetch --from-checkpoint 318000000 --count 1000 --output mainnet.sqlite
jun replay --from-checkpoint 0 --count 1000 --output genesis.sqlite
```

Tables: `transactions`, `events`, `balance_changes`.

## Architecture

```
src/
  cli.ts            CLI entry point (commander.js)
  cli-helpers.ts    Shared formatting helpers
  index.ts          defineIndexer() orchestrator + run() combined mode
  grpc.ts           Native gRPC client (@grpc/grpc-js, HTTP/2)
  rpc.ts            SuiGrpcClient factory (@mysten/sui)
  archive.ts        Checkpoint archive client (fetch + zstd + proto + BCS)
  processor.ts      Event matching + BCS decode
  schema.ts         Field DSL → BCS schemas + Postgres DDL
  buffer.ts         WriteBuffer — batched Postgres writes with backpressure
  throttle.ts       AdaptiveThrottle — concurrency control via latency feedback
  gaps.ts           GapDetector — find and repair missing checkpoints
  state.ts          Cursor/watermark management
  logger.ts         Structured logging (pino)
  config.ts         Named environment config (~/.jun/config.yml)
  cache.ts          Local checkpoint cache with LRU eviction
  verify.ts         Transaction/object verification (kei)
  message.ts        Personal message signing/verification
  mcp.ts            MCP server (chain queries + SQLite tools)
  sui-bcs.ts        Complete TransactionKind BCS (all 11 variants)
  codegen.ts        On-chain type → field DSL mapping
  serve.ts          HTTP API server (Bun.serve) + metrics
  views.ts          Materialized view lifecycle management
  indexer-config.ts YAML config parser + env var substitution
  output/
    postgres.ts     Batch inserts (Bun.sql)
    sqlite.ts       SQLite export (bun:sqlite)
  # Barrel exports for SDK subpaths
  checkpoints.ts    jun/checkpoints — archive + cache
  events.ts         jun/events — processor
  pipeline.ts       jun/pipeline — buffer + throttle + gaps
  cursor.ts         jun/cursor — state manager
  signing.ts        jun/signing — message signing
proto/              Sui gRPC proto files (fetched on install)
```

## Why

Sui's gRPC API returns events as BCS binary, not JSON. The `@mysten/sui` SDK uses gRPC-web (HTTP/1.1 polyfill) and wraps everything in protobuf classes. Jun uses `@grpc/grpc-js` with proto files directly from [MystenLabs/sui-apis](https://github.com/MystenLabs/sui-apis). Data arrives as plain JS objects. BCS bytes go straight to `@mysten/bcs` for decoding.

| | `@mysten/sui` gRPC | Jun |
|---|---|---|
| Transport | gRPC-web over HTTP/1.1 fetch | Native gRPC over HTTP/2 |
| Event data | `ev.json` (never populated) | `ev.contents.value` (BCS bytes) |
| Protobuf types | Wrapped in `google.protobuf.Value` | Plain JS objects |
| Streaming | Long-polling under the hood | Real server-push streaming |

## License

Apache-2.0
