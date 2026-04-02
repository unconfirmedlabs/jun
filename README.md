# jun

Sui data pipeline for Bun. Composable Source → Processor → Destination architecture with native gRPC streaming, BCS decoding, and multi-destination output.

```
Sui Fullnode ──gRPC──▶ Sources ──▶ Processors ──▶ Destinations
                       (live)     (events)        (Postgres)
                       (archive)  (balances)       (SQLite)
                                                   (SSE)
                                                   (NATS)
```

## Install

```bash
bun install
```

## Quick start

### YAML config

```yaml
# config.yml
sources:
  live:
    grpc: fullnode.testnet.sui.io:443
  backfill:
    archive: https://checkpoints.testnet.sui.io
    from: 316756645

processors:
  events:
    my_event:
      type: "0xPKG::module::MyEvent"
  balances:
    coinTypes:
      - "0x2::sui::SUI"

storage:
  postgres:
    url: $DATABASE_URL
```

```bash
jun pipeline run config.yml
```

That's it. Jun fetches event field definitions from the chain automatically, creates tables, runs live + backfill concurrently, and writes events + balance changes to Postgres.

### SDK

```typescript
import { createPipeline } from "jun";
import { createGrpcLiveSource } from "jun/pipeline/sources/grpc-live";
import { createBalanceTracker } from "jun/pipeline/processors/balance-tracker";
import { createPostgresDestination } from "jun/pipeline/destinations/postgres";

await createPipeline()
  .source(createGrpcLiveSource({ url: "fullnode.testnet.sui.io:443" }))
  .processor(createBalanceTracker({ coinTypes: ["0x2::sui::SUI"] }))
  .destination(createPostgresDestination({ url: process.env.DATABASE_URL! }))
  .run();
```

## Pipeline

### Sources

| Source | Description |
|--------|-------------|
| `live` | gRPC subscription to Sui fullnode. Real-time checkpoints. |
| `backfill` | Fetch from checkpoint archive. Historical data from genesis. Worker pool for parallel decode. |

```yaml
sources:
  live:
    grpc: fullnode.testnet.sui.io:443
  backfill:
    archive: https://checkpoints.testnet.sui.io
    from: 316756645          # number, "epoch:N", "timestamp:ISO", "package:0x..."
    concurrency: 10          # parallel fetches (default: 10)
    workers: 4               # decoder threads (default: auto)
```

### Processors

| Processor | Description |
|-----------|-------------|
| `events` | Match events by Move type, auto-decode BCS. Fields resolved from chain. |
| `balances` | Track coin balance changes. Running totals + ledger. |

```yaml
processors:
  events:
    my_event:                          # handler name = table name
      type: "0xPKG::module::MyEvent"
      # Fields auto-resolved from chain — no manual definition needed
    item_listed:
      type: "0xdead...::marketplace::ItemListedEvent"

  balances:
    coinTypes:
      - "0x2::sui::SUI"
      - "0xdba3...::usdc::USDC"
    # or: coinTypes: "*"  for all coin types
```

### Storage (persistent, batched, retried)

| Storage | Description |
|---------|-------------|
| `postgres` | Batch inserts. Event tables + balance tables (ledger + running totals). |
| `sqlite` | Local SQLite file with WAL mode. |

```yaml
storage:
  postgres:
    url: $DATABASE_URL
  # or:
  sqlite:
    path: ./events.db
```

### Broadcast (fire-and-forget, low latency)

| Broadcast | Description |
|-----------|-------------|
| `sse` | Server-Sent Events streaming with filtering. |
| `nats` | NATS pub/sub with subject hierarchy. |
| `stdout` | Console output (JSONL or formatted). Default if nothing configured. |

```yaml
broadcast:
  sse:
    port: 8080
  nats:
    url: nats://localhost:4222
    prefix: jun
  stdout:
    format: jsonl
```

## Example configs

Ready-to-use configs in the `configs/` directory:

| Config | Use case |
|--------|----------|
| [`balance-tracker.yml`](configs/balance-tracker.yml) | Track SUI holders with running totals. Whale dashboards, balance alerts. |
| [`event-indexer.yml`](configs/event-indexer.yml) | Index specific Move events to Postgres. dApp analytics, contract monitoring. |
| [`multi-coin-tracker.yml`](configs/multi-coin-tracker.yml) | Track all coin types (`*`). DeFi dashboards, portfolio tracking. |
| [`live-stream.yml`](configs/live-stream.yml) | Stream to console — no database. Quick inspection, debugging. |
| [`full-stack.yml`](configs/full-stack.yml) | Events + balances + Postgres + SSE + NATS. Full production setup. |
| [`local-dev.yml`](configs/local-dev.yml) | SQLite storage, no external deps. Prototyping, local exploration. |

```bash
# Try any config:
jun pipeline run configs/live-stream.yml
DATABASE_URL=postgres://... jun pipeline run configs/balance-tracker.yml
```

## Balance indexing

Jun tracks coin holder balances in real-time with two tables:

- `balance_changes` — every individual change (the ledger)
- `balances` — current running totals (the snapshot)

```sql
-- Top SUI holders
SELECT address, balance FROM balances
WHERE coin_type = '0x2::sui::SUI' AND balance > 0
ORDER BY balance DESC LIMIT 100;

-- Single address balance (O(1) primary key lookup)
SELECT balance FROM balances
WHERE address = '0x...' AND coin_type = '0x2::sui::SUI';

-- Recent transfers
SELECT * FROM balance_changes
WHERE address = '0x...'
ORDER BY checkpoint_seq DESC LIMIT 20;
```

Balance changes are computed from:
- **Live (gRPC)**: pre-computed by fullnode
- **Archive (backfill)**: derived from coin object diffs + accumulator writes in worker threads

## Remote config

Fetch config from S3 or HTTPS instead of a local file:

```bash
jun pipeline run --config-url s3://my-bucket/configs/indexer.yml
# or
JUN_CONFIG_URL=s3://my-bucket/configs/indexer.yml jun pipeline run
```

S3 credentials via standard AWS env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`).

## Logging

Two independent output channels:

| Flag | Human output (stdout) | Machine logs (stderr) | Use case |
|------|----------------------|----------------------|----------|
| (default) | ✅ | ❌ | Developer, demo |
| `--log` | ✅ | ✅ info | Local dev with logs |
| `--log=debug` | ✅ | ✅ debug | Debugging |
| `--quiet` | ❌ | ✅ info | Production, Docker |

```bash
jun pipeline run config.yml                    # human output only
jun pipeline run config.yml --log              # + JSON logs to stderr
jun pipeline run config.yml --log=debug        # + debug logs
jun pipeline run config.yml --quiet            # production (logs only)
jun pipeline run config.yml --quiet --log 2>/var/log/jun.jsonl
```

## CLI reference

### Pipeline

```bash
jun pipeline run config.yml                    # run from YAML config
jun pipeline run --config-url s3://bucket/key  # run from remote config
jun pipeline run config.yml --quiet --log      # production mode
```

### Chain queries

```bash
jun c info                          # chain ID, epoch, checkpoint
jun c object 0x5                    # get object
jun c balance 0xADDR                # SUI balance
jun c tx-block DIGEST               # transaction details
jun c package 0x1                   # package modules
jun c ping                          # gRPC latency
```

### Other commands

```bash
jun verify tx DIGEST                # cryptographic verification
jun codegen 0xPKG::module::Struct   # generate field definitions
jun mcp                             # MCP server for AI
jun chat --live                     # AI chat with chain data
jun config show                     # environment management
jun cache show                      # checkpoint cache stats
jun ns resolve-address name.sui     # name service
```

## SDK exports

```
jun                                     createPipeline, types
jun/pipeline/sources/grpc-live          createGrpcLiveSource
jun/pipeline/sources/archive            createArchiveSource
jun/pipeline/processors/event-decoder   createEventDecoder
jun/pipeline/processors/balance-tracker createBalanceTracker
jun/pipeline/destinations/postgres      createPostgresDestination
jun/pipeline/destinations/sqlite        createSqliteDestination
jun/pipeline/destinations/sse           createSseDestination
jun/pipeline/destinations/nats          createNatsDestination
jun/pipeline/destinations/stdout        createStdoutDestination
jun/pipeline/config                     parsePipelineConfig
jun/grpc                                createGrpcClient
jun/schema                              buildBcsSchema, generateDDL
jun/checkpoints                         createArchiveClient, cache utils
jun/verify                              verifyTransaction, verifyObject
jun/codegen                             generateFieldDSL
jun/mcp                                 createMcpServer
jun/signing                             signMessage, verifyMessage
jun/logger                              createLogger
jun/normalize                           normalizeSuiAddress, normalizeCoinType
```

## Performance

Production pipeline benchmarks on AMD Ryzen 9 9950X3D (16C/32T, 10gbit). Archive backfill with balance tracking for all coin types (`coinTypes: "*"`). Full pipeline: HTTP fetch + zstd decompress + protobuf decode + BCS event decode + balance computation.

### Backfill throughput

| Mode | Throughput | Notes |
|------|-----------|-------|
| Cached (disk) | **1,919 cp/s** | 100K checkpoints sustained, 8 workers |
| Network (10gbit) | **1,247 cp/s** | 74K checkpoints in 60s, no cache |

### Scaling by worker count (5,000 checkpoints, cached)

| Workers | Throughput |
|---------|-----------|
| 1 | 363 cp/s |
| 4 | 1,111 cp/s |
| 8 | **1,687 cp/s** |
| 16 | 1,375 cp/s |

**Notes:**
- Sweet spot is 8 workers. Beyond that, Worker IPC overhead and GC pressure cause regression.
- Custom BCS parsers (coin objects, transaction effects, events) provide 2-7x speedup over native `@mysten/sui/bcs`.
- Custom protobuf wire format parser bypasses protobufjs for checkpoint decoding.
- Streaming pipeline: fetch, decode, and yield run concurrently with backpressure.
- Set `JUN_NATIVE_BCS=1` to fall back to native parsers for debugging.

## Architecture

```
src/
  pipeline/
    types.ts              Source, Processor, Destination interfaces
    pipeline.ts           Orchestrator (connects S→P→D)
    config-parser.ts      YAML config → pipeline components
    sources/
      grpc-live.ts        Live gRPC streaming
      archive.ts          Archive with worker pool
    processors/
      event-decoder.ts    Event matching + BCS decode
      balance-tracker.ts  Balance change extraction
    destinations/
      postgres.ts         Batch inserts (events + balances)
      sqlite.ts           SQLite with WAL
      sse.ts              SSE streaming
      nats.ts             NATS publishing
      stdout.ts           Console output
  grpc.ts                 Native gRPC client (@grpc/grpc-js)
  archive.ts              Checkpoint archive client
  archive-balance.ts      Balance computation from archive ObjectSet
  schema.ts               Field DSL → BCS schemas + Postgres DDL
  normalize.ts            @mysten/sui/utils normalization
  logger.ts               Structured logging (pino → stderr)
  cli.ts                  CLI commands
  mcp.ts                  MCP server (chain queries + dev tools)
```

## License

Apache-2.0
