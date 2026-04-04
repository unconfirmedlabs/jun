# Jun

Sui data pipeline and chain toolkit for Bun. Native gRPC streaming, Zig-accelerated checkpoint decoding, BCS binary decoding, SQLite/Postgres storage.

## Runtime

- Bun only. No Node.js.
- `Bun.SQL` for Postgres, `bun:sqlite` for SQLite
- `@grpc/grpc-js` for native gRPC (HTTP/2)
- Bun auto-loads `.env`

## Architecture

```
src/
  pipeline/
    pipeline.ts               Orchestrator: Source → Processor → Writer → Broadcast
    config-parser.ts           YAML config + CLI flags → pipeline components
    types.ts                   Core interfaces (Source, Processor, Storage, etc.)
    writer.ts                  WriterChannel interface (async writes)
    write-buffer.ts            Batching + backpressure
    sources/
      grpc.ts                  Live gRPC subscription (native HTTP/2)
      grpc-web.ts              Browser gRPC-Web source
      archive.ts               Archive backfill (parallel fetch + Zig decode)
      archive-web.ts           Browser archive backfill (fzstd)
    processors/
      events.ts                BCS event decoding
      balanceChanges.ts        Balance change extraction
      transactionBlocks.ts     Transaction + move call extraction
    destinations/
      sql.ts                   Unified SQLite/Postgres storage
      sql-web.ts               Browser sql.js storage
      sse.ts                   Server-Sent Events broadcast
      nats.ts                  NATS broadcast
      stdout.ts                JSONL stdout broadcast
    writers/
      sqlite.ts                SQLite WriterChannel (Bun.Worker)
      sqlite.worker.ts         Worker script for async SQLite writes
      postgres.ts              Postgres WriterChannel (async queue)
  grpc.ts                      gRPC client (subscribe, getCheckpoint, getEpoch, getDatatype)
  schema.ts                    Field DSL → BCS schemas + DDL generation
  normalize.ts                 Address/type normalization + stripGenerics
  codegen.ts                   On-chain struct → field DSL auto-resolution
  decompiler.ts                Move bytecode decompiler (auto-detects native/WASM)
  decompiler-wasm.ts           Browser WASM decompiler
  decompiler-native.ts         Server native decompiler
  package-reader.ts            Fetch + parse Sui Move package modules
  db.ts                        createPostgresConnection + createSqliteConnection
  sql-helpers.ts               Shared SQL utilities
  timestamp.ts                 Proto timestamp parsing
  uleb.ts                      ULEB128 encoding
  state.ts                     Cursor/watermark persistence
  cli.ts                       CLI entry point (Commander.js)
  config.ts                    Local config (~/.jun/config.yml)
native/
  checkpoint_processor.zig     Zig FFI checkpoint decoder
  build.zig                    Cross-compilation build system
  vendor/zstd/                 Vendored zstd C source
  lib/                         Prebuilt binaries (darwin-arm64, darwin-x64, linux-x64, linux-arm64)
```

## CLI

### Pipeline

```bash
# Continuous (backfill + live)
jun pipeline run config.yml

# Snapshot (backfill only, exit when done)
jun pipeline snapshot \
  --epoch 1080 \
  --transaction-blocks \
  --coin-type '*' \
  --sqlite /tmp/epoch.db \
  --sqlite-export s3://bucket/mainnet/epoch_1080.db \
  --sqlite-export-split-datasets \
  --concurrency 300 \
  --workers 24 \
  --quiet --yes
```

### CLI Flags (match config keys 1:1)

| Flag | Config Key | Description |
|------|-----------|-------------|
| `--grpc-url` | `sources.grpcUrl` | gRPC endpoint |
| `--archive-url` | `sources.archiveUrl` | Archive base URL |
| `--epoch` | `sources.epoch` | Backfill completed epoch |
| `--start-checkpoint` | `sources.startCheckpoint` | Start (inclusive) |
| `--end-checkpoint` | `sources.endCheckpoint` | End (inclusive) |
| `--concurrency` | `sources.concurrency` | Archive fetch concurrency |
| `--workers` | `sources.workers` | Decoder worker threads |
| `--transaction-blocks` | `processors.transactionBlocks` | Index transactions + move calls |
| `--coin-type` | `processors.balances.coinTypes` | Balance tracking (repeatable, "*" for all) |
| `--event-type` | `processors.events.{name}.type` | Event types (repeatable) |
| `--sqlite` | `storage.sqlite` | SQLite output path |
| `--postgres` | `storage.postgres` | Postgres URL |
| `--sqlite-export` | `storage.sqliteExport` | VACUUM + upload to S3 |
| `--sqlite-export-split-datasets` | - | Split export into `transactions`, `balance_changes`, `balances`, and `events` DBs |
| `--stdout` | `broadcast.stdout` | JSONL stdout |
| `--sse` | `broadcast.sse` | SSE server port |
| `--nats` | `broadcast.nats` | NATS URL |
| `--snapshot` | (command) | `jun pipeline snapshot` = backfill only |
| `--quiet` | quiet | Suppress stdout (progress bar still shows on stderr) |
| `--yes` | - | Skip confirmation prompt |

### Move Decompiler

```bash
jun move decompile 0x2 -m coin -m bag
jun move decompile 0x2 --output ./sources
```

### Other Commands

```bash
jun client balance <address>
jun client object <id>
jun ns resolve <name>
jun verify tx <digest>
```

## Config Schema (Canonical)

```yaml
sources:
  grpcUrl: "hayabusa.mainnet.unconfirmed.cloud:443"
  archiveUrl: "https://checkpoints.mainnet.sui.io"
  epoch: 1080
  startCheckpoint: 100000
  endCheckpoint: 200000
  concurrency: 300
  workers: 24

processors:
  transactionBlocks: true
  balances:
    coinTypes: "*"
  events:
    swaps:
      type: "0xdee9::clob_v2::OrderPlaced"

storage:
  sqlite: ./data.db
  # OR
  postgres: postgres://localhost/mydb

broadcast:
  stdout: true
  sse: 8080
  nats: nats://localhost:4222

network: mainnet
```

Legacy config keys (`sources.live.grpc`, `sources.backfill.from`) still work via `normalizeConfig()`.

## Processors

| Processor | Name | Output |
|-----------|------|--------|
| Events | `events` | Decoded BCS events matching configured types |
| Balance Changes | `balanceChanges` | Per-transaction balance changes (all or filtered coin types) |
| Transaction Blocks | `transactionBlocks` | Transaction records + move call records |

## Storage Tables

### transactions
`digest, sender, success, computation_cost, storage_cost, storage_rebate, move_call_count, checkpoint_seq, sui_timestamp`

### move_calls
`tx_digest, call_index, package, module, function, checkpoint_seq, sui_timestamp`

### balance_changes
`tx_digest, checkpoint_seq, address, coin_type, amount, sui_timestamp`

### balances (materialized)
`address, coin_type, balance, last_checkpoint`

## Snapshot Mode

`jun pipeline snapshot` optimizations:
- Tables created without PRIMARY KEY/UNIQUE constraints (bulk insert speed)
- `PRAGMA synchronous=OFF, journal_mode=OFF, locking_mode=EXCLUSIVE`
- No `ON CONFLICT` checks (sequential checkpoints = no duplicates)
- Deferred index creation at shutdown
- Balance materialization at shutdown (skip incremental upserts)
- Duplicate row dedup before unique index creation

## Native Zig Decoder

Checkpoint decoding uses a native Zig library via Bun FFI:
- Decompresses zstd, parses protobuf, extracts BCS data
- Returns binary format: transactions + move calls + events + balance changes
- 24 worker threads for parallel decode
- Prebuilt for darwin-arm64, darwin-x64, linux-x64, linux-arm64
- Falls back to JS if native lib not available
- Cross-compile: `cd native && zig build -Doptimize=ReleaseFast [-Dtarget=x86_64-linux-musl]`

## Browser Targets

Jun exports browser-compatible variants:
- `jun/pipeline/sources/grpc-web` — gRPC-Web via @mysten/sui SuiGrpcClient
- `jun/pipeline/sources/archive-web` — archive backfill with fzstd
- `jun/pipeline/destinations/sql-web` — sql.js SQLite WASM
- `jun/decompiler/wasm` — Move bytecode decompiler (87KB WASM)
- `jun/package-reader` — fetch + parse package modules

## Key Rules

- BCS field order must match Move struct exactly (positional encoding)
- Phantom type params don't affect BCS encoding
- Config keys match CLI flags 1:1 (no translation layer)
- `normalizeConfig()` handles legacy config formats
- Database connection factories in `db.ts` (single source of truth)
- Shared utilities: `sql-helpers.ts`, `timestamp.ts`, `uleb.ts`, `normalize.ts`
