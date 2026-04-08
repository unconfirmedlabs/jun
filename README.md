# jun

Sui blockchain data pipeline for Bun. Rust-accelerated checkpoint decoding, native gRPC streaming, and multi-destination storage.

```
Archive (zstd) ──▶ Rust decoder (worker pool) ──▶ SQLite / Postgres / ClickHouse
gRPC stream    ──▶                              ──▶ NATS / SSE / stdout
```

## Install

```bash
bun install
```

Prebuilt Rust decoder included for `darwin-arm64` and `linux-x64`. Build from source: `cd native/rust-decoder && cargo build --release`.

## Commands

### Cache fill (prefetch)

Download checkpoints to local disk at maximum throughput before indexing. Single event loop, 3K concurrent connections — saturates a 10 Gbit link.

```bash
# Prefetch a completed epoch
jun index prefetch \
  --epoch 1090 \
  --archive-url https://checkpoints.mainnet.sui.io \
  --grpc-url hayabusa.mainnet.unconfirmed.cloud:443

# Prefetch explicit range
jun index prefetch \
  --from 261843573 --to 262511698 \
  --archive-url https://checkpoints.mainnet.sui.io
```

Checkpoints are cached to `~/.jun/cache/checkpoints/{network}/`. Subsequent `replay-chain` runs skip the download phase entirely.

**Key flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--epoch <n>` | — | Prefetch a completed epoch (resolves range via gRPC) |
| `--from / --to` | — | Explicit checkpoint range |
| `--archive-url` | `JUN_ARCHIVE_URL` | Archive base URL |
| `--grpc-url` | `JUN_GRPC_URL` | gRPC endpoint (epoch resolution only) |
| `--concurrency <n>` | `3000` | Concurrent fetch connections |
| `--network <name>` | `mainnet` | Cache sub-directory |

### Backfill (replay-chain)

Index historical checkpoints from the Sui archive. Two-phase: parallel HTTP fetch → cached disk → parallel Rust decode → write. Run `prefetch` first to maximize throughput.

```bash
# Backfill a completed epoch → per-table SQLite files
jun index replay-chain \
  --epoch 1090 \
  --archive-url https://checkpoints.mainnet.sui.io \
  --grpc-url hayabusa.mainnet.unconfirmed.cloud:443 \
  --output ./epoch-1090

# Explicit checkpoint range → ClickHouse
jun index replay-chain \
  --from 261843573 --to 262511698 \
  --archive-url https://checkpoints.mainnet.sui.io \
  --grpc-url hayabusa.mainnet.unconfirmed.cloud:443 \
  --clickhouse http://localhost:8123

# Explicit checkpoint range → Postgres
jun index replay-chain \
  --from 261843573 --to 262511698 \
  --archive-url https://checkpoints.mainnet.sui.io \
  --grpc-url hayabusa.mainnet.unconfirmed.cloud:443 \
  --postgres postgresql://user:pass@localhost/jun
```

**Key flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--epoch <n>` | — | Backfill a completed epoch (resolves range via gRPC) |
| `--from / --to` | — | Explicit checkpoint range |
| `--archive-url` | `JUN_ARCHIVE_URL` | Checkpoint archive base URL |
| `--grpc-url` | `JUN_GRPC_URL` | gRPC endpoint for epoch resolution |
| `--network <name>` | `mainnet` | Network name for cache directory scoping |
| `--output <dir>` | `./replay-chain` | Per-table SQLite output directory |
| `--clickhouse <url>` | — | ClickHouse HTTP URL |
| `--postgres <url>` | — | Postgres connection URL |
| `--workers <n>` | auto | Rust decoder threads |
| `--concurrency <n>` | `3000` | Archive fetch concurrency |
| `--batch-size <n>` | `1000` | Write buffer flush threshold |

### Live indexing (stream-chain)

Subscribe to the gRPC checkpoint stream and write continuously.

```bash
# Live stream → NATS
jun index stream-chain \
  --grpc-url hayabusa.mainnet.unconfirmed.cloud:443 \
  --nats nats://localhost:4222

# Live stream → SSE on port 8080
jun index stream-chain \
  --grpc-url hayabusa.mainnet.unconfirmed.cloud:443 \
  --sse 8080

# Live stream → Postgres
jun index stream-chain \
  --grpc-url hayabusa.mainnet.unconfirmed.cloud:443 \
  --postgres postgresql://user:pass@localhost/jun
```

### Chain queries

```bash
jun client balance <address>      # coin balances
jun client object <id>            # object state
jun ns resolve <name>             # .sui name resolution
jun verify tx <digest>            # cryptographic verification
```

## Table selection

By default all tables are indexed. Select specific tables with flags:

```bash
jun index replay-chain --transactions --balance-changes --object-changes ...
```

| Flag | Tables written |
|------|---------------|
| `--transactions` | `transactions`, `move_calls` |
| `--balance-changes` | `balance_changes` |
| `--object-changes` | `object_changes` |
| `--events` | `raw_events` |
| `--dependencies` | `transaction_dependencies` |
| `--inputs` | `transaction_inputs` |
| `--commands` | `commands` |
| `--system-transactions` | `system_transactions` |
| `--unchanged-consensus-objects` | `unchanged_consensus_objects` |
| `--checkpoints` | `checkpoints` |

## Storage tables

| Table | Key columns |
|-------|-------------|
| `transactions` | digest, sender, success, gas costs, epoch, error info, lamport_version |
| `move_calls` | tx_digest, call_index, package, module, function |
| `balance_changes` | tx_digest, address, coin_type, amount |
| `object_changes` | tx_digest, object_id, change_type, object_type, owner before/after |
| `transaction_dependencies` | tx_digest, depends_on_digest |
| `transaction_inputs` | tx_digest, input_index, kind, object_id, coin_type, amount |
| `commands` | tx_digest, command_index, kind, package, module, function, args |
| `system_transactions` | tx_digest, kind, data (JSON) |
| `unchanged_consensus_objects` | tx_digest, object_id, kind, version |
| `checkpoints` | sequence_number, epoch, digest, timestamp, rolling gas totals |
| `raw_events` | tx_digest, event_seq, package_id, module, event_type, contents |

## Performance

Benchmarked on a bare metal server (AMD EPYC, 10 Gbit), 50K mainnet checkpoints from disk cache, 24 Rust decoder workers.

### Per-table throughput (SQLite, replay)

Write throughput varies by row cardinality. Plan accordingly when backfilling:

| Table | cp/s |
|-------|------|
| `checkpoints` | 20,955 |
| `commands` | 18,268 |
| `balance_changes` | 16,622 |
| `events` | 11,701 |
| `system_transactions` | 11,103 |
| `unchanged_consensus_objects` | 10,810 |
| `transaction_inputs` | 10,469 |
| `transactions` + `move_calls` | 4,589 |
| `transaction_dependencies` | 3,246 |
| `object_changes` | 2,318 |

`object_changes` is the bottleneck for full-table runs (~15–30 rows/tx, 15 columns). If you don't need object change history, exclude it with table selection flags for a 5–9× throughput improvement.

### Backend comparison (all tables, replay)

| Backend | cp/s | Notes |
|---------|------|-------|
| SQLite (UNLOGGED → WAL at shutdown) | ~2,300 | Bottlenecked by object_changes |
| ClickHouse (LZ4, async insert off) | ~6,300 | Best for analytics queries |
| Postgres (UNNEST, UNLOGGED tables) | ~5,360 | Good for OLTP + analytics |

All backends use replay-optimized adapters: UNLOGGED tables or equivalent bulk-insert settings during load, indexes built at shutdown.

### Checkpoint cache

Phase 1 fetches and caches compressed checkpoints to `~/.jun/cache/checkpoints/{network}/` before decoding. Subsequent runs skip the download entirely. At 300 concurrency: ~2,500–3,000 req/s fetch rate (~26 KB/checkpoint compressed).

## Architecture

```
src/
  pipeline/
    pipeline.ts               Orchestrator: Source → Processor → Writer → Broadcast
    types.ts                   Core interfaces (Source, Processor, Storage, etc.)
    write-buffer.ts            Batching + backpressure
    sources/
      grpc.ts                  Live gRPC subscription (native HTTP/2)
      archive.ts               Archive backfill (parallel fetch + Rust decode)
    processors/
      events.ts                BCS event decoding
      balanceChanges.ts        Balance change extraction
      transactionBlocks.ts     Transaction + move call extraction
    destinations/
      clickhouse.ts            ClickHouse (replay + live adapters)
      per-table-sqlite.ts      Per-table SQLite files (replay + live adapters)
      per-table-postgres.ts    Per-table Postgres (replay + live adapters)
      sse.ts                   Server-Sent Events broadcast
      nats.ts                  NATS broadcast
      stdout.ts                JSONL stdout broadcast
  grpc.ts                      gRPC client
  schema.ts                    Field DSL → BCS schemas + DDL generation
  normalize.ts                 Address/type normalization
  db.ts                        Postgres + SQLite connection factories
  cli.ts                       CLI entry point
native/
  rust-decoder/                Rust checkpoint decoder source
  lib/                         Prebuilt binaries (darwin-arm64, linux-x64)
```

Each backend has separate **replay** and **live** adapters optimized for their workload:
- **Replay** (batch backfill): bulk-optimized — UNLOGGED tables, deferred indexes, LZ4 compression, no deduplication overhead
- **Live** (continuous indexing): reliability-optimized — logged tables, immediate indexes, idempotent ON CONFLICT DO NOTHING, async insert for ClickHouse

## License

Apache-2.0
