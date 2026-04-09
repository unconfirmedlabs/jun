# Jun

Sui data pipeline and chain toolkit for Bun. Pure infra — no app-specific code. Native gRPC streaming, Rust-accelerated checkpoint decoding, BCS binary decoding, ClickHouse/SQLite/Postgres storage.

## Runtime

- Bun only. No Node.js.
- `Bun.SQL` for Postgres, `bun:sqlite` for SQLite
- `@grpc/grpc-js` for native gRPC (HTTP/2)
- Bun auto-loads `.env`

## Configuration (12-factor)

All environment-identity config follows the 12-factor pattern: **CLI flag > env var > config file > default**.

### Environment variables

| Env var | Default | Purpose |
|---------|---------|---------|
| `JUN_NETWORK` | `mainnet` | Network identity — affects gRPC client, coin/protocol resolution, cache scoping |
| `JUN_GRPC_URL` | from `~/.jun/config.yml` | Sui gRPC endpoint |
| `JUN_ARCHIVE_URL` | from `~/.jun/config.yml` | Sui checkpoint archive base URL |
| `JUN_CLICKHOUSE_URL` | `http://localhost:8123` | ClickHouse HTTP endpoint |
| `JUN_CLICKHOUSE_DATABASE` | `jun` | ClickHouse database name |
| `JUN_CLICKHOUSE_USERNAME` | `default` | ClickHouse auth username |
| `JUN_CLICKHOUSE_PASSWORD` | (empty) | ClickHouse auth password |
| `JUN_POSTGRES_URL` | (none) | Postgres connection URL |
| `JUN_BCS_DECODER` | (auto) | Set to `js` to force JS decoder instead of native Rust |
| `JUN_DECODER_MAX_CAPACITY_MB` | `128` | Rust decoder output buffer cap |
| `JUN_DECODER_BINARY_MAX_CAPACITY_MB` | `256` | Rust decoder binary buffer cap |

### Multi-network setup

```bash
# Testnet — just set env vars, same codebase
export JUN_NETWORK=testnet
export JUN_CLICKHOUSE_DATABASE=jun_testnet
jun index replay-chain --epoch 500 --clickhouse http://localhost:8123
```

### Config file (`~/.jun/config.yml`)

Managed via `jun config` commands. Stores named environments with gRPC and archive URLs.

```yaml
active_env: mainnet
envs:
  mainnet:
    grpc_url: https://fullnode.mainnet.sui.io
    archive_url: https://checkpoints.mainnet.sui.io
  testnet:
    grpc_url: https://fullnode.testnet.sui.io
    archive_url: https://checkpoints.testnet.sui.io
```

Switch active environment: `jun config use testnet`

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
      archive.ts               Archive backfill (parallel fetch + Rust decode)
    processors/
      events.ts                BCS event decoding
      balanceChanges.ts        Balance change extraction
      transactionBlocks.ts     Transaction + move call extraction
    destinations/
      clickhouse.ts            ClickHouse analytics storage
      per-table-sqlite.ts      Per-table SQLite files (one file per table type)
      sse.ts                   Server-Sent Events broadcast
      nats.ts                  NATS broadcast
      stdout.ts                JSONL stdout broadcast
  grpc.ts                      gRPC client (subscribe, getCheckpoint, getEpoch, getDatatype)
  rpc.ts                       Shared SuiGrpcClient factory (network-aware via JUN_NETWORK)
  schema.ts                    Field DSL → BCS schemas + DDL generation
  normalize.ts                 Address/type normalization + stripGenerics
  codegen.ts                   On-chain struct → field DSL auto-resolution
  package-reader.ts            Fetch + parse Sui Move package modules
  db.ts                        createPostgresConnection + createSqliteConnection
  sql-helpers.ts               Shared SQL utilities
  timestamp.ts                 Proto timestamp parsing
  uleb.ts                      ULEB128 encoding
  state.ts                     Cursor/watermark persistence
  cli.ts                       CLI entry point (Commander.js)
  config.ts                    Local config (~/.jun/config.yml)
native/
  rust-decoder/                Rust checkpoint decoder source
  lib/                         Prebuilt Rust binaries (darwin-arm64, linux-x64)
```

## CLI

```bash
# Backfill a completed epoch → per-table SQLite files
jun index replay-chain \
  --epoch 1080 \
  --output ./epoch-1080 \
  --concurrency 300 \
  --workers 24 \
  --batch-size 1000 \
  --quiet --yes

# Backfill to ClickHouse (uses JUN_CLICKHOUSE_URL if set, or --clickhouse flag)
jun index replay-chain \
  --from 258828309 --to 258878308 \
  --clickhouse http://localhost:8123

# Live gRPC stream → ClickHouse
jun index stream-chain \
  --clickhouse http://localhost:8123

# Live gRPC stream → SSE broadcast
jun index stream-chain --sse 8080

jun client balance <address>
jun client object <id>
jun ns resolve <name>
jun verify tx <digest>
```

### replay-chain flags

| Flag | Description |
|------|-------------|
| `--epoch <n>` | Backfill completed epoch (resolves range via gRPC) |
| `--from / --to` | Explicit checkpoint range |
| `--archive-url <url>` | Archive base URL (or `JUN_ARCHIVE_URL`) |
| `--grpc-url <url>` | gRPC endpoint (or `JUN_GRPC_URL`) |
| `--network <name>` | Network name for cache scoping (or `JUN_NETWORK`, default: mainnet) |
| `--output <dir>` | Per-table SQLite output directory |
| `--clickhouse <url>` | ClickHouse HTTP URL (or `JUN_CLICKHOUSE_URL`) |
| `--clickhouse-database <db>` | ClickHouse database (or `JUN_CLICKHOUSE_DATABASE`, default: jun) |
| `--concurrency <n>` | Archive fetch concurrency (default: 100) |
| `--workers <n>` | Decoder worker threads (default: auto) |
| `--batch-size <n>` | Write buffer flush threshold in checkpoints (default: 1000) |
| `--all` | Index all tables (default) |
| `--transactions` `--balance-changes` `--object-changes` `--events` etc. | Per-table flags |

## Processors

| Processor | Name | Output |
|-----------|------|--------|
| Events | `events` | Decoded BCS events matching configured types |
| Balance Changes | `balanceChanges` | Per-transaction balance changes (all or filtered coin types) |
| Transaction Blocks | `transactionBlocks` | Transaction records + move call records |
| Object Changes | `objectChanges` | Per-object state change (CREATED/MUTATED/DELETED/WRAPPED/UNWRAPPED/PACKAGE_WRITE) |
| Dependencies | `dependencies` | Transaction dependency edges from `effects.dependencies` |
| Inputs | `inputs` | Programmable transaction inputs (PURE, IMMUTABLE_OR_OWNED, SHARED, RECEIVING, FUNDS_WITHDRAWAL) |
| Commands | `commands` | Every PTB command (MoveCall, TransferObjects, SplitCoins, MergeCoins, Publish, Upgrade, MakeMoveVector) |
| System Transactions | `systemTransactions` | Non-programmable transactions (Genesis, ChangeEpoch, ConsensusCommitPrologue*, etc.) |
| Unchanged Consensus Objects | `unchangedConsensusObjects` | Read-only consensus object references |

## Storage Tables

### transactions
`digest, sender, success, computation_cost, storage_cost, storage_rebate, non_refundable_storage_fee, move_call_count, checkpoint_seq, sui_timestamp, epoch, error_kind, error_description, error_command_index, error_abort_code, error_module, error_function, events_digest, lamport_version, dependency_count`

### move_calls
`tx_digest, call_index, package, module, function, checkpoint_seq, sui_timestamp`

### balance_changes
`tx_digest, checkpoint_seq, address, coin_type, amount, sui_timestamp`

### balances (materialized)
`address, coin_type, balance, last_checkpoint`

### checkpoints
`sequence_number, epoch, digest, previous_digest, content_digest, sui_timestamp, total_network_transactions, rolling_computation_cost, rolling_storage_cost, rolling_storage_rebate, rolling_non_refundable_storage_fee`

### object_changes
`tx_digest, object_id, change_type, object_type, input_version, input_digest, input_owner, input_owner_kind, output_version, output_digest, output_owner, output_owner_kind, is_gas_object, checkpoint_seq, sui_timestamp`

### transaction_dependencies
`tx_digest, depends_on_digest, checkpoint_seq, sui_timestamp`

### transaction_inputs
`tx_digest, input_index, kind, object_id, version, digest, mutability, initial_shared_version, pure_bytes, amount, coin_type, source, checkpoint_seq, sui_timestamp`

### commands
`tx_digest, command_index, kind, package, module, function, type_arguments, args, checkpoint_seq, sui_timestamp`

### system_transactions
`tx_digest, kind, data (JSON), checkpoint_seq, sui_timestamp`

### unchanged_consensus_objects
`tx_digest, object_id, kind, version, digest, object_type, checkpoint_seq, sui_timestamp`

## Native Rust Decoder

Checkpoint decoding uses a prebuilt Rust library via Bun FFI:
- Decompresses zstd, parses protobuf, extracts BCS data
- Returns binary format: transactions + move calls + events + balance changes
- 24 worker threads for parallel decode
- Prebuilt for darwin-arm64, linux-x64
- Falls back to JS if native lib not available
- Build: `cd native/rust-decoder && cargo build --release`

## Code Quality

- **No `as any`** — define proper types or use generics. If interfacing with untyped external libraries (e.g., `@mysten/sui/bcs` parse output), create typed wrappers.
- **No non-null assertions (`!`)** — use narrowing, optional chaining, or extract to a guarded local variable. Exception: array index access in a bounded loop where the index is provably in-range (e.g., `array[i]!` inside `for (let i = 0; i < array.length; i++)`).
- **No `any` in function signatures** — parameters and return types must be explicit. `unknown` is acceptable when the type genuinely cannot be known.

## Key Rules

- BCS field order must match Move struct exactly (positional encoding)
- Phantom type params don't affect BCS encoding
- **Exactly one storage backend per run** — `--clickhouse`, `--postgres`, or `--sqlite` are mutually exclusive. The CLI enforces this and exits with an error if multiple are specified.
- **Config precedence**: CLI flag > env var > config file (`~/.jun/config.yml`) > hardcoded default
- **No app-specific code in jun** — trading strategies, analytics tools, UIs go in separate repos (juna, etc.). Jun is pure pipeline infra.
- Database connection factories in `db.ts` (single source of truth)
- Shared utilities: `sql-helpers.ts`, `timestamp.ts`, `uleb.ts`, `normalize.ts`
