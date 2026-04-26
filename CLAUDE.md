# Jun

Sui checkpoint ingestion infrastructure: archive fetch + live gRPC + decode + broadcast.
Pure infra. **No databases.** Data goes to consumers via NATS broadcast or local pipe;
they pick what to do with it.

## Boundary

`jun` is the OSS unit. It does:

- Fetch checkpoint bytes (custom archive at `unconfirmed.cloud`, live gRPC subscribe).
- Decode proto → flat record types in `jun-types`.
- Broadcast over NATS (live) and stdout pipe (backfill, recovery).

Database ingestion, schema design, derived tables, LLM-tuned analytics — all of that
lives downstream in private consumers (e.g. `juna`). The OSS contract is the wire
formats: NATS subjects + pipe stream layout.

## Workspace

```
crates/
  jun-types       Record structs (TransactionRecord, BalanceChangeRecord, …)
                  and the ExtractMask bitfield. No deps on infrastructure crates.
  jun-archive     Client for the unconfirmed-cloud archive. Per-epoch zstd, ranged
                  GETs, custom .idx format. Not interchangeable with Mysten's
                  sui-archival (different layout).
  jun-decoder    Proto bytes → ExtractedCheckpoint. Uses sui-types for typed
                  structs; flattens into analytics-shaped records. proto.rs is a
                  hand-rolled wire parser slated to be replaced by prost codegen
                  against proto/sui/rpc/v2/*.proto.
  jun-pipeline   Orchestrator: fetch → decode → sink. Sinks are pluggable via the
                  Sink trait; the pipeline crate itself has no DB knowledge.
  jun (binary)   CLI: `jun replay`. NATS broadcast and pipe sinks are coming online.
```

## proto/

Sui's published gRPC protos for the v2 API (`proto/sui/rpc/v2/*.proto`) plus the
google standard imports they pull in. Used for prost codegen when `jun-decoder/proto.rs`
is replaced.

## Output formats (planned)

**NATS (live)** — core NATS, no JetStream. Per-record-type subjects:
```
jun.sui.checkpoints
jun.sui.transactions
jun.sui.balance_changes
jun.sui.events
jun.sui.object_changes
…
```
Payload: protobuf (~3–5× smaller than JSON, schema'd, fits NATS 1 MiB default).

**Pipe (backfill / recovery)** — `jun replay --pipe [--format=jsonl|proto] [--table=...]`:
- `jsonl` (default): debuggable, drops directly into `clickhouse-client INSERT FORMAT JSONEachRow`
- `proto`: fast / compact for typed consumers
- `--table=t1,t2`: doubles as ExtractMask projection (decoder skips work for
  unselected types) and output filter (single-table mode emits untagged rows for
  zero-glue ingestion).

## Robustness pattern

Core NATS has no replay/durability. Recovery is gap-detection + pipe-fill: every
NATS message carries `sequence_number`; on gap, restart, or noticed lag, the
consumer spawns `jun replay --pipe --from <gap_start> --to <head>` to fill.
Same `jun replay --pipe` code serves both initial backfill and live-mode
recovery — pipe is load-bearing.

## CLI

```bash
# Today (sinks not wired):
jun replay --from 1 --to 1000 --mode no-write
jun replay --from 1 --to 1000 --mode fetch-only

# Coming online:
jun replay --from 1 --to 1000 --pipe --format=jsonl --table=transactions
jun stream --nats nats://localhost:4222
```

Environment variables for tuning (see `crates/jun/src/main.rs`):
`JUN_FROM`, `JUN_TO`, `JUN_ARCHIVE_PROXY`, `JUN_ARCHIVE_URL`,
`JUN_CHUNK_CONCURRENCY`, `JUN_DECODE_CONCURRENCY`, `JUN_BATCH_SIZE`,
`JUN_WRITE_CONCURRENCY`, `JUN_MODE`, `RUST_LOG`.

## Code Quality

- **No `as any`** equivalent in Rust: avoid `unsafe` outside FFI; avoid `.unwrap()`
  in library code (use `?` or named errors).
- **Lean dep tree** is part of the OSS pitch. Don't add heavy upstream crates
  unless they replace meaningfully more local code than they bring in transitively.
- **Hand-rolled work is justifiable when** the upstream alternative pulls in deps
  out of proportion to what it replaces (case in point: jun-archive talks to a
  custom format Mysten's sui-archival doesn't speak).

## Key Rules

- No DB code in jun. Anything that writes to ClickHouse / Postgres / SQLite belongs
  downstream.
- The `Sink` trait in `jun-pipeline` is the integration boundary. New sinks
  (NATS, stdout pipe) live as their own crates and implement it.
- ExtractMask bits are append-only — don't reuse positions when adding new record types.
- `jun-types` field names are snake_case; consumers map to their own conventions.
- BCS field order in upstream `sui-types` structs must match Move struct exactly;
  jun-decoder relies on `bcs::from_bytes::<T>(...)` for that, don't hand-decode.
