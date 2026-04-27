# jun

Sui checkpoint ingestion infrastructure: archive fetch, live gRPC subscribe,
decode, broadcast. Pure Rust workspace. **No databases.** Records flow out via
NATS broadcast (live) or stdout pipe (backfill, recovery); downstream
consumers attach to those wire formats and decide what to do with them.

License: Apache-2.0.

## What jun is

- **`jun ingest`** — pull per-checkpoint files from Mysten's archive,
  BLS-verify against the epoch committee, package as per-epoch zstd + idx,
  upload to R2 via streaming multipart. One epoch per invocation.
- **`jun replay`** — read epoch zstd from the R2 archive, decode, broadcast
  on NATS or stdout pipe. Used for backfill, gap recovery, ad-hoc historical
  queries.
- **`jun stream`** — subscribe to a Sui fullnode's gRPC `SubscribeCheckpoints`,
  decode, broadcast on NATS or stdout pipe. Always-on, single instance per
  deployment.

## What jun is not

Not a database. Not an analytics engine. Not opinionated about your
downstream schema. The OSS contract is the wire formats — NATS subjects and
the JSONL pipe layout. Database ingestion (ClickHouse, Postgres, SQLite),
derived tables, and analytics live in private downstream consumers (e.g.
`juna`).

## Architecture

```
                    +-----------------+
   Mysten archive ─▶│   jun ingest    │─▶ R2 (epoch-N.zst + epoch-N.idx)
                    +-----------------+
                                                    │
                                                    ▼
                    +-----------------+      +-----------------+
   R2 archive ─────▶│   jun replay    │─────▶│  NATS / stdout  │─▶ consumers
                    +-----------------+      +-----------------+
                                                    ▲
                    +-----------------+              │
   fullnode gRPC ──▶│   jun stream    │──────────────┘
                    +-----------------+
```

### Crates

| Crate | Role |
|---|---|
| `jun-types` | Record structs (`TransactionRecord`, `BalanceChangeRecord`, …) and the `ExtractMask` bitfield. No infra deps. |
| `jun-archive-reader` | Client for the unconfirmed.cloud archive: epoch routing, `.idx` parsing (20-byte entries: seq/offset/length), ranged GETs against `epoch-N.zst` in R2. |
| `jun-checkpoint-decoder` | Proto bytes → `ExtractedCheckpoint`. Decodes the v2 RPC envelope with prost, then BCS-decodes the canonical inner structs via `sui-types`, then flattens into analytics-shaped records. |
| `jun-checkpoint-verifier` | BLS verification of checkpoint summaries against an epoch committee. Pre-deserializes validator pubkeys once per committee — that caching is the dominant per-checkpoint optimisation, not pairing or final_exp. |
| `jun-checkpoint-ingest` | Streaming ingester: fetches per-checkpoint `.binpb.zst` from Mysten, BLS-verifies, writes a single concatenated `epoch-N.zst` (RFC 8878 multi-frame stream) plus `epoch-N.idx` to R2 via streaming multipart upload — no disk staging. |
| `jun-checkpoint-stream` | Live fullnode gRPC subscription. Backfills the gap from a requested resume seq via unary `GetCheckpoint`, then tails `SubscribeCheckpoints`. Same shape on reconnect. |
| `jun-pipeline` | Replay orchestrator: fetch → decode → sink, with bounded channels for backpressure. Sinks are pluggable via the `Sink` trait. No DB knowledge in the pipeline crate itself. |
| `jun-nats` | NATS sink. Subjects `<prefix>.transactions`, `<prefix>.balance_changes`, etc. Payload is protobuf — see `crates/jun-nats/proto/jun.proto`. |
| `jun` | The CLI binary: `ingest`, `replay`, `stream`. |

The dep tree is intentionally lean. Heavy upstream crates (e.g. `aws-sdk-s3`,
mainline `sui-types` in full) only enter where they replace meaningfully more
local code than they bring in transitively. `rust-s3` over `aws-sdk-s3` saves
~30 transitive crates and avoids the `aws-smithy-http-client` / `hyper-014`
glue layer.

## Build

```bash
cargo build --release --bin jun
```

The release profile uses `lto = "fat"`, `codegen-units = 1`, and replaces
the system allocator with mimalloc. Decode is allocation-heavy (prost
full-tree decode + BCS Object decoding); the allocator choice is
load-bearing.

## Quick start

### `jun ingest`

Build one epoch's archive object and upload to R2. Designed for ephemeral
fly machines: one machine per epoch boundary.

```bash
export S3_ENDPOINT=https://<account>.r2.cloudflarestorage.com
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=auto

jun ingest \
  --epoch 1106 \
  --bucket sui-checkpoints \
  --source https://checkpoints.mainnet.sui.io
```

Key flags (see `jun ingest --help` for all):

| Flag / env | Default | Notes |
|---|---|---|
| `--epoch` / `JUN_INGEST_EPOCH` | required | Epoch number to package. |
| `--source` / `JUN_INGEST_SOURCE` | `https://checkpoints.mainnet.sui.io` | Mysten per-checkpoint archive. |
| `--bucket` / `S3_BUCKET` | required | R2 bucket name. |
| `--fetch-workers` | 500 | Parallel per-checkpoint GETs against Mysten. |
| `--s3-pool` | 8 | Independent S3 clients for UploadPart (each = own TCP+TLS, breaks past the ~30 MB/s single-connection ceiling). |
| `--http-pool` | 8 | Independent reqwest clients for source fetches. |
| `--upload-concurrency` | 64 | Max simultaneous UploadPart RPCs across the pool. RAM bound = `concurrency × 32 MiB`. |
| `--source-ips` / `JUN_INGEST_SOURCE_IPS` | `auto` | `auto` discovers all non-loopback public IPv4. Comma-list pins exact addresses. `off` lets the kernel choose. |
| `--no-verify` | off | Skip BLS verification. ~5–10× faster on small machines; only safe when you trust the upstream archive bytes. |
| `--fetch-only` | off | Bench-only: fetch + verify, no upload. |

### `jun replay`

Decode an epoch (or arbitrary seq range) out of the R2 archive and broadcast
records on NATS or stdout. Used for backfill, gap-fill recovery after a NATS
gap, and ad-hoc historical queries.

```bash
# Whole epoch → JSONL on stdout, only transactions + balance_changes
jun replay --epoch 1106 --pipe --table=transactions,balance_changes \
  | clickhouse-client --query "INSERT INTO transactions FORMAT JSONEachRow"

# Whole epoch → NATS broadcast
jun replay --epoch 1106 --nats nats://localhost:4222

# Explicit seq range, decode-only (drop output, used for benchmarking)
jun replay --from 100000000 --to 100100000 --mode no-write
```

Key flags:

| Flag / env | Default | Notes |
|---|---|---|
| `--epoch` / `JUN_EPOCH` | — | Process exactly one epoch (resolves seq range from the archive proxy). |
| `--from` / `--to` | — | Explicit seq range. Mutually exclusive with `--epoch`. |
| `--archive-url` / `JUN_ARCHIVE_URL` | `https://archive.checkpoints.mainnet.sui.unconfirmed.cloud` | Comma-separated for multi-host rotation across distinct CF custom domains. |
| `--proxy-url` / `JUN_ARCHIVE_PROXY` | `https://checkpoints.mainnet.sui.unconfirmed.cloud` | Used for epoch-metadata lookups. |
| `--chunk-mb` | 16 | Size of each ranged GET against the `.zst`. |
| `--client-pool` / `JUN_CLIENT_POOL` | 4 | Independent reqwest clients (each = own TCP). Single-connection HTTP/2 saturates near 5 Gbit on a typical CDN edge; pool to go past it. |
| `--decode-concurrency` | 4 | Decoder workers. |
| `--nats <url>` | — | NATS sink. Implies `--mode full`. |
| `--pipe` | off | JSONL on stdout. Implies `--mode full`. Logs go to stderr so stdout stays clean. |
| `--table` / `JUN_TABLE` | all | Restrict pipe output to a comma-list of: `checkpoints, transactions, move_calls, balance_changes, object_changes, dependencies`. |

### `jun stream`

Tail a fullnode's gRPC subscription:

```bash
jun stream --grpc grpc://your-fullnode:9000 --nats nats://localhost:4222
```

Resume after restart:

```bash
jun stream --grpc grpc://your-fullnode:9000 --from-seq 100123456 \
  --nats nats://localhost:4222
```

`SubscribeCheckpoints` always starts at the server's latest executed
checkpoint. `--from-seq` causes a unary `GetCheckpoint` backfill of the gap
before tailing the live stream. Same path runs after a transient reconnect.

## Output formats

### NATS (live)

Core NATS, no JetStream. Per-record-type subjects with a configurable prefix
(default `jun`):

```
jun.checkpoints
jun.transactions
jun.move_calls
jun.balance_changes
jun.object_changes
jun.dependencies
jun.events
...
```

Payload is protobuf. The schema lives at `crates/jun-nats/proto/jun.proto` —
that file is the source of truth subscribers should generate against.
Protobuf is roughly 3–5× smaller than the equivalent JSON and fits well
inside NATS's default 1 MiB message limit.

### Pipe (backfill / recovery)

`jun replay --pipe` and `jun stream --pipe` emit JSON Lines on stdout. Each
line is one flat record. By default every line carries a `_table`
discriminator so a single stream can be demultiplexed downstream. With
`--table=<one>` the discriminator is dropped and the lines are ready for:

```bash
clickhouse-client --query "INSERT INTO transactions FORMAT JSONEachRow"
```

Stderr carries logs (`tracing` / `RUST_LOG`); stdout is reserved for records.

### Robustness pattern

Core NATS has no replay or durability. Recovery is gap-detection plus
pipe-fill: every NATS message carries a sequence number. On a detected gap,
restart, or noticed lag, the consumer spawns
`jun replay --pipe --from <gap_start> --to <head>` to fill. The same
`jun replay --pipe` code path serves both initial backfill and live-mode
recovery — pipe is load-bearing.

## Performance

Measured 2026-04-26 on a 32-core / 10 Gbit host, single epoch (1106), full
BLS verification:

| Metric | Value |
|---|---|
| Throughput | ~9,000 cps/s |
| Bandwidth | ~340 MB/s |
| Wall time, 375K-checkpoint epoch | ~40 s |

Key choices behind those numbers:

- **Per-epoch zstd + 20-byte-entry idx layout.** `epoch-N.zst` is a
  concatenation of per-checkpoint zstd frames; `epoch-N.idx` is a flat array
  of `(seq:u64, offset:u64, length:u32)` records, so locating sequence `s`
  inside an epoch is a constant-time `(s - first_seq) * 20` index plus one
  ranged GET.
- **Streaming multipart upload.** No disk staging during ingest; checkpoint
  bytes are fetched, verified, framed into the rolling zstd stream, and
  pushed into in-flight `UploadPart` calls.
- **BLS verify at ingest, never at read.** The R2 archive is treated as
  pre-verified; replay does no crypto.
- **Pubkey cache per epoch.** The dominant per-checkpoint cost when verify
  is on is `PublicKey::from_bytes` × ~100–150 signers — not pairing, not
  final_exp. Deserializing once at committee construction and indexing by
  signer position turned a 104-second epoch into a 16-second epoch on the
  same hardware (6.6× speedup over the baseline pairing-only path).
- **Multi-client pools.** Single TCP+TLS caps at ~30 MB/s on a busy CDN
  edge regardless of the underlying pipe (single-core symmetric crypto
  ceiling). Both ingest (S3 upload, source-IP-rotated HTTP fetch) and
  replay (archive ranged GETs) round-robin across N independent clients to
  break past it.
- **R2 upload is per-account-throttled, not per-IP.** Source-IP binding
  helps the fetch side (Mysten/Cloudflare CDN egress is per-IP); for the
  upload side, only multi-TCP per account moves the needle. Confirmed
  empirically.

## Deployment

The Dockerfile bakes only `ENTRYPOINT ["jun"]` — every value comes from
`--env` at machine spawn:

```bash
fly machine run <image> \
  --app jun-ingest --rm --restart no \
  --entrypoint "jun ingest" \
  --env JUN_INGEST_EPOCH=1106 \
  --env S3_ENDPOINT=https://<account>.r2.cloudflarestorage.com \
  --env S3_BUCKET=sui-checkpoints \
  --env AWS_ACCESS_KEY_ID=... \
  --env AWS_SECRET_ACCESS_KEY=... \
  --env AWS_REGION=auto
```

One ephemeral fly machine per epoch boundary. `--restart no` and `--rm`
ensure each machine handles exactly one epoch and exits. The same image
serves `replay` and `stream` by changing `--entrypoint`.

`replay` and `stream` deployments are long-running by contrast — a single
fly machine per consumer cluster, attached to NATS.

## Environment variables

CLI flags and env vars are aliased throughout. The full set:

```
# replay
JUN_FROM, JUN_TO, JUN_EPOCH
JUN_ARCHIVE_PROXY, JUN_ARCHIVE_URL
JUN_CHUNK_CONCURRENCY, JUN_CHUNK_MB, JUN_CLIENT_POOL
JUN_DECODE_CONCURRENCY, JUN_BATCH_SIZE, JUN_WRITE_CONCURRENCY
JUN_MODE, JUN_NATS_URL, JUN_NATS_PREFIX, JUN_PIPE, JUN_TABLE

# ingest
JUN_INGEST_EPOCH, JUN_INGEST_SOURCE, JUN_GRAPHQL_URL
JUN_INGEST_FETCH_WORKERS, JUN_INGEST_S3_POOL, JUN_INGEST_HTTP_POOL
JUN_INGEST_UPLOAD_CONCURRENCY, JUN_INGEST_SOURCE_IPS
JUN_INGEST_NO_VERIFY, JUN_INGEST_FETCH_ONLY
S3_ENDPOINT, S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

# stream
JUN_GRPC_URL, JUN_STREAM_FROM_SEQ
JUN_NATS_URL, JUN_NATS_PREFIX, JUN_PIPE, JUN_TABLE

# logging
RUST_LOG  # default: info
```

## Scope and stability

- The wire formats (NATS subjects + payload protos, JSONL pipe layout) and
  the three CLI commands are the OSS contract.
- `ExtractMask` bit positions are append-only — never reuse a position when
  adding a new record type.
- `jun-types` field names are snake_case; consumers map to their own
  conventions.
- Database integration and LLM-tuned analytics live downstream in private
  consumers like `juna`. Don't expect them in this repo.
- `jun` is built for Cloudflare R2 + D1 as the archive substrate. That
  coupling is deliberate, not a leak to be abstracted away.

## Repository layout

```
crates/
  jun-types/                  Record structs + ExtractMask
  jun-archive-reader/         Archive client (.idx + ranged GETs)
  jun-checkpoint-decoder/     Proto + BCS → flat records
  jun-checkpoint-verifier/    BLS verification, committee handling
  jun-checkpoint-ingest/      Streaming ingester (Mysten → R2)
  jun-checkpoint-stream/      Live gRPC subscription
  jun-pipeline/               Replay orchestrator (fetch → decode → sink)
  jun-nats/                   NATS sink + protobuf schema
  jun/                        CLI binary
proto/
  sui/rpc/v2/                 Sui's published gRPC protos (vendored)
Dockerfile                    Single-binary image, ENTRYPOINT ["jun"]
```
