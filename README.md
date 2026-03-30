# jun

Sui swiss army knife for Bun. Checkpoint streaming, cryptographic verification, message signing, and event indexing — all with native gRPC over HTTP/2 and BCS binary decoding.

> **Status:** Early development, building in public.

## What it does

Jun subscribes to Sui checkpoint streams via native gRPC (`@grpc/grpc-js`, not gRPC-web) and decodes event data from BCS binary — the same wire format Sui uses internally. No protobuf JSON wrappers, no lossy conversions.

```
Sui Fullnode ──gRPC/HTTP2──▶ Jun ──BCS decode──▶ SQLite / Postgres / stdout
```

## Quick start

```bash
bun install
bun run src/cli.ts stream
```

```
── checkpoint 317936045 ── 2026-03-27T14:23:02.440Z ── 3 tx(s) ──
  events (1):
    type:   0x7d12...::marketplace::ItemListedEvent
    sender: 0xac27...
    tx:     8Fj2kN...
    bcs:    104 bytes
```

## CLI

### Stream

Stream live checkpoints from a Sui fullnode via gRPC.

```bash
# Stream everything (events, effects, balance changes)
jun stream

# Stream only events
jun stream --include events

# Filter to specific event types
jun stream --include events --filter "marketplace::ItemListedEvent"

# Multiple data types
jun stream --include events effects balance-changes

# Stop conditions
jun stream --duration 5m                           # stream for 5 minutes
jun stream --until-checkpoint 318200000            # stop at checkpoint
jun stream --until "2026-03-28T00:00:00Z"          # stop at timestamp

# Output formats
jun stream --jsonl                                 # JSONL to stdout
jun stream --jsonl --output events.jsonl            # JSONL to file
jun stream --output testnet.sqlite                  # SQLite database
jun stream --jsonl | jq '.events[]?.type'           # pipe to jq

# Custom gRPC endpoint
jun stream --url "your-fullnode:443"
```

`--include` values: `events`, `effects`, `balance-changes`, `objects`, `transactions` (default: all).

### Fetch

Fetch historical checkpoints by range via gRPC. Concurrent with retry logic.

```bash
# Fetch 1000 checkpoints starting from a specific point
jun fetch --from-checkpoint 318000000 --count 1000

# Fetch by epoch range
jun fetch --from-epoch 1080
jun fetch --from-epoch 1080 --to-epoch 1082

# Fetch a specific range to SQLite
jun fetch --from-checkpoint 318000000 --to-checkpoint 318001000 --output mainnet.sqlite

# High concurrency
jun fetch --from-checkpoint 318000000 --count 10000 --concurrency 32 --output backfill.sqlite

# Filter events during fetch
jun fetch --from-checkpoint 318000000 --count 100 --include events --filter "marketplace::"

# Include system transactions (filtered by default)
jun fetch --from-checkpoint 318000000 --count 100 --include-system-txs
```

### Replay

Replay historical checkpoints from the [Sui checkpoint archive](https://checkpoints.mainnet.sui.io/) — every checkpoint from genesis, no pruning, no fullnode required.

```bash
# Replay 1000 checkpoints from mainnet genesis
jun replay --from-checkpoint 0 --count 1000

# Replay by epoch range
jun replay --from-epoch 1080
jun replay --from-epoch 1080 --to-epoch 1082

# Replay a specific range to SQLite
jun replay --from-checkpoint 259000000 --to-checkpoint 259001000 --output mainnet.sqlite

# Replay with cryptographic verification
jun replay --from-checkpoint 259000000 --count 100 --verify

# High concurrency for fast backfill
jun replay --from-checkpoint 100000000 --count 10000 --concurrency 32 --output backfill.sqlite
```

#### Light client verification

With `--verify`, each checkpoint is cryptographically verified using [kei](https://github.com/unconfirmedlabs/kei) — a pure TypeScript Sui light client. Every checkpoint's BLS12-381 aggregate signature is checked against the validator committee, proving the data was signed by a quorum (≥66.67%) of validators.

```bash
jun replay --from-checkpoint 259063382 --count 100 --verify --output verified.sqlite
```

The validator committee is fetched once per epoch (~24h) and cached. Verification adds ~11ms per checkpoint — negligible vs the network fetch time.

No trust required in the archive CDN — the math proves the data is authentic.

#### Data parity

Stream, fetch, and replay produce identical output for the same checkpoint range — transactions, digests, senders, status, gas costs, events, and timestamps all match.

### Verify

Cryptographically verify transactions and objects against signed checkpoints. Fetches data from the [checkpoint archive](https://checkpoints.mainnet.sui.io/) and runs the full [kei](https://github.com/unconfirmedlabs/kei) verification chain: BLS signature → contents digest → effects → events → objects.

```bash
# Verify a transaction
jun verify tx 8bFJ3kN...
jun verify transaction 8bFJ3kN...

# Verify an object's last state
jun verify obj 0x5
jun verify object 0xabc123...
```

```
  ✓ Transaction located — checkpoint 259687000
  ✓ Checkpoint fetched from archive — 12 transactions
  ✓ Checkpoint signature verified — epoch 1082, 127 validators
  ✓ Checkpoint contents verified — 12 transactions
  ✓ Transaction found in checkpoint — index 1
  ✓ Transaction effects verified — 1735 bytes
  ✓ Transaction events verified — 616 bytes

  Transaction 8bFJ3kN... is cryptographically verified
  via checkpoint 259687000 (epoch 1082)
```

Options: `--url` (RPC endpoint), `--archive-url` (archive CDN), `--grpc-url` (committee fetching).

### Message

Sign and verify Sui personal messages using your local keystore (`~/.sui/sui_config/sui.keystore`).

```bash
# Sign a message with your active keypair
jun message sign "hello world"
jun msg sign "hello world"

# Sign with a specific address
jun msg sign "hello" --address 0xabc...

# Verify a signature
jun message verify "hello world" <signature> <address>
jun msg verify "hello world" <signature> <address>
```

```json
{
  "message": "hello world",
  "address": "0xf84c...",
  "signature": "AH+Gl+brQVCq..."
}
```

Supports ed25519, secp256k1, and secp256r1 key schemes.

### Client

Query Sui objects, transactions, and epoch info via gRPC.

```bash
# Get object data
jun client object 0x5
jun client obj 0x5 --bcs                     # include BCS bytes
jun client obj 0x5 --json                    # raw JSON output

# Get transaction block
jun client tx-block 8bFJ3kN...
jun client txb 8bFJ3kN... --json

# Get epoch info (current or specific)
jun client epoch                             # current epoch
jun client epoch 1080                        # historical epoch
jun client epoch --json
```

```
  epoch               1082 (current)
  protocol            v118
  ref gas price       556 MIST
  safe mode           false

  started             2026-03-29T20:28:15.000Z
  duration            24.0h (8.3h remaining)

  first checkpoint    259495252

  validators          127
  total stake         7,466,619,106 SUI

  subsidy balance     288,705,748 SUI
  subsidy/epoch       348,678 SUI
```

### Name Service

SuiNS name resolution via gRPC.

```bash
jun ns resolve-address adeniyi.sui           # name → address
jun ns resolve-name 0x1eb7...                # address → name
jun ns resolve-address adeniyi.sui --json    # JSON output
```

### Config

Manage jun configuration at `~/.jun/config.yml`. Named environments with gRPC and archive URLs.

```bash
jun config show                              # view active env + cache stats
jun config set testnet                       # switch to testnet
jun config set mainnet                       # switch to mainnet
jun config add staging --grpc-url https://my-staging.example.com
jun config path                              # print config file path
```

```yaml
# ~/.jun/config.yml
active_env: mainnet
cache_max_mb: 1000

envs:
  mainnet:
    grpc_url: https://fullnode.mainnet.sui.io
    archive_url: https://checkpoints.mainnet.sui.io
  testnet:
    grpc_url: https://fullnode.testnet.sui.io
    archive_url: https://checkpoints.testnet.sui.io
```

All commands read defaults from the active environment. CLI `--url` / `--archive-url` flags still override.

### Cache

Local checkpoint cache at `~/.jun/cache/checkpoints/`. Caches compressed `.binpb.zst` files from archive fetches — subsequent replays and verifications over the same range are instant.

```bash
jun cache show                               # cache stats
jun cache fill --from-epoch 1080             # prefill an epoch
jun cache fill --from-checkpoint 259000000 --count 1000 --concurrency 64
jun cache clear                              # wipe all cached checkpoints
```

LRU eviction when cache exceeds `cache_max_mb` (default 1GB, configurable in `~/.jun/config.yml`).

### Codegen

Auto-generate field definitions from on-chain Move structs:

```bash
jun codegen 0xPACKAGE::module::EventName
```

```
ItemListedEvent — all fields are primitive ✓

{
  listing_id: "address",
  marketplace_id: "address",
  item_id: "address",
  price: "u64",
  seller: "address",
  expiry_ms: "u64",
}
```

Non-primitive fields are flagged:

```
OrderFilledEvent — 1 field is not primitive

{
  order_id: "address",
  buyer: "address",
  // metadata: Metadata — not a primitive type. Use a granular event or "json" override.
  amount: "u64",
  ...
}
```

### MCP

Start an [MCP](https://modelcontextprotocol.io/) server for AI-assisted analysis of a SQLite checkpoint database.

```bash
jun mcp mainnet.sqlite
```

Exposes:

| | Name | Description |
|---|---|---|
| Resource | `schema` | Database DDL with row counts for all tables |
| Tool | `query` | Execute read-only SQL (SELECT, WITH, EXPLAIN) |
| Tool | `summary` | Quick overview — row counts, time range, status breakdown, top events/senders |

### Chat

Stream or replay checkpoints into a temporary SQLite database and open an interactive Claude session with MCP access to the data.

```bash
# Chat about live mainnet data
jun chat --live

# Chat about a historical range
jun chat --from-checkpoint 259063382 --count 100

# With cryptographic verification
jun chat --from-checkpoint 259063382 --count 100 --verify
```

In both modes, the database grows in real-time — checkpoints stream in the background while Claude runs. Claude can query fresh data on every turn.

### SQLite export

Stream, fetch, or replay checkpoint data directly into a portable SQLite file:

```bash
jun stream --duration 5m --output testnet.sqlite
jun fetch --from-checkpoint 318000000 --count 1000 --output mainnet.sqlite
jun replay --from-checkpoint 0 --count 1000 --output genesis.sqlite
```

This creates a SQLite database with three tables:

| Table | Contents |
|-------|----------|
| `transactions` | Transaction digests, checkpoint, timestamp, sender, status, gas costs |
| `events` | Event type, package, module, sender, raw BCS bytes |
| `balance_changes` | Coin transfers with owner, coin type, amount |

Query examples:

```bash
# Top event types
sqlite3 -column -header testnet.sqlite "
  SELECT event_type, COUNT(*) as count
  FROM events
  GROUP BY event_type
  ORDER BY count DESC
  LIMIT 10
"

# Balance changes by coin
sqlite3 -column -header testnet.sqlite "
  SELECT coin_type, COUNT(*) as transfers, SUM(amount) as net_flow
  FROM balance_changes
  GROUP BY coin_type
  ORDER BY transfers DESC
"

# Events from a specific package
sqlite3 -column -header testnet.sqlite "
  SELECT event_type, sender, LENGTH(bcs) as bcs_bytes
  FROM events
  WHERE package_id = '0x10ad578...'
"

# Transaction success rate and gas costs
sqlite3 -column -header testnet.sqlite "
  SELECT status, COUNT(*) as count, SUM(gas_computation) as total_gas
  FROM transactions
  GROUP BY status
"
```

## Programmatic API

```typescript
import { createGrpcClient } from "jun/grpc";

const client = createGrpcClient({
  url: "hayabusa.mainnet.unconfirmed.cloud:443",
});

for await (const response of client.subscribeCheckpoints()) {
  const { cursor, checkpoint } = response;

  for (const tx of checkpoint.transactions) {
    for (const event of tx.events?.events ?? []) {
      console.log(event.eventType);
      console.log(event.contents.value); // BCS bytes (Uint8Array)
    }
  }
}
```

## Why

Sui's gRPC API returns events as BCS binary, not JSON. The `@mysten/sui` SDK uses gRPC-web (HTTP/1.1 polyfill) and wraps everything in protobuf classes that aren't fully exported. We hit four distinct bugs building an indexer on that stack before switching to native gRPC.

Jun uses `@grpc/grpc-js` with proto files directly from [MystenLabs/sui-apis](https://github.com/MystenLabs/sui-apis). Data arrives as plain JS objects. BCS bytes go straight to `@mysten/bcs` for decoding. No intermediate protobuf layer.

| | `@mysten/sui` gRPC | Jun |
|---|---|---|
| Transport | gRPC-web over HTTP/1.1 fetch | Native gRPC over HTTP/2 |
| Event data | `ev.json` (never populated) | `ev.contents.value` (BCS bytes) |
| Protobuf types | Wrapped in `google.protobuf.Value` | Plain JS objects |
| Streaming | Long-polling under the hood | Real server-push streaming |

## Roadmap

- [x] Native gRPC checkpoint streaming (live from tip)
- [x] Archive replay (historical from genesis)
- [x] Light client verification via [kei](https://github.com/unconfirmedlabs/kei)
- [x] BCS event decoding via `@mysten/bcs`
- [x] Full TransactionKind BCS support (all 11 variants)
- [x] Transaction and object verification (effects, events, objects)
- [x] Personal message signing and verification
- [x] Object, transaction, and epoch queries via gRPC
- [x] SuiNS name resolution
- [x] Named environment config (`~/.jun/config.yml`)
- [x] Local checkpoint cache with LRU eviction
- [x] Epoch-based ranges for fetch and replay
- [x] System transaction filtering (excluded by default)
- [x] `--json` output for all query commands
- [x] CLI: stream, fetch, replay, verify, message, client, ns, config, cache, codegen, mcp, chat
- [x] SQLite + JSONL export
- [x] Auto-generate BCS schemas from on-chain type layouts
- [x] Data parity between stream, fetch, and replay
- [x] Postgres output with batch inserts (`Bun.sql`)
- [x] MCP server for AI-assisted analysis
- [ ] Typed event indexer (`defineIndexer()` with config file)
- [ ] Parquet export (local files + S3)
- [ ] `bun create jun` project scaffold

## Architecture

```
src/
  cli.ts          CLI (stream, fetch, replay, verify, message, client, ns, config, cache, codegen, mcp, chat)
  grpc.ts         Native gRPC client (subscribe + fetch + type introspection)
  archive.ts      Checkpoint archive client (fetch + zstd + proto + BCS decode + verify)
  rpc.ts          Shared SuiGrpcClient factory (gRPC-web via @mysten/sui)
  config.ts       Named environment config (~/.jun/config.yml)
  cache.ts        Local checkpoint cache with LRU eviction (~/.jun/cache/checkpoints/)
  verify.ts       Transaction and object verification orchestration (kei)
  message.ts      Personal message signing/verification (keystore integration)
  mcp.ts          MCP server (schema resource + query/summary tools)
  sui-bcs.ts      Complete TransactionKind BCS (all 11 variants the SDK is missing)
  codegen.ts      On-chain type → field DSL mapping
  schema.ts       Field DSL → BCS schemas + DDL
  processor.ts    Event matching + BCS decode
  state.ts        Cursor/watermark management
  index.ts        defineIndexer() orchestrator
  output/
    sqlite.ts     SQLite export via bun:sqlite
    postgres.ts   Batch inserts via Bun.sql
proto/            Sui gRPC proto files (fetched on install from sui-apis)
```

## Dependencies

```
@grpc/grpc-js                  Native gRPC client (HTTP/2)
@grpc/proto-loader             Runtime proto file loading
@modelcontextprotocol/sdk      MCP server for AI integration
@mysten/bcs                    BCS binary codec
@mysten/sui                    Sui SDK (type layouts, address utils)
@noble/curves                  BLS12-381 signature verification
@noble/hashes                  Cryptographic hash functions
@unconfirmed/kei               Light client checkpoint verification
commander                      CLI framework
p-map                          Concurrent backfill
p-retry                        Retry with exponential backoff
protobufjs                     Archive checkpoint decoding
```

Zero additional runtime dependencies beyond these + Bun builtins (`bun:sqlite`, `zlib`).

### System transactions

System transactions (consensus commit prologue, authenticator state updates, etc.) are filtered by default across all commands. They bloat SQLite databases without being useful for indexing. Add `--include-system-txs` to any stream/fetch/replay command to include them.

## License

Apache-2.0
