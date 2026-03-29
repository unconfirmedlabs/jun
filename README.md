# jun

Sui event indexer for Bun. Native gRPC streaming over HTTP/2 with BCS binary decoding.

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
jun fetch --from 318000000 --count 1000

# Fetch a specific range to SQLite
jun fetch --from 318000000 --to 318001000 --output mainnet.sqlite

# High concurrency
jun fetch --from 318000000 --count 10000 --concurrency 32 --output backfill.sqlite

# Filter events during fetch
jun fetch --from 318000000 --count 100 --include events --filter "marketplace::"
```

### Replay

Replay historical checkpoints from the [Sui checkpoint archive](https://checkpoints.mainnet.sui.io/) — every checkpoint from genesis, no pruning, no fullnode required.

```bash
# Replay 1000 checkpoints from mainnet genesis
jun replay --from 0 --count 1000

# Replay a specific range to SQLite
jun replay --from 259000000 --to 259001000 --output mainnet.sqlite

# Replay with cryptographic verification
jun replay --from 259000000 --count 100 --verify

# High concurrency for fast backfill
jun replay --from 100000000 --count 10000 --concurrency 32 --output backfill.sqlite
```

#### Light client verification

With `--verify`, each checkpoint is cryptographically verified using [kei](https://github.com/unconfirmedlabs/kei) — a pure TypeScript Sui light client. Every checkpoint's BLS12-381 aggregate signature is checked against the validator committee, proving the data was signed by a quorum (≥66.67%) of validators.

```bash
jun replay --from 259063382 --count 100 --verify --output verified.sqlite
```

The validator committee is fetched once per epoch (~24h) and cached. Verification adds ~11ms per checkpoint — negligible vs the network fetch time.

No trust required in the archive CDN — the math proves the data is authentic.

#### Data parity

Stream, fetch, and replay produce identical output for the same checkpoint range — transactions, digests, senders, status, gas costs, events, and timestamps all match.

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
jun chat --from 259063382 --count 100

# With cryptographic verification
jun chat --from 259063382 --count 100 --verify
```

In live mode, the database grows in real-time as checkpoints arrive — Claude can query fresh data on every turn.

### SQLite export

Stream, fetch, or replay checkpoint data directly into a portable SQLite file:

```bash
jun stream --duration 5m --output testnet.sqlite
jun fetch --from 318000000 --count 1000 --output mainnet.sqlite
jun replay --from 0 --count 1000 --output genesis.sqlite
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
- [x] CLI: stream, fetch, replay, codegen, mcp, chat
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
  cli.ts          CLI (jun stream, jun fetch, jun replay, jun codegen, jun mcp, jun chat)
  grpc.ts         Native gRPC client (subscribe + fetch + type introspection)
  archive.ts      Checkpoint archive client (fetch + zstd + proto + BCS decode + verify)
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

## License

MIT
