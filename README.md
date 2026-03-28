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
    type:   0x53ee...::marketplace::ListingCancelled
    sender: 0xac27...
    tx:     8Fj2kN...
    bcs:    104 bytes
```

## CLI

### Stream

```bash
# Stream everything (events, effects, balance changes)
jun stream

# Stream only events
jun stream --include events

# Filter to specific event types
jun stream --include events --filter "pressing::RecordPressedEvent"

# Multiple data types
jun stream --include events effects

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

### SQLite export

Stream checkpoint data directly into a portable SQLite file:

```bash
jun stream --duration 5m --output testnet.sqlite
```

This creates a SQLite database with five tables:

| Table | Contents |
|-------|----------|
| `checkpoints` | Checkpoint sequence numbers and timestamps |
| `transactions` | Transaction digests, checkpoint references, senders |
| `events` | Event type, sender, package, module, raw BCS bytes |
| `effects` | Transaction status, gas costs |
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

# Transaction success rate
sqlite3 -column -header testnet.sqlite "
  SELECT status, COUNT(*) as count
  FROM effects
  GROUP BY status
"

# Checkpoint throughput
sqlite3 -column -header testnet.sqlite "
  SELECT
    MIN(sequence_number) as first_checkpoint,
    MAX(sequence_number) as last_checkpoint,
    COUNT(*) as total,
    MIN(timestamp) as start_time,
    MAX(timestamp) as end_time
  FROM checkpoints
"
```

### Codegen

Auto-generate field definitions from on-chain Move structs:

```bash
jun codegen 0xPACKAGE::module::EventName
```

```
RecordPressedEvent — all fields are primitive ✓

{
  pressing_id: "address",
  release_id: "address",
  edition: "u16",
  record_id: "address",
  record_number: "u64",
  quantity: "u64",
  pressed_by: "address",
  paid_value: "u64",
  timestamp_ms: "u64",
}
```

Non-primitive fields are flagged:

```
CompositionPublishedEvent — 1 field is not primitive

{
  composition_id: "address",
  title: "string",
  // split_bps: BPS — not a primitive type. Use a granular event or "json" override.
  has_lyrics: "bool",
  ...
}
```

## Programmatic API

```typescript
import { createGrpcClient } from "jun/grpc";

const client = createGrpcClient({
  url: "slc1.rpc.testnet.sui.mirai.cloud:443",
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

- [x] Native gRPC checkpoint streaming
- [x] BCS event decoding via `@mysten/bcs`
- [x] CLI stream viewer with stop conditions
- [x] SQLite export
- [x] JSONL export
- [x] Auto-generate BCS schemas from on-chain type layouts
- [ ] Typed event indexer (`defineIndexer()` with config file)
- [ ] Postgres output with batch inserts (`Bun.sql`)
- [ ] Parquet export (local files + S3)
- [ ] `bun create jun` project scaffold

## Architecture

```
src/
  cli.ts          CLI (jun stream, jun codegen)
  grpc.ts         Native gRPC client (subscribe + fetch + type introspection)
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
@grpc/grpc-js       Native gRPC client (HTTP/2)
@grpc/proto-loader   Runtime proto file loading
@mysten/bcs          BCS binary codec
commander            CLI framework
p-map                Concurrent backfill
```

Zero runtime dependencies beyond these + Bun builtins (`bun:sqlite`).

## License

MIT
