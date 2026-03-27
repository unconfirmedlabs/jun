# jun

Sui event indexer for Bun. Native gRPC streaming over HTTP/2 with BCS binary decoding.

> **Status:** Early development, building in public. gRPC streaming works today. Postgres and Parquet export coming soon.

## What it does

Jun subscribes to Sui checkpoint streams via native gRPC (`@grpc/grpc-js`, not gRPC-web) and decodes event data from BCS binary — the same wire format Sui uses internally. No protobuf JSON wrappers, no lossy conversions.

```
Sui Fullnode ──gRPC/HTTP2──▶ Jun ──BCS decode──▶ Postgres / Parquet / stdout
```

## Quick start

```bash
bun install
bun run src/cli.ts stream
```

```
[jun] streaming from slc1.rpc.testnet.sui.mirai.cloud:443...

── checkpoint 317936045 ── 2026-03-27T14:23:02.440Z ── 1 event(s) ──
    type:   0x53ee...::marketplace::ListingCancelled
    sender: 0xac27...
    tx:     8Fj2kN...
    bcs:    104 bytes

── checkpoint 317936048 ── 2026-03-27T14:23:02.684Z ── 7 event(s) ──
    type:   0x01db...::oracle::OraclePricesUpdated
    sender: 0xe64e...
    tx:     3Kp9mR...
    bcs:    56 bytes
```

## CLI

```bash
# Stream all events from testnet
bun run src/cli.ts stream

# Filter to specific event types
bun run src/cli.ts stream --filter "pressing::RecordPressedEvent"

# JSON lines output (pipe to jq, files, etc.)
bun run src/cli.ts stream --json

# Custom gRPC endpoint
bun run src/cli.ts stream --url "your-fullnode:443"
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
- [x] CLI stream viewer
- [ ] Postgres output with batch inserts (`Bun.sql`)
- [ ] Parquet export (local files + S3)
- [ ] Auto-generate BCS schemas from on-chain type layouts
- [ ] `bun create jun` project scaffold

## Architecture

```
src/
  cli.ts          CLI entry point (jun stream)
  grpc.ts         Native gRPC client (subscribe + fetch)
  schema.ts       Field DSL → BCS schemas + Postgres DDL
  processor.ts    Event matching + BCS decode
  state.ts        Cursor/watermark management
  index.ts        defineIndexer() orchestrator
  output/
    postgres.ts   Batch inserts via Bun.sql
proto/            Sui gRPC proto files (vendored from sui-apis)
```

## Dependencies

```
@grpc/grpc-js       Native gRPC client (HTTP/2)
@grpc/proto-loader   Runtime proto file loading
@mysten/bcs          BCS binary codec
commander            CLI framework
p-map                Concurrent backfill
```

## Name

Jun (潤) — moisture, enrich, profit. Data flows from chain to storage.

## License

MIT
