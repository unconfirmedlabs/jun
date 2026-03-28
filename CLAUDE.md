# Jun

Sui event indexer for Bun. Native gRPC streaming over HTTP/2 with BCS binary decoding.

## Runtime

Default to using Bun instead of Node.js.

- Use `bun <file>` instead of `node <file>` or `ts-node <file>`
- Use `bun test` instead of `jest` or `vitest`
- Use `bun install` instead of `npm install`
- Use `bun run <script>` instead of `npm run <script>`
- Bun automatically loads .env, so don't use dotenv.
- `Bun.sql` for Postgres. Don't use `pg` or `postgres.js`.

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
proto/            Sui gRPC proto files (fetched on install, gitignored)
```

## Standard Event Columns

Every event table gets these automatically — users never declare them:

| Column | Type | Source |
|--------|------|--------|
| `checkpoint` | BIGINT | `response.cursor` |
| `tx_digest` | TEXT | `tx.digest` |
| `event_seq` | INTEGER | index within `tx.events.events[]` |
| `sender` | TEXT | `ev.sender` |
| `timestamp` | TIMESTAMPTZ | `checkpoint.summary.timestamp` |

No `gas_sponsor` — that's transaction metadata, not event metadata.

## Default gRPC readMask

```
transactions.events
transactions.digest
summary.timestamp
```

Minimal bandwidth. Only add more paths if the user explicitly requests via `--include`.

## Supported Field Types (Primitives Only)

- `address` / `ID` → TEXT (0x hex string)
- `u8`, `u16`, `u32` → INTEGER
- `u64`, `u128`, `u256` → BIGINT / NUMERIC
- `bool` → BOOLEAN
- `string` → TEXT
- `option<T>` where T is primitive → nullable column
- `vector<T>` where T is primitive → array column

Non-primitive fields (VecMap, nested structs, complex enums) → use granular events or JSONB via custom handler.

## Live vs Backfill

Two separate modes, never combined in the same process:
- `live` — long-running gRPC subscription, real-time
- `backfill` — one-shot from start checkpoint to chain tip, exits when done

## Key Rules

- BCS field order must match Move struct exactly (positional encoding)
- Phantom type params don't affect BCS encoding
- `ON CONFLICT (tx_digest, event_seq) DO NOTHING` for idempotent replay
- JSONL (one JSON object per line) for streaming output format
- Proto files are fetched from MystenLabs/sui-apis on `bun install` (postinstall script)
