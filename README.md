# jun

Sui swiss army knife for Bun. Full gRPC client, checkpoint streaming, cryptographic verification, event indexing, and AI integration — all with native gRPC over HTTP/2 and BCS binary decoding.

```
Sui Fullnode ──gRPC/HTTP2──▶ Jun ──BCS decode──▶ SQLite / Postgres / stdout
```

## Install

```bash
bun install
```

## Quick start

```bash
# Chain info
jun c info

# Check an object
jun c object 0x5

# Stream live checkpoints
jun stream

# Replay from archive
jun replay --from-checkpoint 259000000 --count 1000 --output mainnet.sqlite

# AI analysis
jun chat --from-checkpoint 259063382 --count 100
```

## CLI reference

`jun c` is shorthand for `jun client`. All query commands support `--json` for machine-readable output and `--url` to override the gRPC endpoint.

### Client — query the Sui blockchain

#### Chain info

```bash
jun c info                          # chain ID, epoch, checkpoint, server version
jun c gas-price                     # current reference gas price
jun c epoch                         # current epoch (validators, stake, subsidy)
jun c epoch 1080                    # historical epoch
jun c system-state                  # full system state snapshot
jun c protocol-config               # protocol feature flags and attributes
jun c protocol-config --flags-only  # just feature flags
jun c checkpoint                    # latest checkpoint
jun c checkpoint 259000000          # specific checkpoint
jun c ping                          # gRPC latency test (native HTTP/2)
jun c ping --count 10               # more samples
```

#### Objects & accounts

```bash
jun c object 0x5                    # get object by ID
jun c obj 0x5 --bcs                 # include BCS bytes
jun c objects 0x5 0x6 0x7           # batch get multiple objects
jun c owned 0xADDR                  # list objects owned by address
jun c own 0xADDR --type 0x2::coin::Coin  # filter by type
jun c dynamic-fields 0x5            # list dynamic fields
jun c df 0x5 --limit 20             # with pagination
```

#### Balances & coins

```bash
jun c balance 0xADDR                # SUI balance (formatted)
jun c bal 0xADDR --coin-type 0x...  # specific coin type
jun c balances 0xADDR               # all coin balances
jun c coins 0xADDR                  # list individual coin objects
```

#### Transactions

```bash
jun c tx-block DIGEST               # get transaction by digest
jun c txb DIGEST --json             # raw JSON
jun c txs DIGEST1 DIGEST2           # batch get
jun c simulate BASE64_TX            # dry-run (read-only)
```

#### Move packages

```bash
jun c package 0x1                   # package modules and types
jun c function 0x2::transfer::public_transfer  # function signature
jun c package-versions 0x1          # version history
jun c coin-meta 0x2::sui::SUI      # coin metadata (name, symbol, decimals)
```

#### Name service

```bash
jun ns resolve-address adeniyi.sui  # name → address
jun ns resolve-name 0x1eb7...       # address → name
```

### Stream

Stream live checkpoints from a Sui fullnode via gRPC.

```bash
jun stream                                       # everything
jun stream --include events                      # only events
jun stream --include events --filter "marketplace::ItemListedEvent"
jun stream --duration 5m                         # stop after 5 minutes
jun stream --until-checkpoint 318200000          # stop at checkpoint
jun stream --output testnet.sqlite               # write to SQLite
jun stream --jsonl | jq '.events[]?.type'        # pipe to jq
```

### Fetch

Fetch historical checkpoints by range via gRPC. Concurrent with retry logic.

```bash
jun fetch --from-checkpoint 318000000 --count 1000
jun fetch --from-epoch 1080 --to-epoch 1082
jun fetch --from-checkpoint 318000000 --count 10000 --concurrency 32 --output backfill.sqlite
```

### Replay

Replay historical checkpoints from the [Sui checkpoint archive](https://checkpoints.mainnet.sui.io/). Every checkpoint from genesis, no pruning.

```bash
jun replay --from-checkpoint 0 --count 1000
jun replay --from-epoch 1080
jun replay --from-checkpoint 259000000 --count 100 --verify  # with cryptographic verification
jun replay --from-checkpoint 100000000 --count 10000 --concurrency 32 --output backfill.sqlite
```

With `--verify`, each checkpoint is cryptographically verified using [kei](https://github.com/unconfirmedlabs/kei) — BLS12-381 aggregate signatures checked against the validator committee. No trust in the archive CDN required.

### Verify

Cryptographically verify transactions and objects against signed checkpoints.

```bash
jun verify tx 8bFJ3kN...            # verify transaction inclusion
jun verify object 0x5               # verify object state
```

### Message

Sign and verify Sui personal messages using your local keystore.

```bash
jun msg sign "hello world"                       # sign with active keypair
jun msg sign "hello" --address 0xabc...          # specific address
jun msg verify "hello world" <signature> <addr>  # verify signature
```

### Codegen

Auto-generate field definitions from on-chain Move structs.

```bash
jun codegen 0xPACKAGE::module::EventName
```

### Config

```bash
jun config show           # view active env + cache stats
jun config set testnet    # switch environment
jun config add staging --grpc-url https://my-node.example.com
```

### Cache

Local checkpoint cache at `~/.jun/cache/checkpoints/`.

```bash
jun cache show
jun cache fill --from-epoch 1080
jun cache clear
```

### MCP

Start an [MCP](https://modelcontextprotocol.io/) server for AI assistants. Exposes Sui chain query tools and optional SQLite analysis.

```bash
jun mcp                    # chain query tools only
jun mcp mainnet.sqlite     # chain + SQLite tools
```

**Chain query tools** (always available):

| Tool | Description |
|------|-------------|
| `sui_info` | Chain overview (epoch, checkpoint, gas price, server) |
| `sui_object` | Get object by ID (type, owner, JSON content) |
| `sui_transaction` | Get transaction by digest (events, effects, balance changes) |
| `sui_balance` | Coin balance for an address (formatted with symbol) |
| `sui_owned_objects` | List objects owned by an address (with type filter) |
| `sui_dynamic_fields` | List dynamic fields of an object |
| `sui_epoch` | Epoch info (validators, stake, subsidy, timing) |
| `sui_move_function` | Move function signature (params, returns, visibility) |
| `sui_package` | Move package metadata (modules, types, linkage) |
| `sui_resolve_name` | SuiNS name ↔ address resolution |

**SQLite tools** (when database provided):

| Tool | Description |
|------|-------------|
| `query` | Run read-only SQL against checkpoint database |
| `summary` | Quick overview (row counts, time range, top events) |
| `schema` | Database DDL with row counts (resource) |

### Chat

Stream or replay checkpoints into a temporary SQLite database and chat with Claude about the data.

```bash
jun chat --live
jun chat --from-checkpoint 259063382 --count 100
jun chat --from-checkpoint 259063382 --count 100 --verify
```

### SQLite export

All data commands can write to portable SQLite files:

```bash
jun stream --duration 5m --output testnet.sqlite
jun fetch --from-checkpoint 318000000 --count 1000 --output mainnet.sqlite
jun replay --from-checkpoint 0 --count 1000 --output genesis.sqlite
```

Tables: `transactions`, `events`, `balance_changes`.

## Programmatic API

```typescript
import { defineIndexer } from "jun";

const indexer = defineIndexer({
  network: "mainnet",
  grpcUrl: "fullnode.mainnet.sui.io:443",
  database: "postgresql://user:pass@localhost/jun",
  events: {
    ItemListed: {
      type: "0x2::marketplace::ItemListed",
      fields: { item_id: "address", seller: "address", price: "u64" },
    },
  },
});

await indexer.live();
```

Or use the raw gRPC client:

```typescript
import { createGrpcClient } from "jun/grpc";

const client = createGrpcClient({ url: "fullnode.mainnet.sui.io:443" });

for await (const response of client.subscribeCheckpoints()) {
  for (const tx of response.checkpoint.transactions) {
    for (const event of tx.events?.events ?? []) {
      console.log(event.eventType, event.contents.value.length, "bytes");
    }
  }
}
```

## Architecture

```
src/
  cli.ts          CLI entry point
  cli-helpers.ts  Shared formatting helpers
  grpc.ts         Native gRPC client (@grpc/grpc-js, HTTP/2)
  rpc.ts          SuiGrpcClient factory (@mysten/sui)
  archive.ts      Checkpoint archive client (fetch + zstd + proto + BCS)
  config.ts       Named environment config (~/.jun/config.yml)
  cache.ts        Local checkpoint cache with LRU eviction
  verify.ts       Transaction/object verification (kei)
  message.ts      Personal message signing/verification
  mcp.ts          MCP server (chain queries + SQLite tools)
  sui-bcs.ts      Complete TransactionKind BCS (all 11 variants)
  codegen.ts      On-chain type → field DSL mapping
  schema.ts       Field DSL → BCS schemas + DDL
  processor.ts    Event matching + BCS decode
  state.ts        Cursor/watermark management
  index.ts        defineIndexer() orchestrator
  output/
    sqlite.ts     SQLite export (bun:sqlite)
    postgres.ts   Batch inserts (Bun.sql)
proto/            Sui gRPC proto files (fetched on install)
```

## Why

Sui's gRPC API returns events as BCS binary, not JSON. The `@mysten/sui` SDK uses gRPC-web (HTTP/1.1 polyfill) and wraps everything in protobuf classes. Jun uses `@grpc/grpc-js` with proto files directly from [MystenLabs/sui-apis](https://github.com/MystenLabs/sui-apis). Data arrives as plain JS objects. BCS bytes go straight to `@mysten/bcs` for decoding.

| | `@mysten/sui` gRPC | Jun |
|---|---|---|
| Transport | gRPC-web over HTTP/1.1 fetch | Native gRPC over HTTP/2 |
| Event data | `ev.json` (never populated) | `ev.contents.value` (BCS bytes) |
| Protobuf types | Wrapped in `google.protobuf.Value` | Plain JS objects |
| Streaming | Long-polling under the hood | Real server-push streaming |

## License

Apache-2.0
