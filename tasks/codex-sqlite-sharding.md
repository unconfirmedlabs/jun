# Codex Task: SQLite Sharding — Workers Own Decode + Write

## Goal

For snapshot mode (`jun pipeline snapshot`), each Bun Worker should own the full pipeline: read cached file → Rust FFI decode → parse binary → INSERT into its own SQLite shard file. The main thread merges shards at shutdown.

This eliminates:
- postMessage transfer of decoded binary from worker → main thread
- Main thread binary parsing (208ms per 1000 checkpoints)
- Single-threaded SQLite write bottleneck
- Write buffer backpressure complexity

**Current flow:**
```
Worker: readFile → Rust decode → postMessage(binary) → Main thread
Main thread: receive binary → parse → push to write buffer → flush → SQLite INSERT
```

**New flow (snapshot mode only):**
```
Worker: readFile → Rust decode → parse binary → INSERT into /tmp/shard-{i}.db
Main thread: wait for workers → merge shards into final DB → create indexes
```

## Architecture

### New message type: `decode-and-write-range`

Add a new worker message type in `src/checkpoint-decoder.ts`:

```typescript
interface DecodeAndWriteRangeMessage {
  type: "decode-and-write-range";
  from: string;
  to: string;
  cacheDir: string;
  workerIndex: number;
  sqlitePath: string;        // e.g. "/tmp/jun-shard-0.db"
  enabledProcessors: {       // which tables to create/write
    balances: boolean;
    transactions: boolean;
    objectChanges: boolean;
    dependencies: boolean;
    inputs: boolean;
    commands: boolean;
    systemTransactions: boolean;
    unchangedConsensusObjects: boolean;
    events: boolean;
    checkpoints: boolean;
  };
}
```

### Worker-side handler

In `src/checkpoint-decoder.ts`, add `handleDecodeAndWriteRange`:

```typescript
async function handleDecodeAndWriteRange(msg: DecodeAndWriteRangeMessage): Promise<void> {
  const { Database } = await import("bun:sqlite");
  const { readFileSync } = await import("fs");
  const { join } = await import("path");
  const { parseBinaryCheckpoint } = await import("./binary-parser.ts");
  const {
    decodeArchiveCheckpointBinary,
    isNativeCheckpointDecoderAvailable,
  } = await import("./checkpoint-native-decoder.ts");
  
  // Open shard DB
  const db = new Database(msg.sqlitePath);
  db.exec("PRAGMA synchronous = OFF");
  db.exec("PRAGMA journal_mode = OFF");
  db.exec("PRAGMA locking_mode = EXCLUSIVE");
  db.exec("PRAGMA temp_store = MEMORY");
  db.exec("PRAGMA page_size = 32768");
  
  // Create tables (no indexes, no PKs — snapshot mode)
  createShardTables(db, msg.enabledProcessors);
  
  // Prepare INSERT statements (cached by db.query())
  const stmts = prepareInsertStatements(db, msg.enabledProcessors);
  
  const from = BigInt(msg.from);
  const to = BigInt(msg.to);
  const useBinary = isNativeCheckpointDecoderAvailable();
  let processed = 0;
  const batchSize = 1000; // checkpoints per transaction
  
  let batchCount = 0;
  db.exec("BEGIN");
  
  for (let seq = from; seq <= to; seq++) {
    const cachePath = join(msg.cacheDir, `${seq}.binpb.zst`);
    try {
      const compressed = new Uint8Array(readFileSync(cachePath));
      
      if (useBinary) {
        const binary = decodeArchiveCheckpointBinary(compressed);
        if (binary) {
          const parsed = parseBinaryCheckpoint(binary);
          // INSERT each record type using prepared statements
          insertCheckpointData(stmts, parsed, msg.enabledProcessors);
          processed++;
          batchCount++;
          
          if (batchCount >= batchSize) {
            db.exec("COMMIT");
            db.exec("BEGIN");
            batchCount = 0;
          }
          
          if (processed % 5000 === 0) {
            const elapsed = (performance.now() - startedAt) / 1000;
            console.error(
              `[write-worker ${msg.workerIndex}] ${processed} cp ${Math.round(processed / elapsed)} cp/s`,
            );
          }
          continue;
        }
      }
      
      // JS fallback — decode and write
      // ... similar pattern using JSON decode
    } catch (err) {
      console.error(`[write-worker ${msg.workerIndex}] checkpoint ${seq} failed:`, err);
    }
  }
  
  if (batchCount > 0) db.exec("COMMIT");
  db.close();
  
  postMessage({ type: "done" });
}
```

### Helper functions for the worker

```typescript
function createShardTables(db: Database, enabled: EnabledProcessors): void {
  // Create the same tables as sql.ts but without PKs/UNIQUE constraints (snapshot mode)
  // Use the exact same DDL from sql.ts's initialize() — the snapshot-mode variant
  // Copy the CREATE TABLE statements for each enabled processor
}

function prepareInsertStatements(db: Database, enabled: EnabledProcessors) {
  // Return an object with one cached prepared statement per table
  return {
    transactions: enabled.transactions 
      ? db.query("INSERT INTO transactions (digest, sender, success, ...) VALUES (?,?,?, ...)")
      : null,
    moveCalls: enabled.transactions
      ? db.query("INSERT INTO move_calls (tx_digest, call_index, ...) VALUES (?,?,?, ...)")  
      : null,
    objectChanges: enabled.objectChanges
      ? db.query("INSERT INTO object_changes (...) VALUES (?,...)")
      : null,
    // ... same for all tables
  };
}

function insertCheckpointData(stmts: PreparedStmts, parsed: ParsedBinaryCheckpoint, enabled: EnabledProcessors): void {
  // Insert checkpoint summary
  if (stmts.checkpoints) {
    const cp = parsed.checkpoint;
    stmts.checkpoints.run(
      cp.sequenceNumber.toString(),
      cp.epoch?.toString() ?? "0",
      // ... all checkpoint fields
    );
  }
  
  // Insert transactions
  if (stmts.transactions) {
    for (const tx of parsed.processed.transactions) {
      stmts.transactions.run(tx.digest, tx.sender, tx.success ? 1 : 0, ...);
    }
  }
  
  // Insert object changes
  if (stmts.objectChanges) {
    for (const oc of parsed.processed.objectChanges) {
      stmts.objectChanges.run(oc.txDigest, oc.objectId, oc.changeType, ...);
    }
  }
  
  // ... same for all record types
}
```

### Pool method: `decodeAndWriteRange`

In `src/checkpoint-decoder-pool.ts`, add a new method that sends `decode-and-write-range` to workers:

```typescript
decodeAndWriteRange(
  from: bigint,
  to: bigint,
  cacheDir: string,
  shardDir: string,
  enabledProcessors: EnabledProcessors,
): Promise<string[]> {
  // Split range across workers (same as decodeCachedRange)
  const ranges = splitRange(from, to, workers.length);
  const shardPaths: string[] = [];
  
  return new Promise((resolve) => {
    let doneCount = 0;
    
    for (let i = 0; i < workers.length; i++) {
      const shardPath = `${shardDir}/shard-${i}.db`;
      shardPaths.push(shardPath);
      
      workers[i].postMessage({
        type: "decode-and-write-range",
        from: ranges[i].from.toString(),
        to: ranges[i].to.toString(),
        cacheDir,
        workerIndex: i,
        sqlitePath: shardPath,
        enabledProcessors,
      });
    }
    
    // Listen for "done" from each worker
    // When all done, resolve with shard paths
    // (Use the existing activeStream/doneWorkers pattern)
  });
}
```

### Archive source integration

In `src/pipeline/sources/archive.ts`, add a new Phase 2 path for snapshot + SQLite:

```typescript
// ─── Phase 2: Decode + Write to SQLite shards ─────────────
if (config.sqliteSharding) {
  const shardDir = join(cacheDir, "..", "shards");
  mkdirSync(shardDir, { recursive: true });
  
  const shardPaths = await pool.decodeAndWriteRange(
    config.from, endSeq, cacheDir, shardDir, config.enabledProcessors,
  );
  
  // Yield a single "done" checkpoint with metadata for the merge step
  // The pipeline consumer will handle merging
  const checkpoint: Checkpoint = {
    sequenceNumber: endSeq,
    timestamp: new Date(), // will be overwritten during merge
    transactions: [],
    source: "backfill",
    // ... attach shard paths as metadata
  };
  (checkpoint as any)._shardPaths = shardPaths;
  yield checkpoint;
}
```

### Merge step in sql.ts

In `sql.ts`'s `write()` method, detect shard paths and merge:

```typescript
if ((batch[0] as any)?._shardPaths) {
  const shardPaths: string[] = (batch[0] as any)._shardPaths;
  await mergeSqliteShards(driver, shardPaths);
  return;
}
```

```typescript
async function mergeSqliteShards(driver: SqlDriver, shardPaths: string[]): Promise<void> {
  for (let i = 0; i < shardPaths.length; i++) {
    const alias = `shard${i}`;
    await driver.exec(`ATTACH DATABASE '${shardPaths[i]}' AS ${alias}`);
    
    // Merge each table
    const tables = ['transactions', 'move_calls', 'balance_changes', 'object_changes',
                    'transaction_dependencies', 'transaction_inputs', 'commands',
                    'system_transactions', 'unchanged_consensus_objects', 'checkpoints'];
    
    for (const table of tables) {
      // Check if table exists in shard
      const exists = await driver.exec(
        `SELECT name FROM ${alias}.sqlite_master WHERE type='table' AND name='${table}'`
      );
      if (exists.length > 0) {
        await driver.exec(`INSERT INTO ${table} SELECT * FROM ${alias}.${table}`);
      }
    }
    
    await driver.exec(`DETACH DATABASE ${alias}`);
    // Delete shard file
    unlinkSync(shardPaths[i]);
  }
}
```

## Important Details

### Table DDL

The worker needs the exact same CREATE TABLE statements as `sql.ts` snapshot mode. The simplest approach: extract the DDL generation into a shared module that both `sql.ts` and the worker can import.

Create `src/pipeline/destinations/sql-ddl.ts` with:
```typescript
export function getSnapshotCreateTableSQL(table: string): string {
  // Return the CREATE TABLE statement for snapshot mode (no PK, no UNIQUE)
  // Extracted from sql.ts initialize()
}
```

### Binary Parser in Worker

The worker needs to import `parseBinaryCheckpoint` from `src/binary-parser.ts`. This is a pure JS function that reads DataView + TextDecoder — works fine in workers.

### Rust FFI in Worker

The worker already imports `decodeArchiveCheckpointBinary` from `src/checkpoint-native-decoder.ts`. This does Bun FFI `dlopen`. Workers support FFI — the existing `handleDecodeCachedRange` already does this.

### Balance Materialization

The merge step should happen BEFORE deferred index creation and balance materialization in `sql.ts`'s `shutdown()`. The materialization query (`INSERT INTO balances SELECT ... FROM balance_changes GROUP BY ...`) runs on the merged main DB.

### Shard Directory

Use a deterministic session directory based on the indexing config. The path should be under `~/.jun/shards/` with a hash of the config:

```typescript
import { createHash } from "crypto";

function shardSessionDir(from: bigint, to: bigint, sqlitePath: string): string {
  const hash = createHash("sha256")
    .update(`${from}-${to}-${sqlitePath}`)
    .digest("hex")
    .slice(0, 12);
  const dir = join(homedir(), ".jun", "shards", hash);
  mkdirSync(dir, { recursive: true });
  return dir;
}
```

This gives paths like `~/.jun/shards/a1b2c3d4e5f6/shard-0.db`. Deterministic so re-runs with the same config reuse the same directory. Clean up the session directory after successful merge.

### When to Use Sharding

Only for snapshot mode with SQLite. The regular `pipeline run` (live streaming) and Postgres continue using the existing write buffer path.

Detection: `config.deferIndexes && isSqlite` → use sharding path.

Or add a flag: `--sqlite-sharding` / `config.sqliteSharding`.

### Row Mapping Reference

Each record type needs its fields mapped to INSERT parameters. Here's the column order for each table (from `sql.ts`):

**transactions** (20 columns):
`digest, sender, success, computation_cost, storage_cost, storage_rebate, non_refundable_storage_fee, move_call_count, checkpoint_seq, sui_timestamp, epoch, error_kind, error_description, error_command_index, error_abort_code, error_module, error_function, events_digest, lamport_version, dependency_count`

**move_calls** (7 columns):
`tx_digest, call_index, package, module, function, checkpoint_seq, sui_timestamp`

**balance_changes** (6 columns):
`tx_digest, checkpoint_seq, address, coin_type, amount, sui_timestamp`

**object_changes** (15 columns):
`tx_digest, object_id, change_type, object_type, input_version, input_digest, input_owner, input_owner_kind, output_version, output_digest, output_owner, output_owner_kind, is_gas_object, checkpoint_seq, sui_timestamp`

**transaction_dependencies** (4 columns):
`tx_digest, depends_on_digest, checkpoint_seq, sui_timestamp`

**transaction_inputs** (14 columns):
`tx_digest, input_index, kind, object_id, version, digest, mutability, initial_shared_version, pure_bytes, amount, coin_type, source, checkpoint_seq, sui_timestamp`

**commands** (10 columns):
`tx_digest, command_index, kind, package, module, function, type_arguments, args, checkpoint_seq, sui_timestamp`

**system_transactions** (5 columns):
`tx_digest, kind, data, checkpoint_seq, sui_timestamp`

**unchanged_consensus_objects** (8 columns):
`tx_digest, object_id, kind, version, digest, object_type, checkpoint_seq, sui_timestamp`

**checkpoints** (11 columns):
`sequence_number, epoch, digest, previous_digest, content_digest, sui_timestamp, total_network_transactions, rolling_computation_cost, rolling_storage_cost, rolling_storage_rebate, rolling_non_refundable_storage_fee`

### Field Mapping from ParsedBinaryCheckpoint

The binary parser returns `ParsedBinaryCheckpoint` with:
- `checkpoint: Checkpoint` — has `sequenceNumber`, `epoch`, `timestamp`, `digest`, etc.
- `processed: ProcessedCheckpoint` — has `transactions: TransactionRecord[]`, `objectChanges: ObjectChangeRecord[]`, etc.

Each record type has camelCase fields that map to snake_case columns. Example:
- `TransactionRecord.digest` → `digest`
- `TransactionRecord.computationCost` → `computation_cost`
- `TransactionRecord.checkpointSeq` → `checkpoint_seq`
- `TransactionRecord.timestamp` → `.toISOString()` → `sui_timestamp`

For the timestamp, `TransactionRecord.timestamp` is a `Date` object. Use `.toISOString()` for SQLite TEXT storage.

For boolean fields like `success`: use `1`/`0` for SQLite.

For bigint fields like `checkpointSeq`: use `.toString()` for SQLite TEXT storage.

## Files to Modify

1. **`src/checkpoint-decoder.ts`** — add `handleDecodeAndWriteRange` handler
2. **`src/checkpoint-decoder-pool.ts`** — add `decodeAndWriteRange` pool method
3. **`src/pipeline/sources/archive.ts`** — add sharding Phase 2 path
4. **`src/pipeline/destinations/sql.ts`** — add shard merge in write(), detect sharding mode
5. **NEW: `src/pipeline/destinations/sql-ddl.ts`** — shared DDL generation

## Files to Read

- `src/checkpoint-decoder.ts` — existing worker handlers, especially `handleDecodeCachedRange`
- `src/checkpoint-decoder-pool.ts` — pool methods, especially `decodeCachedRange`
- `src/pipeline/destinations/sql.ts` — current write path, DDL, shutdown
- `src/binary-parser.ts` — `parseBinaryCheckpoint()` function and return types
- `src/pipeline/types.ts` — `TransactionRecord`, `ObjectChangeRecord`, etc. field definitions

## Testing

```bash
# Snapshot with sharding (10K checkpoints)
rm -f /tmp/shard-test.db && bun run src/cli.ts pipeline snapshot \
  --start-checkpoint 260650000 --end-checkpoint 260660000 \
  --everything --sqlite /tmp/shard-test.db \
  --workers 8 --concurrency 500 --quiet --yes

# Verify row counts match non-sharded version
bun -e "const{Database}=require('bun:sqlite');const db=new Database('/tmp/shard-test.db',{readonly:true});const tables=db.query(\"SELECT name FROM sqlite_master WHERE type='table' AND name!='sqlite_sequence'\").all().map(r=>r.name);for(const t of tables)console.log(t,db.query('SELECT COUNT(*) as c FROM '+t).get().c);"
```

Expected: identical row counts to the non-sharded version.

## Performance Target

Current: ~1,475 cp/s with SQLite writes (single-threaded)
Target: ~4,000+ cp/s (8 parallel SQLite writers, each handling 1/8 of the data)

The merge step at shutdown adds a fixed cost (bulk INSERT from shard to main DB) but this scales linearly with data size and uses SQLite's internal bulk transfer which is fast.
