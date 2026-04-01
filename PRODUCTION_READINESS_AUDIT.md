# Production Readiness Audit

Scope: every file under `src/`

Date: 2026-04-01

Test run: `bun test`

Result: `239` passing, `33` failing

Notes:
- Every file in `src/` was reviewed.
- Files not called out below were reviewed and did not surface production-blocking findings beyond re-export/minimal wrapper concerns.
- This report is grouped by severity, highest first.

## Critical

### 1. Backfill watermark can advance past unbuffered checkpoints

- File: `src/index.ts:349`, `src/index.ts:400`, `src/index.ts:404`
- Severity: `CRITICAL`
- Category: `race condition`

Description:

`completed.add(seq)` and `advanceWatermark()` happen before `buffer.push()`. With concurrent `pMap` workers, checkpoint `N+1` can advance the watermark to `N+1` before checkpoint `N` has actually been enqueued. If the process crashes after persisting that cursor, `N` is skipped on resume.

Snippet:

```ts
completed.add(seq);
const wm = advanceWatermark();

await buffer.push(decoded, cursorKey, wm);
```

Concrete fix:

```ts
const pending = new Map<bigint, DecodedEvent[]>();
let watermark = startFrom - 1n;

async function drainContiguousReadyCheckpoints() {
  while (pending.has(watermark + 1n)) {
    const next = watermark + 1n;
    const ready = pending.get(next)!;
    await buffer.push(ready, cursorKey, next);
    pending.delete(next);
    watermark = next;
    metrics.setBackfillCursor(watermark);
    metrics.recordBackfillCheckpoint();
  }
}

// inside each worker:
pending.set(seq, decoded);
await drainContiguousReadyCheckpoints();
```

### 2. WriteBuffer drops batches after retry exhaustion

- File: `src/buffer.ts:79`, `src/buffer.ts:97`, `src/buffer.ts:160`
- Severity: `CRITICAL`
- Category: `error handling`

Description:

The buffer and cursor map are cleared before the flush I/O begins. If Postgres stays down long enough for `p-retry` to exhaust, the in-memory batch is gone and cannot be retried.

Snippet:

```ts
const batch = buffer;
const cursorSnapshot = new Map(cursors);
buffer = [];
cursors = new Map();

const flushPromise = pRetry(async () => {
  ...
});
```

Concrete fix:

```ts
const batch = buffer;
const cursorSnapshot = new Map(cursors);
buffer = [];
cursors = new Map();

try {
  await flushBatch(batch, cursorSnapshot);
} catch (err) {
  buffer = batch.concat(buffer);
  for (const [key, seq] of cursorSnapshot) {
    const prev = cursors.get(key);
    if (prev === undefined || seq > prev) {
      cursors.set(key, seq);
    }
  }
  throw err;
}
```

Also split the body into a dedicated `flushBatch(batch, cursorSnapshot)` helper so the restore path is explicit.

### 3. `/admin/reload` is unauthenticated and reaches unsafe DDL paths

- File: `src/serve.ts:327`, `src/serve.ts:399`, `src/indexer-config.ts:107`, `src/schema.ts:153`, `src/hot-reload.ts:160`
- Severity: `CRITICAL`
- Category: `security`

Description:

`/admin/reload` accepts arbitrary YAML without authentication. That data flows into DDL generation and `sql.unsafe()` through handler names and field names. A reachable admin endpoint can mutate schema and can be driven into SQL injection via crafted identifiers.

Snippet:

```ts
if (url.pathname === "/admin/reload" && req.method === "POST") {
  return handleReload(req, hotReload, log);
}
```

```ts
const ddl = generateDDL(tableName, handler.fields);
await ctx.sql.unsafe(ddl);
```

Concrete fix:

```ts
async function handleReload(
  req: Request,
  hotReload: HotReloadContext | undefined,
  log: Logger,
): Promise<Response> {
  const token = process.env.JUN_ADMIN_TOKEN;
  const auth = req.headers.get("authorization");

  if (!token || auth !== `Bearer ${token}`) {
    return Response.json({ error: "unauthorized" }, { status: 401 });
  }

  ...
}
```

And validate identifiers during config parsing and DDL generation:

```ts
for (const [name, handler] of Object.entries(config.events) as [string, any][]) {
  validateIdentifier(toTableName(name));
  for (const field of Object.keys(handler.fields)) {
    validateIdentifier(field);
  }
}
```

```ts
export function generateDDL(tableName: string, fields: FieldDefs): string {
  validateIdentifier(tableName);
  for (const name of Object.keys(fields)) {
    validateIdentifier(name);
  }
  ...
}
```

## High

### 4. Hot reload updates metadata but not the active Postgres writer

- File: `src/hot-reload.ts:163`, `src/hot-reload.ts:193`, `src/output/postgres.ts:39`, `src/output/postgres.ts:64`
- Severity: `HIGH`
- Category: `bug`

Description:

`applyReload()` updates `ctx.handlerTables`, but `createPostgresOutput()` captured its own internal `tables` map at startup. New handlers get dropped by `if (!table) return`, and altered handlers keep the old column list.

Snippet:

```ts
const tables = new Map<string, TableConfig>();
...
const table = tables.get(handlerName);
if (!table) return;
```

Concrete fix:

```ts
export interface PostgresOutput extends StorageBackend {
  reloadHandlers(handlers: Record<string, { tableName: string; fields: FieldDefs }>): void;
}

function buildTables(
  handlers: Record<string, { tableName: string; fields: FieldDefs }>,
): Map<string, TableConfig> {
  const tables = new Map<string, TableConfig>();
  for (const [handlerName, config] of Object.entries(handlers)) {
    const fieldNames = Object.keys(config.fields);
    tables.set(handlerName, {
      name: config.tableName,
      fields: config.fields,
      columns: ["tx_digest", "event_seq", "sender", "sui_timestamp", ...fieldNames],
    });
  }
  return tables;
}

export function createPostgresOutput(...): PostgresOutput {
  let tables = buildTables(handlers);

  return {
    ...
    reloadHandlers(nextHandlers) {
      tables = buildTables(nextHandlers);
    },
  };
}
```

Then in `applyReload()`:

```ts
ctx.output.reloadHandlers?.(ctx.handlerTables);
```

### 5. `processed_checkpoints` tracking is incorrect for empty checkpoints

- File: `src/buffer.ts:85`, `src/buffer.ts:121`, `src/buffer.ts:174`, `src/gaps.ts:144`
- Severity: `HIGH`
- Category: `bug`

Description:

Gap tracking uses cursor snapshots to cover empty checkpoints, but cursor coalescing keeps only the highest sequence per key. If many checkpoints have no matching events in one flush, only the max sequence is recorded and the gap repair loop later sees false gaps.

Snippet:

```ts
for (const seq of cursorSnapshot.values()) {
  checkpointSeqs.add(seq);
}
```

Concrete fix:

Track every pushed checkpoint explicitly:

```ts
let processedSeqs = new Set<bigint>();

async push(events: DecodedEvent[], cursorKey: string, seq: bigint): Promise<void> {
  processedSeqs.add(seq);
  ...
}

async function doFlush(): Promise<void> {
  const processedSnapshot = processedSeqs;
  processedSeqs = new Set();
  ...
  await state.recordProcessedCheckpoints([...processedSnapshot]);
}
```

### 6. `/query` enforces row limits after executing the full query

- File: `src/serve.ts:295`, `src/serve.ts:301`, `src/serve.ts:307`
- Severity: `HIGH`
- Category: `security`

Description:

`rowLimit` is applied only after fetching all rows from Postgres and materializing them into memory. An expensive or huge `SELECT` can still overwhelm the database and the Bun process.

Snippet:

```ts
const rows = await sql.begin(async (tx: any) => {
  await tx.unsafe(`SET TRANSACTION READ ONLY`);
  await tx.unsafe(`SET LOCAL statement_timeout = '${timeoutMs}ms'`);
  return await tx.unsafe(rawSql.trim());
});

const result = Array.from(rows);
const truncated = result.length > rowLimit;
```

Concrete fix:

```ts
const stmt = rawSql.trim().replace(/;+\s*$/, "");
const rows = await sql.begin(async (tx: any) => {
  await tx.unsafe("SET TRANSACTION READ ONLY");
  await tx.unsafe(`SET LOCAL statement_timeout = '${timeoutMs}ms'`);
  return tx.unsafe(
    `SELECT * FROM (${stmt}) AS q LIMIT $1`,
    [rowLimit + 1],
  );
});

const result = Array.from(rows);
const truncated = result.length > rowLimit;
const sliced = truncated ? result.slice(0, rowLimit) : result;
```

If `EXPLAIN` support is required, handle it as a separate allowed code path.

### 7. SSE fan-out has no backpressure or connection cap

- File: `src/broadcast.ts:117`, `src/broadcast.ts:201`, `src/broadcast.ts:246`
- Severity: `HIGH`
- Category: `memory leak`

Description:

`enqueue()` is called blindly for every client. Slow or stalled SSE consumers can build unbounded internal queues. There is also no max client count, so clients can exhaust memory and CPU.

Snippet:

```ts
client.controller.enqueue(data);
```

Concrete fix:

```ts
const MAX_SSE_CLIENTS = 1000;

addSSEClient(client: SSEClient): () => void {
  if (sseClients.size >= MAX_SSE_CLIENTS) {
    throw new Error("too many SSE clients");
  }
  sseClients.add(client);
  ...
}

function sendToSSE(client: SSEClient, data: string): boolean {
  try {
    if ((client.controller.desiredSize ?? 1) <= 0) {
      sseClients.delete(client);
      return false;
    }
    client.controller.enqueue(data);
    return true;
  } catch {
    sseClients.delete(client);
    return false;
  }
}
```

### 8. Broadcast targets can throw into the indexing path

- File: `src/broadcast.ts:146`, `src/broadcast.ts:169`, `src/broadcast.ts:195`, `src/broadcast.ts:241`
- Severity: `HIGH`
- Category: `error handling`

Description:

Target publishes are unguarded. A NATS publish failure or a buggy custom target can break the live/backfill path because broadcast happens inline with checkpoint processing.

Snippet:

```ts
for (const target of targets) {
  target.publishCheckpoint(checkpointData);
}
```

Concrete fix:

```ts
function publishSafely(fn: () => void) {
  try {
    fn();
  } catch (err) {
    broadcastLog.error({ err }, "broadcast target publish failed");
  }
}

for (const target of targets) {
  publishSafely(() => target.publishCheckpoint(checkpointData));
}
```

### 9. gRPC async iterator uses an unbounded in-memory queue

- File: `src/grpc.ts:191`, `src/grpc.ts:199`, `src/grpc.ts:236`
- Severity: `HIGH`
- Category: `memory leak`

Description:

If downstream handling is slower than the gRPC stream, `buffer.push(response)` grows without limit. This is a real 24/7 risk under Postgres slowdown or broadcast pressure.

Snippet:

```ts
const buffer: GrpcCheckpointResponse[] = [];
...
buffer.push(response);
```

Concrete fix:

```ts
const buffer: GrpcCheckpointResponse[] = [];
const MAX_BUFFER = 1024;
let streamError: Error | null = null;

call.on("data", (response: GrpcCheckpointResponse) => {
  if (buffer.length >= MAX_BUFFER) {
    call.pause();
  }
  ...
  buffer.push(response);
});

call.on("error", (err: any) => {
  streamError = err;
  ...
});

next(): Promise<IteratorResult<GrpcCheckpointResponse>> {
  if (buffer.length > 0) {
    const value = buffer.shift()!;
    if (buffer.length < MAX_BUFFER / 2) {
      call.resume();
    }
    return Promise.resolve({ value, done: false });
  }
  if (streamError) return Promise.reject(streamError);
  ...
}
```

### 10. Production backfill has no retry/timeout around archive fetches

- File: `src/index.ts:381`, `src/index.ts:387`, `src/archive.ts:111`, `src/throttle.ts:153`
- Severity: `HIGH`
- Category: `error handling`

Description:

The main backfill path fetches archive checkpoints once, with no timeout and no retry. Transient CDN failures are logged and silently skipped. The throttle failure path is defined but never used.

Snippet:

```ts
const compressed = await fetchCompressed(seq, resolvedArchiveUrl);
const response = await decoderPool.decode(seq, compressed);
```

Concrete fix:

```ts
const response = await pRetry(
  async () => {
    const compressed = await fetchCompressed(seq, resolvedArchiveUrl, AbortSignal.timeout(30_000));
    return decoderPool.decode(seq, compressed);
  },
  {
    retries: 5,
    factor: 2,
    minTimeout: 1000,
  },
);
```

And wire buffer failures into throttle:

```ts
onFlushError: () => {
  throttle.recordFailure();
}
```

### 11. Parquet backend loses data on flush failure and corrupts large integers

- File: `src/output/parquet.ts:61`, `src/output/parquet.ts:151`, `src/output/parquet.ts:182`, `src/output/parquet.ts:229`
- Severity: `HIGH`
- Category: `bug`

Description:

The accumulator is cleared before file and upload I/O. A failed write loses the batch. Also `u64` is mapped to `INT64`, but `parquetjs` converts via `parseInt`, which loses precision above `2^53 - 1`.

Snippet:

```ts
case "u64": return { type: "INT64", optional: false };
...
const snapshot = new Map(accumulated);
accumulated.clear();
```

Concrete fix:

```ts
case "u64":
case "u128":
case "u256":
  return { type: "UTF8", optional: false };
```

```ts
const snapshot = new Map(accumulated);
const snapshotCount = accumulatedCount;
const snapshotSeq = maxCheckpointSeq;

try {
  await writeSnapshot(snapshot);
  accumulated.clear();
  accumulatedCount = 0;
  if (snapshotSeq > 0n) writeState(statePath, snapshotSeq);
} catch (err) {
  for (const [handlerName, events] of snapshot) {
    const existing = accumulated.get(handlerName);
    if (existing) existing.unshift(...events);
    else accumulated.set(handlerName, [...events]);
  }
  accumulatedCount += snapshotCount;
  throw err;
}
```

### 12. Config validation accepts unsafe or broken operational values

- File: `src/indexer-config.ts:126`, `src/index.ts:744`, `src/cli.ts:2723`, `src/checkpoint-decoder-pool.ts:46`
- Severity: `HIGH`
- Category: `inconsistency`

Description:

Invalid concurrency/worker values are accepted, and unknown `network` silently falls back to mainnet archive. This can point a custom network indexer at mainnet history or break worker pool startup.

Snippet:

```ts
const workerCount = backfillWorkers ?? defaultWorkerCount();
...
default:
  return "https://checkpoints.mainnet.sui.io";
```

Concrete fix:

```ts
if (config.backfillConcurrency !== undefined) {
  if (!Number.isInteger(config.backfillConcurrency) || config.backfillConcurrency < 1) {
    throw new Error("Invalid config: backfillConcurrency must be an integer >= 1");
  }
}

if (config.backfillWorkers !== undefined) {
  if (!Number.isInteger(config.backfillWorkers) || config.backfillWorkers < 1) {
    throw new Error("Invalid config: backfillWorkers must be an integer >= 1");
  }
}

if (!["mainnet", "testnet"].includes(config.network) && !config.archiveUrl) {
  throw new Error("Invalid config: custom networks must set archiveUrl explicitly");
}
```

And:

```ts
function getDefaultArchiveUrl(network: string): string {
  switch (network) {
    case "mainnet":
      return "https://checkpoints.mainnet.sui.io";
    case "testnet":
      return "https://checkpoints.testnet.sui.io";
    default:
      throw new Error(`No default archive URL for network "${network}"`);
  }
}
```

## Medium

### 13. Cursor updates can move backwards under concurrent writers

- File: `src/state.ts:57`
- Severity: `MEDIUM`
- Category: `race condition`

Description:

`setCheckpointCursor()` blindly overwrites the existing value. Concurrent processes or out-of-order callers can regress the cursor.

Snippet:

```ts
ON CONFLICT (key)
DO UPDATE SET checkpoint_seq = ${seq.toString()}, updated_at = NOW()
```

Concrete fix:

```ts
await sql`
  INSERT INTO indexer_checkpoints (key, checkpoint_seq, updated_at)
  VALUES (${key}, ${seq.toString()}, NOW())
  ON CONFLICT (key)
  DO UPDATE SET
    checkpoint_seq = GREATEST(indexer_checkpoints.checkpoint_seq, EXCLUDED.checkpoint_seq),
    updated_at = NOW()
`;
```

### 14. Live transaction sender filtering is inconsistent with the default read mask

- File: `src/grpc.ts:19`, `src/broadcast.ts:155`, `src/serve.ts:220`
- Severity: `MEDIUM`
- Category: `inconsistency`

Description:

The default live read mask does not request `transactions.transaction`, but transaction broadcasts and SSE `sender=` filtering depend on `tx.transaction?.sender`.

Snippet:

```ts
const READ_MASK_PATHS = [
  "transactions.events",
  "transactions.digest",
  "summary.timestamp",
];
```

Concrete fix:

```ts
const READ_MASK_PATHS = [
  "transactions.events",
  "transactions.digest",
  "transactions.transaction",
  "summary.timestamp",
];
```

### 15. Checkpoint worker pool clones large buffers instead of transferring them

- File: `src/checkpoint-decoder-pool.ts:94`
- Severity: `MEDIUM`
- Category: `performance`

Description:

Compressed checkpoint payloads are copied into workers rather than transferred, increasing CPU and memory pressure in the hottest backfill path.

Snippet:

```ts
worker.postMessage({
  id,
  seq: seq.toString(),
  compressed,
});
```

Concrete fix:

```ts
worker.postMessage(
  {
    id,
    seq: seq.toString(),
    compressed,
  },
  [compressed.buffer],
);
```

## Low

### 16. `stream --jsonl` writes to an undefined variable

- File: `src/cli.ts:396`
- Severity: `LOW`
- Category: `bug`

Description:

The stream command creates `jsonWriter`, but writes to `writer`. That path throws when JSONL output is used without SQLite.

Snippet:

```ts
writer.write(JSON.stringify(record));
```

Concrete fix:

```ts
jsonWriter!.write(JSON.stringify(record));
```

## Test Coverage Gaps

Untested or effectively uncovered production paths:

- `src/index.ts`
- `src/grpc.ts`
- `src/archive.ts`
- `src/broadcast.ts`
- `src/cache.ts`
- `src/checkpoint-decoder-pool.ts`
- `src/checkpoint-decoder.ts`
- `src/output/parquet.ts`
- `src/output/postgres.ts`
- `src/state.ts`
- `src/mcp.ts`
- `src/message.ts`
- `src/verify.ts`
- `src/rpc.ts`
- `src/sui-bcs.ts`

Missing test classes:

- End-to-end durability test for `buffer -> storage -> cursor`
- Postgres outage and recovery tests
- Archive `404`, `500`, and timeout tests in the production backfill path
- Worker crash / worker restart tests
- Broadcast target failure isolation tests
- SSE slow-consumer / client cap tests
- `run()` shutdown and cleanup tests
- `/admin/reload` auth tests
- Hot reload integration test proving new handlers actually write after reload
- Parquet precision and flush-failure recovery tests

Current test-suite reliability issues:

- `src/serve.test.ts` fails locally because `Bun.serve({ port: 0 })` does not start successfully in this environment.
- Many `src/cli.test.ts` cases are live-network dependent and fail without successful external RPC access.

## Reviewed With No Material Findings

Reviewed and not individually called out for production-readiness defects:

- `src/checkpoints.ts`
- `src/events.ts`
- `src/pipeline.ts`
- `src/cursor.ts`
- `src/logger.ts`
- `src/config.ts`
- `src/output/storage.ts`
- `src/output/sqlite-storage.ts`
- `src/output/sqlite.ts`
- `src/codegen.ts`
- `src/cli-helpers.ts`
- `src/signing.ts`
- `src/message.ts`

