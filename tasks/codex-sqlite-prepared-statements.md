# Codex Task: Switch SQLite Writes to Prepared Statements

## Goal

Replace the current dynamic `INSERT INTO ... VALUES (?,?,...),(?,?,...),...` SQL string building approach with cached prepared statements and per-row `stmt.run()` calls inside a `db.transaction()`. This eliminates SQL string construction, parameter array building, and the chunking logic for SQLite's 32K parameter limit.

**Current (slow):**
```
Build SQL string: "INSERT INTO table (c1,c2,...) VALUES (?,?,...),(?,?,...),...(?,?,...)"
Collect all params into flat array: [v1, v2, ..., vN]
Chunk if params > 30000
Execute dynamic SQL with params
```

**Target (fast):**
```
const stmt = db.query("INSERT INTO table (c1,c2,...) VALUES (?,?,...)"); // cached, created once
db.transaction(() => {
  for (const row of rows) stmt.run(row[0], row[1], ...row[N]);
})();
```

Benchmark shows prepared statements are 58% faster than batched VALUES for 65K rows × 15 columns (55ms vs 87ms).

## File to Modify

`src/pipeline/destinations/sql.ts`

## What to Change

### 1. Add prepared statement cache to SqliteDriver

The `createSqliteDriver` function creates the SQLite driver. Add a `Map<string, Statement>` cache and a method to get-or-create prepared statements:

```typescript
function createSqliteDriver(path: string): SqlDriver {
  const database = createSqliteConnection(path);
  database.exec("PRAGMA temp_store = MEMORY;");
  
  // Cache prepared INSERT statements per table
  const stmtCache = new Map<string, ReturnType<typeof database.query>>();
  
  function getInsertStmt(table: string, columns: string[]): ReturnType<typeof database.query> {
    const key = table;
    let stmt = stmtCache.get(key);
    if (!stmt) {
      const placeholders = columns.map(() => "?").join(", ");
      stmt = database.query(`INSERT INTO ${table} (${columns.join(", ")}) VALUES (${placeholders})`);
      stmtCache.set(key, stmt);
    }
    return stmt;
  }
  
  return {
    dialect: "sqlite",
    // ... existing exec/transaction/close methods ...
    
    // NEW: bulk insert using prepared statements
    insertBulk(table: string, columns: string[], rows: unknown[][]): void {
      if (rows.length === 0) return;
      const stmt = getInsertStmt(table, columns);
      for (const row of rows) {
        stmt.run(...(row as any[]));
      }
    },
  };
}
```

Add `insertBulk` to the `SqlDriver` interface as an optional method:
```typescript
interface SqlDriver {
  dialect: Dialect;
  exec(query: string, params?: unknown[]): Promise<any>;
  transaction(fn: (exec: ExecFn, rawTx?: any) => Promise<void>): Promise<void>;
  close(): Promise<void>;
  /** Optimized bulk insert for SQLite — prepared statement per row inside existing transaction. */
  insertBulk?(table: string, columns: string[], rows: unknown[][]): void;
}
```

### 2. Update `insertRows` to use prepared statements for SQLite

In the `insertRows` function, add a fast path for SQLite when `insertBulk` is available:

```typescript
async function insertRows(
  ctx: WriteContext,
  table: string,
  columns: string[],
  rows: unknown[][],
  conflictClause: string,
  pgTypes?: string[],
): Promise<void> {
  if (rows.length === 0) return;

  // SQLite fast path: prepared statement per row (no SQL string building, no chunking)
  if (ctx.dialect === "sqlite" && ctx.snapshotMode && ctx.driver?.insertBulk) {
    ctx.driver.insertBulk(table, columns, rows);
    return;
  }

  // ... existing Postgres unnest path and SQLite VALUES fallback ...
}
```

**Important**: Only use `insertBulk` in `snapshotMode` (no conflict clause needed). For non-snapshot mode, keep the existing `ON CONFLICT` approach since prepared statements can't include conditional conflict handling per-batch.

Actually, you CAN add the conflict clause to the prepared statement SQL:
```sql
INSERT INTO table (c1, c2) VALUES (?, ?) ON CONFLICT (pk) DO NOTHING
```
This works fine as a prepared statement. So the fast path can be used for both snapshot and non-snapshot modes.

### 3. Pass driver reference through WriteContext

Currently `WriteContext` has `{dialect, exec, rawTx, snapshotMode}`. Add the driver reference:

```typescript
interface WriteContext {
  dialect: Dialect;
  exec: ExecFn;
  rawTx?: any;
  snapshotMode: boolean;
  driver?: SqlDriver; // NEW: for direct access to insertBulk
}
```

Update the `write()` method to pass `driver` through:
```typescript
const ctx: WriteContext = { dialect, exec, rawTx, snapshotMode, driver: d };
```

### 4. Update the transaction in createSqliteDriver

The current SQLite transaction uses manual `BEGIN`/`COMMIT`. Switch to Bun's `db.transaction()` API which is optimized:

```typescript
async transaction(fn) {
  const txFn = database.transaction(async () => {
    const exec = async (query: string, params?: unknown[]) => {
      if (params && params.length > 0) return database.prepare(query).all(...(params as any[]));
      return database.exec(query);
    };
    await fn(exec);
  });
  txFn();
},
```

Wait — `db.transaction()` returns a sync callable. But `fn` is async. Bun's `db.transaction()` may not handle async correctly. Keep the manual `BEGIN`/`COMMIT` approach for now. The important thing is that `insertBulk` calls `stmt.run()` synchronously inside the existing transaction.

### 5. Remove chunking for SQLite path

The `MAX_SQLITE_PARAMS` constant and the chunking logic in `insertRows` become unnecessary for the prepared statement path since each `stmt.run()` only binds one row's worth of parameters (never exceeds the limit). Keep the chunking for the fallback VALUES path.

## What NOT to Change

- **Postgres path**: Keep the `unnest()` bulk insert approach for Postgres — it's already optimized for that dialect.
- **Non-snapshot conflict handling**: The prepared statement can include `ON CONFLICT DO NOTHING`. Include it in the cached SQL when not in snapshot mode.
- **Write function signatures**: Keep `writeTransactions()`, `writeObjectChanges()`, etc. unchanged. They build `rows` arrays and call `insertRows()`. The optimization is inside `insertRows()`.
- **Binary parsing**: Don't change `parseBinaryCheckpoint()` in this task.

## Performance Profile (current, for 969 checkpoints)

```
binary_parse=208ms
events=0ms checkpoints=3ms balances=10ms transactions=28ms
move_calls=35ms object_changes=155ms dependencies=55ms
inputs=189ms commands=63ms system_txs=4ms uco=54ms
total_sql=596ms
```

Expected improvement: inputs and object_changes should each drop by ~50% since they're the highest-row-count tables with the most columns (most affected by SQL string building overhead).

## Testing

```bash
# Before (current):
rm -f /tmp/perf-old.db && bun run src/cli.ts pipeline snapshot --start-checkpoint 260650000 --end-checkpoint 260651000 --everything --sqlite /tmp/perf-old.db --workers 4 --concurrency 50 --quiet --yes 2>&1 | grep write-profile

# After (prepared statements):
rm -f /tmp/perf-new.db && bun run src/cli.ts pipeline snapshot --start-checkpoint 260650000 --end-checkpoint 260651000 --everything --sqlite /tmp/perf-new.db --workers 4 --concurrency 50 --quiet --yes 2>&1 | grep write-profile
```

Note: The `write-profile` logging is already instrumented in `sql.ts` (timing around each table write). Compare the per-table timings.

Also verify data correctness:
```bash
bun -e "const{Database}=require('bun:sqlite');const db=new Database('/tmp/perf-new.db',{readonly:true});const tables=db.query(\"SELECT name FROM sqlite_master WHERE type='table' AND name!='sqlite_sequence'\").all().map(r=>r.name);for(const t of tables)console.log(t,db.query('SELECT COUNT(*) as c FROM '+t).get().c);"
```

Row counts should match the old approach exactly.

## Files to Read

- `src/pipeline/destinations/sql.ts` — the entire write path
- `src/db.ts` — `createSqliteConnection()` returns a `bun:sqlite` Database instance
- Bun SQLite docs: `db.query()` caches prepared statements, `stmt.run()` is fastest for writes, `db.transaction()` wraps in BEGIN/COMMIT
