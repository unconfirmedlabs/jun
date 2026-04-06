# Codex Task: Per-Table SQLite Writers

## Goal

Replace the single monolithic SQLite database with per-table SQLite files. Each table gets its own `bun:sqlite` Database connection and prepared statement cache. All writers run on the main thread.

**Current:**
```
All 10 tables → one SQLite file → one connection → sequential INSERTs in one transaction
```

**Target:**
```
transactions.db      ← own Database, own prepared stmt, own transaction
move_calls.db        ← own Database, own prepared stmt, own transaction  
balance_changes.db   ← own Database, own prepared stmt, own transaction
... (10 separate files)
```

## Why

1. Index creation parallelizes — 10 small DBs index in parallel vs 1 huge DB sequential
2. No merge step needed — split datasets are the natural output
3. File sizes stay manageable (~500MB each vs 44GB monolith)
4. Aligns with existing `--sqlite-export-split-datasets` feature
5. Each table can flush independently

## Architecture

### Output directory

When `--sqlite` is a directory path or ends with `/`, create per-table files:
```
--sqlite /tmp/epoch-1086/
```
Creates:
```
/tmp/epoch-1086/transactions.db
/tmp/epoch-1086/move_calls.db
/tmp/epoch-1086/balance_changes.db
/tmp/epoch-1086/object_changes.db
/tmp/epoch-1086/transaction_dependencies.db
/tmp/epoch-1086/transaction_inputs.db
/tmp/epoch-1086/commands.db
/tmp/epoch-1086/system_transactions.db
/tmp/epoch-1086/unchanged_consensus_objects.db
/tmp/epoch-1086/checkpoints.db
/tmp/epoch-1086/balances.db
```

When `--sqlite` is a file path (e.g. `/tmp/data.db`), keep the existing single-file behavior for backwards compatibility.

### Detection logic

In the CLI or config parser, detect if the path looks like a directory:
```typescript
const isSplitMode = sqlitePath.endsWith("/") || sqlitePath.endsWith(path.sep);
```

If split mode, `mkdirSync(sqlitePath, { recursive: true })` and create per-table DBs inside.

### SqlDriver changes

Currently `createSqliteDriver(path)` returns a single SqlDriver with one Database connection. For split mode, create a new driver that manages multiple connections:

```typescript
interface PerTableSqliteDriver extends SqlDriver {
  // Each table gets its own connection
  getTableDb(tableName: string): Database;
}

function createPerTableSqliteDriver(dir: string): SqlDriver {
  const connections = new Map<string, Database>();
  const stmtCaches = new Map<string, Map<string, Statement>>();
  
  function getOrCreateDb(tableName: string): Database {
    let db = connections.get(tableName);
    if (!db) {
      const dbPath = path.join(dir, `${tableName}.db`);
      db = new Database(dbPath);
      db.exec("PRAGMA synchronous = OFF");
      db.exec("PRAGMA journal_mode = OFF");
      db.exec("PRAGMA locking_mode = EXCLUSIVE");
      db.exec("PRAGMA temp_store = MEMORY");
      db.exec("PRAGMA page_size = 32768");
      connections.set(tableName, db);
    }
    return db;
  }
  
  return {
    dialect: "sqlite",
    
    async exec(query: string, params?: unknown[]) {
      // For DDL/schema queries, determine which table DB to use
      // Parse table name from CREATE TABLE, INSERT INTO, etc.
      const tableName = extractTableName(query);
      const db = getOrCreateDb(tableName ?? "default");
      if (params && params.length > 0) {
        return db.prepare(query).all(...(params as any[]));
      }
      return db.exec(query);
    },
    
    async transaction(fn) {
      // Begin transaction on ALL open connections
      for (const db of connections.values()) db.exec("BEGIN");
      try {
        const exec = async (query: string, params?: unknown[]) => {
          const tableName = extractTableName(query);
          const db = getOrCreateDb(tableName ?? "default");
          if (params && params.length > 0) return db.prepare(query).all(...(params as any[]));
          return db.exec(query);
        };
        await fn(exec);
        for (const db of connections.values()) db.exec("COMMIT");
      } catch (e) {
        for (const db of connections.values()) {
          try { db.exec("ROLLBACK"); } catch {}
        }
        throw e;
      }
    },
    
    insertBulk(table: string, columns: string[], rows: unknown[][], conflictClause = "") {
      if (rows.length === 0) return;
      const db = getOrCreateDb(table);
      const placeholders = columns.map(() => "?").join(", ");
      const normalizedConflict = conflictClause.trim();
      const sql = normalizedConflict
        ? `INSERT INTO ${table} (${columns.join(", ")}) VALUES (${placeholders}) ${normalizedConflict}`
        : `INSERT INTO ${table} (${columns.join(", ")}) VALUES (${placeholders})`;
      
      let cache = stmtCaches.get(table);
      if (!cache) { cache = new Map(); stmtCaches.set(table, cache); }
      let stmt = cache.get(sql);
      if (!stmt) { stmt = db.query(sql); cache.set(sql, stmt); }
      
      for (const row of rows) {
        stmt.run(...(row as any[]));
      }
    },
    
    async close() {
      for (const [name, db] of connections) {
        db.close();
      }
      connections.clear();
      stmtCaches.clear();
    },
  };
}
```

### Table name extraction

Need a helper to route queries to the right DB:

```typescript
function extractTableName(query: string): string | null {
  // Match: CREATE TABLE [IF NOT EXISTS] tablename
  // Match: INSERT INTO tablename
  // Match: SELECT ... FROM tablename
  // Match: CREATE [UNIQUE] INDEX ... ON tablename
  const match = query.match(
    /(?:CREATE\s+(?:UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?|INSERT\s+INTO\s+|FROM\s+|ON\s+)(\w+)/i
  );
  return match?.[1] ?? null;
}
```

### DDL: each table created in its own DB

The `initialize()` method in sql.ts creates tables. With per-table mode, each CREATE TABLE goes to the corresponding DB file. The existing code already calls `driver.exec(ddl)` per table — the routing in `exec()` handles the rest.

### Deferred indexes at shutdown

Currently creates indexes sequentially on one DB. With per-table mode, create indexes on each DB. This can be parallelized since they're independent files:

```typescript
async shutdown(): Promise<void> {
  if (isPerTableMode) {
    // Group deferred indexes by table
    const indexesByTable = new Map<string, string[]>();
    for (const sql of deferredIndexes) {
      const table = extractTableName(sql);
      if (table) {
        const list = indexesByTable.get(table) ?? [];
        list.push(sql);
        indexesByTable.set(table, list);
      }
    }
    
    // Create indexes in parallel (each on its own DB file)
    await Promise.all(
      Array.from(indexesByTable.entries()).map(async ([table, indexes]) => {
        const db = getOrCreateDb(table);
        for (const sql of indexes) {
          db.exec(sql);
        }
      })
    );
  }
  
  // Balance materialization
  if (config.balances) {
    // balances table lives in balance_changes.db (or balances.db)
    // The materialization query needs both balance_changes and balances tables
    // in the same DB. Create balances table in balance_changes.db:
    const bcDb = getOrCreateDb("balance_changes");
    bcDb.exec("CREATE TABLE IF NOT EXISTS balances (...)");
    bcDb.exec("INSERT INTO balances SELECT ... FROM balance_changes GROUP BY ...");
    // Or: create a separate balances.db and ATTACH balance_changes.db to populate it
  }
}
```

### Balance materialization

The `balances` table is derived from `balance_changes` via aggregation:
```sql
INSERT INTO balances (address, coin_type, balance, last_checkpoint)
SELECT address, coin_type, SUM(CAST(amount AS INTEGER)), MAX(checkpoint_seq)
FROM balance_changes GROUP BY address, coin_type
```

Two options:
1. **Same DB**: Put `balances` table inside `balance_changes.db`. Query only touches one file.
2. **Separate DB**: Create `balances.db`, ATTACH `balance_changes.db`, run the query. Cleaner separation.

Option 1 is simpler. The `balance_changes.db` file contains both tables.

### Integration with sharding

If the SQLite sharding path is active (workers write their own shard files), the per-table mode changes the shard structure. Instead of each worker creating a shard with ALL tables, each worker writes to per-table shard files:

```
~/.jun/shards/{hash}/worker-0/transactions.db
~/.jun/shards/{hash}/worker-0/object_changes.db
~/.jun/shards/{hash}/worker-1/transactions.db
~/.jun/shards/{hash}/worker-1/object_changes.db
...
```

Merge becomes: for each table, ATTACH all worker shards and INSERT SELECT.

**BUT**: for this first implementation, skip the sharding integration. Just use the main-thread per-table writer with the existing decode → main thread → write pipeline. The sharding optimization comes later.

### Backwards compatibility

- `--sqlite /path/to/file.db` → single file, existing behavior (unchanged)
- `--sqlite /path/to/dir/` → per-table files in that directory (new)

The trailing slash (or `--sqlite-split` flag) activates per-table mode.

## Files to Modify

1. **`src/pipeline/destinations/sql.ts`** — add `createPerTableSqliteDriver`, modify `createSqlStorage` to detect split mode
2. **`src/cli.ts`** — detect directory path for `--sqlite`
3. **`src/pipeline/config-parser.ts`** — pass split mode flag through config

## Files to Read

- `src/pipeline/destinations/sql.ts` — current single-file SQLite driver and write path
- `src/db.ts` — `createSqliteConnection()` 
- `src/pipeline/destinations/sql-ddl.ts` — shared DDL generation (if it exists)
- `src/sqlite-export.ts` — existing split-dataset export logic (for reference)

## Testing

```bash
# Per-table mode
rm -rf /tmp/epoch-split/ && mkdir -p /tmp/epoch-split/ && \
bun run src/cli.ts pipeline snapshot \
  --start-checkpoint 260650000 --end-checkpoint 260660000 \
  --everything --sqlite /tmp/epoch-split/ \
  --workers 8 --concurrency 500 --quiet --yes

# Verify each table DB has correct row counts
ls -la /tmp/epoch-split/*.db
for f in /tmp/epoch-split/*.db; do
  echo "$f: $(bun -e "const{Database}=require('bun:sqlite');console.log(new Database('$f',{readonly:true}).query('SELECT COUNT(*) as c FROM sqlite_master').get().c)")"
done

# Compare row counts with single-file mode
rm -f /tmp/epoch-single.db && \
bun run src/cli.ts pipeline snapshot \
  --start-checkpoint 260650000 --end-checkpoint 260660000 \
  --everything --sqlite /tmp/epoch-single.db \
  --workers 8 --concurrency 500 --quiet --yes
```

Row counts across all tables should be identical between split and single-file modes.

## Performance Target

Current single-file: ~1,475 cp/s sustained, shutdown (index creation) takes 10+ minutes on full epoch
Target per-table: similar or better sustained write speed, shutdown index creation parallelized across 10 files (should be 3-5x faster)

The main win is shutdown time — creating a UNIQUE index on a 500MB file is much faster than on a 44GB file.
