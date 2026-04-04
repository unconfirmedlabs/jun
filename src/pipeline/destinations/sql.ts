/**
 * Unified SQL storage backend — supports Postgres and SQLite.
 *
 * Single implementation, two dialects. Handles event tables, balance tables,
 * and running totals identically for both databases.
 */
import type { Storage, ProcessedCheckpoint, DecodedEvent, BalanceChange, TransactionRecord, MoveCallRecord } from "../types.ts";
import type { FieldDefs } from "../../schema.ts";
import { generateDDL } from "../../schema.ts";
import { validateIdentifier } from "../../output/storage.ts";
import type { Logger } from "../../logger.ts";
import { createLogger } from "../../logger.ts";
import { createPostgresConnection, createSqliteConnection } from "../../db.ts";
import { fieldTypeToSqlite, generateSqliteDDL } from "../../sql-helpers.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface SqlStorageConfig {
  /** Connection string: "postgres://..." or "sqlite:./file.db" */
  url: string;
  /** Event handler table definitions */
  handlers?: Record<string, { tableName: string; fields: FieldDefs }>;
  /** Enable balance tables */
  balances?: boolean;
  /** Enable transaction tables (transactions + move_calls) */
  transactions?: boolean;
  /** Defer index creation until shutdown (faster bulk inserts for snapshots) */
  deferIndexes?: boolean;
}

type Dialect = "postgres" | "sqlite";

type ExecFn = (query: string, params?: unknown[]) => Promise<any>;

interface SqlDriver {
  dialect: Dialect;
  exec(query: string, params?: unknown[]): Promise<any>;
  /** Execute multiple statements inside a transaction. rawTx is the Bun.SQL transaction instance (Postgres only). */
  transaction(fn: (exec: ExecFn, rawTx?: any) => Promise<void>): Promise<void>;
  close(): Promise<void>;
}

// ---------------------------------------------------------------------------
// Drivers
// ---------------------------------------------------------------------------

function createPostgresDriver(url: string): SqlDriver {
  const sql = createPostgresConnection(url);
  return {
    dialect: "postgres",
    async exec(query: string, params?: unknown[]) {
      return params ? sql.unsafe(query, params) : sql.unsafe(query);
    },
    async transaction(fn) {
      await sql.begin(async (tx: any) => {
        const exec: ExecFn = (query: string, params?: unknown[]) =>
          params ? tx.unsafe(query, params) : tx.unsafe(query);
        await fn(exec, tx);
      });
    },
    async close() {
      await sql.close();
    },
  };
}

function createSqliteDriver(path: string): SqlDriver {
  const database = createSqliteConnection(path);
  database.exec("PRAGMA temp_store = MEMORY;");     // temp tables in memory
  return {
    dialect: "sqlite",
    async exec(query: string, params?: unknown[]) {
      if (params && params.length > 0) {
        return database.prepare(query).all(...params);
      }
      return database.exec(query);
    },
    async transaction(fn) {
      database.exec("BEGIN");
      try {
        const exec = async (query: string, params?: unknown[]) => {
          if (params && params.length > 0) return database.prepare(query).all(...params);
          return database.exec(query);
        };
        await fn(exec);
        database.exec("COMMIT");
      } catch (e) {
        database.exec("ROLLBACK");
        throw e;
      }
    },
    async close() {
      database.close();
    },
  };
}

// ---------------------------------------------------------------------------
// SQL dialect helpers
// ---------------------------------------------------------------------------

/** Build placeholder string: Postgres uses $1,$2 -- SQLite uses ?,? */
function placeholders(dialect: Dialect, count: number, offset = 0): string[] {
  if (dialect === "postgres") {
    return Array.from({ length: count }, (_, i) => `$${offset + i + 1}`);
  }
  return Array.from({ length: count }, () => "?");
}

// ---------------------------------------------------------------------------
// Postgres bulk write helpers — uses unnest() for columnar inserts
// ---------------------------------------------------------------------------

/** Format a JavaScript array as a Postgres array literal string. */
function pgArray(values: (string | number | boolean | null)[]): string {
  if (values.length === 0) return "{}";
  return (
    "{" +
    values
      .map((v) => {
        if (v === null) return "NULL";
        if (typeof v === "boolean") return v ? "t" : "f";
        if (typeof v === "number") return String(v);
        const s = String(v);
        return '"' + s.replace(/\\/g, "\\\\").replace(/"/g, '\\"') + '"';
      })
      .join(",") +
    "}"
  );
}

const PG_BC_CHUNK = 10000;

async function writeBalanceChangesPgBulk(
  tx: any,
  changes: BalanceChange[],
  snapshotMode: boolean,
): Promise<void> {
  // 1. Insert into balance_changes ledger using unnest (columnar)
  for (let offset = 0; offset < changes.length; offset += PG_BC_CHUNK) {
    const chunk = changes.slice(offset, Math.min(offset + PG_BC_CHUNK, changes.length));
    const n = chunk.length;
    const txDigests = new Array(n);
    const checkpoints = new Array(n);
    const addresses = new Array(n);
    const coinTypes = new Array(n);
    const amounts = new Array(n);
    const timestamps = new Array(n);

    for (let i = 0; i < n; i++) {
      const c = chunk[i];
      txDigests[i] = c.txDigest;
      checkpoints[i] = c.checkpointSeq.toString();
      addresses[i] = c.address;
      coinTypes[i] = c.coinType;
      amounts[i] = c.amount;
      timestamps[i] = c.timestamp.toISOString();
    }

    const conflict = snapshotMode ? "" : "ON CONFLICT (tx_digest, address, coin_type) DO NOTHING";
    await tx.unsafe(
      `INSERT INTO balance_changes (tx_digest, checkpoint_seq, address, coin_type, amount, sui_timestamp)
       SELECT * FROM unnest($1::text[], $2::numeric[], $3::text[], $4::text[], $5::numeric[], $6::timestamptz[])
       ${conflict}`,
      [pgArray(txDigests), pgArray(checkpoints), pgArray(addresses), pgArray(coinTypes), pgArray(amounts), pgArray(timestamps)],
    );
  }

  // In snapshot mode, skip incremental balance upserts — materialized at shutdown
  if (snapshotMode) return;

  // 2. Aggregate per (address, coinType)
  const aggregated = new Map<string, { address: string; coinType: string; totalAmount: bigint; maxCheckpoint: bigint }>();
  for (const change of changes) {
    const key = `${change.address}:${change.coinType}`;
    const existing = aggregated.get(key);
    if (existing) {
      existing.totalAmount += BigInt(change.amount);
      if (change.checkpointSeq > existing.maxCheckpoint) existing.maxCheckpoint = change.checkpointSeq;
    } else {
      aggregated.set(key, {
        address: change.address,
        coinType: change.coinType,
        totalAmount: BigInt(change.amount),
        maxCheckpoint: change.checkpointSeq,
      });
    }
  }

  // 3. Upsert into balances
  if (aggregated.size > 0) {
    const addrs: string[] = [];
    const coins: string[] = [];
    const bals: string[] = [];
    const cps: string[] = [];
    for (const a of aggregated.values()) {
      addrs.push(a.address);
      coins.push(a.coinType);
      bals.push(a.totalAmount.toString());
      cps.push(a.maxCheckpoint.toString());
    }
    await tx.unsafe(
      `INSERT INTO balances (address, coin_type, balance, last_checkpoint)
       SELECT * FROM unnest($1::text[], $2::text[], $3::numeric[], $4::numeric[])
       ON CONFLICT (address, coin_type) DO UPDATE SET
         balance = balances.balance + EXCLUDED.balance,
         last_checkpoint = GREATEST(balances.last_checkpoint, EXCLUDED.last_checkpoint),
         last_updated = NOW()`,
      [pgArray(addrs), pgArray(coins), pgArray(bals), pgArray(cps)],
    );
  }
}

async function writeTransactionsPgBulk(
  tx: any,
  transactions: TransactionRecord[],
  snapshotMode: boolean,
): Promise<void> {
  for (let offset = 0; offset < transactions.length; offset += PG_BC_CHUNK) {
    const chunk = transactions.slice(offset, Math.min(offset + PG_BC_CHUNK, transactions.length));
    const n = chunk.length;
    const digests = new Array(n);
    const senders = new Array(n);
    const successes = new Array(n);
    const compCosts = new Array(n);
    const storCosts = new Array(n);
    const storRebates = new Array(n);
    const mcCounts = new Array(n);
    const checkpoints = new Array(n);
    const timestamps = new Array(n);

    for (let i = 0; i < n; i++) {
      const t = chunk[i];
      digests[i] = t.digest;
      senders[i] = t.sender;
      successes[i] = t.success;
      compCosts[i] = t.computationCost;
      storCosts[i] = t.storageCost;
      storRebates[i] = t.storageRebate;
      mcCounts[i] = t.moveCallCount;
      checkpoints[i] = t.checkpointSeq.toString();
      timestamps[i] = t.timestamp.toISOString();
    }

    const conflict = snapshotMode ? "" : "ON CONFLICT (digest) DO NOTHING";
    await tx.unsafe(
      `INSERT INTO transactions (digest, sender, success, computation_cost, storage_cost, storage_rebate, move_call_count, checkpoint_seq, sui_timestamp)
       SELECT * FROM unnest($1::text[], $2::text[], $3::boolean[], $4::numeric[], $5::numeric[], $6::numeric[], $7::integer[], $8::numeric[], $9::timestamptz[])
       ${conflict}`,
      [pgArray(digests), pgArray(senders), pgArray(successes), pgArray(compCosts), pgArray(storCosts), pgArray(storRebates), pgArray(mcCounts), pgArray(checkpoints), pgArray(timestamps)],
    );
  }
}

async function writeMoveCallsPgBulk(
  tx: any,
  moveCalls: MoveCallRecord[],
  snapshotMode: boolean,
): Promise<void> {
  for (let offset = 0; offset < moveCalls.length; offset += PG_BC_CHUNK) {
    const chunk = moveCalls.slice(offset, Math.min(offset + PG_BC_CHUNK, moveCalls.length));
    const n = chunk.length;
    const txDigests = new Array(n);
    const callIndexes = new Array(n);
    const packages = new Array(n);
    const modules = new Array(n);
    const functions = new Array(n);
    const checkpoints = new Array(n);
    const timestamps = new Array(n);

    for (let i = 0; i < n; i++) {
      const mc = chunk[i];
      txDigests[i] = mc.txDigest;
      callIndexes[i] = mc.callIndex;
      packages[i] = mc.package;
      modules[i] = mc.module;
      functions[i] = mc.function;
      checkpoints[i] = mc.checkpointSeq.toString();
      timestamps[i] = mc.timestamp.toISOString();
    }

    const conflict = snapshotMode ? "" : "ON CONFLICT (tx_digest, call_index) DO NOTHING";
    await tx.unsafe(
      `INSERT INTO move_calls (tx_digest, call_index, package, module, function, checkpoint_seq, sui_timestamp)
       SELECT * FROM unnest($1::text[], $2::integer[], $3::text[], $4::text[], $5::text[], $6::numeric[], $7::timestamptz[])
       ${conflict}`,
      [pgArray(txDigests), pgArray(callIndexes), pgArray(packages), pgArray(modules), pgArray(functions), pgArray(checkpoints), pgArray(timestamps)],
    );
  }
}

function isUniqueConstraintError(error: unknown): error is Error {
  return error instanceof Error && error.message.includes("UNIQUE constraint failed:");
}

async function repairSqliteSnapshotDuplicates(
  driver: SqlDriver,
  error: Error,
  log: Logger,
): Promise<boolean> {
  const repairs: Array<{ match: string; sql: string; table: string }> = [
    {
      match: "transactions.digest",
      table: "transactions",
      sql: `DELETE FROM transactions
            WHERE rowid NOT IN (
              SELECT MIN(rowid) FROM transactions GROUP BY digest
            )`,
    },
    {
      match: "balance_changes.tx_digest, balance_changes.address, balance_changes.coin_type",
      table: "balance_changes",
      sql: `DELETE FROM balance_changes
            WHERE rowid NOT IN (
              SELECT MIN(rowid) FROM balance_changes GROUP BY tx_digest, address, coin_type
            )`,
    },
    {
      match: "move_calls.tx_digest, move_calls.call_index",
      table: "move_calls",
      sql: `DELETE FROM move_calls
            WHERE rowid NOT IN (
              SELECT MIN(rowid) FROM move_calls GROUP BY tx_digest, call_index
            )`,
    },
  ];

  const repair = repairs.find(candidate => error.message.includes(candidate.match));
  if (!repair) return false;

  log.warn({ table: repair.table }, "repairing duplicate snapshot rows before retrying unique index");
  await driver.exec(repair.sql);
  return true;
}

async function createDeferredIndexes(
  driver: SqlDriver,
  indexes: string[],
  log: Logger,
  allowSnapshotRepair = false,
): Promise<void> {
  for (const sql of indexes) {
    try {
      await driver.exec(sql);
    } catch (error) {
      if (!allowSnapshotRepair || !isUniqueConstraintError(error)) throw error;
      const repaired = await repairSqliteSnapshotDuplicates(driver, error, log);
      if (!repaired) throw error;
      await driver.exec(sql);
    }
  }
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

export function createSqlStorage(config: SqlStorageConfig): Storage {
  const log: Logger = createLogger().child({ component: "storage:sql" });
  let driver: SqlDriver | null = null;

  const isPostgres = config.url.startsWith("postgres");
  const isSqlite = config.url.startsWith("sqlite:");
  const sqlitePath = isSqlite ? config.url.slice(7) : "";

  // Snapshot optimization: use aggressive SQLite pragmas + skip conflict checks
  const snapshotMode = !!config.deferIndexes;

  // Pre-computed table configs
  const tables = new Map<string, { name: string; columns: string[] }>();
  const deferredIndexes: string[] = [];

  const storageObj: Storage & { _config?: SqlStorageConfig } = {
    name: isPostgres ? "postgres" : "sqlite",

    async initialize(): Promise<void> {
      driver = isPostgres
        ? createPostgresDriver(config.url)
        : createSqliteDriver(sqlitePath);

      // Snapshot mode: drop existing tables to avoid stale constraints
      if (snapshotMode) {
        await driver.exec("DROP TABLE IF EXISTS transactions");
        await driver.exec("DROP TABLE IF EXISTS move_calls");
        await driver.exec("DROP TABLE IF EXISTS balance_changes");
        await driver.exec("DROP TABLE IF EXISTS balances");
      }

      // Aggressive SQLite pragmas for snapshot mode (bulk insert optimization)
      if (snapshotMode && isSqlite) {
        await driver.exec("PRAGMA synchronous = OFF");
        await driver.exec("PRAGMA journal_mode = OFF");
        await driver.exec("PRAGMA locking_mode = EXCLUSIVE");
        await driver.exec("PRAGMA temp_store = MEMORY");
        await driver.exec("PRAGMA page_size = 32768");
        log.info("snapshot mode: aggressive SQLite pragmas enabled");
      }

      // Create event tables
      if (config.handlers) {
        for (const [handlerName, handler] of Object.entries(config.handlers)) {
          validateIdentifier(handler.tableName);
          const ddl = driver.dialect === "postgres"
            ? generateDDL(handler.tableName, handler.fields)
            : generateSqliteDDL(handler.tableName, handler.fields);
          await driver.exec(ddl);

          const fieldNames = Object.keys(handler.fields);
          const columns = ["tx_digest", "event_seq", "sender", "sui_timestamp", ...fieldNames];
          columns.forEach(validateIdentifier);
          tables.set(handlerName, { name: handler.tableName, columns });

          log.info({ handler: handlerName, table: handler.tableName }, "event table created");
        }
      }

      // Helper: execute index SQL now or defer it
      const indexOrDefer = async (sql: string) => {
        if (config.deferIndexes) deferredIndexes.push(sql);
        else await driver!.exec(sql);
      };

      // Create balance tables
      if (config.balances) {
        const pgUnlogged = snapshotMode && isPostgres ? "UNLOGGED" : "";
        if (driver.dialect === "postgres") {
          if (snapshotMode) {
            // Snapshot: UNLOGGED table, no PK/UNIQUE — constraints deferred to shutdown
            await driver.exec(`
              CREATE ${pgUnlogged} TABLE IF NOT EXISTS balance_changes (
                tx_digest TEXT NOT NULL,
                checkpoint_seq NUMERIC NOT NULL,
                address TEXT NOT NULL,
                coin_type TEXT NOT NULL,
                amount NUMERIC NOT NULL,
                sui_timestamp TIMESTAMPTZ NOT NULL
              );
            `);
            deferredIndexes.push(`CREATE UNIQUE INDEX IF NOT EXISTS idx_bc_unique ON balance_changes(tx_digest, address, coin_type)`);
          } else {
            await driver.exec(`
              CREATE TABLE IF NOT EXISTS balance_changes (
                id SERIAL PRIMARY KEY,
                tx_digest TEXT NOT NULL,
                checkpoint_seq NUMERIC NOT NULL,
                address TEXT NOT NULL,
                coin_type TEXT NOT NULL,
                amount NUMERIC NOT NULL,
                sui_timestamp TIMESTAMPTZ NOT NULL,
                UNIQUE (tx_digest, address, coin_type)
              );
            `);
          }
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_bc_address ON balance_changes(address)`);
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_bc_coin_type ON balance_changes(coin_type)`);
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_bc_checkpoint ON balance_changes(checkpoint_seq)`);
          if (snapshotMode) {
            // Snapshot: balances materialized at shutdown, just create the table
            await driver.exec(`
              CREATE ${pgUnlogged} TABLE IF NOT EXISTS balances (
                address TEXT NOT NULL,
                coin_type TEXT NOT NULL,
                balance NUMERIC NOT NULL DEFAULT 0,
                last_checkpoint NUMERIC NOT NULL DEFAULT 0,
                last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
              );
            `);
            deferredIndexes.push(`CREATE UNIQUE INDEX IF NOT EXISTS idx_balances_pk ON balances(address, coin_type)`);
          } else {
            await driver.exec(`
              CREATE TABLE IF NOT EXISTS balances (
                address TEXT NOT NULL,
                coin_type TEXT NOT NULL,
                balance NUMERIC NOT NULL DEFAULT 0,
                last_checkpoint NUMERIC NOT NULL DEFAULT 0,
                last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (address, coin_type)
              );
            `);
          }
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_balances_coin ON balances(coin_type, balance DESC)`);
        } else {
          if (snapshotMode) {
            await driver.exec(`
              CREATE TABLE IF NOT EXISTS balance_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tx_digest TEXT NOT NULL,
                checkpoint_seq TEXT NOT NULL,
                address TEXT NOT NULL,
                coin_type TEXT NOT NULL,
                amount TEXT NOT NULL,
                sui_timestamp TEXT NOT NULL
              );
            `);
            deferredIndexes.push(`CREATE UNIQUE INDEX IF NOT EXISTS idx_bc_unique ON balance_changes(tx_digest, address, coin_type)`);
          } else {
            await driver.exec(`
              CREATE TABLE IF NOT EXISTS balance_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tx_digest TEXT NOT NULL,
                checkpoint_seq TEXT NOT NULL,
                address TEXT NOT NULL,
                coin_type TEXT NOT NULL,
                amount TEXT NOT NULL,
                sui_timestamp TEXT NOT NULL,
                UNIQUE (tx_digest, address, coin_type)
              );
            `);
          }
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_bc_address ON balance_changes(address)`);
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_bc_coin_type ON balance_changes(coin_type)`);
          await driver.exec(`
            CREATE TABLE IF NOT EXISTS balances (
              address TEXT NOT NULL,
              coin_type TEXT NOT NULL,
              balance TEXT NOT NULL DEFAULT '0',
              last_checkpoint TEXT NOT NULL DEFAULT '0',
              PRIMARY KEY (address, coin_type)
            );
          `);
        }

        log.info("balance tables created");
      }

      // Create transaction tables
      if (config.transactions) {
        const pgUnlogged = snapshotMode && isPostgres ? "UNLOGGED" : "";
        if (driver.dialect === "postgres") {
          if (snapshotMode) {
            // Snapshot: UNLOGGED, no PK — deferred to shutdown
            await driver.exec(`
              CREATE ${pgUnlogged} TABLE IF NOT EXISTS transactions (
                digest TEXT NOT NULL,
                sender TEXT NOT NULL,
                success BOOLEAN NOT NULL,
                computation_cost NUMERIC NOT NULL,
                storage_cost NUMERIC NOT NULL,
                storage_rebate NUMERIC NOT NULL,
                move_call_count INTEGER NOT NULL DEFAULT 0,
                checkpoint_seq NUMERIC NOT NULL,
                sui_timestamp TIMESTAMPTZ NOT NULL
              );
            `);
            deferredIndexes.push(`CREATE UNIQUE INDEX IF NOT EXISTS idx_tx_digest ON transactions(digest)`);
          } else {
            await driver.exec(`
              CREATE TABLE IF NOT EXISTS transactions (
                digest TEXT PRIMARY KEY,
                sender TEXT NOT NULL,
                success BOOLEAN NOT NULL,
                computation_cost NUMERIC NOT NULL,
                storage_cost NUMERIC NOT NULL,
                storage_rebate NUMERIC NOT NULL,
                move_call_count INTEGER NOT NULL DEFAULT 0,
                checkpoint_seq NUMERIC NOT NULL,
                sui_timestamp TIMESTAMPTZ NOT NULL
              );
            `);
          }
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_tx_sender ON transactions(sender)`);
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_tx_checkpoint ON transactions(checkpoint_seq)`);
          if (snapshotMode) {
            await driver.exec(`
              CREATE ${pgUnlogged} TABLE IF NOT EXISTS move_calls (
                tx_digest TEXT NOT NULL,
                call_index INTEGER NOT NULL,
                package TEXT NOT NULL,
                module TEXT NOT NULL,
                function TEXT NOT NULL,
                checkpoint_seq NUMERIC NOT NULL,
                sui_timestamp TIMESTAMPTZ NOT NULL
              );
            `);
            deferredIndexes.push(`CREATE UNIQUE INDEX IF NOT EXISTS idx_mc_pk ON move_calls(tx_digest, call_index)`);
          } else {
            await driver.exec(`
              CREATE TABLE IF NOT EXISTS move_calls (
                tx_digest TEXT NOT NULL,
                call_index INTEGER NOT NULL,
                package TEXT NOT NULL,
                module TEXT NOT NULL,
                function TEXT NOT NULL,
                checkpoint_seq NUMERIC NOT NULL,
                sui_timestamp TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (tx_digest, call_index)
              );
            `);
          }
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_mc_package ON move_calls(package)`);
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_mc_module ON move_calls(package, module)`);
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_mc_function ON move_calls(package, module, function)`);
        } else {
          if (snapshotMode) {
            // Snapshot: no primary keys during bulk insert — added as deferred indexes
            await driver.exec(`
              CREATE TABLE IF NOT EXISTS transactions (
                digest TEXT NOT NULL,
                sender TEXT NOT NULL,
                success INTEGER NOT NULL,
                computation_cost TEXT NOT NULL,
                storage_cost TEXT NOT NULL,
                storage_rebate TEXT NOT NULL,
                move_call_count INTEGER NOT NULL DEFAULT 0,
                checkpoint_seq TEXT NOT NULL,
                sui_timestamp TEXT NOT NULL
              );
            `);
            deferredIndexes.push(`CREATE UNIQUE INDEX IF NOT EXISTS idx_tx_digest ON transactions(digest)`);
          } else {
            await driver.exec(`
              CREATE TABLE IF NOT EXISTS transactions (
                digest TEXT PRIMARY KEY,
                sender TEXT NOT NULL,
                success INTEGER NOT NULL,
                computation_cost TEXT NOT NULL,
                storage_cost TEXT NOT NULL,
                storage_rebate TEXT NOT NULL,
                move_call_count INTEGER NOT NULL DEFAULT 0,
                checkpoint_seq TEXT NOT NULL,
                sui_timestamp TEXT NOT NULL
              );
            `);
          }
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_tx_sender ON transactions(sender)`);
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_tx_checkpoint ON transactions(checkpoint_seq)`);
          if (snapshotMode) {
            // Snapshot: no primary key during bulk insert
            await driver.exec(`
              CREATE TABLE IF NOT EXISTS move_calls (
                tx_digest TEXT NOT NULL,
                call_index INTEGER NOT NULL,
                package TEXT NOT NULL,
                module TEXT NOT NULL,
                function TEXT NOT NULL,
                checkpoint_seq TEXT NOT NULL,
                sui_timestamp TEXT NOT NULL
              );
            `);
            deferredIndexes.push(`CREATE UNIQUE INDEX IF NOT EXISTS idx_mc_pk ON move_calls(tx_digest, call_index)`);
          } else {
            await driver.exec(`
              CREATE TABLE IF NOT EXISTS move_calls (
                tx_digest TEXT NOT NULL,
                call_index INTEGER NOT NULL,
                package TEXT NOT NULL,
                module TEXT NOT NULL,
                function TEXT NOT NULL,
                checkpoint_seq TEXT NOT NULL,
                sui_timestamp TEXT NOT NULL,
                PRIMARY KEY (tx_digest, call_index)
              );
            `);
          }
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_mc_package ON move_calls(package)`);
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_mc_module ON move_calls(package, module)`);
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_mc_function ON move_calls(package, module, function)`);
        }

        log.info("transaction tables created");
      }

      if (deferredIndexes.length > 0) {
        log.info({ count: deferredIndexes.length }, "indexes deferred to shutdown");
      }

      log.info({ dialect: driver.dialect }, "sql storage initialized");
    },

    async write(batch: ProcessedCheckpoint[]): Promise<void> {
      if (!driver) return;
      const d = driver; // capture for closure
      const dialect = d.dialect;

      // Collect events, balance changes, transactions, and move calls
      const groupedEvents = new Map<string, DecodedEvent[]>();
      const allBalanceChanges: BalanceChange[] = [];
      const allTransactions: TransactionRecord[] = [];
      const allMoveCalls: MoveCallRecord[] = [];

      for (const processed of batch) {
        for (const event of processed.events) {
          const list = groupedEvents.get(event.handlerName);
          if (list) list.push(event);
          else groupedEvents.set(event.handlerName, [event]);
        }
        allBalanceChanges.push(...processed.balanceChanges);
        allTransactions.push(...processed.transactions);
        allMoveCalls.push(...processed.moveCalls);
      }

      // Wrap all writes in a single transaction
      await d.transaction(async (exec, rawTx) => {

      // Write events (uses exec for both dialects — user-defined schemas)
      for (const [handlerName, events] of groupedEvents) {
        const table = tables.get(handlerName);
        if (!table) continue;

        const values: unknown[] = [];
        const rowClauses: string[] = [];

        for (const event of events) {
          const ph = placeholders(dialect, table.columns.length, values.length);
          rowClauses.push(`(${ph.join(", ")})`);
          for (const column of table.columns) {
            if (column === "tx_digest") values.push(event.txDigest);
            else if (column === "event_seq") values.push(event.eventSeq);
            else if (column === "sender") values.push(event.sender);
            else if (column === "sui_timestamp") values.push(dialect === "postgres" ? event.timestamp : event.timestamp.toISOString());
            else values.push(event.data[column]);
          }
        }

        if (rowClauses.length > 0) {
          const conflictClause = snapshotMode ? "" : "ON CONFLICT (tx_digest, event_seq) DO NOTHING";
          await exec(
            `INSERT INTO ${table.name} (${table.columns.join(", ")}) VALUES ${rowClauses.join(", ")} ${conflictClause}`,
            values,
          );
        }
      }

      // Write balance changes — Postgres uses unnest() bulk path
      if (config.balances && allBalanceChanges.length > 0) {
        if (rawTx) {
          await writeBalanceChangesPgBulk(rawTx, allBalanceChanges, snapshotMode);
        } else {
          await writeBalanceChangesWithExec(exec, dialect, allBalanceChanges, snapshotMode);
        }
      }

      // Write transactions
      if (config.transactions && allTransactions.length > 0) {
        if (rawTx) {
          await writeTransactionsPgBulk(rawTx, allTransactions, snapshotMode);
        } else {
          await writeTransactionsWithExec(exec, dialect, allTransactions, snapshotMode);
        }
      }

      // Write move calls
      if (config.transactions && allMoveCalls.length > 0) {
        if (rawTx) {
          await writeMoveCallsPgBulk(rawTx, allMoveCalls, snapshotMode);
        } else {
          await writeMoveCallsWithExec(exec, dialect, allMoveCalls, snapshotMode);
        }
      }
      }); // end transaction
    },

    async shutdown(): Promise<void> {
      if (driver && deferredIndexes.length > 0) {
        // Boost maintenance_work_mem for faster index creation (Postgres)
        if (isPostgres) {
          await driver.exec("SET maintenance_work_mem = '512MB'");
        }

        const balanceIndexes = deferredIndexes.filter(sql => /\bON balances\b/i.test(sql));
        const otherIndexes = deferredIndexes.filter(sql => !/\bON balances\b/i.test(sql));

        log.info({ count: otherIndexes.length }, "creating deferred indexes...");
        const startTime = performance.now();
        if (otherIndexes.length > 0) {
          await createDeferredIndexes(driver, otherIndexes, log, snapshotMode && isSqlite);
        }
        const elapsed = ((performance.now() - startTime) / 1000).toFixed(1);
        log.info({ elapsed: `${elapsed}s` }, "deferred indexes created");

        if (balanceIndexes.length > 0) {
          log.info({ count: balanceIndexes.length }, "creating balances indexes...");
          const balancesStartTime = performance.now();
          await createDeferredIndexes(driver, balanceIndexes, log, false);
          const balancesElapsed = ((performance.now() - balancesStartTime) / 1000).toFixed(1);
          log.info({ elapsed: `${balancesElapsed}s` }, "balances indexes created");
        }
      }

      // Materialize balances from balance_changes (snapshot mode)
      if (driver && snapshotMode && config.balances) {
        log.info("materializing balances from balance_changes...");
        const startTime = performance.now();
        if (isPostgres) {
          await driver.exec(`
            INSERT INTO balances (address, coin_type, balance, last_checkpoint)
            SELECT address, coin_type,
              SUM(amount),
              MAX(checkpoint_seq)
            FROM balance_changes
            GROUP BY address, coin_type
          `);
        } else {
          await driver.exec(`
            INSERT INTO balances (address, coin_type, balance, last_checkpoint)
            SELECT address, coin_type,
              CAST(SUM(CAST(amount AS INTEGER)) AS TEXT),
              CAST(MAX(checkpoint_seq) AS TEXT)
            FROM balance_changes
            GROUP BY address, coin_type
          `);
        }
        const elapsed = ((performance.now() - startTime) / 1000).toFixed(1);
        log.info({ elapsed: `${elapsed}s` }, "balances materialized");
      }
      if (driver) {
        // Convert UNLOGGED tables back to LOGGED for durability (Postgres snapshot)
        if (snapshotMode && isPostgres) {
          log.info("converting UNLOGGED tables to LOGGED...");
          const logStart = performance.now();
          if (config.balances) {
            await driver.exec("ALTER TABLE balance_changes SET LOGGED");
            await driver.exec("ALTER TABLE balances SET LOGGED");
          }
          if (config.transactions) {
            await driver.exec("ALTER TABLE transactions SET LOGGED");
            await driver.exec("ALTER TABLE move_calls SET LOGGED");
          }
          const logElapsed = ((performance.now() - logStart) / 1000).toFixed(1);
          log.info({ elapsed: `${logElapsed}s` }, "tables converted to LOGGED");
        }
        if (snapshotMode && isSqlite) {
          await driver.exec("PRAGMA locking_mode = NORMAL");
          log.info("VACUUMing database...");
          const vacStart = performance.now();
          await driver.exec("VACUUM");
          const vacElapsed = ((performance.now() - vacStart) / 1000).toFixed(1);
          log.info({ elapsed: `${vacElapsed}s` }, "VACUUM complete");
        }
        await driver.close();
        driver = null;
      }
      log.info("sql storage shut down");
    },
  };

  // Expose config for WriterChannel extraction
  storageObj._config = config;

  return storageObj;
}

// ---------------------------------------------------------------------------
// Balance change writer (shared between Postgres and SQLite)
// ---------------------------------------------------------------------------

async function writeBalanceChangesWithExec(
  exec: (query: string, params?: unknown[]) => Promise<any>,
  dialect: Dialect,
  changes: BalanceChange[],
  snapshotMode = false,
): Promise<void> {
  // 1. Insert into balance_changes ledger
  const ledgerValues: unknown[] = [];
  const ledgerRows: string[] = [];

  for (const change of changes) {
    const ph = placeholders(dialect, 6, ledgerValues.length);
    ledgerRows.push(`(${ph.join(", ")})`);
    ledgerValues.push(
      change.txDigest,
      change.checkpointSeq.toString(),
      change.address,
      change.coinType,
      change.amount,
      dialect === "postgres" ? change.timestamp : change.timestamp.toISOString(),
    );
  }

  const conflictClause = snapshotMode ? "" : "ON CONFLICT (tx_digest, address, coin_type) DO NOTHING";
  await exec(
    `INSERT INTO balance_changes (tx_digest, checkpoint_seq, address, coin_type, amount, sui_timestamp) VALUES ${ledgerRows.join(", ")} ${conflictClause}`,
    ledgerValues,
  );

  // In snapshot mode, skip incremental balance upserts — materialized at shutdown
  if (snapshotMode) return;

  // 2. Aggregate per (address, coinType)
  const aggregated = new Map<string, { address: string; coinType: string; totalAmount: bigint; maxCheckpoint: bigint }>();

  for (const change of changes) {
    const key = `${change.address}:${change.coinType}`;
    const existing = aggregated.get(key);
    if (existing) {
      existing.totalAmount += BigInt(change.amount);
      if (change.checkpointSeq > existing.maxCheckpoint) existing.maxCheckpoint = change.checkpointSeq;
    } else {
      aggregated.set(key, {
        address: change.address,
        coinType: change.coinType,
        totalAmount: BigInt(change.amount),
        maxCheckpoint: change.checkpointSeq,
      });
    }
  }

  // 3. Upsert into balances (running totals)
  const balanceValues: unknown[] = [];
  const balanceRows: string[] = [];

  for (const aggregation of aggregated.values()) {
    const ph = placeholders(dialect, 4, balanceValues.length);
    balanceRows.push(`(${ph.join(", ")})`);
    balanceValues.push(
      aggregation.address,
      aggregation.coinType,
      aggregation.totalAmount.toString(),
      aggregation.maxCheckpoint.toString(),
    );
  }

  if (balanceRows.length > 0) {
    if (dialect === "postgres") {
      await exec(
        `INSERT INTO balances (address, coin_type, balance, last_checkpoint) VALUES ${balanceRows.join(", ")} ON CONFLICT (address, coin_type) DO UPDATE SET balance = balances.balance + EXCLUDED.balance, last_checkpoint = GREATEST(balances.last_checkpoint, EXCLUDED.last_checkpoint), last_updated = NOW()`,
        balanceValues,
      );
    } else {
      // SQLite: CAST for numeric addition on TEXT columns
      await exec(
        `INSERT INTO balances (address, coin_type, balance, last_checkpoint) VALUES ${balanceRows.join(", ")} ON CONFLICT (address, coin_type) DO UPDATE SET balance = CAST(CAST(balances.balance AS INTEGER) + CAST(excluded.balance AS INTEGER) AS TEXT), last_checkpoint = MAX(balances.last_checkpoint, excluded.last_checkpoint)`,
        balanceValues,
      );
    }
  }
}

// ---------------------------------------------------------------------------
// Transaction writer
// ---------------------------------------------------------------------------

async function writeTransactionsWithExec(
  exec: (query: string, params?: unknown[]) => Promise<any>,
  dialect: Dialect,
  transactions: TransactionRecord[],
  snapshotMode = false,
): Promise<void> {
  const values: unknown[] = [];
  const rows: string[] = [];

  for (const tx of transactions) {
    const ph = placeholders(dialect, 9, values.length);
    rows.push(`(${ph.join(", ")})`);
    values.push(
      tx.digest,
      tx.sender,
      dialect === "postgres" ? tx.success : (tx.success ? 1 : 0),
      tx.computationCost,
      tx.storageCost,
      tx.storageRebate,
      tx.moveCallCount,
      tx.checkpointSeq.toString(),
      dialect === "postgres" ? tx.timestamp : tx.timestamp.toISOString(),
    );
  }

  if (rows.length > 0) {
    await exec(
      `INSERT ${snapshotMode ? "" : "OR IGNORE "}INTO transactions (digest, sender, success, computation_cost, storage_cost, storage_rebate, move_call_count, checkpoint_seq, sui_timestamp) VALUES ${rows.join(", ")}`,
      values,
    );
  }
}

// ---------------------------------------------------------------------------
// Move call writer
// ---------------------------------------------------------------------------

async function writeMoveCallsWithExec(
  exec: (query: string, params?: unknown[]) => Promise<any>,
  dialect: Dialect,
  moveCalls: MoveCallRecord[],
  snapshotMode = false,
): Promise<void> {
  const values: unknown[] = [];
  const rows: string[] = [];

  for (const mc of moveCalls) {
    const ph = placeholders(dialect, 7, values.length);
    rows.push(`(${ph.join(", ")})`);
    values.push(
      mc.txDigest,
      mc.callIndex,
      mc.package,
      mc.module,
      mc.function,
      mc.checkpointSeq.toString(),
      dialect === "postgres" ? mc.timestamp : mc.timestamp.toISOString(),
    );
  }

  if (rows.length > 0) {
    await exec(
      `INSERT ${snapshotMode ? "" : "OR IGNORE "}INTO move_calls (tx_digest, call_index, package, module, function, checkpoint_seq, sui_timestamp) VALUES ${rows.join(", ")}`,
      values,
    );
  }
}
