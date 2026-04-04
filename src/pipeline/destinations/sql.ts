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
}

type Dialect = "postgres" | "sqlite";

interface SqlDriver {
  dialect: Dialect;
  exec(query: string, params?: unknown[]): Promise<any>;
  /** Execute multiple statements inside a transaction. */
  transaction(fn: (exec: (query: string, params?: unknown[]) => Promise<any>) => Promise<void>): Promise<void>;
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
        await fn((query: string, params?: unknown[]) =>
          params ? tx.unsafe(query, params) : tx.unsafe(query)
        );
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
// Implementation
// ---------------------------------------------------------------------------

export function createSqlStorage(config: SqlStorageConfig): Storage {
  const log: Logger = createLogger().child({ component: "storage:sql" });
  let driver: SqlDriver | null = null;

  const isPostgres = config.url.startsWith("postgres");
  const isSqlite = config.url.startsWith("sqlite:");
  const sqlitePath = isSqlite ? config.url.slice(7) : "";

  // Pre-computed table configs
  const tables = new Map<string, { name: string; columns: string[] }>();

  return {
    name: isPostgres ? "postgres" : "sqlite",

    async initialize(): Promise<void> {
      driver = isPostgres
        ? createPostgresDriver(config.url)
        : createSqliteDriver(sqlitePath);

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

      // Create balance tables
      if (config.balances) {
        if (driver.dialect === "postgres") {
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
            CREATE INDEX IF NOT EXISTS idx_bc_address ON balance_changes(address);
            CREATE INDEX IF NOT EXISTS idx_bc_coin_type ON balance_changes(coin_type);
            CREATE INDEX IF NOT EXISTS idx_bc_checkpoint ON balance_changes(checkpoint_seq);
          `);
          await driver.exec(`
            CREATE TABLE IF NOT EXISTS balances (
              address TEXT NOT NULL,
              coin_type TEXT NOT NULL,
              balance NUMERIC NOT NULL DEFAULT 0,
              last_checkpoint NUMERIC NOT NULL DEFAULT 0,
              last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              PRIMARY KEY (address, coin_type)
            );
            CREATE INDEX IF NOT EXISTS idx_balances_coin ON balances(coin_type, balance DESC);
          `);
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
            CREATE INDEX IF NOT EXISTS idx_bc_address ON balance_changes(address);
            CREATE INDEX IF NOT EXISTS idx_bc_coin_type ON balance_changes(coin_type);
          `);
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
        if (driver.dialect === "postgres") {
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
            CREATE INDEX IF NOT EXISTS idx_tx_sender ON transactions(sender);
            CREATE INDEX IF NOT EXISTS idx_tx_checkpoint ON transactions(checkpoint_seq);
          `);
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
            CREATE INDEX IF NOT EXISTS idx_mc_package ON move_calls(package);
            CREATE INDEX IF NOT EXISTS idx_mc_module ON move_calls(package, module);
            CREATE INDEX IF NOT EXISTS idx_mc_function ON move_calls(package, module, function);
          `);
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
            CREATE INDEX IF NOT EXISTS idx_tx_sender ON transactions(sender);
            CREATE INDEX IF NOT EXISTS idx_tx_checkpoint ON transactions(checkpoint_seq);
          `);
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
            CREATE INDEX IF NOT EXISTS idx_mc_package ON move_calls(package);
            CREATE INDEX IF NOT EXISTS idx_mc_module ON move_calls(package, module);
            CREATE INDEX IF NOT EXISTS idx_mc_function ON move_calls(package, module, function);
          `);
        }

        log.info("transaction tables created");
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
      await d.transaction(async (exec) => {

      // Write events
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
          const conflictClause = dialect === "postgres"
            ? "ON CONFLICT (tx_digest, event_seq) DO NOTHING"
            : "ON CONFLICT (tx_digest, event_seq) DO NOTHING";
          await exec(
            `INSERT INTO ${table.name} (${table.columns.join(", ")}) VALUES ${rowClauses.join(", ")} ${conflictClause}`,
            values,
          );
        }
      }

      // Write balance changes
      if (config.balances && allBalanceChanges.length > 0) {
        await writeBalanceChangesWithExec(exec, dialect, allBalanceChanges);
      }

      // Write transactions
      if (config.transactions && allTransactions.length > 0) {
        await writeTransactionsWithExec(exec, dialect, allTransactions);
      }

      // Write move calls
      if (config.transactions && allMoveCalls.length > 0) {
        await writeMoveCallsWithExec(exec, dialect, allMoveCalls);
      }
      }); // end transaction
    },

    async shutdown(): Promise<void> {
      if (driver) {
        await driver.close();
        driver = null;
      }
      log.info("sql storage shut down");
    },
  };
}

// ---------------------------------------------------------------------------
// Balance change writer (shared between Postgres and SQLite)
// ---------------------------------------------------------------------------

async function writeBalanceChangesWithExec(
  exec: (query: string, params?: unknown[]) => Promise<any>,
  dialect: Dialect,
  changes: BalanceChange[],
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

  await exec(
    `INSERT INTO balance_changes (tx_digest, checkpoint_seq, address, coin_type, amount, sui_timestamp) VALUES ${ledgerRows.join(", ")} ON CONFLICT (tx_digest, address, coin_type) DO NOTHING`,
    ledgerValues,
  );

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
      `INSERT INTO transactions (digest, sender, success, computation_cost, storage_cost, storage_rebate, move_call_count, checkpoint_seq, sui_timestamp) VALUES ${rows.join(", ")} ON CONFLICT (digest) DO NOTHING`,
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
      `INSERT INTO move_calls (tx_digest, call_index, package, module, function, checkpoint_seq, sui_timestamp) VALUES ${rows.join(", ")} ON CONFLICT (tx_digest, call_index) DO NOTHING`,
      values,
    );
  }
}
