/**
 * Unified SQL storage backend — supports Postgres and SQLite.
 *
 * Single implementation, two dialects. Handles event tables, balance tables,
 * and running totals identically for both databases.
 */
import { Database } from "bun:sqlite";
import type { Storage, ProcessedCheckpoint, DecodedEvent, BalanceChange } from "../types.ts";
import type { FieldDefs, FieldType } from "../../schema.ts";
import { generateDDL } from "../../schema.ts";
import { validateIdentifier } from "../../output/storage.ts";
import type { Logger } from "../../logger.ts";
import { createLogger } from "../../logger.ts";

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
  const sql = new Bun.SQL(url);
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
  const database = new Database(path);
  database.exec("PRAGMA journal_mode = WAL;");
  database.exec("PRAGMA synchronous = NORMAL;");   // safe with WAL, avoids fsync per commit
  database.exec("PRAGMA temp_store = MEMORY;");     // temp tables in memory
  database.exec("PRAGMA mmap_size = 268435456;");   // 256MB mmap for reads
  database.exec("PRAGMA cache_size = -64000;");     // 64MB page cache
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

function fieldTypeToSqlite(type: FieldType): string {
  if (type.startsWith("option<")) return fieldTypeToSqlite(type.slice(7, -1) as FieldType);
  if (type.startsWith("vector<")) return "TEXT";
  switch (type) {
    case "address": case "string": return "TEXT";
    case "bool": return "INTEGER";
    case "u8": case "u16": case "u32": return "INTEGER";
    case "u64": case "u128": case "u256": return "TEXT";
    default: return "TEXT";
  }
}

function generateSqliteDDL(tableName: string, fields: FieldDefs): string {
  const columns = [
    "id INTEGER PRIMARY KEY AUTOINCREMENT",
    "tx_digest TEXT NOT NULL",
    "event_seq INTEGER NOT NULL",
    "sender TEXT NOT NULL",
    "sui_timestamp TEXT NOT NULL",
    "indexed_at TEXT NOT NULL DEFAULT (datetime('now'))",
  ];
  for (const [name, type] of Object.entries(fields)) {
    const nullable = type.startsWith("option<") ? "" : " NOT NULL";
    columns.push(`${name} ${fieldTypeToSqlite(type)}${nullable}`);
  }
  columns.push("UNIQUE (tx_digest, event_seq)");
  return `CREATE TABLE IF NOT EXISTS ${tableName} (\n  ${columns.join(",\n  ")}\n);`;
}

/** Build placeholder string: Postgres uses $1,$2 — SQLite uses ?,? */
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

      log.info({ dialect: driver.dialect }, "sql storage initialized");
    },

    async write(batch: ProcessedCheckpoint[]): Promise<void> {
      if (!driver) return;
      const d = driver; // capture for closure
      const dialect = d.dialect;

      // Collect events and balance changes
      const groupedEvents = new Map<string, DecodedEvent[]>();
      const allBalanceChanges: BalanceChange[] = [];

      for (const processed of batch) {
        for (const event of processed.events) {
          const list = groupedEvents.get(event.handlerName);
          if (list) list.push(event);
          else groupedEvents.set(event.handlerName, [event]);
        }
        allBalanceChanges.push(...processed.balanceChanges);
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
