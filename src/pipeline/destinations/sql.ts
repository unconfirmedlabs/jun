/**
 * Unified SQL storage backend — supports Postgres and SQLite.
 *
 * Single implementation, two dialects. Handles event tables, balance tables,
 * and running totals identically for both databases.
 */
import type {
  Storage, ProcessedCheckpoint, DecodedEvent, BalanceChange,
  TransactionRecord, MoveCallRecord, Checkpoint, ObjectChangeRecord,
  TransactionDependencyRecord, TransactionInputRecord, CommandRecord,
  SystemTransactionRecord, UnchangedConsensusObjectRecord,
} from "../types.ts";
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
  /** Enable checkpoints table (one row per checkpoint with summary stats) */
  checkpoints?: boolean;
  /** Enable object_changes table (per-object state change from effects) */
  objectChanges?: boolean;
  /** Enable transaction_dependencies table */
  dependencies?: boolean;
  /** Enable transaction_inputs table */
  inputs?: boolean;
  /** Enable commands table (superset of move_calls covering all PTB command types) */
  commands?: boolean;
  /** Enable system_transactions table */
  systemTransactions?: boolean;
  /** Enable unchanged_consensus_objects table */
  unchangedConsensusObjects?: boolean;
  /** Defer index creation until shutdown (faster bulk inserts for snapshots) */
  deferIndexes?: boolean;
  /** Use UNLOGGED tables for Postgres (no WAL during inserts, faster bulk load) */
  pgUnlogged?: boolean;
}

type Dialect = "postgres" | "sqlite";

type ExecFn = (query: string, params?: unknown[]) => Promise<any>;

/** Write context passed to consolidated writer functions. */
interface WriteContext {
  dialect: Dialect;
  exec: ExecFn;
  /** Bun.SQL transaction instance (Postgres only, used for unnest bulk path). */
  rawTx?: any;
  snapshotMode: boolean;
}

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
        return database.prepare(query).all(...(params as any[]));
      }
      return database.exec(query);
    },
    async transaction(fn) {
      database.exec("BEGIN");
      try {
        const exec = async (query: string, params?: unknown[]) => {
          if (params && params.length > 0) return database.prepare(query).all(...(params as any[]));
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

const PG_BULK_CHUNK = 10000;

// SQLite's hard upper limit for host parameters in a single SQL statement is
// 32767 — parameter numbers are stored in a signed 16-bit integer (2^15 - 1).
// SQLITE_LIMIT_VARIABLE_NUMBER defaults to 32766 since SQLite 3.32.0 (2020).
// Reference: https://www.sqlite.org/limits.html#max_variable_number
// We chunk to 30000 to stay comfortably under the limit with headroom.
const MAX_SQLITE_PARAMS = 30000;

// ---------------------------------------------------------------------------
// Consolidated writers — one function per table, dialect dispatch inside.
//
// Postgres uses unnest(...) for columnar bulk insert, chunked at PG_BULK_CHUNK.
// SQLite uses multi-row VALUES (...) insert, chunked to stay under the
// parameter limit (MAX_SQLITE_PARAMS / column_count rows per statement).
// ---------------------------------------------------------------------------

/** Build placeholders for a multi-row VALUES insert — ($1,$2,$3), ($4,$5,$6), ... for Postgres or (?,?,?), (?,?,?), ... for SQLite. */
function buildRowPlaceholders(dialect: Dialect, columnCount: number, rowCount: number): string {
  const rows: string[] = [];
  let cursor = 0;
  for (let i = 0; i < rowCount; i++) {
    const phs: string[] = [];
    for (let j = 0; j < columnCount; j++) {
      phs.push(dialect === "postgres" ? `$${++cursor}` : "?");
    }
    rows.push(`(${phs.join(", ")})`);
  }
  return rows.join(", ");
}

/** Insert a batch of rows via multi-row VALUES (SQLite path) or unnest (Postgres path). */
async function insertRows(
  ctx: WriteContext,
  table: string,
  columns: string[],
  rows: unknown[][],
  conflictClause: string,
  pgTypes?: string[],
): Promise<void> {
  if (rows.length === 0) return;

  const chunkSize = ctx.dialect === "postgres"
    ? PG_BULK_CHUNK
    : Math.max(1, Math.floor(MAX_SQLITE_PARAMS / columns.length));

  for (let offset = 0; offset < rows.length; offset += chunkSize) {
    const chunk = rows.slice(offset, Math.min(offset + chunkSize, rows.length));

    if (ctx.dialect === "postgres" && ctx.rawTx && pgTypes) {
      // Postgres: unnest bulk path. Build columnar arrays per column.
      const arrays: unknown[][] = columns.map(() => new Array(chunk.length));
      for (let r = 0; r < chunk.length; r++) {
        const row = chunk[r]!;
        for (let c = 0; c < columns.length; c++) arrays[c]![r] = row[c];
      }
      const unnestArgs = pgTypes.map((t, i) => `$${i + 1}::${t}[]`).join(", ");
      await ctx.rawTx.unsafe(
        `INSERT INTO ${table} (${columns.join(", ")})
         SELECT * FROM unnest(${unnestArgs})
         ${conflictClause}`,
        arrays.map((a) => pgArray(a as (string | number | boolean | null)[])),
      );
    } else {
      // SQLite (or Postgres fallback without rawTx): multi-row VALUES insert.
      const values: unknown[] = [];
      for (const row of chunk) values.push(...row);
      const placeholders = buildRowPlaceholders(ctx.dialect, columns.length, chunk.length);
      await ctx.exec(
        `INSERT INTO ${table} (${columns.join(", ")}) VALUES ${placeholders} ${conflictClause}`,
        values,
      );
    }
  }
}

// ---------------------------------------------------------------------------

async function writeBalanceChanges(ctx: WriteContext, changes: BalanceChange[]): Promise<void> {
  if (changes.length === 0) return;

  // 1. Insert into balance_changes ledger
  const conflict = ctx.snapshotMode
    ? ""
    : ctx.dialect === "postgres"
      ? "ON CONFLICT (tx_digest, address, coin_type) DO NOTHING"
      : "ON CONFLICT (tx_digest, address, coin_type) DO NOTHING";
  const rows = changes.map(c => [
    c.txDigest,
    c.checkpointSeq.toString(),
    c.address,
    c.coinType,
    c.amount,
    ctx.dialect === "postgres" ? c.timestamp : c.timestamp.toISOString(),
  ]);
  await insertRows(
    ctx,
    "balance_changes",
    ["tx_digest", "checkpoint_seq", "address", "coin_type", "amount", "sui_timestamp"],
    rows,
    conflict,
    ["text", "numeric", "text", "text", "numeric", "timestamptz"],
  );

  // In snapshot mode, skip incremental balance upserts — materialized at shutdown
  if (ctx.snapshotMode) return;

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

  // 3. Upsert into balances (running totals) — dialect-specific upsert clauses
  if (aggregated.size === 0) return;

  if (ctx.dialect === "postgres" && ctx.rawTx) {
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
    await ctx.rawTx.unsafe(
      `INSERT INTO balances (address, coin_type, balance, last_checkpoint)
       SELECT * FROM unnest($1::text[], $2::text[], $3::numeric[], $4::numeric[])
       ON CONFLICT (address, coin_type) DO UPDATE SET
         balance = balances.balance + EXCLUDED.balance,
         last_checkpoint = GREATEST(balances.last_checkpoint, EXCLUDED.last_checkpoint),
         last_updated = NOW()`,
      [pgArray(addrs), pgArray(coins), pgArray(bals), pgArray(cps)],
    );
  } else {
    // SQLite: CAST for numeric addition on TEXT columns
    const values: unknown[] = [];
    const rowClauses: string[] = [];
    for (const a of aggregated.values()) {
      rowClauses.push("(?, ?, ?, ?)");
      values.push(a.address, a.coinType, a.totalAmount.toString(), a.maxCheckpoint.toString());
    }
    await ctx.exec(
      `INSERT INTO balances (address, coin_type, balance, last_checkpoint) VALUES ${rowClauses.join(", ")}
       ON CONFLICT (address, coin_type) DO UPDATE SET
         balance = CAST(CAST(balances.balance AS INTEGER) + CAST(excluded.balance AS INTEGER) AS TEXT),
         last_checkpoint = MAX(balances.last_checkpoint, excluded.last_checkpoint)`,
      values,
    );
  }
}

async function writeTransactions(ctx: WriteContext, transactions: TransactionRecord[]): Promise<void> {
  if (transactions.length === 0) return;

  const conflict = ctx.snapshotMode ? "" : "ON CONFLICT (digest) DO NOTHING";
  const rows = transactions.map(t => [
    t.digest,
    t.sender,
    ctx.dialect === "postgres" ? t.success : (t.success ? 1 : 0),
    t.computationCost,
    t.storageCost,
    t.storageRebate,
    t.nonRefundableStorageFee ?? null,
    t.moveCallCount,
    t.checkpointSeq.toString(),
    ctx.dialect === "postgres" ? t.timestamp : t.timestamp.toISOString(),
    (t.epoch ?? 0n).toString(),
    t.errorKind ?? null,
    t.errorDescription ?? null,
    t.errorCommandIndex ?? null,
    t.errorAbortCode ?? null,
    t.errorModule ?? null,
    t.errorFunction ?? null,
    t.eventsDigest ?? null,
    t.lamportVersion ?? null,
    t.dependencyCount ?? 0,
  ]);
  await insertRows(
    ctx,
    "transactions",
    [
      "digest", "sender", "success",
      "computation_cost", "storage_cost", "storage_rebate", "non_refundable_storage_fee",
      "move_call_count", "checkpoint_seq", "sui_timestamp",
      "epoch", "error_kind", "error_description", "error_command_index",
      "error_abort_code", "error_module", "error_function",
      "events_digest", "lamport_version", "dependency_count",
    ],
    rows,
    conflict,
    [
      "text", "text", "boolean",
      "numeric", "numeric", "numeric", "numeric",
      "integer", "numeric", "timestamptz",
      "numeric", "text", "text", "integer",
      "numeric", "text", "text",
      "text", "numeric", "integer",
    ],
  );
}

async function writeCheckpoints(ctx: WriteContext, checkpoints: Checkpoint[]): Promise<void> {
  if (checkpoints.length === 0) return;

  const conflict = ctx.snapshotMode ? "" : "ON CONFLICT (sequence_number) DO NOTHING";
  const rows = checkpoints.map(cp => [
    cp.sequenceNumber.toString(),
    (cp.epoch ?? 0n).toString(),
    cp.digest ?? "",
    cp.previousDigest ?? null,
    cp.contentDigest ?? null,
    ctx.dialect === "postgres" ? cp.timestamp : cp.timestamp.toISOString(),
    (cp.totalNetworkTransactions ?? 0n).toString(),
    cp.epochRollingGasCostSummary?.computationCost ?? "0",
    cp.epochRollingGasCostSummary?.storageCost ?? "0",
    cp.epochRollingGasCostSummary?.storageRebate ?? "0",
    cp.epochRollingGasCostSummary?.nonRefundableStorageFee ?? "0",
  ]);
  await insertRows(
    ctx,
    "checkpoints",
    [
      "sequence_number", "epoch", "digest", "previous_digest", "content_digest",
      "sui_timestamp", "total_network_transactions",
      "rolling_computation_cost", "rolling_storage_cost", "rolling_storage_rebate",
      "rolling_non_refundable_storage_fee",
    ],
    rows,
    conflict,
    [
      "numeric", "numeric", "text", "text", "text",
      "timestamptz", "numeric",
      "numeric", "numeric", "numeric", "numeric",
    ],
  );
}

async function writeObjectChanges(ctx: WriteContext, records: ObjectChangeRecord[]): Promise<void> {
  if (records.length === 0) return;
  const conflict = ctx.snapshotMode ? "" : "ON CONFLICT (tx_digest, object_id) DO NOTHING";
  const rows = records.map(r => [
    r.txDigest, r.objectId, r.changeType, r.objectType,
    r.inputVersion, r.inputDigest, r.inputOwner, r.inputOwnerKind,
    r.outputVersion, r.outputDigest, r.outputOwner, r.outputOwnerKind,
    ctx.dialect === "postgres" ? r.isGasObject : (r.isGasObject ? 1 : 0),
    r.checkpointSeq.toString(),
    ctx.dialect === "postgres" ? r.timestamp : r.timestamp.toISOString(),
  ]);
  await insertRows(
    ctx,
    "object_changes",
    [
      "tx_digest", "object_id", "change_type", "object_type",
      "input_version", "input_digest", "input_owner", "input_owner_kind",
      "output_version", "output_digest", "output_owner", "output_owner_kind",
      "is_gas_object", "checkpoint_seq", "sui_timestamp",
    ],
    rows,
    conflict,
    [
      "text", "text", "text", "text",
      "numeric", "text", "text", "text",
      "numeric", "text", "text", "text",
      "boolean", "numeric", "timestamptz",
    ],
  );
}

async function writeDependencies(ctx: WriteContext, records: TransactionDependencyRecord[]): Promise<void> {
  if (records.length === 0) return;
  const conflict = ctx.snapshotMode ? "" : "ON CONFLICT (tx_digest, depends_on_digest) DO NOTHING";
  const rows = records.map(r => [
    r.txDigest,
    r.dependsOnDigest,
    r.checkpointSeq.toString(),
    ctx.dialect === "postgres" ? r.timestamp : r.timestamp.toISOString(),
  ]);
  await insertRows(
    ctx,
    "transaction_dependencies",
    ["tx_digest", "depends_on_digest", "checkpoint_seq", "sui_timestamp"],
    rows,
    conflict,
    ["text", "text", "numeric", "timestamptz"],
  );
}

async function writeTransactionInputs(ctx: WriteContext, records: TransactionInputRecord[]): Promise<void> {
  if (records.length === 0) return;
  const conflict = ctx.snapshotMode ? "" : "ON CONFLICT (tx_digest, input_index) DO NOTHING";
  const rows = records.map(r => [
    r.txDigest, r.inputIndex, r.kind,
    r.objectId, r.version, r.digest, r.mutability, r.initialSharedVersion,
    r.pureBytes, r.amount, r.coinType, r.source,
    r.checkpointSeq.toString(),
    ctx.dialect === "postgres" ? r.timestamp : r.timestamp.toISOString(),
  ]);
  await insertRows(
    ctx,
    "transaction_inputs",
    [
      "tx_digest", "input_index", "kind",
      "object_id", "version", "digest", "mutability", "initial_shared_version",
      "pure_bytes", "amount", "coin_type", "source",
      "checkpoint_seq", "sui_timestamp",
    ],
    rows,
    conflict,
    [
      "text", "integer", "text",
      "text", "numeric", "text", "text", "numeric",
      "text", "numeric", "text", "text",
      "numeric", "timestamptz",
    ],
  );
}

async function writeCommands(ctx: WriteContext, records: CommandRecord[]): Promise<void> {
  if (records.length === 0) return;
  const conflict = ctx.snapshotMode ? "" : "ON CONFLICT (tx_digest, command_index) DO NOTHING";
  const rows = records.map(r => [
    r.txDigest, r.commandIndex, r.kind,
    r.package, r.module, r.function, r.typeArguments, r.args,
    r.checkpointSeq.toString(),
    ctx.dialect === "postgres" ? r.timestamp : r.timestamp.toISOString(),
  ]);
  await insertRows(
    ctx,
    "commands",
    [
      "tx_digest", "command_index", "kind",
      "package", "module", "function", "type_arguments", "args",
      "checkpoint_seq", "sui_timestamp",
    ],
    rows,
    conflict,
    [
      "text", "integer", "text",
      "text", "text", "text", "text", "text",
      "numeric", "timestamptz",
    ],
  );
}

async function writeSystemTransactions(ctx: WriteContext, records: SystemTransactionRecord[]): Promise<void> {
  if (records.length === 0) return;
  const conflict = ctx.snapshotMode ? "" : "ON CONFLICT (tx_digest) DO NOTHING";
  const rows = records.map(r => [
    r.txDigest, r.kind, r.data,
    r.checkpointSeq.toString(),
    ctx.dialect === "postgres" ? r.timestamp : r.timestamp.toISOString(),
  ]);
  await insertRows(
    ctx,
    "system_transactions",
    ["tx_digest", "kind", "data", "checkpoint_seq", "sui_timestamp"],
    rows,
    conflict,
    ["text", "text", "text", "numeric", "timestamptz"],
  );
}

async function writeUnchangedConsensusObjects(ctx: WriteContext, records: UnchangedConsensusObjectRecord[]): Promise<void> {
  if (records.length === 0) return;
  const conflict = ctx.snapshotMode ? "" : "ON CONFLICT (tx_digest, object_id) DO NOTHING";
  const rows = records.map(r => [
    r.txDigest, r.objectId, r.kind,
    r.version, r.digest, r.objectType,
    r.checkpointSeq.toString(),
    ctx.dialect === "postgres" ? r.timestamp : r.timestamp.toISOString(),
  ]);
  await insertRows(
    ctx,
    "unchanged_consensus_objects",
    [
      "tx_digest", "object_id", "kind",
      "version", "digest", "object_type",
      "checkpoint_seq", "sui_timestamp",
    ],
    rows,
    conflict,
    [
      "text", "text", "text",
      "numeric", "text", "text",
      "numeric", "timestamptz",
    ],
  );
}

async function writeMoveCalls(ctx: WriteContext, moveCalls: MoveCallRecord[]): Promise<void> {
  if (moveCalls.length === 0) return;

  const conflict = ctx.snapshotMode ? "" : "ON CONFLICT (tx_digest, call_index) DO NOTHING";
  const rows = moveCalls.map(mc => [
    mc.txDigest,
    mc.callIndex,
    mc.package,
    mc.module,
    mc.function,
    mc.checkpointSeq.toString(),
    ctx.dialect === "postgres" ? mc.timestamp : mc.timestamp.toISOString(),
  ]);
  await insertRows(
    ctx,
    "move_calls",
    ["tx_digest", "call_index", "package", "module", "function", "checkpoint_seq", "sui_timestamp"],
    rows,
    conflict,
    ["text", "integer", "text", "text", "text", "numeric", "timestamptz"],
  );
}

function isDuplicateIndexError(error: unknown): error is Error {
  if (!(error instanceof Error)) return false;
  // SQLite: "UNIQUE constraint failed: table.column"
  // Postgres: "could not create unique index"
  return error.message.includes("UNIQUE constraint failed:") || error.message.includes("could not create unique index");
}

/** Repair duplicate rows for SQLite (uses rowid). */
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

/** Repair duplicate rows for Postgres (uses ctid). */
async function repairPostgresSnapshotDuplicates(
  driver: SqlDriver,
  error: Error,
  log: Logger,
): Promise<boolean> {
  // Extract index name from error: 'could not create unique index "idx_tx_digest"'
  const indexMatch = error.message.match(/could not create unique index "(\w+)"/);
  if (!indexMatch || !indexMatch[1]) return false;
  const indexName = indexMatch[1];

  const repairs: Record<string, { table: string; sql: string }> = {
    idx_tx_digest: {
      table: "transactions",
      sql: `DELETE FROM transactions a USING transactions b
            WHERE a.ctid > b.ctid AND a.digest = b.digest`,
    },
    idx_bc_unique: {
      table: "balance_changes",
      sql: `DELETE FROM balance_changes a USING balance_changes b
            WHERE a.ctid > b.ctid AND a.tx_digest = b.tx_digest AND a.address = b.address AND a.coin_type = b.coin_type`,
    },
    idx_mc_pk: {
      table: "move_calls",
      sql: `DELETE FROM move_calls a USING move_calls b
            WHERE a.ctid > b.ctid AND a.tx_digest = b.tx_digest AND a.call_index = b.call_index`,
    },
  };

  const repair = repairs[indexName];
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
      if (!allowSnapshotRepair || !isDuplicateIndexError(error)) throw error;
      const repaired = driver.dialect === "postgres"
        ? await repairPostgresSnapshotDuplicates(driver, error as Error, log)
        : await repairSqliteSnapshotDuplicates(driver, error as Error, log);
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
  // UNLOGGED tables for Postgres bulk loading (no WAL writes)
  const pgUnlogged = !!(config.pgUnlogged || (snapshotMode && isPostgres));

  // Pre-computed table configs
  const tables = new Map<string, { name: string; columns: string[] }>();
  const deferredIndexes: string[] = [];

  const storageObj: Storage = {
    name: isPostgres ? "postgres" : "sqlite",

    async initialize(): Promise<void> {
      driver = isPostgres
        ? createPostgresDriver(config.url)
        : createSqliteDriver(sqlitePath);

      // Snapshot mode: drop existing tables to avoid stale constraints
      if (snapshotMode || pgUnlogged) {
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
        const unloggedKw = pgUnlogged ? "UNLOGGED" : "";
        if (driver.dialect === "postgres") {
          if (snapshotMode) {
            await driver.exec(`
              CREATE ${unloggedKw} TABLE IF NOT EXISTS balance_changes (
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
              CREATE ${unloggedKw} TABLE IF NOT EXISTS balance_changes (
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
            await driver.exec(`
              CREATE ${unloggedKw} TABLE IF NOT EXISTS balances (
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
              CREATE ${unloggedKw} TABLE IF NOT EXISTS balances (
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
        const unloggedKw = pgUnlogged ? "UNLOGGED" : "";
        if (driver.dialect === "postgres") {
          if (snapshotMode) {
            await driver.exec(`
              CREATE ${unloggedKw} TABLE IF NOT EXISTS transactions (
                digest TEXT NOT NULL,
                sender TEXT NOT NULL,
                success BOOLEAN NOT NULL,
                computation_cost NUMERIC NOT NULL,
                storage_cost NUMERIC NOT NULL,
                storage_rebate NUMERIC NOT NULL,
                non_refundable_storage_fee NUMERIC,
                move_call_count INTEGER NOT NULL DEFAULT 0,
                checkpoint_seq NUMERIC NOT NULL,
                sui_timestamp TIMESTAMPTZ NOT NULL,
                epoch NUMERIC NOT NULL DEFAULT 0,
                error_kind TEXT,
                error_description TEXT,
                error_command_index INTEGER,
                error_abort_code NUMERIC,
                error_module TEXT,
                error_function TEXT,
                events_digest TEXT,
                lamport_version NUMERIC,
                dependency_count INTEGER NOT NULL DEFAULT 0
              );
            `);
            deferredIndexes.push(`CREATE UNIQUE INDEX IF NOT EXISTS idx_tx_digest ON transactions(digest)`);
          } else {
            await driver.exec(`
              CREATE ${unloggedKw} TABLE IF NOT EXISTS transactions (
                digest TEXT PRIMARY KEY,
                sender TEXT NOT NULL,
                success BOOLEAN NOT NULL,
                computation_cost NUMERIC NOT NULL,
                storage_cost NUMERIC NOT NULL,
                storage_rebate NUMERIC NOT NULL,
                non_refundable_storage_fee NUMERIC,
                move_call_count INTEGER NOT NULL DEFAULT 0,
                checkpoint_seq NUMERIC NOT NULL,
                sui_timestamp TIMESTAMPTZ NOT NULL,
                epoch NUMERIC NOT NULL DEFAULT 0,
                error_kind TEXT,
                error_description TEXT,
                error_command_index INTEGER,
                error_abort_code NUMERIC,
                error_module TEXT,
                error_function TEXT,
                events_digest TEXT,
                lamport_version NUMERIC,
                dependency_count INTEGER NOT NULL DEFAULT 0
              );
            `);
          }
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_tx_epoch ON transactions(epoch)`);
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_tx_error_kind ON transactions(error_kind) WHERE error_kind IS NOT NULL`);
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_tx_sender ON transactions(sender)`);
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_tx_checkpoint ON transactions(checkpoint_seq)`);
          if (snapshotMode) {
            await driver.exec(`
              CREATE ${unloggedKw} TABLE IF NOT EXISTS move_calls (
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
              CREATE ${unloggedKw} TABLE IF NOT EXISTS move_calls (
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
                non_refundable_storage_fee TEXT,
                move_call_count INTEGER NOT NULL DEFAULT 0,
                checkpoint_seq TEXT NOT NULL,
                sui_timestamp TEXT NOT NULL,
                epoch TEXT NOT NULL DEFAULT '0',
                error_kind TEXT,
                error_description TEXT,
                error_command_index INTEGER,
                error_abort_code TEXT,
                error_module TEXT,
                error_function TEXT,
                events_digest TEXT,
                lamport_version TEXT,
                dependency_count INTEGER NOT NULL DEFAULT 0
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
                non_refundable_storage_fee TEXT,
                move_call_count INTEGER NOT NULL DEFAULT 0,
                checkpoint_seq TEXT NOT NULL,
                sui_timestamp TEXT NOT NULL,
                epoch TEXT NOT NULL DEFAULT '0',
                error_kind TEXT,
                error_description TEXT,
                error_command_index INTEGER,
                error_abort_code TEXT,
                error_module TEXT,
                error_function TEXT,
                events_digest TEXT,
                lamport_version TEXT,
                dependency_count INTEGER NOT NULL DEFAULT 0
              );
            `);
          }
          await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_tx_epoch ON transactions(epoch)`);
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

      // ─── Checkpoints table (one row per checkpoint) ─────────────────────
      if (config.checkpoints) {
        const unloggedKw = pgUnlogged ? "UNLOGGED" : "";
        if (driver.dialect === "postgres") {
          const pkClause = snapshotMode ? "" : "PRIMARY KEY";
          await driver.exec(`
            CREATE ${unloggedKw} TABLE IF NOT EXISTS checkpoints (
              sequence_number NUMERIC NOT NULL ${pkClause},
              epoch NUMERIC NOT NULL,
              digest TEXT NOT NULL,
              previous_digest TEXT,
              content_digest TEXT,
              sui_timestamp TIMESTAMPTZ NOT NULL,
              total_network_transactions NUMERIC NOT NULL,
              rolling_computation_cost NUMERIC NOT NULL,
              rolling_storage_cost NUMERIC NOT NULL,
              rolling_storage_rebate NUMERIC NOT NULL,
              rolling_non_refundable_storage_fee NUMERIC NOT NULL
            );
          `);
          if (snapshotMode) {
            deferredIndexes.push(`CREATE UNIQUE INDEX IF NOT EXISTS idx_checkpoints_seq ON checkpoints(sequence_number)`);
          }
        } else {
          const pkClause = snapshotMode ? "" : "PRIMARY KEY";
          await driver.exec(`
            CREATE TABLE IF NOT EXISTS checkpoints (
              sequence_number TEXT NOT NULL ${pkClause},
              epoch TEXT NOT NULL,
              digest TEXT NOT NULL,
              previous_digest TEXT,
              content_digest TEXT,
              sui_timestamp TEXT NOT NULL,
              total_network_transactions TEXT NOT NULL,
              rolling_computation_cost TEXT NOT NULL,
              rolling_storage_cost TEXT NOT NULL,
              rolling_storage_rebate TEXT NOT NULL,
              rolling_non_refundable_storage_fee TEXT NOT NULL
            );
          `);
          if (snapshotMode) {
            deferredIndexes.push(`CREATE UNIQUE INDEX IF NOT EXISTS idx_checkpoints_seq ON checkpoints(sequence_number)`);
          }
        }
        await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_checkpoints_epoch ON checkpoints(epoch)`);
        log.info("checkpoints table created");
      }

      // ─── object_changes ──────────────────────────────────────────────────
      if (config.objectChanges) {
        const unloggedKw = pgUnlogged ? "UNLOGGED" : "";
        const pkClause = snapshotMode ? "" : ", PRIMARY KEY (tx_digest, object_id)";
        if (driver.dialect === "postgres") {
          await driver.exec(`
            CREATE ${unloggedKw} TABLE IF NOT EXISTS object_changes (
              tx_digest TEXT NOT NULL,
              object_id TEXT NOT NULL,
              change_type TEXT NOT NULL,
              object_type TEXT,
              input_version NUMERIC,
              input_digest TEXT,
              input_owner TEXT,
              input_owner_kind TEXT,
              output_version NUMERIC,
              output_digest TEXT,
              output_owner TEXT,
              output_owner_kind TEXT,
              is_gas_object BOOLEAN NOT NULL,
              checkpoint_seq NUMERIC NOT NULL,
              sui_timestamp TIMESTAMPTZ NOT NULL${pkClause}
            );
          `);
        } else {
          await driver.exec(`
            CREATE TABLE IF NOT EXISTS object_changes (
              tx_digest TEXT NOT NULL,
              object_id TEXT NOT NULL,
              change_type TEXT NOT NULL,
              object_type TEXT,
              input_version TEXT,
              input_digest TEXT,
              input_owner TEXT,
              input_owner_kind TEXT,
              output_version TEXT,
              output_digest TEXT,
              output_owner TEXT,
              output_owner_kind TEXT,
              is_gas_object INTEGER NOT NULL,
              checkpoint_seq TEXT NOT NULL,
              sui_timestamp TEXT NOT NULL${pkClause}
            );
          `);
        }
        if (snapshotMode) {
          deferredIndexes.push(`CREATE UNIQUE INDEX IF NOT EXISTS idx_oc_pk ON object_changes(tx_digest, object_id)`);
        }
        await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_oc_object_id ON object_changes(object_id)`);
        await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_oc_change_type ON object_changes(change_type)`);
        await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_oc_output_owner ON object_changes(output_owner)`);
        await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_oc_object_type ON object_changes(object_type)`);
        log.info("object_changes table created");
      }

      // ─── transaction_dependencies ────────────────────────────────────────
      if (config.dependencies) {
        const unloggedKw = pgUnlogged ? "UNLOGGED" : "";
        const pkClause = snapshotMode ? "" : ", PRIMARY KEY (tx_digest, depends_on_digest)";
        if (driver.dialect === "postgres") {
          await driver.exec(`
            CREATE ${unloggedKw} TABLE IF NOT EXISTS transaction_dependencies (
              tx_digest TEXT NOT NULL,
              depends_on_digest TEXT NOT NULL,
              checkpoint_seq NUMERIC NOT NULL,
              sui_timestamp TIMESTAMPTZ NOT NULL${pkClause}
            );
          `);
        } else {
          await driver.exec(`
            CREATE TABLE IF NOT EXISTS transaction_dependencies (
              tx_digest TEXT NOT NULL,
              depends_on_digest TEXT NOT NULL,
              checkpoint_seq TEXT NOT NULL,
              sui_timestamp TEXT NOT NULL${pkClause}
            );
          `);
        }
        if (snapshotMode) {
          deferredIndexes.push(`CREATE UNIQUE INDEX IF NOT EXISTS idx_td_pk ON transaction_dependencies(tx_digest, depends_on_digest)`);
        }
        await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_td_depends_on ON transaction_dependencies(depends_on_digest)`);
        log.info("transaction_dependencies table created");
      }

      // ─── transaction_inputs ──────────────────────────────────────────────
      if (config.inputs) {
        const unloggedKw = pgUnlogged ? "UNLOGGED" : "";
        const pkClause = snapshotMode ? "" : ", PRIMARY KEY (tx_digest, input_index)";
        if (driver.dialect === "postgres") {
          await driver.exec(`
            CREATE ${unloggedKw} TABLE IF NOT EXISTS transaction_inputs (
              tx_digest TEXT NOT NULL,
              input_index INTEGER NOT NULL,
              kind TEXT NOT NULL,
              object_id TEXT,
              version NUMERIC,
              digest TEXT,
              mutability TEXT,
              initial_shared_version NUMERIC,
              pure_bytes TEXT,
              amount NUMERIC,
              coin_type TEXT,
              source TEXT,
              checkpoint_seq NUMERIC NOT NULL,
              sui_timestamp TIMESTAMPTZ NOT NULL${pkClause}
            );
          `);
        } else {
          await driver.exec(`
            CREATE TABLE IF NOT EXISTS transaction_inputs (
              tx_digest TEXT NOT NULL,
              input_index INTEGER NOT NULL,
              kind TEXT NOT NULL,
              object_id TEXT,
              version TEXT,
              digest TEXT,
              mutability TEXT,
              initial_shared_version TEXT,
              pure_bytes TEXT,
              amount TEXT,
              coin_type TEXT,
              source TEXT,
              checkpoint_seq TEXT NOT NULL,
              sui_timestamp TEXT NOT NULL${pkClause}
            );
          `);
        }
        if (snapshotMode) {
          deferredIndexes.push(`CREATE UNIQUE INDEX IF NOT EXISTS idx_ti_pk ON transaction_inputs(tx_digest, input_index)`);
        }
        await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_ti_object_id ON transaction_inputs(object_id)`);
        await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_ti_kind ON transaction_inputs(kind)`);
        log.info("transaction_inputs table created");
      }

      // ─── commands ────────────────────────────────────────────────────────
      if (config.commands) {
        const unloggedKw = pgUnlogged ? "UNLOGGED" : "";
        const pkClause = snapshotMode ? "" : ", PRIMARY KEY (tx_digest, command_index)";
        if (driver.dialect === "postgres") {
          await driver.exec(`
            CREATE ${unloggedKw} TABLE IF NOT EXISTS commands (
              tx_digest TEXT NOT NULL,
              command_index INTEGER NOT NULL,
              kind TEXT NOT NULL,
              package TEXT,
              module TEXT,
              function TEXT,
              type_arguments TEXT,
              args TEXT,
              checkpoint_seq NUMERIC NOT NULL,
              sui_timestamp TIMESTAMPTZ NOT NULL${pkClause}
            );
          `);
        } else {
          await driver.exec(`
            CREATE TABLE IF NOT EXISTS commands (
              tx_digest TEXT NOT NULL,
              command_index INTEGER NOT NULL,
              kind TEXT NOT NULL,
              package TEXT,
              module TEXT,
              function TEXT,
              type_arguments TEXT,
              args TEXT,
              checkpoint_seq TEXT NOT NULL,
              sui_timestamp TEXT NOT NULL${pkClause}
            );
          `);
        }
        if (snapshotMode) {
          deferredIndexes.push(`CREATE UNIQUE INDEX IF NOT EXISTS idx_cmd_pk ON commands(tx_digest, command_index)`);
        }
        await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_cmd_kind ON commands(kind)`);
        await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_cmd_package ON commands(package)`);
        await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_cmd_module ON commands(package, module)`);
        await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_cmd_function ON commands(package, module, function)`);
        log.info("commands table created");
      }

      // ─── system_transactions ─────────────────────────────────────────────
      if (config.systemTransactions) {
        const unloggedKw = pgUnlogged ? "UNLOGGED" : "";
        const pkClause = snapshotMode ? "" : "PRIMARY KEY";
        if (driver.dialect === "postgres") {
          await driver.exec(`
            CREATE ${unloggedKw} TABLE IF NOT EXISTS system_transactions (
              tx_digest TEXT NOT NULL ${pkClause},
              kind TEXT NOT NULL,
              data TEXT NOT NULL,
              checkpoint_seq NUMERIC NOT NULL,
              sui_timestamp TIMESTAMPTZ NOT NULL
            );
          `);
        } else {
          await driver.exec(`
            CREATE TABLE IF NOT EXISTS system_transactions (
              tx_digest TEXT NOT NULL ${pkClause},
              kind TEXT NOT NULL,
              data TEXT NOT NULL,
              checkpoint_seq TEXT NOT NULL,
              sui_timestamp TEXT NOT NULL
            );
          `);
        }
        if (snapshotMode) {
          deferredIndexes.push(`CREATE UNIQUE INDEX IF NOT EXISTS idx_st_pk ON system_transactions(tx_digest)`);
        }
        await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_st_kind ON system_transactions(kind)`);
        await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_st_checkpoint ON system_transactions(checkpoint_seq)`);
        log.info("system_transactions table created");
      }

      // ─── unchanged_consensus_objects ─────────────────────────────────────
      if (config.unchangedConsensusObjects) {
        const unloggedKw = pgUnlogged ? "UNLOGGED" : "";
        const pkClause = snapshotMode ? "" : ", PRIMARY KEY (tx_digest, object_id)";
        if (driver.dialect === "postgres") {
          await driver.exec(`
            CREATE ${unloggedKw} TABLE IF NOT EXISTS unchanged_consensus_objects (
              tx_digest TEXT NOT NULL,
              object_id TEXT NOT NULL,
              kind TEXT NOT NULL,
              version NUMERIC,
              digest TEXT,
              object_type TEXT,
              checkpoint_seq NUMERIC NOT NULL,
              sui_timestamp TIMESTAMPTZ NOT NULL${pkClause}
            );
          `);
        } else {
          await driver.exec(`
            CREATE TABLE IF NOT EXISTS unchanged_consensus_objects (
              tx_digest TEXT NOT NULL,
              object_id TEXT NOT NULL,
              kind TEXT NOT NULL,
              version TEXT,
              digest TEXT,
              object_type TEXT,
              checkpoint_seq TEXT NOT NULL,
              sui_timestamp TEXT NOT NULL${pkClause}
            );
          `);
        }
        if (snapshotMode) {
          deferredIndexes.push(`CREATE UNIQUE INDEX IF NOT EXISTS idx_uco_pk ON unchanged_consensus_objects(tx_digest, object_id)`);
        }
        await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_uco_object_id ON unchanged_consensus_objects(object_id)`);
        await indexOrDefer(`CREATE INDEX IF NOT EXISTS idx_uco_kind ON unchanged_consensus_objects(kind)`);
        log.info("unchanged_consensus_objects table created");
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

      // Collect every record type from the batch
      const groupedEvents = new Map<string, DecodedEvent[]>();
      const allBalanceChanges: BalanceChange[] = [];
      const allTransactions: TransactionRecord[] = [];
      const allMoveCalls: MoveCallRecord[] = [];
      const allObjectChanges: ObjectChangeRecord[] = [];
      const allDependencies: TransactionDependencyRecord[] = [];
      const allInputs: TransactionInputRecord[] = [];
      const allCommands: CommandRecord[] = [];
      const allSystemTransactions: SystemTransactionRecord[] = [];
      const allUnchangedConsensusObjects: UnchangedConsensusObjectRecord[] = [];

      for (const processed of batch) {
        for (const event of processed.events ?? []) {
          const list = groupedEvents.get(event.handlerName);
          if (list) list.push(event);
          else groupedEvents.set(event.handlerName, [event]);
        }
        allBalanceChanges.push(...(processed.balanceChanges ?? []));
        allTransactions.push(...(processed.transactions ?? []));
        allMoveCalls.push(...(processed.moveCalls ?? []));
        allObjectChanges.push(...(processed.objectChanges ?? []));
        allDependencies.push(...(processed.dependencies ?? []));
        allInputs.push(...(processed.inputs ?? []));
        allCommands.push(...(processed.commands ?? []));
        allSystemTransactions.push(...(processed.systemTransactions ?? []));
        allUnchangedConsensusObjects.push(...(processed.unchangedConsensusObjects ?? []));
      }

      // Wrap all writes in a single transaction
      await d.transaction(async (exec, rawTx) => {
      const ctx: WriteContext = { dialect, exec, rawTx, snapshotMode };

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

      // Write checkpoints (one row per checkpoint; dedup by sequence number)
      if (config.checkpoints) {
        const seen = new Set<string>();
        const uniqueCheckpoints: Checkpoint[] = [];
        for (const processed of batch) {
          const seq = processed.checkpoint.sequenceNumber.toString();
          if (seen.has(seq)) continue;
          seen.add(seq);
          uniqueCheckpoints.push(processed.checkpoint);
        }
        if (uniqueCheckpoints.length > 0) {
          await writeCheckpoints(ctx, uniqueCheckpoints);
        }
      }

      // Write balance changes
      if (config.balances && allBalanceChanges.length > 0) {
        await writeBalanceChanges(ctx, allBalanceChanges);
      }

      // Write transactions
      if (config.transactions && allTransactions.length > 0) {
        await writeTransactions(ctx, allTransactions);
      }

      // Write move calls
      if (config.transactions && allMoveCalls.length > 0) {
        await writeMoveCalls(ctx, allMoveCalls);
      }

      // Write object changes
      if (config.objectChanges && allObjectChanges.length > 0) {
        await writeObjectChanges(ctx, allObjectChanges);
      }

      // Write transaction dependencies
      if (config.dependencies && allDependencies.length > 0) {
        await writeDependencies(ctx, allDependencies);
      }

      // Write transaction inputs
      if (config.inputs && allInputs.length > 0) {
        await writeTransactionInputs(ctx, allInputs);
      }

      // Write commands
      if (config.commands && allCommands.length > 0) {
        await writeCommands(ctx, allCommands);
      }

      // Write system transactions
      if (config.systemTransactions && allSystemTransactions.length > 0) {
        await writeSystemTransactions(ctx, allSystemTransactions);
      }

      // Write unchanged consensus objects
      if (config.unchangedConsensusObjects && allUnchangedConsensusObjects.length > 0) {
        await writeUnchangedConsensusObjects(ctx, allUnchangedConsensusObjects);
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

        const allIndexes = [...otherIndexes, ...balanceIndexes];
        log.info({ count: allIndexes.length }, "creating deferred indexes...");
        const startTime = performance.now();

        if (isPostgres && allIndexes.length > 1) {
          // Postgres: create indexes in parallel (each exec() uses a separate pool connection)
          const results = await Promise.allSettled(
            allIndexes.map(async (sql) => {
              try {
                await driver!.exec(sql);
              } catch (error) {
                if (!snapshotMode || !isDuplicateIndexError(error)) throw error;
                const repaired = await repairPostgresSnapshotDuplicates(driver!, error as Error, log);
                if (!repaired) throw error;
                await driver!.exec(sql);
              }
            })
          );
          const failures = results.filter(r => r.status === "rejected");
          if (failures.length > 0) {
            throw (failures[0] as PromiseRejectedResult).reason;
          }
        } else if (allIndexes.length > 0) {
          // SQLite: sequential (single connection)
          await createDeferredIndexes(driver, allIndexes, log, snapshotMode);
        }

        const elapsed = ((performance.now() - startTime) / 1000).toFixed(1);
        log.info({ elapsed: `${elapsed}s` }, "deferred indexes created");
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
        // Skip UNLOGGED→LOGGED conversion — snapshot data is re-indexable from
        // the blockchain, so crash-safety of UNLOGGED tables is acceptable.
        // If durability is needed, users can run: ALTER TABLE <t> SET LOGGED;
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

  return storageObj;
}

