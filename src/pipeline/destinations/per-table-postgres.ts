/**
 * Per-table Postgres storage — two adapters for different workloads.
 *
 * createReplayPostgresStorage  — for replay-chain (batch backfill)
 *   UNLOGGED tables during load (skips WAL entirely), no secondary indexes
 *   while writing, then SET LOGGED + CREATE INDEX + ANALYZE at shutdown.
 *   ~2x faster than live mode for bulk loads.
 *
 * createLivePostgresStorage    — for stream-chain (continuous indexing)
 *   Standard logged tables with indexes throughout. UNNEST typed arrays for
 *   one round-trip per table per batch. ON CONFLICT DO NOTHING for idempotency
 *   (live stream replays recent checkpoints on reconnect).
 *
 * Both: synchronous_commit=off (set in createPostgresConnection) — skips WAL
 * flush per commit; safe since blockchain is the source of truth.
 *
 * Docs: https://www.postgresql.org/docs/current/populate.html
 */
import { createPostgresConnection } from "../../db.ts";
import type { ProcessedCheckpoint, Storage } from "../types.ts";
import { TABLES, TABLE_MASK_BIT } from "./per-table-sqlite.ts";

// ---------------------------------------------------------------------------
// Shared types and helpers
// ---------------------------------------------------------------------------

type PgType = "text" | "int4";

/** Postgres types for each column, matched to column order in TABLES. */
export const PG_COLUMN_TYPES: Record<string, PgType[]> = {
  transactions: [
    "text", "text", "int4",         // digest, sender, success
    "text", "text", "text", "text", // computation_cost, storage_cost, storage_rebate, non_refundable_storage_fee
    "int4", "text", "text",         // move_call_count, checkpoint_seq, sui_timestamp
    "text", "text", "text", "int4", // epoch, error_kind, error_description, error_command_index
    "text", "text", "text",         // error_abort_code, error_module, error_function
    "text", "text", "int4",         // events_digest, lamport_version, dependency_count
  ],
  move_calls: [
    "text", "int4", "text", "text", "text", "text", "text",
  ],
  balance_changes: [
    "text", "text", "text", "text", "text", "text",
  ],
  object_changes: [
    "text", "text", "text", "text",
    "text", "text", "text", "text",
    "text", "text", "text", "text",
    "int4", "text", "text",
  ],
  transaction_dependencies: [
    "text", "text", "text", "text",
  ],
  transaction_inputs: [
    "text", "int4", "text",
    "text", "text", "text", "text", "text",
    "text", "text", "text", "text",
    "text", "text",
  ],
  commands: [
    "text", "int4", "text",
    "text", "text", "text",
    "text", "text",
    "text", "text",
  ],
  system_transactions: [
    "text", "text", "text", "text", "text",
  ],
  unchanged_consensus_objects: [
    "text", "text", "text",
    "text", "text", "text",
    "text", "text",
  ],
  checkpoints: [
    "text", "text", "text", "text", "text",
    "text", "text",
    "text", "text", "text", "text",
  ],
  raw_events: [
    "text", "int4", "text", "text", "text", "text", "text", "text", "text",
  ],
};

/** Serialize a column's values as a Postgres array literal: {val1,val2,...} */
function pgArrayLiteral(values: unknown[], pgType: PgType): string {
  const items = values.map(v => {
    if (v === null || v === undefined) return "NULL";
    if (pgType === "int4") return String(v);
    const s = String(v);
    return `"${s.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
  });
  return `{${items.join(",")}}`;
}

/** Single INSERT via UNNEST — one round-trip per table regardless of row count. */
export async function insertUnnest(
  sql: ReturnType<typeof createPostgresConnection>,
  table: string,
  columns: string[],
  pgTypes: PgType[],
  rows: unknown[][],
): Promise<void> {
  if (rows.length === 0) return;
  const colArrays = columns.map((_, i) =>
    pgArrayLiteral(rows.map(row => row[i]), pgTypes[i]!)
  );
  const unnestExprs = columns.map((_, i) => `$${i + 1}::${pgTypes[i]}[]`).join(", ");
  await sql.unsafe(
    `INSERT INTO ${table} (${columns.join(", ")})
     SELECT * FROM UNNEST(${unnestExprs})
     ON CONFLICT DO NOTHING`,
    colArrays,
  );
}

/** Collect rows per table from a batch (pure JS, no I/O). */
export function collectTableRows(
  enabledTables: typeof TABLES,
  batch: ProcessedCheckpoint[],
): unknown[][][] {
  return enabledTables.map(tableDef => {
    const rows: unknown[][] = [];
    for (const cp of batch) {
      for (const record of tableDef.getRecords(cp)) {
        rows.push(tableDef.mapRow(record));
      }
    }
    return rows;
  });
}

/**
 * Split a DDL string (may contain CREATE TABLE + CREATE INDEX statements)
 * into table DDL (just CREATE TABLE) and index DDL (CREATE INDEX statements).
 */
function splitDdl(ddl: string): { tableDdl: string; indexDdl: string[] } {
  const stmts = ddl.split(";").map(s => s.trim()).filter(Boolean);
  const tableDdl = stmts.filter(s => /^CREATE\s+(UNLOGGED\s+)?TABLE/i.test(s)).join(";") + ";";
  const indexDdl = stmts.filter(s => /^CREATE\s+INDEX/i.test(s));
  return { tableDdl, indexDdl };
}

// ---------------------------------------------------------------------------
// Replay adapter — maximum throughput for batch backfill
// ---------------------------------------------------------------------------

/**
 * Postgres storage optimized for replay-chain (batch backfill).
 *
 * Uses UNLOGGED tables during the load phase — skips WAL entirely, which
 * is the single largest throughput gain for Postgres bulk loads. Secondary
 * indexes are created at shutdown after all data is written, which is far
 * faster than incremental index maintenance per insert.
 *
 * Safety: data is recoverable by re-running replay-chain. UNLOGGED tables
 * are automatically truncated on crash, but for a backfill tool that's fine.
 */
export function createReplayPostgresStorage(url: string, enabledMask = 0x7FF): Storage {
  const db = createPostgresConnection(url);
  const enabledTables = TABLES.filter(t => enabledMask & (TABLE_MASK_BIT[t.name] ?? 0));

  // Split DDL upfront so initialize() and shutdown() each run their part.
  const splitDdls = enabledTables.map(t => splitDdl(t.ddl));

  return {
    name: "replay-postgres",

    async initialize(): Promise<void> {
      for (let i = 0; i < enabledTables.length; i++) {
        const tableDdl = splitDdls[i]!.tableDdl;
        // Create as UNLOGGED to skip WAL during load
        const unloggedDdl = tableDdl.replace(
          /CREATE TABLE IF NOT EXISTS/gi,
          "CREATE UNLOGGED TABLE IF NOT EXISTS",
        );
        for (const stmt of unloggedDdl.split(";").map(s => s.trim()).filter(Boolean)) {
          await db.unsafe(stmt, []);
        }
      }
    },

    async write(batch: ProcessedCheckpoint[]): Promise<void> {
      const tableRows = collectTableRows(enabledTables, batch);
      await db.begin(async (tx) => {
        for (let i = 0; i < enabledTables.length; i++) {
          const tableDef = enabledTables[i]!;
          const pgTypes = PG_COLUMN_TYPES[tableDef.name];
          if (!pgTypes) throw new Error(`No PG_COLUMN_TYPES for table ${tableDef.name}`);
          await insertUnnest(
            tx as ReturnType<typeof createPostgresConnection>,
            tableDef.name,
            tableDef.columns,
            pgTypes,
            tableRows[i]!,
          );
        }
      });
    },

    async shutdown(): Promise<void> {
      // Convert UNLOGGED → LOGGED, create indexes, analyze.
      // Done outside a transaction — CREATE INDEX cannot run inside one.
      for (let i = 0; i < enabledTables.length; i++) {
        const tableDef = enabledTables[i]!;
        await db.unsafe(`ALTER TABLE ${tableDef.name} SET LOGGED`, []);
        for (const indexDdl of splitDdls[i]!.indexDdl) {
          await db.unsafe(indexDdl, []);
        }
        await db.unsafe(`ANALYZE ${tableDef.name}`, []);
      }
      await db.end?.();
    },
  };
}

// ---------------------------------------------------------------------------
// Live adapter — reliable continuous indexing for stream-chain
// ---------------------------------------------------------------------------

/**
 * Postgres storage for stream-chain (live continuous indexing).
 *
 * Standard logged tables with all indexes in place throughout. Boring and
 * reliable — idempotent via ON CONFLICT DO NOTHING, indexes always consistent,
 * no deferred operations. At 5-10 cp/s the throughput overhead is irrelevant.
 */
export function createLivePostgresStorage(url: string, enabledMask = 0x7FF): Storage {
  const db = createPostgresConnection(url);
  const enabledTables = TABLES.filter(t => enabledMask & (TABLE_MASK_BIT[t.name] ?? 0));

  return {
    name: "live-postgres",

    async initialize(): Promise<void> {
      for (const tableDef of enabledTables) {
        for (const stmt of tableDef.ddl.split(";").map(s => s.trim()).filter(Boolean)) {
          await db.unsafe(stmt, []);
        }
      }
    },

    async write(batch: ProcessedCheckpoint[]): Promise<void> {
      const tableRows = collectTableRows(enabledTables, batch);
      await db.begin(async (tx) => {
        for (let i = 0; i < enabledTables.length; i++) {
          const tableDef = enabledTables[i]!;
          const pgTypes = PG_COLUMN_TYPES[tableDef.name];
          if (!pgTypes) throw new Error(`No PG_COLUMN_TYPES for table ${tableDef.name}`);
          await insertUnnest(
            tx as ReturnType<typeof createPostgresConnection>,
            tableDef.name,
            tableDef.columns,
            pgTypes,
            tableRows[i]!,
          );
        }
      });
    },

    async shutdown(): Promise<void> {
      await db.end?.();
    },
  };
}
