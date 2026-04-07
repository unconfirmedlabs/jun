/**
 * Per-table Postgres storage for replay-chain / stream-chain.
 *
 * Uses UNNEST with typed column arrays for bulk inserts — one round trip per
 * table per batch regardless of row count. Far faster than row-by-row VALUES
 * placeholders, and supports ON CONFLICT DO NOTHING for idempotency.
 *
 * Per Postgres docs (https://www.postgresql.org/docs/current/populate.html):
 * COPY FROM is faster for pure bulk loads but has no ON CONFLICT support.
 * UNNEST is the recommended alternative when upsert semantics are needed.
 *
 * Connection settings (via createPostgresConnection):
 *   synchronous_commit = off  — skips WAL flush per commit; safe since the
 *   blockchain is the source of truth and data can be re-indexed.
 */
import { createPostgresConnection } from "../../db.ts";
import type { ProcessedCheckpoint, Storage } from "../types.ts";
import { TABLES, TABLE_MASK_BIT } from "./per-table-sqlite.ts";

// Postgres types for each column, matched to the column order in TABLES.
// int4 = 32-bit integer (stored as 0/1 for booleans). All bigints stored as text.
type PgType = "text" | "int4";

const PG_COLUMN_TYPES: Record<string, PgType[]> = {
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
    // tx_digest, call_index, package, module, function, checkpoint_seq, sui_timestamp
  ],
  balance_changes: [
    "text", "text", "text", "text", "text", "text",
    // tx_digest, checkpoint_seq, address, coin_type, amount, sui_timestamp
  ],
  object_changes: [
    "text", "text", "text", "text", // tx_digest, object_id, change_type, object_type
    "text", "text", "text", "text", // input_version, input_digest, input_owner, input_owner_kind
    "text", "text", "text", "text", // output_version, output_digest, output_owner, output_owner_kind
    "int4", "text", "text",         // is_gas_object, checkpoint_seq, sui_timestamp
  ],
  transaction_dependencies: [
    "text", "text", "text", "text",
    // tx_digest, depends_on_digest, checkpoint_seq, sui_timestamp
  ],
  transaction_inputs: [
    "text", "int4", "text",                         // tx_digest, input_index, kind
    "text", "text", "text", "text", "text",         // object_id, version, digest, mutability, initial_shared_version
    "text", "text", "text", "text",                 // pure_bytes, amount, coin_type, source
    "text", "text",                                  // checkpoint_seq, sui_timestamp
  ],
  commands: [
    "text", "int4", "text",         // tx_digest, command_index, kind
    "text", "text", "text",         // package, module, function
    "text", "text",                  // type_arguments, args
    "text", "text",                  // checkpoint_seq, sui_timestamp
  ],
  system_transactions: [
    "text", "text", "text", "text", "text",
    // tx_digest, kind, data, checkpoint_seq, sui_timestamp
  ],
  unchanged_consensus_objects: [
    "text", "text", "text",         // tx_digest, object_id, kind
    "text", "text", "text",         // version, digest, object_type
    "text", "text",                  // checkpoint_seq, sui_timestamp
  ],
  checkpoints: [
    "text", "text", "text", "text", "text", // sequence_number, epoch, digest, previous_digest, content_digest
    "text", "text",                           // sui_timestamp, total_network_transactions
    "text", "text", "text", "text",          // rolling_computation_cost, rolling_storage_cost, rolling_storage_rebate, rolling_non_refundable_storage_fee
  ],
  raw_events: [
    "text", "int4", "text", "text", "text", "text", "text", "text", "text",
    // tx_digest, event_seq, package_id, module, event_type, sender, contents, checkpoint_seq, sui_timestamp
  ],
};

/**
 * Serialize a column's values as a Postgres array literal string.
 * e.g. ["a", null, "b\"c"] → '{"a",NULL,"b\\"c"}'
 */
function pgArrayLiteral(values: unknown[], pgType: PgType): string {
  const items = values.map(v => {
    if (v === null || v === undefined) return "NULL";
    if (pgType === "int4") return String(v);
    const s = String(v);
    return `"${s.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
  });
  return `{${items.join(",")}}`;
}

async function insertUnnest(
  sql: ReturnType<typeof createPostgresConnection>,
  table: string,
  columns: string[],
  pgTypes: PgType[],
  rows: unknown[][],
): Promise<void> {
  if (rows.length === 0) return;

  // Build one array literal per column, then UNNEST all together.
  // Single round-trip regardless of row count — O(cols) params, not O(rows*cols).
  const colArrays = columns.map((_, colIdx) =>
    pgArrayLiteral(rows.map(row => row[colIdx]), pgTypes[colIdx]!)
  );
  const unnestExprs = columns.map((_, i) => `$${i + 1}::${pgTypes[i]}[]`).join(", ");
  await sql.unsafe(
    `INSERT INTO ${table} (${columns.join(", ")})
     SELECT * FROM UNNEST(${unnestExprs})
     ON CONFLICT DO NOTHING`,
    colArrays,
  );
}

export function createPerTablePostgresStorage(url: string, enabledMask = 0x7FF): Storage {
  const db = createPostgresConnection(url);
  const enabledTables = TABLES.filter(t => enabledMask & (TABLE_MASK_BIT[t.name] ?? 0));

  return {
    name: "per-table-postgres",

    async initialize(): Promise<void> {
      for (const tableDef of enabledTables) {
        for (const stmt of tableDef.ddl.split(";").map(s => s.trim()).filter(Boolean)) {
          await db.unsafe(stmt, []);
        }
      }
    },

    async write(batch: ProcessedCheckpoint[]): Promise<void> {
      // Collect rows per table before the transaction (pure JS, no I/O).
      const tableRows = enabledTables.map(tableDef => {
        const rows: unknown[][] = [];
        for (const cp of batch) {
          for (const record of tableDef.getRecords(cp)) {
            rows.push(tableDef.mapRow(record));
          }
        }
        return rows;
      });

      await db.begin(async (tx) => {
        for (let i = 0; i < enabledTables.length; i++) {
          const tableDef = enabledTables[i]!;
          const rows = tableRows[i]!;
          const pgTypes = PG_COLUMN_TYPES[tableDef.name];
          if (!pgTypes) throw new Error(`No PG_COLUMN_TYPES for table ${tableDef.name}`);
          await insertUnnest(
            tx as ReturnType<typeof createPostgresConnection>,
            tableDef.name,
            tableDef.columns,
            pgTypes,
            rows,
          );
        }
      });
    },

    async shutdown(): Promise<void> {
      await db.end?.();
    },
  };
}
