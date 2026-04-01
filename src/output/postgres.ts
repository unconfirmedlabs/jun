/**
 * Postgres output: batch inserts via Bun.sql with auto-table creation.
 *
 * Each event handler gets its own table, auto-created from the schema.
 * Inserts use ON CONFLICT (tx_digest, event_seq) DO NOTHING for idempotency.
 */
import type { DecodedEvent } from "../processor.ts";
import type { FieldDefs } from "../schema.ts";
import { generateDDL } from "../schema.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface PostgresOutput {
  /** Ensure all event tables exist. Called once on startup. */
  migrate(): Promise<void>;

  /** Write a batch of decoded events to Postgres. */
  write(events: DecodedEvent[]): Promise<void>;

  /** Write events for a single handler. Used by WriteBuffer for parallel per-table inserts. */
  writeHandler(handlerName: string, events: DecodedEvent[]): Promise<void>;
}

interface TableConfig {
  name: string;
  fields: FieldDefs;
  columns: string[];
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

/**
 * Create a Postgres output writer.
 *
 * Uses Bun.sql tagged template literals for batch inserts:
 *   sql`INSERT INTO table ${sql(rows)} ON CONFLICT (tx_digest, event_seq) DO NOTHING`
 *
 * @param sql - Bun.sql instance (the tagged template function)
 * @param handlers - Map of handler name to table config
 */
export function createPostgresOutput(
  sql: any,
  handlers: Record<string, { tableName: string; fields: FieldDefs }>,
): PostgresOutput {
  // Pre-compute table configs
  const tables = new Map<string, TableConfig>();
  for (const [handlerName, config] of Object.entries(handlers)) {
    const fieldNames = Object.keys(config.fields);
    const columns = ["tx_digest", "event_seq", "sender", "sui_timestamp", ...fieldNames];
    tables.set(handlerName, {
      name: config.tableName,
      fields: config.fields,
      columns,
    });
  }

  return {
    async migrate(): Promise<void> {
      for (const [, config] of Object.entries(handlers)) {
        const ddl = generateDDL(config.tableName, config.fields);
        await sql.unsafe(ddl);
      }
    },

    async writeHandler(handlerName: string, events: DecodedEvent[]): Promise<void> {
      if (events.length === 0) return;

      const table = tables.get(handlerName);
      if (!table) return;

      const rows = events.map((ev) => ({
        tx_digest: ev.txDigest,
        event_seq: ev.eventSeq,
        sender: ev.sender,
        sui_timestamp: ev.timestamp,
        ...ev.data,
      }));

      await insertBatch(sql, table, rows);
    },

    async write(events: DecodedEvent[]): Promise<void> {
      if (events.length === 0) return;

      // Group events by handler
      const grouped = new Map<string, DecodedEvent[]>();
      for (const event of events) {
        const list = grouped.get(event.handlerName);
        if (list) {
          list.push(event);
        } else {
          grouped.set(event.handlerName, [event]);
        }
      }

      // Batch insert per handler (sequential for backward compatibility)
      for (const [handlerName, handlerEvents] of grouped) {
        await this.writeHandler(handlerName, handlerEvents);
      }
    },
  };
}

/**
 * Insert a batch of rows into a table with ON CONFLICT DO NOTHING.
 *
 * Uses Bun.sql's unsafe() with manually-built parameterized queries
 * to support the ON CONFLICT clause alongside batch values.
 */
async function insertBatch(
  sql: any,
  table: TableConfig,
  rows: Record<string, unknown>[],
): Promise<void> {
  if (rows.length === 0) return;

  const { name, columns } = table;
  const colList = columns.join(", ");

  // Build parameterized VALUES clause
  // Each row produces ($1, $2, ..., $N), ($N+1, ..., $2N), etc.
  const values: unknown[] = [];
  const rowClauses: string[] = [];

  for (const row of rows) {
    const placeholders: string[] = [];
    for (const col of columns) {
      values.push(row[col]);
      placeholders.push(`$${values.length}`);
    }
    rowClauses.push(`(${placeholders.join(", ")})`);
  }

  const query = `
    INSERT INTO ${name} (${colList})
    VALUES ${rowClauses.join(", ")}
    ON CONFLICT (tx_digest, event_seq) DO NOTHING
  `;

  await sql.unsafe(query, values);
}
