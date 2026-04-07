/**
 * Per-table Postgres storage for replay-chain / stream-chain.
 *
 * Same schema and table definitions as per-table-sqlite, backed by Postgres
 * via Bun.SQL. Uses chunked INSERT ... ON CONFLICT DO NOTHING inside a single
 * BEGIN/COMMIT per batch.
 */
import { createPostgresConnection } from "../../db.ts";
import type { ProcessedCheckpoint, Storage } from "../types.ts";
import { TABLES, TABLE_MASK_BIT } from "./per-table-sqlite.ts";

// Max rows per INSERT statement — keeps param count well under Postgres 65535 limit.
// Worst case: transactions table (20 cols) × 3000 rows = 60000 params.
const CHUNK_ROWS = 3000;

async function insertChunked(
  sql: ReturnType<typeof createPostgresConnection>,
  table: string,
  columns: string[],
  rows: unknown[][],
): Promise<void> {
  if (rows.length === 0) return;
  const numCols = columns.length;
  for (let offset = 0; offset < rows.length; offset += CHUNK_ROWS) {
    const chunk = rows.slice(offset, offset + CHUNK_ROWS);
    const placeholders = chunk
      .map((_, rowIdx) =>
        `(${columns.map((_, colIdx) => `$${rowIdx * numCols + colIdx + 1}`).join(", ")})`
      )
      .join(", ");
    const params = chunk.flat();
    await sql.unsafe(
      `INSERT INTO ${table} (${columns.join(", ")}) VALUES ${placeholders} ON CONFLICT DO NOTHING`,
      params as string[],
    );
  }
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
      // Collect rows per table outside the transaction to avoid holding it open
      // during the (cheap) JS map step.
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
          await insertChunked(tx as ReturnType<typeof createPostgresConnection>, tableDef.name, tableDef.columns, rows);
        }
      });
    },

    async shutdown(): Promise<void> {
      await db.end?.();
    },
  };
}
