/**
 * SQLite storage backend for decoded events.
 *
 * Uses bun:sqlite with WAL mode. Auto-creates event tables from FieldDefs.
 * Same schema as Postgres (tx_digest, event_seq, sender, sui_timestamp, + custom fields).
 * Idempotent via ON CONFLICT (tx_digest, event_seq) DO NOTHING.
 */
import type { Database } from "bun:sqlite";
import type { DecodedEvent } from "../pipeline/types.ts";
import type { FieldDefs } from "../schema.ts";
import type { StorageBackend, HandlerTableConfig } from "./storage.ts";
import { createSqliteConnection } from "../db.ts";
import { generateSqliteDDL } from "../sql-helpers.ts";

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

export function createSqliteStorageBackend(
  dbPath: string,
  handlers: Record<string, HandlerTableConfig>,
): StorageBackend {
  const db = createSqliteConnection(dbPath);

  // Pre-compute table configs + prepared statements
  const tables = new Map<string, {
    tableName: string;
    columns: string[];
    insert: ReturnType<Database["prepare"]>;
  }>();

  return {
    name: "sqlite",

    async migrate(): Promise<void> {
      for (const [handlerName, config] of Object.entries(handlers)) {
        const ddl = generateSqliteDDL(config.tableName, config.fields);
        db.exec(ddl);

        const fieldNames = Object.keys(config.fields);
        const columns = ["tx_digest", "event_seq", "sender", "sui_timestamp", ...fieldNames];
        const placeholders = columns.map(() => "?").join(", ");

        const insert = db.prepare(
          `INSERT OR IGNORE INTO ${config.tableName} (${columns.join(", ")}) VALUES (${placeholders})`,
        );

        tables.set(handlerName, { tableName: config.tableName, columns, insert });
      }
    },

    async writeHandler(handlerName: string, events: DecodedEvent[]): Promise<void> {
      if (events.length === 0) return;

      const table = tables.get(handlerName);
      if (!table) return;

      const txn = db.transaction(() => {
        for (const ev of events) {
          const row: unknown[] = [
            ev.txDigest,
            ev.eventSeq,
            ev.sender,
            ev.timestamp.toISOString(),
          ];
          for (const col of table.columns.slice(4)) { // skip standard columns
            row.push(ev.data[col] ?? null);
          }
          table.insert.run(...row);
        }
      });

      txn();
    },

    async write(events: DecodedEvent[]): Promise<void> {
      if (events.length === 0) return;

      const grouped = new Map<string, DecodedEvent[]>();
      for (const event of events) {
        const list = grouped.get(event.handlerName);
        if (list) list.push(event);
        else grouped.set(event.handlerName, [event]);
      }

      for (const [handlerName, handlerEvents] of grouped) {
        await this.writeHandler(handlerName, handlerEvents);
      }
    },

    async shutdown(): Promise<void> {
      db.close();
    },
  };
}
