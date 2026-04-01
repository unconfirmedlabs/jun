/**
 * SQLite storage backend for decoded events.
 *
 * Uses bun:sqlite with WAL mode. Auto-creates event tables from FieldDefs.
 * Same schema as Postgres (tx_digest, event_seq, sender, sui_timestamp, + custom fields).
 * Idempotent via ON CONFLICT (tx_digest, event_seq) DO NOTHING.
 */
import { Database } from "bun:sqlite";
import type { DecodedEvent } from "../processor.ts";
import type { FieldDefs, FieldType } from "../schema.ts";
import type { StorageBackend, HandlerTableConfig } from "./storage.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function fieldTypeToSqlite(type: FieldType): string {
  if (type.startsWith("option<")) return fieldTypeToSqlite(type.slice(7, -1) as FieldType);
  if (type.startsWith("vector<")) return "TEXT"; // JSON string
  switch (type) {
    case "address": case "string": return "TEXT";
    case "bool": return "INTEGER"; // SQLite has no BOOLEAN
    case "u8": case "u16": case "u32": return "INTEGER";
    case "u64": case "u128": case "u256": return "TEXT"; // Store large numbers as text
    default: return "TEXT";
  }
}

function generateSqliteDDL(tableName: string, fields: FieldDefs): string {
  const cols = [
    "id INTEGER PRIMARY KEY AUTOINCREMENT",
    "tx_digest TEXT NOT NULL",
    "event_seq INTEGER NOT NULL",
    "sender TEXT NOT NULL",
    "sui_timestamp TEXT NOT NULL",
    "indexed_at TEXT NOT NULL DEFAULT (datetime('now'))",
  ];

  for (const [name, type] of Object.entries(fields)) {
    const nullable = type.startsWith("option<") ? "" : " NOT NULL";
    cols.push(`${name} ${fieldTypeToSqlite(type)}${nullable}`);
  }

  cols.push("UNIQUE (tx_digest, event_seq)");

  return `CREATE TABLE IF NOT EXISTS ${tableName} (\n  ${cols.join(",\n  ")}\n);`;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

export function createSqliteStorageBackend(
  dbPath: string,
  handlers: Record<string, HandlerTableConfig>,
): StorageBackend {
  const db = new Database(dbPath);
  db.exec("PRAGMA journal_mode = WAL;");

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
