/**
 * SQLite destination — writes events and balance changes to a local SQLite file.
 *
 * Uses WAL mode for performance. Auto-creates event tables from field definitions.
 * Idempotent via INSERT OR IGNORE.
 */
import type { Database } from "bun:sqlite";
import type { Storage, ProcessedCheckpoint, DecodedEvent } from "../types.ts";
import type { FieldDefs } from "../../schema.ts";
import { createSqliteConnection } from "../../db.ts";
import { fieldTypeToSqlite } from "../../sql-helpers.ts";

export interface SqliteDestinationConfig {
  /** Path to SQLite database file */
  path: string;
  /** Event handler table definitions */
  handlers?: Record<string, { tableName: string; fields: FieldDefs }>;
  /** Enable balance change tables */
  balances?: boolean;
}

export function createSqliteStorage(config: SqliteDestinationConfig): Storage {
  let database: Database | null = null;
  const insertStatements = new Map<string, ReturnType<Database["prepare"]>>();
  const tableColumns = new Map<string, string[]>();

  return {
    name: "sqlite",

    async initialize(): Promise<void> {
      database = createSqliteConnection(config.path);

      // Create event tables
      if (config.handlers) {
        for (const [handlerName, handler] of Object.entries(config.handlers)) {
          const columns = [
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "tx_digest TEXT NOT NULL",
            "event_seq INTEGER NOT NULL",
            "sender TEXT NOT NULL",
            "sui_timestamp TEXT NOT NULL",
          ];
          const fieldNames: string[] = [];
          for (const [name, type] of Object.entries(handler.fields)) {
            const nullable = type.startsWith("option<") ? "" : " NOT NULL";
            columns.push(`${name} ${fieldTypeToSqlite(type)}${nullable}`);
            fieldNames.push(name);
          }
          columns.push("UNIQUE (tx_digest, event_seq)");

          database.exec(`CREATE TABLE IF NOT EXISTS ${handler.tableName} (\n  ${columns.join(",\n  ")}\n);`);

          const allColumns = ["tx_digest", "event_seq", "sender", "sui_timestamp", ...fieldNames];
          const placeholders = allColumns.map(() => "?").join(", ");
          const statement = database.prepare(
            `INSERT OR IGNORE INTO ${handler.tableName} (${allColumns.join(", ")}) VALUES (${placeholders})`,
          );
          insertStatements.set(handlerName, statement);
          tableColumns.set(handlerName, allColumns);
        }
      }

      // Create balance tables
      if (config.balances) {
        database.exec(`
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

        database.exec(`
          CREATE TABLE IF NOT EXISTS balances (
            address TEXT NOT NULL,
            coin_type TEXT NOT NULL,
            balance TEXT NOT NULL DEFAULT '0',
            last_checkpoint TEXT NOT NULL DEFAULT '0',
            PRIMARY KEY (address, coin_type)
          );
        `);
      }
    },

    async write(batch: ProcessedCheckpoint[]): Promise<void> {
      if (!database) return;

      const transaction = database.transaction(() => {
        for (const processed of batch) {
          // Write events
          for (const event of processed.events) {
            const statement = insertStatements.get(event.handlerName);
            const columns = tableColumns.get(event.handlerName);
            if (!statement || !columns) continue;

            const row: unknown[] = [event.txDigest, event.eventSeq, event.sender, event.timestamp.toISOString()];
            for (const column of columns.slice(4)) {
              row.push(event.data[column] ?? null);
            }
            statement.run(...row);
          }

          // Write balance changes
          if (config.balances) {
            for (const change of processed.balanceChanges) {
              database!.run(
                `INSERT OR IGNORE INTO balance_changes (tx_digest, checkpoint_seq, address, coin_type, amount, sui_timestamp) VALUES (?, ?, ?, ?, ?, ?)`,
                change.txDigest, change.checkpointSeq.toString(), change.address, change.coinType, change.amount, change.timestamp.toISOString(),
              );
            }
          }
        }
      });

      transaction();
    },

    async shutdown(): Promise<void> {
      if (database) {
        database.close();
        database = null;
      }
    },
  };
}
