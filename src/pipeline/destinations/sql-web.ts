/**
 * Browser-compatible SQL storage using sql.js (SQLite WASM).
 * Implements the same Storage interface as sql.ts but for browser environments.
 */
import type { Storage, ProcessedCheckpoint, DecodedEvent, BalanceChange } from "../types.ts";
import type { FieldDefs } from "../../schema.ts";
import { generateDDL } from "../../schema.ts";

export interface SqlWebStorageConfig {
  /** sql.js Database instance (caller must initialize sql.js and create the database) */
  database: any;
  /** Event handler table definitions */
  handlers?: Record<string, { tableName: string; fields: FieldDefs }>;
  /** Enable balance tables */
  balances?: boolean;
}

export function createSqlWebStorage(config: SqlWebStorageConfig): Storage {
  const db = config.database;

  return {
    name: "sql-web",

    async initialize(): Promise<void> {
      // Create event tables
      if (config.handlers) {
        for (const [, handler] of Object.entries(config.handlers)) {
          const ddl = generateDDL(handler.tableName, handler.fields)
            .replace("id SERIAL PRIMARY KEY", "id INTEGER PRIMARY KEY AUTOINCREMENT")
            .replace(/TIMESTAMPTZ/g, "TEXT")
            .replace(/NUMERIC/g, "TEXT")
            .replace(/JSONB/g, "TEXT")
            .replace(/DEFAULT NOW\(\)/g, "");
          db.run(ddl);
        }
      }

      // Create balance tables
      if (config.balances) {
        db.run(`
          CREATE TABLE IF NOT EXISTS balance_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tx_digest TEXT NOT NULL,
            checkpoint INTEGER NOT NULL,
            address TEXT NOT NULL,
            coin_type TEXT NOT NULL,
            amount TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            UNIQUE (tx_digest, address, coin_type)
          );
          CREATE INDEX IF NOT EXISTS idx_bc_address ON balance_changes(address);
          CREATE INDEX IF NOT EXISTS idx_bc_coin_type ON balance_changes(coin_type);
          CREATE INDEX IF NOT EXISTS idx_bc_timestamp ON balance_changes(timestamp);
        `);
        db.run(`
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
      const handlers = config.handlers;

      db.run("BEGIN TRANSACTION");
      try {
        for (const processed of batch) {
          // Write events
          if (handlers) {
            for (const event of processed.events) {
              const handler = handlers[event.handlerName];
              if (!handler) continue;

              const fieldNames = Object.keys(handler.fields);
              const allColumns = ["checkpoint", "tx_digest", "event_seq", "sender", "timestamp", ...fieldNames];
              const placeholders = allColumns.map(() => "?").join(", ");
              const sql = `INSERT OR IGNORE INTO ${handler.tableName} (${allColumns.join(", ")}) VALUES (${placeholders})`;

              const params = [
                Number(event.checkpointSeq),
                event.txDigest,
                event.eventSeq,
                event.sender,
                event.timestamp.toISOString(),
                ...fieldNames.map((name) => {
                  const v = event.data[name];
                  if (v === null || v === undefined) return null;
                  if (typeof v === "boolean") return v ? 1 : 0;
                  return v;
                }),
              ];
              db.run(sql, params);
            }
          }

          // Write balance changes
          if (config.balances) {
            for (const bc of processed.balanceChanges) {
              db.run(
                `INSERT OR IGNORE INTO balance_changes (tx_digest, checkpoint, address, coin_type, amount, timestamp) VALUES (?, ?, ?, ?, ?, ?)`,
                [bc.txDigest, Number(bc.checkpointSeq), bc.address, bc.coinType, bc.amount, bc.timestamp.toISOString()]
              );
              db.run(
                `INSERT INTO balances (address, coin_type, balance, last_checkpoint) VALUES (?, ?, ?, ?)
                 ON CONFLICT (address, coin_type) DO UPDATE SET
                   balance = CAST(CAST(balances.balance AS INTEGER) + CAST(excluded.balance AS INTEGER) AS TEXT),
                   last_checkpoint = MAX(balances.last_checkpoint, excluded.last_checkpoint)`,
                [bc.address, bc.coinType, bc.amount, String(Number(bc.checkpointSeq))]
              );
            }
          }
        }
        db.run("COMMIT");
      } catch (e) {
        db.run("ROLLBACK");
        throw e;
      }
    },

    async shutdown(): Promise<void> {
      // Don't close the database — caller manages lifecycle
    },
  };
}
