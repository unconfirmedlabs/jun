/**
 * Postgres destination — batch writes events and balance changes to Postgres.
 *
 * Creates event tables (one per handler) and balance tables (balance_changes + balances).
 * Uses parameterized queries with ON CONFLICT DO NOTHING for idempotency.
 */
import { SQL } from "bun";
import type { Destination, ProcessedCheckpoint, DecodedEvent, BalanceChange } from "../types.ts";
import type { FieldDefs } from "../../schema.ts";
import { generateDDL } from "../../schema.ts";
import { validateIdentifier } from "../../output/storage.ts";
import type { Logger } from "../../logger.ts";
import { createLogger } from "../../logger.ts";

export interface PostgresDestinationConfig {
  /** Postgres connection string */
  url: string;
  /** Event handler table definitions (handlerName → tableName + fields) */
  handlers?: Record<string, { tableName: string; fields: FieldDefs }>;
  /** Enable balance change tables */
  balances?: boolean;
}

export function createPostgresDestination(config: PostgresDestinationConfig): Destination {
  const log: Logger = createLogger().child({ component: "destination:postgres" });
  let sql: any = null;

  // Pre-computed table configs
  const tables = new Map<string, { name: string; columns: string[] }>();

  return {
    name: "postgres",

    async initialize(): Promise<void> {
      sql = new SQL(config.url);

      // Create event tables
      if (config.handlers) {
        for (const [handlerName, handler] of Object.entries(config.handlers)) {
          validateIdentifier(handler.tableName);
          const ddl = generateDDL(handler.tableName, handler.fields);
          await sql.unsafe(ddl);

          const fieldNames = Object.keys(handler.fields);
          const columns = ["tx_digest", "event_seq", "sender", "sui_timestamp", ...fieldNames];
          columns.forEach(validateIdentifier);
          tables.set(handlerName, { name: handler.tableName, columns });

          log.info({ handler: handlerName, table: handler.tableName }, "event table created");
        }
      }

      // Create balance tables
      if (config.balances) {
        await sql.unsafe(`
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

        await sql.unsafe(`
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

        log.info("balance tables created");
      }

      // Create cursor tracking table
      await sql.unsafe(`
        CREATE TABLE IF NOT EXISTS indexer_checkpoints (
          key TEXT PRIMARY KEY,
          checkpoint_seq NUMERIC NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `);

      log.info("postgres destination initialized");
    },

    async write(batch: ProcessedCheckpoint[]): Promise<void> {
      // Collect all events and balance changes across the batch
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

      // Write events (parallel per handler table)
      const eventWrites = Array.from(groupedEvents.entries()).map(async ([handlerName, events]) => {
        const table = tables.get(handlerName);
        if (!table) return;

        const values: unknown[] = [];
        const rowClauses: string[] = [];
        for (const event of events) {
          const placeholders: string[] = [];
          for (const column of table.columns) {
            if (column === "tx_digest") values.push(event.txDigest);
            else if (column === "event_seq") values.push(event.eventSeq);
            else if (column === "sender") values.push(event.sender);
            else if (column === "sui_timestamp") values.push(event.timestamp);
            else values.push(event.data[column]);
            placeholders.push(`$${values.length}`);
          }
          rowClauses.push(`(${placeholders.join(", ")})`);
        }

        if (rowClauses.length > 0) {
          await sql.unsafe(
            `INSERT INTO ${table.name} (${table.columns.join(", ")}) VALUES ${rowClauses.join(", ")} ON CONFLICT (tx_digest, event_seq) DO NOTHING`,
            values,
          );
        }
      });

      // Write balance changes
      const balanceWrite = allBalanceChanges.length > 0 && config.balances
        ? writeBalanceChanges(sql, allBalanceChanges)
        : Promise.resolve();

      await Promise.all([...eventWrites, balanceWrite]);
    },

    async shutdown(): Promise<void> {
      if (sql) {
        await sql.close();
        sql = null;
      }
      log.info("postgres destination shut down");
    },
  };
}

// ---------------------------------------------------------------------------
// Balance change writer
// ---------------------------------------------------------------------------

async function writeBalanceChanges(sql: any, changes: BalanceChange[]): Promise<void> {
  // 1. Insert into balance_changes ledger
  const ledgerValues: unknown[] = [];
  const ledgerRows: string[] = [];
  for (const change of changes) {
    ledgerRows.push(`($${ledgerValues.length + 1}, $${ledgerValues.length + 2}, $${ledgerValues.length + 3}, $${ledgerValues.length + 4}, $${ledgerValues.length + 5}, $${ledgerValues.length + 6})`);
    ledgerValues.push(change.txDigest, change.checkpointSeq.toString(), change.address, change.coinType, change.amount, change.timestamp);
  }
  await sql.unsafe(
    `INSERT INTO balance_changes (tx_digest, checkpoint_seq, address, coin_type, amount, sui_timestamp) VALUES ${ledgerRows.join(", ")} ON CONFLICT (tx_digest, address, coin_type) DO NOTHING`,
    ledgerValues,
  );

  // 2. Aggregate per (address, coinType) and upsert into balances
  const aggregated = new Map<string, { address: string; coinType: string; totalAmount: bigint; maxCheckpoint: bigint }>();
  for (const change of changes) {
    const key = `${change.address}:${change.coinType}`;
    const existing = aggregated.get(key);
    if (existing) {
      existing.totalAmount += BigInt(change.amount);
      if (change.checkpointSeq > existing.maxCheckpoint) existing.maxCheckpoint = change.checkpointSeq;
    } else {
      aggregated.set(key, { address: change.address, coinType: change.coinType, totalAmount: BigInt(change.amount), maxCheckpoint: change.checkpointSeq });
    }
  }

  const balanceValues: unknown[] = [];
  const balanceRows: string[] = [];
  for (const aggregation of aggregated.values()) {
    balanceRows.push(`($${balanceValues.length + 1}, $${balanceValues.length + 2}, $${balanceValues.length + 3}, $${balanceValues.length + 4})`);
    balanceValues.push(aggregation.address, aggregation.coinType, aggregation.totalAmount.toString(), aggregation.maxCheckpoint.toString());
  }
  if (balanceRows.length > 0) {
    await sql.unsafe(
      `INSERT INTO balances (address, coin_type, balance, last_checkpoint) VALUES ${balanceRows.join(", ")} ON CONFLICT (address, coin_type) DO UPDATE SET balance = balances.balance + EXCLUDED.balance, last_checkpoint = GREATEST(balances.last_checkpoint, EXCLUDED.last_checkpoint), last_updated = NOW()`,
      balanceValues,
    );
  }
}
