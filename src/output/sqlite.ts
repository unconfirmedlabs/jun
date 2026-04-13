/**
 * SQLite output for jun stream/replay — stores checkpoint data in a portable .sqlite file.
 *
 * Three tables: transactions, events, balance_changes.
 * No effects table — status and gas are on the transactions table.
 * Uses bun:sqlite with WAL mode and prepared statements for write performance.
 */
import type { CheckpointEvent } from "../grpc.ts";
import { createSqliteConnection } from "../db.ts";

export interface SqliteWriterOptions {
  path: string;
  showEvents: boolean;
  showBalanceChanges: boolean;
}

export interface SqliteWriter {
  writeCheckpoint(data: {
    seq: string;
    timestamp: string;
    transactions: Array<{
      digest: string;
      sender?: string;
      status?: string;
      gasComputation?: number;
      gasStorage?: number;
      gasRebate?: number;
      events: CheckpointEvent[];
      balanceChanges: any[];
    }>;
  }): void;
  close(): void;
}

export function createSqliteWriter(opts: SqliteWriterOptions): SqliteWriter {
  const db = createSqliteConnection(opts.path);

  // Transactions table — always created (join key for events + balance changes)
  db.run(`
    CREATE TABLE IF NOT EXISTS transactions (
      digest TEXT PRIMARY KEY,
      checkpoint INTEGER NOT NULL,
      timestamp TEXT NOT NULL,
      sender TEXT,
      status TEXT,
      gas_computation INTEGER,
      gas_storage INTEGER,
      gas_rebate INTEGER
    )
  `);

  if (opts.showEvents) {
    db.run(`
      CREATE TABLE IF NOT EXISTS events (
        tx_digest TEXT NOT NULL,
        event_seq INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        package_id TEXT,
        module TEXT,
        sender TEXT,
        bcs BLOB,
        PRIMARY KEY (tx_digest, event_seq)
      )
    `);
    db.run("CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type)");
    db.run("CREATE INDEX IF NOT EXISTS idx_events_sender ON events(sender)");
  }

  if (opts.showBalanceChanges) {
    db.run(`
      CREATE TABLE IF NOT EXISTS balance_changes (
        tx_digest TEXT NOT NULL,
        checkpoint INTEGER NOT NULL,
        timestamp TEXT NOT NULL,
        owner TEXT NOT NULL,
        coin_type TEXT NOT NULL,
        amount INTEGER NOT NULL
      )
    `);
    db.run("CREATE INDEX IF NOT EXISTS idx_bc_owner ON balance_changes(owner)");
    db.run("CREATE INDEX IF NOT EXISTS idx_bc_coin ON balance_changes(coin_type)");
  }

  // Prepared statements
  const insertTx = db.prepare(
    "INSERT OR IGNORE INTO transactions (digest, checkpoint, timestamp, sender, status, gas_computation, gas_storage, gas_rebate) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
  );
  const insertEvent = opts.showEvents
    ? db.prepare("INSERT OR IGNORE INTO events (tx_digest, event_seq, event_type, package_id, module, sender, bcs) VALUES (?, ?, ?, ?, ?, ?, ?)")
    : null;
  const insertBalanceChange = opts.showBalanceChanges
    ? db.prepare("INSERT OR IGNORE INTO balance_changes (tx_digest, checkpoint, timestamp, owner, coin_type, amount) VALUES (?, ?, ?, ?, ?, ?)")
    : null;

  const writeCheckpoint = db.transaction((data: Parameters<SqliteWriter["writeCheckpoint"]>[0]) => {
    const checkpoint = BigInt(data.seq);

    for (const tx of data.transactions) {
      insertTx.run(
        tx.digest,
        checkpoint,
        data.timestamp,
        tx.sender ?? null,
        tx.status ?? null,
        tx.gasComputation ?? null,
        tx.gasStorage ?? null,
        tx.gasRebate ?? null,
      );

      if (insertEvent) {
        for (let i = 0; i < tx.events.length; i++) {
          const ev = tx.events[i];
          insertEvent.run(tx.digest, i, ev?.eventType ?? null, ev?.packageId ?? null, ev?.module ?? null, ev?.sender ?? null, ev?.contents?.value ?? null);
        }
      }

      if (insertBalanceChange) {
        for (const bc of tx.balanceChanges) {
          insertBalanceChange.run(tx.digest, checkpoint, data.timestamp, bc.address ?? null, bc.coinType ?? null, bc.amount ? parseInt(bc.amount) : 0);
        }
      }
    }
  });

  return {
    writeCheckpoint: (data) => writeCheckpoint(data),
    close: () => db.close(),
  };
}
