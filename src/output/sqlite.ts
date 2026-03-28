/**
 * SQLite output for jun stream — stores raw checkpoint data in a portable .sqlite file.
 *
 * Tables are created based on what data is included (events, effects, balance_changes).
 * Uses bun:sqlite for zero-dependency embedded storage with WAL mode for write performance.
 */
import { Database } from "bun:sqlite";
import type { GrpcEvent } from "../grpc.ts";

export interface SqliteWriterOptions {
  path: string;
  showEvents: boolean;
  showEffects: boolean;
  showBalanceChanges: boolean;
}

export interface SqliteWriter {
  writeCheckpoint(data: {
    seq: string;
    timestamp: string;
    transactions: Array<{
      digest: string;
      sender?: string;
      events: GrpcEvent[];
      effects: any;
      balanceChanges: any[];
    }>;
  }): void;
  close(): void;
}

export function createSqliteWriter(opts: SqliteWriterOptions): SqliteWriter {
  const db = new Database(opts.path);
  db.run("PRAGMA journal_mode = WAL");
  db.run("PRAGMA synchronous = NORMAL");

  // Always create checkpoints + transactions tables
  db.run(`
    CREATE TABLE IF NOT EXISTS checkpoints (
      sequence_number INTEGER PRIMARY KEY,
      timestamp TEXT NOT NULL
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS transactions (
      digest TEXT PRIMARY KEY,
      checkpoint INTEGER NOT NULL REFERENCES checkpoints(sequence_number),
      sender TEXT
    )
  `);

  if (opts.showEvents) {
    db.run(`
      CREATE TABLE IF NOT EXISTS events (
        tx_digest TEXT NOT NULL REFERENCES transactions(digest),
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
  }

  if (opts.showEffects) {
    db.run(`
      CREATE TABLE IF NOT EXISTS effects (
        tx_digest TEXT PRIMARY KEY REFERENCES transactions(digest),
        status TEXT NOT NULL,
        gas_computation INTEGER,
        gas_storage INTEGER,
        gas_rebate INTEGER
      )
    `);
  }

  if (opts.showBalanceChanges) {
    db.run(`
      CREATE TABLE IF NOT EXISTS balance_changes (
        tx_digest TEXT NOT NULL REFERENCES transactions(digest),
        owner TEXT NOT NULL,
        coin_type TEXT NOT NULL,
        amount INTEGER NOT NULL
      )
    `);
    db.run("CREATE INDEX IF NOT EXISTS idx_balance_changes_owner ON balance_changes(owner)");
    db.run("CREATE INDEX IF NOT EXISTS idx_balance_changes_coin ON balance_changes(coin_type)");
  }

  // Prepare statements for performance
  const insertCheckpoint = db.prepare("INSERT OR IGNORE INTO checkpoints (sequence_number, timestamp) VALUES (?, ?)");
  const insertTx = db.prepare("INSERT OR IGNORE INTO transactions (digest, checkpoint, sender) VALUES (?, ?, ?)");
  const insertEvent = opts.showEvents
    ? db.prepare("INSERT OR IGNORE INTO events (tx_digest, event_seq, event_type, package_id, module, sender, bcs) VALUES (?, ?, ?, ?, ?, ?, ?)")
    : null;
  const insertEffect = opts.showEffects
    ? db.prepare("INSERT OR IGNORE INTO effects (tx_digest, status, gas_computation, gas_storage, gas_rebate) VALUES (?, ?, ?, ?, ?)")
    : null;
  const insertBalanceChange = opts.showBalanceChanges
    ? db.prepare("INSERT OR IGNORE INTO balance_changes (tx_digest, owner, coin_type, amount) VALUES (?, ?, ?, ?)")
    : null;

  const writeCheckpoint = db.transaction((data: Parameters<SqliteWriter["writeCheckpoint"]>[0]) => {
    insertCheckpoint.run(BigInt(data.seq), data.timestamp);

    for (const tx of data.transactions) {
      insertTx.run(tx.digest, BigInt(data.seq), tx.sender ?? null);

      if (insertEvent) {
        for (let i = 0; i < tx.events.length; i++) {
          const ev = tx.events[i];
          insertEvent.run(
            tx.digest,
            i,
            ev.eventType,
            ev.packageId ?? null,
            ev.module ?? null,
            ev.sender ?? null,
            ev.contents?.value ?? null,
          );
        }
      }

      if (insertEffect && tx.effects) {
        const status = tx.effects.status?.success ? "success" : "failure";
        insertEffect.run(
          tx.digest,
          status,
          tx.effects.gasUsed?.computationCost ? parseInt(tx.effects.gasUsed.computationCost) : null,
          tx.effects.gasUsed?.storageCost ? parseInt(tx.effects.gasUsed.storageCost) : null,
          tx.effects.gasUsed?.storageRebate ? parseInt(tx.effects.gasUsed.storageRebate) : null,
        );
      }

      if (insertBalanceChange) {
        for (const bc of tx.balanceChanges) {
          insertBalanceChange.run(
            tx.digest,
            bc.address ?? null,
            bc.coinType ?? null,
            bc.amount ? parseInt(bc.amount) : 0,
          );
        }
      }
    }
  });

  return {
    writeCheckpoint: (data) => writeCheckpoint(data),
    close: () => db.close(),
  };
}
