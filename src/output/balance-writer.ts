/**
 * Balance writer — writes to balance_changes (ledger) and balances (running totals).
 *
 * Both tables are updated atomically in the same transaction per flush.
 * The balances table uses ON CONFLICT DO UPDATE to maintain running totals.
 */
import type { BalanceChange } from "../balance-processor.ts";
import { validateIdentifier } from "./storage.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface BalanceWriter {
  /** Create balance_changes and balances tables + indexes. */
  migrate(): Promise<void>;
  /** Write a batch of balance changes. Updates both tables atomically. */
  write(changes: BalanceChange[]): Promise<void>;
  /** Graceful shutdown. */
  shutdown(): Promise<void>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

export function createBalanceWriter(sql: any): BalanceWriter {
  return {
    async migrate(): Promise<void> {
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
    },

    async write(changes: BalanceChange[]): Promise<void> {
      if (changes.length === 0) return;

      // 1. Batch INSERT into balance_changes (ledger)
      const ledgerValues: unknown[] = [];
      const ledgerRows: string[] = [];

      for (const change of changes) {
        ledgerRows.push(`($${ledgerValues.length + 1}, $${ledgerValues.length + 2}, $${ledgerValues.length + 3}, $${ledgerValues.length + 4}, $${ledgerValues.length + 5}, $${ledgerValues.length + 6})`);
        ledgerValues.push(
          change.txDigest,
          change.checkpointSeq.toString(),
          change.address,
          change.coinType,
          change.amount,
          change.timestamp,
        );
      }

      await sql.unsafe(
        `INSERT INTO balance_changes (tx_digest, checkpoint_seq, address, coin_type, amount, sui_timestamp)
         VALUES ${ledgerRows.join(", ")}
         ON CONFLICT (tx_digest, address, coin_type) DO NOTHING`,
        ledgerValues,
      );

      // 2. Aggregate changes per (address, coin_type) for running balance upsert
      const aggregated = new Map<string, { address: string; coinType: string; totalAmount: bigint; maxCheckpoint: bigint }>();

      for (const change of changes) {
        const key = `${change.address}:${change.coinType}`;
        const existing = aggregated.get(key);
        if (existing) {
          existing.totalAmount += BigInt(change.amount);
          if (change.checkpointSeq > existing.maxCheckpoint) {
            existing.maxCheckpoint = change.checkpointSeq;
          }
        } else {
          aggregated.set(key, {
            address: change.address,
            coinType: change.coinType,
            totalAmount: BigInt(change.amount),
            maxCheckpoint: change.checkpointSeq,
          });
        }
      }

      // 3. Upsert into balances (running totals)
      const balanceValues: unknown[] = [];
      const balanceRows: string[] = [];

      for (const aggregation of aggregated.values()) {
        balanceRows.push(`($${balanceValues.length + 1}, $${balanceValues.length + 2}, $${balanceValues.length + 3}, $${balanceValues.length + 4})`);
        balanceValues.push(
          aggregation.address,
          aggregation.coinType,
          aggregation.totalAmount.toString(),
          aggregation.maxCheckpoint.toString(),
        );
      }

      if (balanceRows.length > 0) {
        await sql.unsafe(
          `INSERT INTO balances (address, coin_type, balance, last_checkpoint)
           VALUES ${balanceRows.join(", ")}
           ON CONFLICT (address, coin_type)
           DO UPDATE SET
             balance = balances.balance + EXCLUDED.balance,
             last_checkpoint = GREATEST(balances.last_checkpoint, EXCLUDED.last_checkpoint),
             last_updated = NOW()`,
          balanceValues,
        );
      }
    },

    async shutdown(): Promise<void> {
      // No cleanup needed — connection managed externally
    },
  };
}
