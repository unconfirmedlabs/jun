/**
 * jun/balance-processor — Extract and filter balance changes from checkpoints.
 *
 * Balance changes are transaction-level metadata (not BCS events).
 * Each change records an address gaining or losing a specific coin type.
 */
import type { GrpcCheckpointResponse } from "./grpc.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface BalanceChange {
  txDigest: string;
  checkpointSeq: bigint;
  address: string;
  coinType: string;
  amount: string; // signed numeric string (positive = received, negative = sent)
  timestamp: Date;
}

export interface BalanceProcessor {
  /** Extract balance changes from a checkpoint, applying coin type filters. */
  extract(response: GrpcCheckpointResponse): BalanceChange[];
  /** Get current coin type filter. */
  getCoinTypes(): string[] | "*";
  /** Update coin type filter (for hot reload). */
  setCoinTypes(coinTypes: string[] | "*"): void;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function parseTimestamp(ts: { seconds: string; nanos: number } | null | undefined): Date {
  if (!ts) return new Date(0);
  const ms = BigInt(ts.seconds) * 1000n + BigInt(Math.floor(ts.nanos / 1_000_000));
  return new Date(Number(ms));
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

export function createBalanceProcessor(coinTypes: string[] | "*"): BalanceProcessor {
  let filter: Set<string> | null = coinTypes === "*" ? null : new Set(coinTypes);

  return {
    extract(response: GrpcCheckpointResponse): BalanceChange[] {
      const changes: BalanceChange[] = [];
      const cp = response.checkpoint;
      if (!cp?.transactions) return changes;

      const checkpointSeq = BigInt(response.cursor);
      const timestamp = parseTimestamp(cp.summary?.timestamp);

      for (const tx of cp.transactions) {
        const bcs = tx.balanceChanges;
        if (!bcs?.length) continue;

        for (const bc of bcs) {
          // Apply coin type filter
          if (filter && !filter.has(bc.coinType)) continue;

          changes.push({
            txDigest: tx.digest,
            checkpointSeq,
            address: bc.address,
            coinType: bc.coinType,
            amount: bc.amount,
            timestamp,
          });
        }
      }

      return changes;
    },

    getCoinTypes(): string[] | "*" {
      return filter ? [...filter] : "*";
    },

    setCoinTypes(coinTypes: string[] | "*"): void {
      filter = coinTypes === "*" ? null : new Set(coinTypes);
    },
  };
}
