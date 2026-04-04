/**
 * jun/balance-processor — Extract and filter balance changes from checkpoints.
 *
 * Balance changes are transaction-level metadata (not BCS events).
 * Each change records an address gaining or losing a specific coin type.
 */
import type { GrpcCheckpointResponse } from "./grpc.ts";
import { normalizeCoinType } from "./normalize.ts";

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

import { parseTimestamp } from "./timestamp.ts";

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

export function createBalanceProcessor(coinTypes: string[] | "*"): BalanceProcessor {
  // Normalize filter coin types for matching against full-address gRPC responses
  let filter: Set<string> | null = coinTypes === "*"
    ? null
    : new Set(coinTypes.map(normalizeCoinType));

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

        for (const balanceChange of bcs) {
          // Normalize the coin type (gRPC returns full zero-padded addresses)
          const normalizedCoinType = normalizeCoinType(balanceChange.coinType);

          // Apply coin type filter
          if (filter && !filter.has(normalizedCoinType)) continue;

          changes.push({
            txDigest: tx.digest,
            checkpointSeq,
            address: balanceChange.address,
            coinType: normalizedCoinType,
            amount: balanceChange.amount,
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
      filter = coinTypes === "*" ? null : new Set(coinTypes.map(normalizeCoinType));
    },
  };
}
