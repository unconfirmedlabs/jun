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
// Helpers
// ---------------------------------------------------------------------------

/**
 * Normalize a Sui address: strip leading zeros after 0x prefix.
 * "0x0000000000000000000000000000000000000000000000000000000000000002" → "0x2"
 */
function normalizeAddress(addr: string): string {
  if (!addr.startsWith("0x")) return addr;
  const hex = addr.slice(2).replace(/^0+/, "");
  return `0x${hex || "0"}`;
}

/**
 * Normalize a coin type string by normalizing the package address.
 * "0x000...002::sui::SUI" → "0x2::sui::SUI"
 */
function normalizeCoinType(coinType: string): string {
  const parts = coinType.split("::");
  if (parts.length >= 3) {
    parts[0] = normalizeAddress(parts[0]!);
  }
  return parts.join("::");
}

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
