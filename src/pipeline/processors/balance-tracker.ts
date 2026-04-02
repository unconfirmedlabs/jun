/**
 * Balance tracker processor — extracts balance changes from checkpoints.
 *
 * For live (gRPC) checkpoints: reads pre-computed balanceChanges field.
 * For archive checkpoints: computes from ObjectSet coin diffs + accumulator writes.
 * Filters by coin type if configured.
 */
import type { Processor, Checkpoint, ProcessedCheckpoint, BalanceChange } from "../types.ts";
import { normalizeCoinType } from "../../normalize.ts";
import { computeBalanceChangesFromArchive } from "../../archive-balance.ts";
import type { Logger } from "../../logger.ts";
import { createLogger } from "../../logger.ts";

export interface BalanceTrackerConfig {
  /** Coin types to track: specific list or "*" for all */
  coinTypes: string[] | "*";
}

export function createBalanceTracker(config: BalanceTrackerConfig): Processor {
  const log: Logger = createLogger().child({ component: "processor:balance-tracker" });

  // Normalize filter
  let filter: Set<string> | null = config.coinTypes === "*"
    ? null
    : new Set(config.coinTypes.map(normalizeCoinType));

  return {
    name: "balance-tracker",

    process(checkpoint: Checkpoint): ProcessedCheckpoint {
      let changes: BalanceChange[];

      // Use pre-computed balance changes from archive worker if available
      if (checkpoint.precomputedBalanceChanges && checkpoint.precomputedBalanceChanges.length > 0) {
        changes = checkpoint.precomputedBalanceChanges;
        return { checkpoint, events: [], balanceChanges: changes };
      }

      if (checkpoint.rawProto) {
        // Archive: compute from ObjectSet + effects
        changes = computeBalanceChangesFromArchive(
          checkpoint.rawProto,
          checkpoint.sequenceNumber,
          checkpoint.timestamp,
          filter,
        );
      } else {
        // Live (gRPC): read pre-computed balance changes from transactions
        changes = [];
        for (const transaction of checkpoint.transactions) {
          if (!transaction.balanceChanges?.length) continue;

          for (const balanceChange of transaction.balanceChanges) {
            const normalizedCoinType = normalizeCoinType(balanceChange.coinType);
            if (filter && !filter.has(normalizedCoinType)) continue;

            changes.push({
              txDigest: transaction.digest,
              checkpointSeq: checkpoint.sequenceNumber,
              address: balanceChange.address,
              coinType: normalizedCoinType,
              amount: balanceChange.amount,
              timestamp: checkpoint.timestamp,
            });
          }
        }
      }

      return { checkpoint, events: [], balanceChanges: changes };
    },

    reload(newConfig: BalanceTrackerConfig): void {
      filter = newConfig.coinTypes === "*"
        ? null
        : new Set(newConfig.coinTypes.map(normalizeCoinType));
      log.info({ coinTypes: newConfig.coinTypes }, "balance tracker reloaded");
    },
  };
}
