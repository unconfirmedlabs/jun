/**
 * Transaction dependency processor — fans out effects.dependencies[] into
 * one (tx_digest, depends_on_digest) edge per prior transaction.
 */
import type { Processor, Checkpoint, ProcessedCheckpoint, TransactionDependencyRecord } from "../types.ts";
import { emptyProcessed } from "../types.ts";

export function createDependencyTracker(): Processor {
  return {
    name: "dependency-tracker",

    process(checkpoint: Checkpoint): ProcessedCheckpoint {
      const records: TransactionDependencyRecord[] = [];

      for (const tx of checkpoint.transactions) {
        const deps = tx.effects?.dependencies;
        if (!deps?.length) continue;
        for (const dep of deps) {
          records.push({
            txDigest: tx.digest,
            dependsOnDigest: dep,
            checkpointSeq: checkpoint.sequenceNumber,
            timestamp: checkpoint.timestamp,
          });
        }
      }

      return { ...emptyProcessed(checkpoint), dependencies: records };
    },
  };
}
