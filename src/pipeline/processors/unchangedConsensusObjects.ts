/**
 * Unchanged consensus object processor — fans out
 * effects.unchangedConsensusObjects[] into per-object records.
 * These represent read-only or state-ended references to consensus
 * (shared) objects that weren't mutated by the transaction.
 */
import type { Processor, Checkpoint, ProcessedCheckpoint, UnchangedConsensusObjectRecord } from "../types.ts";
import { emptyProcessed } from "../types.ts";

export function createUnchangedConsensusObjectTracker(): Processor {
  return {
    name: "unchanged-consensus-tracker",

    process(checkpoint: Checkpoint): ProcessedCheckpoint {
      const records: UnchangedConsensusObjectRecord[] = [];

      for (const tx of checkpoint.transactions) {
        const objects = tx.effects?.unchangedConsensusObjects;
        if (!objects?.length) continue;

        for (const obj of objects) {
          records.push({
            txDigest: tx.digest,
            objectId: obj.objectId,
            kind: obj.kind,
            version: obj.version ?? null,
            digest: obj.digest ?? null,
            objectType: obj.objectType ?? null,
            checkpointSeq: checkpoint.sequenceNumber,
            timestamp: checkpoint.timestamp,
          });
        }
      }

      return { ...emptyProcessed(checkpoint), unchangedConsensusObjects: records };
    },
  };
}
