/**
 * System transaction processor — detects non-programmable transactions
 * (Genesis, ChangeEpoch, ConsensusCommitPrologue*, AuthenticatorStateUpdate,
 * EndOfEpoch, RandomnessStateUpdate, ProgrammableSystemTransaction) and
 * emits one record per tx with kind + full data as JSON.
 */
import type { Processor, Checkpoint, ProcessedCheckpoint, SystemTransactionRecord } from "../types.ts";
import { emptyProcessed } from "../types.ts";

/** JSON.stringify with Uint8Array → hex and bigint → string support. */
function safeStringify(value: unknown): string {
  return JSON.stringify(value, (_key, v) => {
    if (typeof v === "bigint") return v.toString();
    if (v instanceof Uint8Array) {
      let s = "0x";
      for (let i = 0; i < v.length; i++) s += v[i]!.toString(16).padStart(2, "0");
      return s;
    }
    return v;
  });
}

export function createSystemTransactionTracker(): Processor {
  return {
    name: "system-tx-tracker",

    process(checkpoint: Checkpoint): ProcessedCheckpoint {
      const records: SystemTransactionRecord[] = [];

      for (const tx of checkpoint.transactions) {
        const kind = tx.transaction?.systemKind;
        if (!kind) continue;

        records.push({
          txDigest: tx.digest,
          kind,
          data: typeof tx.transaction?.systemData === "string"
            ? tx.transaction.systemData
            : safeStringify(tx.transaction?.systemData ?? {}),
          checkpointSeq: checkpoint.sequenceNumber,
          timestamp: checkpoint.timestamp,
        });
      }

      return { ...emptyProcessed(checkpoint), systemTransactions: records };
    },
  };
}
