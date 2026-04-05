/**
 * Transaction input processor — extracts per-input records from
 * programmable transaction inputs[]. Covers PURE, IMMUTABLE_OR_OWNED,
 * SHARED, RECEIVING, and FUNDS_WITHDRAWAL variants.
 */
import type { Processor, Checkpoint, ProcessedCheckpoint, TransactionInputRecord } from "../types.ts";
import { emptyProcessed } from "../types.ts";

/** Hex-encode a Uint8Array as a "0x..."-prefixed string. */
function bytesToHex(bytes: Uint8Array): string {
  let s = "0x";
  for (let i = 0; i < bytes.length; i++) s += bytes[i]!.toString(16).padStart(2, "0");
  return s;
}

export function createInputTracker(): Processor {
  return {
    name: "input-tracker",

    process(checkpoint: Checkpoint): ProcessedCheckpoint {
      const records: TransactionInputRecord[] = [];

      for (const tx of checkpoint.transactions) {
        const inputs = tx.transaction?.inputs;
        if (!inputs?.length) continue;

        for (let i = 0; i < inputs.length; i++) {
          const inp = inputs[i]!;
          records.push({
            txDigest: tx.digest,
            inputIndex: i,
            kind: inp.kind,
            objectId: inp.objectId ?? null,
            version: inp.version ?? null,
            digest: inp.digest ?? null,
            mutability: inp.mutability ?? null,
            initialSharedVersion: inp.initialSharedVersion ?? null,
            pureBytes: inp.pure ? bytesToHex(inp.pure) : null,
            amount: inp.amount ?? null,
            coinType: inp.coinType ?? null,
            source: inp.source ?? null,
            checkpointSeq: checkpoint.sequenceNumber,
            timestamp: checkpoint.timestamp,
          });
        }
      }

      return { ...emptyProcessed(checkpoint), inputs: records };
    },
  };
}
