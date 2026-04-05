/**
 * Transaction tracker processor — extracts transaction records and move calls.
 *
 * Produces TransactionRecord[] and MoveCallRecord[] from checkpoint data.
 * Move calls are extracted from PTB commands (programmableTransaction.commands).
 * Also captures effects-level metadata: epoch, error details, events digest,
 * lamport version, and dependency count.
 */
import type { Processor, Checkpoint, ProcessedCheckpoint, TransactionRecord, MoveCallRecord } from "../types.ts";
import { emptyProcessed } from "../types.ts";

export function createTransactionTracker(): Processor {
  return {
    name: "transaction-tracker",

    process(checkpoint: Checkpoint): ProcessedCheckpoint {
      const transactions: TransactionRecord[] = [];
      const moveCalls: MoveCallRecord[] = [];

      for (const tx of checkpoint.transactions) {
        const err = tx.effects?.status?.error;
        const effects = tx.effects;

        // Transaction record
        transactions.push({
          digest: tx.digest,
          sender: tx.transaction?.sender ?? "",
          success: effects?.status?.success ?? true,
          computationCost: effects?.gasUsed?.computationCost ?? "0",
          storageCost: effects?.gasUsed?.storageCost ?? "0",
          storageRebate: effects?.gasUsed?.storageRebate ?? "0",
          nonRefundableStorageFee: effects?.gasUsed?.nonRefundableStorageFee ?? null,
          checkpointSeq: checkpoint.sequenceNumber,
          timestamp: checkpoint.timestamp,
          moveCallCount: 0, // updated below
          epoch: effects?.epoch ? BigInt(effects.epoch) : checkpoint.epoch,
          errorKind: err?.kind ?? null,
          errorDescription: err?.description ?? null,
          errorCommandIndex: err?.commandIndex ?? null,
          errorAbortCode: err?.abortCode ?? null,
          errorModule: err?.moveLocation ? `${err.moveLocation.package}::${err.moveLocation.module}` : null,
          errorFunction: err?.moveLocation?.function ?? null,
          eventsDigest: effects?.eventsDigest ?? null,
          lamportVersion: effects?.lamportVersion ?? null,
          dependencyCount: effects?.dependencies?.length ?? 0,
        });

        // Extract Move calls from PTB commands.
        // The structure depends on how proto-loader maps the fields.
        // Try multiple paths since the proto structure can vary.
        const commands =
          tx.transaction?.commands ??
          tx.transaction?.programmableTransaction?.commands ??
          tx.transaction?.kind?.programmableTransaction?.commands ??
          [];

        let callIndex = 0;
        for (const cmd of commands) {
          const mc = cmd.moveCall ?? (cmd as any).MoveCall;
          if (!mc) continue;

          moveCalls.push({
            txDigest: tx.digest,
            callIndex,
            package: mc.package ?? "",
            module: mc.module ?? "",
            function: mc.function ?? (mc as any).function_ ?? "",
            checkpointSeq: checkpoint.sequenceNumber,
            timestamp: checkpoint.timestamp,
          });
          callIndex++;
        }

        // Update move call count
        transactions[transactions.length - 1]!.moveCallCount = callIndex;
      }

      return { ...emptyProcessed(checkpoint), transactions, moveCalls };
    },
  };
}
