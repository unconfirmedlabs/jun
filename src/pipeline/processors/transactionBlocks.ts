/**
 * Transaction tracker processor — extracts transaction records and move calls.
 *
 * Produces TransactionRecord[] and MoveCallRecord[] from checkpoint data.
 * Move calls are extracted from PTB commands (programmableTransaction.commands).
 */
import type { Processor, Checkpoint, ProcessedCheckpoint, TransactionRecord, MoveCallRecord } from "../types.ts";

export function createTransactionTracker(): Processor {
  return {
    name: "transaction-tracker",

    process(checkpoint: Checkpoint): ProcessedCheckpoint {
      const transactions: TransactionRecord[] = [];
      const moveCalls: MoveCallRecord[] = [];

      for (const tx of checkpoint.transactions) {
        // Transaction record
        transactions.push({
          digest: tx.digest,
          sender: tx.transaction?.sender ?? "",
          success: tx.effects?.status?.success ?? true,
          computationCost: tx.effects?.gasUsed?.computationCost ?? "0",
          storageCost: tx.effects?.gasUsed?.storageCost ?? "0",
          storageRebate: tx.effects?.gasUsed?.storageRebate ?? "0",
          checkpointSeq: checkpoint.sequenceNumber,
          timestamp: checkpoint.timestamp,
          moveCallCount: 0, // updated below
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

      return { checkpoint, events: [], balanceChanges: [], transactions, moveCalls };
    },
  };
}
