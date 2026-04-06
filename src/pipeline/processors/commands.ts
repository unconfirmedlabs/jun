/**
 * Command processor — emits one record per programmable transaction command.
 * Superset of move_calls covering every PTB command variant:
 *   MoveCall, TransferObjects, SplitCoins, MergeCoins, Publish, Upgrade,
 *   MakeMoveVector.
 *
 * For MoveCall, the package/module/function/type_arguments columns are
 * populated. For other kinds, args is a JSON blob with the raw command data.
 */
import type { Processor, Checkpoint, ProcessedCheckpoint, CommandRecord } from "../types.ts";
import { emptyProcessed } from "../types.ts";
import type { GrpcCommand } from "../../grpc.ts";

function commandKind(cmd: GrpcCommand): string {
  if (cmd.moveCall) return "MoveCall";
  if (cmd.transferObjects) return "TransferObjects";
  if (cmd.splitCoins) return "SplitCoins";
  if (cmd.mergeCoins) return "MergeCoins";
  if (cmd.publish) return "Publish";
  if (cmd.upgrade) return "Upgrade";
  if (cmd.makeMoveVector) return "MakeMoveVector";
  return "Unknown";
}

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

export function createCommandTracker(): Processor {
  return {
    name: "command-tracker",

    process(checkpoint: Checkpoint): ProcessedCheckpoint {
      const records: CommandRecord[] = [];

      for (const tx of checkpoint.transactions) {
        const commands =
          tx.transaction?.commands ??
          tx.transaction?.programmableTransaction?.commands ??
          tx.transaction?.kind?.programmableTransaction?.commands ??
          [];

        for (let i = 0; i < commands.length; i++) {
          const cmd = commands[i]!;
          const kind = commandKind(cmd);
          const mc = cmd.moveCall;

          let args: string | null = null;
          if (!mc) {
            // Serialize the specific variant (everything except the MoveCall key)
            const variant = cmd.transferObjects ?? cmd.splitCoins ?? cmd.mergeCoins
              ?? cmd.publish ?? cmd.upgrade ?? cmd.makeMoveVector;
            if (variant) args = safeStringify(variant);
          }

          records.push({
            txDigest: tx.digest,
            commandIndex: i,
            kind,
            package: mc?.package ?? null,
            module: mc?.module ?? null,
            function: mc?.function ?? null,
            typeArguments: mc ? safeStringify(mc.typeArguments ?? []) : null,
            args,
            checkpointSeq: checkpoint.sequenceNumber,
            timestamp: checkpoint.timestamp,
          });
        }
      }

      return { ...emptyProcessed(checkpoint), commands: records };
    },
  };
}
