/**
 * Object change processor — extracts per-object state changes from
 * TransactionEffects.changed_objects.
 *
 * Derives a human-friendly change_type from the (inputState, outputState,
 * idOperation) tuple following Sui semantics:
 *
 *   DOES_NOT_EXIST → OBJECT_WRITE + CREATED   → CREATED
 *   EXISTS         → OBJECT_WRITE + NONE      → MUTATED
 *   EXISTS         → DOES_NOT_EXIST + DELETED → DELETED
 *   EXISTS         → DOES_NOT_EXIST + NONE    → WRAPPED   (object moved into parent)
 *   DOES_NOT_EXIST → OBJECT_WRITE + NONE      → UNWRAPPED (emerged from parent; heuristic)
 *   *              → PACKAGE_WRITE            → PACKAGE_WRITE
 */
import type { Processor, Checkpoint, ProcessedCheckpoint, ObjectChangeRecord } from "../types.ts";
import { emptyProcessed } from "../types.ts";
import type { GrpcChangedObject, GrpcOwner } from "../../grpc.ts";

function deriveChangeType(change: GrpcChangedObject): ObjectChangeRecord["changeType"] {
  const inputExists = change.inputState === "EXISTS";
  const outputState = change.outputState;
  const idOp = change.idOperation;

  if (outputState === "PACKAGE_WRITE") return "PACKAGE_WRITE";

  if (outputState === "OBJECT_WRITE") {
    if (!inputExists && idOp === "CREATED") return "CREATED";
    if (!inputExists && idOp === "NONE") return "UNWRAPPED";
    if (inputExists) return "MUTATED";
  }

  if (outputState === "DOES_NOT_EXIST") {
    if (idOp === "DELETED") return "DELETED";
    if (inputExists) return "WRAPPED";
  }

  // Accumulator writes aren't object-level changes in the usual sense; skip them
  // (they show up under outputState === "ACCUMULATOR_WRITE" and have no meaningful
  // change_type). Callers should filter them out.
  return "MUTATED";
}

function flattenOwner(owner: GrpcOwner | undefined): { address: string | null; kind: string | null } {
  if (!owner) return { address: null, kind: null };
  return { address: owner.address ?? null, kind: owner.kind };
}

export function createObjectChangeTracker(): Processor {
  return {
    name: "object-change-tracker",

    process(checkpoint: Checkpoint): ProcessedCheckpoint {
      const records: ObjectChangeRecord[] = [];

      for (const tx of checkpoint.transactions) {
        const changes = tx.effects?.changedObjects;
        if (!changes?.length) continue;

        // Identify the gas object — effects don't always flag it, but we can
        // match against the gas_payment objects if present on the tx.
        const gasObjectIds = new Set<string>();
        for (const gas of tx.transaction?.gasPayment?.objects ?? []) {
          if (gas.objectId) gasObjectIds.add(gas.objectId);
        }

        for (const change of changes) {
          // Skip accumulator writes — they aren't object-level state changes
          if (change.outputState === "ACCUMULATOR_WRITE") continue;

          const changeType = deriveChangeType(change);
          const input = flattenOwner(change.inputOwner);
          const output = flattenOwner(change.outputOwner);

          records.push({
            txDigest: tx.digest,
            objectId: change.objectId,
            changeType,
            objectType: change.objectType ?? null,
            inputVersion: change.inputVersion ?? null,
            inputDigest: change.inputDigest ?? null,
            inputOwner: input.address,
            inputOwnerKind: input.kind,
            outputVersion: change.outputVersion ?? null,
            outputDigest: change.outputDigest ?? null,
            outputOwner: output.address,
            outputOwnerKind: output.kind,
            isGasObject: gasObjectIds.has(change.objectId),
            checkpointSeq: checkpoint.sequenceNumber,
            timestamp: checkpoint.timestamp,
          });
        }
      }

      return { ...emptyProcessed(checkpoint), objectChanges: records };
    },
  };
}
