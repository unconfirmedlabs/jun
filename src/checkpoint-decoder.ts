/**
 * Worker thread for checkpoint decoding + processing.
 *
 * Does ALL the heavy work: zstd decompress → protobufjs decode →
 * @mysten/sui/bcs decode → run processors → serialize to JSON string.
 *
 * Returns a single JSON string via postMessage (zero-copy in Bun 1.2.21+
 * since strings ≥256 chars are shared without structured clone).
 *
 * Main thread only does: JSON.parse → SQL writes.
 */
/// <reference lib="webworker" />
import { decodeCheckpointFromProto, getCheckpointType } from "./archive.ts";
import { computeBalanceChangesFromArchive } from "./archive-balance.ts";
import { parseTimestamp } from "./timestamp.ts";
import { zstdDecompressSync } from "zlib";
import { emptyProcessed } from "./pipeline/types.ts";
import type { Checkpoint, ProcessedCheckpoint } from "./pipeline/types.ts";
import { createTransactionTracker } from "./pipeline/processors/transactionBlocks.ts";
import { createObjectChangeTracker } from "./pipeline/processors/objectChanges.ts";
import { createDependencyTracker } from "./pipeline/processors/dependencies.ts";
import { createCommandTracker } from "./pipeline/processors/commands.ts";
import { createInputTracker } from "./pipeline/processors/transactionInputs.ts";
import { createSystemTransactionTracker } from "./pipeline/processors/systemTransactions.ts";
import { createUnchangedConsensusObjectTracker } from "./pipeline/processors/unchangedConsensusObjects.ts";
import type { Processor } from "./pipeline/types.ts";

declare var self: Worker;

// Processors are created once per worker and reused across all checkpoints.
let processors: Processor[] | null = null;
let enabledProcessors: Set<string> | null = null;

function getProcessors(config: { processors?: string[] }): Processor[] {
  if (processors) return processors;

  const enabled = new Set(config.processors ?? []);
  enabledProcessors = enabled;
  const procs: Processor[] = [];

  if (enabled.has("transaction-tracker")) procs.push(createTransactionTracker());
  if (enabled.has("object-change-tracker")) procs.push(createObjectChangeTracker());
  if (enabled.has("dependency-tracker")) procs.push(createDependencyTracker());
  if (enabled.has("command-tracker")) procs.push(createCommandTracker());
  if (enabled.has("input-tracker")) procs.push(createInputTracker());
  if (enabled.has("system-tx-tracker")) procs.push(createSystemTransactionTracker());
  if (enabled.has("unchanged-consensus-tracker")) procs.push(createUnchangedConsensusObjectTracker());

  processors = procs;
  return procs;
}

/** JSON replacer that converts bigint → string and Date → ISO string.
 *  Avoids the overhead of a per-key function call by pre-converting
 *  known bigint fields before stringify. */
function serializeProcessed(processed: ProcessedCheckpoint): string {
  // Convert the checkpoint-level bigint fields to strings
  const cp = processed.checkpoint;
  const serializedCp = {
    sequenceNumber: cp.sequenceNumber.toString(),
    timestamp: cp.timestamp.toISOString(),
    source: cp.source,
    epoch: cp.epoch.toString(),
    digest: cp.digest,
    previousDigest: cp.previousDigest,
    contentDigest: cp.contentDigest,
    totalNetworkTransactions: cp.totalNetworkTransactions.toString(),
    epochRollingGasCostSummary: cp.epochRollingGasCostSummary,
  };

  // Transaction records have bigint checkpointSeq + epoch, Date timestamp
  const txs = processed.transactions.map(t => ({
    ...t,
    checkpointSeq: t.checkpointSeq.toString(),
    epoch: t.epoch.toString(),
    timestamp: t.timestamp.toISOString(),
  }));

  // All other record types have bigint checkpointSeq + Date timestamp
  const mapRecord = (r: { checkpointSeq: bigint; timestamp: Date; [k: string]: unknown }) => ({
    ...r,
    checkpointSeq: r.checkpointSeq.toString(),
    timestamp: (r.timestamp as Date).toISOString(),
  });

  return JSON.stringify({
    checkpoint: serializedCp,
    events: processed.events.map(e => ({
      ...e,
      checkpointSeq: e.checkpointSeq.toString(),
      timestamp: e.timestamp.toISOString(),
    })),
    balanceChanges: processed.balanceChanges.map(mapRecord),
    transactions: txs,
    moveCalls: processed.moveCalls.map(mapRecord),
    objectChanges: processed.objectChanges.map(mapRecord),
    dependencies: processed.dependencies.map(mapRecord),
    inputs: processed.inputs.map(mapRecord),
    commands: processed.commands.map(mapRecord),
    systemTransactions: processed.systemTransactions.map(mapRecord),
    unchangedConsensusObjects: processed.unchangedConsensusObjects.map(mapRecord),
  });
}

self.onmessage = async (event: MessageEvent) => {
  const { id, seq, compressed, balanceCoinTypes, processorNames } = event.data as {
    id: number;
    seq: string;
    compressed: Uint8Array;
    balanceCoinTypes: string[] | null;
    processorNames?: string[];
  };

  try {
    const compressedBytes = new Uint8Array(compressed);
    const sequenceNumber = BigInt(seq);
    const decompressed = zstdDecompressSync(Buffer.from(compressedBytes));

    const Checkpoint = await getCheckpointType();
    const protoDecoded = Checkpoint.decode(decompressed);
    const checkpointProto = Checkpoint.toObject(protoDecoded, { longs: String, enums: String, defaults: false });

    const decoded = await decodeCheckpointFromProto(sequenceNumber, checkpointProto);

    // Build the Checkpoint object for processors
    const summary = decoded.checkpoint.summary;
    const timestampDate = parseTimestamp(summary?.timestamp);
    const rollingGas = summary?.epochRollingGasCostSummary;

    const checkpoint: Checkpoint = {
      sequenceNumber,
      timestamp: timestampDate,
      transactions: decoded.checkpoint.transactions,
      source: "backfill",
      rawProto: checkpointProto, // needed for balance computation
      epoch: summary?.epoch ? BigInt(summary.epoch) : 0n,
      digest: summary?.digest ?? "",
      previousDigest: summary?.previousDigest ?? null,
      contentDigest: summary?.contentDigest ?? null,
      totalNetworkTransactions: summary?.totalNetworkTransactions
        ? BigInt(summary.totalNetworkTransactions)
        : 0n,
      epochRollingGasCostSummary: {
        computationCost: rollingGas?.computationCost ?? "0",
        storageCost: rollingGas?.storageCost ?? "0",
        storageRebate: rollingGas?.storageRebate ?? "0",
        nonRefundableStorageFee: rollingGas?.nonRefundableStorageFee ?? "0",
      },
    };

    // Run processors inside the worker
    const procs = getProcessors({ processors: processorNames });
    const processed = emptyProcessed(checkpoint);
    for (const proc of procs) {
      const result = proc.process(checkpoint);
      processed.events.push(...result.events);
      processed.balanceChanges.push(...result.balanceChanges);
      processed.transactions.push(...result.transactions);
      processed.moveCalls.push(...result.moveCalls);
      processed.objectChanges.push(...result.objectChanges);
      processed.dependencies.push(...result.dependencies);
      processed.inputs.push(...result.inputs);
      processed.commands.push(...result.commands);
      processed.systemTransactions.push(...result.systemTransactions);
      processed.unchangedConsensusObjects.push(...result.unchangedConsensusObjects);
    }

    // Balance changes from archive (separate from processor — uses ObjectSet)
    if (balanceCoinTypes !== null) {
      const coinTypeFilter = balanceCoinTypes.length === 0 ? null : new Set(balanceCoinTypes);
      const archiveBalances = computeBalanceChangesFromArchive(
        checkpointProto, sequenceNumber, timestampDate, coinTypeFilter,
      );
      processed.precomputedBalanceChanges = archiveBalances;
      processed.balanceChanges.push(...archiveBalances);
    }

    // Serialize to JSON string — zero-copy postMessage in Bun 1.2.21+
    const json = `${id}\n${serializeProcessed(processed)}`;
    postMessage(json);
  } catch (err) {
    // Error path: send as object (small, clone is fine)
    postMessage({ id, error: err instanceof Error ? err.message : String(err) });
  }
};
