/**
 * Archive checkpoint source — historical backfill with worker-owned fetch/decode.
 *
 * Streaming pipeline architecture:
 *   Worker fetch+decode (N workers) → Ordered drain → yield
 */
import { checkpointFromGrpcResponse } from "../../checkpoint-response.ts";
import { parseBinaryCheckpoint } from "../../binary-parser.ts";
import { createCheckpointDecoderPool, type CheckpointDecoderPool } from "../../checkpoint-decoder-pool.ts";
import { createLogger } from "../../logger.ts";
import type { Logger } from "../../logger.ts";
import type { Source, Checkpoint } from "../types.ts";
import type { BalanceChange, ProcessedCheckpoint } from "../types.ts";

export interface ArchiveSourceConfig {
  /** Archive base URL */
  archiveUrl: string;
  /** Starting checkpoint sequence */
  from: bigint;
  /** Optional: ending checkpoint (default: fetch latest from gRPC) */
  to?: bigint;
  /** Concurrent HTTP fetches (default: 50) */
  concurrency?: number;
  /** Number of decoder worker threads (default: auto) */
  workers?: number;
  /** Coin types for archive balance computation (null = disabled) */
  balanceCoinTypes?: string[] | "*";
  /** Enabled processor set used to filter worker-preprocessed rows. */
  enabledProcessors?: {
    balances: boolean;
    transactions: boolean;
    objectChanges: boolean;
    dependencies: boolean;
    inputs: boolean;
    commands: boolean;
    systemTransactions: boolean;
    unchangedConsensusObjects: boolean;
    events: boolean;
  };
  /** gRPC URL for fetching latest checkpoint */
  grpcUrl?: string;
  /** Skip ordered drain — yield checkpoints as they arrive from workers.
   *  Safe for snapshot mode where insert order doesn't matter. */
  unorderedDrain?: boolean;
}

function filterBalanceChanges(
  changes: BalanceChange[],
  balanceCoinTypes: string[] | "*" | undefined,
): BalanceChange[] {
  if (balanceCoinTypes === undefined || balanceCoinTypes === "*") return changes;
  const allowed = new Set(balanceCoinTypes);
  return changes.filter(change => allowed.has(change.coinType));
}

function filterPreProcessedCheckpoint(
  processed: ProcessedCheckpoint,
  enabled: ArchiveSourceConfig["enabledProcessors"],
  balanceCoinTypes: string[] | "*" | undefined,
): ProcessedCheckpoint {
  return {
    ...processed,
    events: enabled?.events ? processed.events : [],
    balanceChanges: enabled?.balances
      ? filterBalanceChanges(processed.balanceChanges, balanceCoinTypes)
      : [],
    transactions: enabled?.transactions ? processed.transactions : [],
    moveCalls: enabled?.transactions ? processed.moveCalls : [],
    objectChanges: enabled?.objectChanges ? processed.objectChanges : [],
    dependencies: enabled?.dependencies ? processed.dependencies : [],
    inputs: enabled?.inputs ? processed.inputs : [],
    commands: enabled?.commands ? processed.commands : [],
    systemTransactions: enabled?.systemTransactions ? processed.systemTransactions : [],
    unchangedConsensusObjects: enabled?.unchangedConsensusObjects
      ? processed.unchangedConsensusObjects
      : [],
  };
}

export function createArchiveSource(config: ArchiveSourceConfig): Source {
  const log: Logger = createLogger().child({ component: "source:archive" });
  let stopped = false;
  let decoderPool: CheckpointDecoderPool | null = null;

  return {
    name: "backfill",

    async *stream(): AsyncIterable<Checkpoint> {
      const fetchConcurrency = config.concurrency ?? 50;
      const workerCount = config.workers ?? Math.max(1, Math.min(4, (navigator.hardwareConcurrency ?? 4) - 1));
      const balanceCoinTypes = config.balanceCoinTypes;

      decoderPool = createCheckpointDecoderPool(workerCount, {
        balanceCoinTypes,
      });
      log.info({ workers: workerCount, fetchConcurrency, from: config.from.toString() }, "archive source starting");

      // Determine upper bound
      let to = config.to;
      if (!to && config.grpcUrl) {
        const { createGrpcClient } = await import("../../grpc.ts");
        const client = createGrpcClient({ url: config.grpcUrl });
        const stream = client.subscribeCheckpoints();
        for await (const response of stream) {
          to = BigInt(response.cursor);
          break;
        }
        client.close();
      }

      if (!to) {
        throw new Error("Archive source requires either 'to' checkpoint or 'grpcUrl' to determine latest");
      }

      log.info({ from: config.from.toString(), to: to.toString(), total: (to - config.from + 1n).toString() }, "backfill range");

      const pool = decoderPool;
      const concurrencyPerWorker = Math.max(1, Math.ceil(fetchConcurrency / workerCount));
      const pending = new Map<bigint, Checkpoint>();
      let watermark = config.from - 1n;
      const endSeq = to;

      let parseTime = 0;
      let totalProcessed = 0;
      const profilingStart = performance.now();

      try {
        for await (const result of pool.stream(config.from, endSeq, config.archiveUrl, concurrencyPerWorker)) {
          if (stopped) break;
          const parseStart = performance.now();

          let checkpoint: Checkpoint;
          if (result.binary) {
            const parsed = parseBinaryCheckpoint(result.binary);
            const processed = filterPreProcessedCheckpoint(
              parsed.processed,
              config.enabledProcessors,
              balanceCoinTypes,
            );
            checkpoint = parsed.checkpoint;
            (checkpoint as Checkpoint & { _preProcessed?: ProcessedCheckpoint })._preProcessed = processed;
          } else if (result.payload) {
            checkpoint = checkpointFromGrpcResponse(
              result.payload.decoded,
              "backfill",
              result.payload.precomputedBalanceChanges,
            );
          } else {
            throw new Error(`Checkpoint ${result.seq} decode returned no payload`);
          }

          parseTime += performance.now() - parseStart;
          totalProcessed += 1;

          if (totalProcessed > 0 && totalProcessed % 5000 === 0) {
            const elapsed = (performance.now() - profilingStart) / 1000;
            const rate = Math.round(totalProcessed / Math.max(elapsed, 0.001));
            const avgParse = (parseTime / totalProcessed).toFixed(2);
            console.error(
              `[archive-main] ${totalProcessed} checkpoints ${rate} checkpoint/s avg_parse=${avgParse}ms pending=${pending.size}`,
            );
          }

          if (config.unorderedDrain) {
            // Snapshot mode: yield immediately, no ordering needed
            yield checkpoint;
          } else {
            // Ordered drain: buffer and yield in sequence
            pending.set(result.seq, checkpoint);
            while (pending.has(watermark + 1n)) {
              watermark += 1n;
              const ready = pending.get(watermark)!;
              pending.delete(watermark);
              yield ready;
            }
          }
        }
      } finally {
        if (decoderPool) {
          decoderPool.shutdown();
          decoderPool = null;
        }
      }

      if (!stopped && watermark < endSeq) {
        log.warn(
          {
            nextExpected: (watermark + 1n).toString(),
            end: endSeq.toString(),
            pending: pending.size,
          },
          "archive stream ended before yielding the full requested range",
        );
      }
    },

    async stop(): Promise<void> {
      stopped = true;
      if (decoderPool) {
        decoderPool.shutdown();
        decoderPool = null;
      }
    },
  };
}
