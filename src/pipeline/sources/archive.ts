/**
 * Archive checkpoint source — historical backfill with native or worker fetch/decode.
 *
 * Streaming pipeline architecture:
 *   Native Rust range downloader or JS worker pool → Ordered drain → yield
 */
import { checkpointFromGrpcResponse } from "../../checkpoint-response.ts";
import { parseBinaryCheckpoint } from "../../binary-parser.ts";
import { downloadAndDecodeRange, hasDownloadAndDecodeRange } from "../../checkpoint-native-decoder.ts";
import { createCheckpointDecoderPool, type CheckpointDecoderPool } from "../../checkpoint-decoder-pool.ts";
import { createLogger } from "../../logger.ts";
import type { Logger } from "../../logger.ts";
import type { Source, Checkpoint } from "../types.ts";
import type { BalanceChange, ProcessedCheckpoint } from "../types.ts";

const textDecoder = new TextDecoder();

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

function checkpointFromBinaryResult(
  binary: Uint8Array,
  enabledProcessors: ArchiveSourceConfig["enabledProcessors"],
  balanceCoinTypes: string[] | "*" | undefined,
): Checkpoint {
  const deferParsing = process.env.DEFER_BINARY_PARSING !== "0";
  if (deferParsing) {
    const view = new DataView(binary.buffer, binary.byteOffset, binary.byteLength);
    let pos = 40;

    const seqLen = view.getUint16(pos, true);
    pos += 2;
    const seqStr = textDecoder.decode(binary.subarray(pos, pos + seqLen));
    pos += seqLen;

    const epochLen = view.getUint16(pos, true);
    pos += 2;
    const epochStr = textDecoder.decode(binary.subarray(pos, pos + epochLen));

    const checkpoint: Checkpoint = {
      sequenceNumber: BigInt(seqStr),
      timestamp: new Date(),
      transactions: [],
      source: "backfill",
      epoch: BigInt(epochStr),
      digest: "",
      previousDigest: null,
      contentDigest: null,
      totalNetworkTransactions: 0n,
      epochRollingGasCostSummary: {
        computationCost: "0",
        storageCost: "0",
        storageRebate: "0",
        nonRefundableStorageFee: "0",
      },
    };
    (checkpoint as any)._rawBinary = binary;
    (checkpoint as any)._enabledProcessors = enabledProcessors;
    (checkpoint as any)._balanceCoinTypes = balanceCoinTypes;
    return checkpoint;
  }

  const parsed = parseBinaryCheckpoint(binary);
  const processed = filterPreProcessedCheckpoint(
    parsed.processed,
    enabledProcessors,
    balanceCoinTypes,
  );
  const checkpoint = parsed.checkpoint;
  (checkpoint as Checkpoint & { _preProcessed?: ProcessedCheckpoint })._preProcessed = processed;
  return checkpoint;
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
      const useNativeRange = hasDownloadAndDecodeRange();

      if (!useNativeRange) {
        decoderPool = createCheckpointDecoderPool(workerCount, {
          balanceCoinTypes,
        });
      }
      log.info(
        {
          workers: workerCount,
          fetchConcurrency,
          from: config.from.toString(),
          mode: useNativeRange ? "native-range" : "worker-pool",
        },
        "archive source starting",
      );

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
      const pending = new Map<bigint, Checkpoint>();
      let watermark = config.from - 1n;
      const endSeq = to;

      let parseTime = 0;
      let totalProcessed = 0;
      const profilingStart = performance.now();

      const emitCheckpoint = async function* (
        seq: bigint,
        checkpoint: Checkpoint,
      ): AsyncGenerator<Checkpoint> {
        if (config.unorderedDrain) {
          yield checkpoint;
          return;
        }

        pending.set(seq, checkpoint);
        while (pending.has(watermark + 1n)) {
          watermark += 1n;
          const ready = pending.get(watermark)!;
          pending.delete(watermark);
          yield ready;
        }
      };

      try {
        if (useNativeRange) {
          log.info({ concurrency: fetchConcurrency }, "archive source using native range downloader");

          const queue: Array<{ seq: bigint; binary: Uint8Array }> = [];
          let wakeDrain: (() => void) | null = null;
          let rangeDone = false;
          let rangeError: Error | null = null;

          const signalDrain = () => {
            if (wakeDrain) {
              const resolve = wakeDrain;
              wakeDrain = null;
              resolve();
            }
          };

          const waitForDrain = () => new Promise<void>(resolve => {
            wakeDrain = resolve;
          });

          const rangePromise = downloadAndDecodeRange(
            config.archiveUrl,
            config.from,
            endSeq,
            fetchConcurrency,
            (seq, binary) => {
              if (stopped) return;
              queue.push({ seq, binary });
              signalDrain();
            },
          ).then(() => {
            rangeDone = true;
            signalDrain();
          }).catch(error => {
            rangeError = error instanceof Error ? error : new Error(String(error));
            rangeDone = true;
            signalDrain();
          });

          while ((!rangeDone || queue.length > 0) && !stopped) {
            if (rangeError) throw rangeError;
            if (queue.length === 0) {
              await waitForDrain();
              continue;
            }

            const result = queue.shift()!;
            const parseStart = performance.now();
            const checkpoint = checkpointFromBinaryResult(
              result.binary,
              config.enabledProcessors,
              balanceCoinTypes,
            );
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

            for await (const ready of emitCheckpoint(result.seq, checkpoint)) {
              yield ready;
            }
          }

          await rangePromise;
          if (rangeError) throw rangeError;
        } else {
          const pool = decoderPool;
          const concurrencyPerWorker = Math.max(1, Math.ceil(fetchConcurrency / workerCount));

          for await (const result of pool.stream(config.from, endSeq, config.archiveUrl, concurrencyPerWorker)) {
            if (stopped) break;
            const parseStart = performance.now();

            let checkpoint: Checkpoint;
            if (result.binary) {
              checkpoint = checkpointFromBinaryResult(
                result.binary,
                config.enabledProcessors,
                balanceCoinTypes,
              );
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

            for await (const ready of emitCheckpoint(result.seq, checkpoint)) {
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
