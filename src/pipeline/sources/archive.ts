/**
 * Archive checkpoint source — fetches historical checkpoints with worker pool decoding.
 *
 * Processes checkpoints in sliding windows with configurable concurrency.
 * CPU-bound decode (zstd + protobuf + BCS) offloaded to Bun Worker threads.
 * Yields checkpoints in strict sequential order via pending map + drain.
 */
import { fetchCompressed, getCheckpointType } from "../../archive.ts";
import { createCheckpointDecoderPool, type CheckpointDecoderPool } from "../../checkpoint-decoder-pool.ts";
import pMap from "p-map";
import pRetry from "p-retry";
import type { Source, Checkpoint } from "../types.ts";
import type { Logger } from "../../logger.ts";
import { createLogger } from "../../logger.ts";

export interface ArchiveSourceConfig {
  /** Archive base URL */
  archiveUrl: string;
  /** Starting checkpoint sequence */
  from: bigint;
  /** Optional: ending checkpoint (default: fetch latest from gRPC) */
  to?: bigint;
  /** Concurrent fetch workers (default: 10) */
  concurrency?: number;
  /** Number of decoder worker threads (default: auto) */
  workers?: number;
  /** Coin types for archive balance computation (null = disabled) */
  balanceCoinTypes?: string[] | "*";
  /** gRPC URL for fetching latest checkpoint */
  grpcUrl?: string;
}

export function createArchiveSource(config: ArchiveSourceConfig): Source {
  const log: Logger = createLogger().child({ component: "source:archive" });
  let stopped = false;
  let decoderPool: CheckpointDecoderPool | null = null;

  return {
    name: "backfill",

    async *stream(): AsyncIterable<Checkpoint> {
      const concurrency = config.concurrency ?? 10;
      const workerCount = config.workers ?? Math.max(1, Math.min(4, (navigator.hardwareConcurrency ?? 4) - 1));
      const balanceCoinTypes = config.balanceCoinTypes;

      decoderPool = createCheckpointDecoderPool(workerCount, balanceCoinTypes);
      log.info({ workers: workerCount, concurrency, from: config.from.toString() }, "archive source starting");

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

      // Process in sliding windows with ordered drain
      const windowSize = 100n;
      let windowStart = config.from;

      while (windowStart <= to && !stopped) {
        // Yield to event loop between windows
        await new Promise(resolve => setTimeout(resolve, 0));

        const windowEnd = windowStart + windowSize - 1n < to ? windowStart + windowSize - 1n : to;
        const batch: bigint[] = [];
        for (let i = windowStart; i <= windowEnd; i++) batch.push(i);

        // Pending map for ordered drain
        const pending = new Map<bigint, Checkpoint>();
        let watermark = windowStart - 1n;

        const pool = decoderPool;
        await pMap(
          batch,
          async (seq) => {
            if (stopped) return;

            try {
              const result = await pRetry(async () => {
                const abortController = new AbortController();
                const timeout = setTimeout(() => abortController.abort(), 30_000);
                try {
                  const compressed = await fetchCompressed(seq, config.archiveUrl, abortController.signal);
                  return await pool.decode(seq, compressed);
                } finally {
                  clearTimeout(timeout);
                }
              }, { retries: 3, minTimeout: 1000 });

              const timestamp = result.decoded.checkpoint.summary?.timestamp;
              const timestampDate = timestamp
                ? new Date(Number(BigInt(timestamp.seconds) * 1000n + BigInt(Math.floor(timestamp.nanos / 1_000_000))))
                : new Date(0);

              pending.set(seq, {
                sequenceNumber: seq,
                timestamp: timestampDate,
                transactions: result.decoded.checkpoint.transactions,
                source: "backfill",
                precomputedBalanceChanges: result.balanceChanges ?? undefined,
              });
            } catch (error) {
              log.error({ checkpoint: seq.toString(), error }, "failed to fetch/decode checkpoint");
            }
          },
          { concurrency },
        );

        // Drain in order
        for (let seq = windowStart; seq <= windowEnd; seq++) {
          const checkpoint = pending.get(seq);
          if (checkpoint) {
            yield checkpoint;
          }
        }

        windowStart = windowEnd + 1n;
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
