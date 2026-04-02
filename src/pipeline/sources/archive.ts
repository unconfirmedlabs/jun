/**
 * Archive checkpoint source — fetches historical checkpoints with worker pool decoding.
 *
 * Streaming pipeline architecture:
 *   Fetcher (N concurrent) → Decoder pool (M workers) → Ordered drain → yield
 *
 * All three stages run concurrently. The fetcher races ahead filling a bounded
 * queue, workers decode in parallel, and the ordered drain yields checkpoints
 * as soon as the next sequential one is ready — no window boundaries.
 */
import { fetchCompressed } from "../../archive.ts";
import { createCheckpointDecoderPool, type CheckpointDecoderPool } from "../../checkpoint-decoder-pool.ts";
import type { Source, Checkpoint } from "../types.ts";
import { createLogger } from "../../logger.ts";
import type { Logger } from "../../logger.ts";

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
      const fetchConcurrency = config.concurrency ?? 50;
      const workerCount = config.workers ?? Math.max(1, Math.min(4, (navigator.hardwareConcurrency ?? 4) - 1));
      const balanceCoinTypes = config.balanceCoinTypes;

      decoderPool = createCheckpointDecoderPool(workerCount, balanceCoinTypes);
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

      // -----------------------------------------------------------------------
      // Streaming pipeline: fetch + decode concurrently, yield in order
      // -----------------------------------------------------------------------

      const pool = decoderPool;
      const pending = new Map<bigint, Checkpoint>();
      let watermark = config.from - 1n; // last yielded checkpoint
      let nextToFetch = config.from;
      const endSeq = to;

      // Bounded in-flight limit: don't let pending grow unbounded
      const maxInFlight = fetchConcurrency * 2;
      let inFlight = 0;

      // Promise-based signaling for the drain loop
      let drainResolve: (() => void) | null = null;

      function signalDrain() {
        if (drainResolve) {
          const resolve = drainResolve;
          drainResolve = null;
          resolve();
        }
      }

      function waitForDrain(): Promise<void> {
        return new Promise(resolve => { drainResolve = resolve; });
      }

      // Backpressure: wait until in-flight drops below limit
      let backpressureResolve: (() => void) | null = null;

      function signalBackpressure() {
        if (backpressureResolve && inFlight < maxInFlight) {
          const resolve = backpressureResolve;
          backpressureResolve = null;
          resolve();
        }
      }

      function waitForBackpressure(): Promise<void> {
        if (inFlight < maxInFlight) return Promise.resolve();
        return new Promise(resolve => { backpressureResolve = resolve; });
      }

      // Launch a single fetch+decode job
      async function processCheckpoint(seq: bigint): Promise<void> {
        try {
          const compressed = await fetchCompressed(seq, config.archiveUrl);
          if (stopped) return;

          const result = await pool.decode(seq, compressed);
          if (stopped) return;

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
        } finally {
          inFlight--;
          signalDrain();
          signalBackpressure();
        }
      }

      // Fetcher: continuously dispatch fetch+decode jobs with concurrency control
      let fetcherDone = false;
      const fetcherPromise = (async () => {
        while (nextToFetch <= endSeq && !stopped) {
          await waitForBackpressure();
          if (stopped) break;

          const seq = nextToFetch++;
          inFlight++;
          // Fire and forget — result lands in pending map
          processCheckpoint(seq);
        }
        fetcherDone = true;
        signalDrain(); // Wake drain loop in case it's waiting
      })();

      // Drain: yield checkpoints in strict sequential order
      while (watermark < endSeq && !stopped) {
        // Yield all contiguous checkpoints from the pending map
        while (pending.has(watermark + 1n)) {
          watermark++;
          const checkpoint = pending.get(watermark)!;
          pending.delete(watermark);
          yield checkpoint;
        }

        // If fetcher is done and nothing pending, we're finished
        if (fetcherDone && inFlight === 0 && !pending.has(watermark + 1n)) {
          break;
        }

        // Wait for the next decoded checkpoint to arrive
        await waitForDrain();
      }

      // Ensure fetcher completes
      await fetcherPromise;
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
