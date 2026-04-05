/**
 * jun/checkpoint-decoder-pool — Pool of Bun Workers for parallel checkpoint decoding.
 *
 * Offloads CPU-bound zstd decompress + protobuf + BCS decode to worker
 * threads, keeping the main thread free for HTTP serving, gRPC streaming,
 * and Postgres writes.
 *
 * @example
 * ```ts
 * const pool = createCheckpointDecoderPool(4);
 * const decoded = await pool.decode(seq, compressedBytes);
 * pool.shutdown();
 * ```
 */
import type { GrpcCheckpointResponse } from "./grpc.ts";
import type { BalanceChange } from "./balance-processor.ts";
import path from "path";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface DecodeResult {
  decoded: GrpcCheckpointResponse;
  balanceChanges: BalanceChange[] | null;
}

export interface CheckpointDecoderPool {
  /** Decode a compressed checkpoint in a worker thread. Returns events + optional balance changes. */
  decode(seq: bigint, compressed: Uint8Array): Promise<DecodeResult>;
  /** Terminate all workers. */
  shutdown(): void;
  /** Number of workers in the pool. */
  readonly size: number;
}

interface PendingJob {
  resolve: (result: DecodeResult) => void;
  reject: (err: Error) => void;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

/** Default worker count: min(4, cpus - 1), at least 1. */
export function defaultWorkerCount(): number {
  const cpus = navigator.hardwareConcurrency ?? 4;
  return Math.max(1, Math.min(4, cpus - 1));
}

export function createCheckpointDecoderPool(
  size: number,
  balanceCoinTypes?: string[] | "*",
  needsTransactions = false,
): CheckpointDecoderPool {
  // Serialize coin types for worker messages: null = disabled, [] = all (*), [...] = specific types
  const workerBalanceCoinTypes: string[] | null = balanceCoinTypes === undefined
    ? null
    : balanceCoinTypes === "*"
      ? []
      : balanceCoinTypes;
  const workerUrl = path.join(import.meta.dir, "checkpoint-decoder.ts");

  const workers: Worker[] = [];
  const pending = new Map<number, PendingJob>();
  let nextId = 0;
  const MAX_ID = 2_000_000_000; // Safe integer wraparound well below 2^53

  // Initialize workers
  for (let i = 0; i < size; i++) {
    const worker = new Worker(workerUrl);

    worker.onmessage = (event: MessageEvent) => {
      const msg = event.data;
      const job = pending.get(msg.id);
      if (!job) return;
      pending.delete(msg.id);

      if (msg.error) {
        job.reject(new Error(msg.error));
      } else {
        job.resolve({ decoded: msg.decoded, balanceChanges: msg.balanceChanges });
      }
    };

    worker.onerror = (event) => {
      // Reject all pending jobs assigned to this worker (round-robin: id % size === i)
      for (const [id, job] of pending) {
        if (id % size === i) {
          job.reject(new Error(`Worker ${i} crashed: ${event.message}`));
          pending.delete(id);
        }
      }
    };

    workers.push(worker);
  }

  return {
    get size() { return size; },

    decode(seq: bigint, compressed: Uint8Array): Promise<DecodeResult> {
      return new Promise((resolve, reject) => {
        const id = nextId;
        nextId = nextId >= MAX_ID ? 0 : nextId + 1;
        pending.set(id, { resolve, reject });

        // Round-robin assignment
        const worker = workers[id % size]!;
        worker.postMessage(
          { id, seq: seq.toString(), compressed, balanceCoinTypes: workerBalanceCoinTypes, needsTransactions },
          [compressed.buffer],
        );
      });
    },

    shutdown(): void {
      for (const worker of workers) {
        worker.terminate();
      }
      workers.length = 0;

      // Reject any pending jobs
      for (const [, job] of pending) {
        job.reject(new Error("Worker pool shut down"));
      }
      pending.clear();
    },
  };
}
