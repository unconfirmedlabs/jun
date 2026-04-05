/**
 * jun/checkpoint-decoder-pool — Pool of Bun Workers for parallel archive
 * checkpoint decoding.
 *
 * Workers decode compressed archive checkpoints into the shared checkpoint
 * response shape and optionally precompute archive balance changes.
 *
 * IPC protocol: workers postMessage a single JSON string (zero-copy in
 * Bun 1.2.21+) prefixed with the job ID + newline. Errors are sent as
 * small objects { id, error }. This avoids structured clone overhead
 * for the large payload path.
 */
import type { GrpcCheckpointResponse } from "./grpc.ts";
import type { SerializedBalanceChange } from "./checkpoint-response.ts";
import path from "path";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface SerializedDecodedCheckpoint {
  decoded: GrpcCheckpointResponse;
  precomputedBalanceChanges?: SerializedBalanceChange[];
}

export interface DecodeResult {
  payload: SerializedDecodedCheckpoint;
}

export interface CheckpointDecoderPool {
  /** Decode a compressed checkpoint in a worker thread. */
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

export function defaultWorkerCount(): number {
  const cpus = navigator.hardwareConcurrency ?? 4;
  return Math.max(1, Math.min(4, cpus - 1));
}

export function createCheckpointDecoderPool(
  size: number,
  balanceCoinTypes?: string[] | "*",
): CheckpointDecoderPool {
  const workerBalanceCoinTypes: string[] | null = balanceCoinTypes === undefined
    ? null
    : balanceCoinTypes === "*"
      ? []
      : balanceCoinTypes;
  const workerUrl = path.join(import.meta.dir, "checkpoint-decoder.ts");

  const workers: Worker[] = [];
  const pending = new Map<number, PendingJob>();
  let nextId = 0;
  const MAX_ID = 2_000_000_000;

  for (let i = 0; i < size; i++) {
    const worker = new Worker(workerUrl);

    worker.onmessage = (event: MessageEvent) => {
      const msg = event.data;

      if (typeof msg === "string") {
        // Fast path: JSON string with "id\n{...}" format
        const newlineIdx = msg.indexOf("\n");
        const id = parseInt(msg.slice(0, newlineIdx), 10);
        const job = pending.get(id);
        if (!job) return;
        pending.delete(id);

        try {
          const payload = JSON.parse(msg.slice(newlineIdx + 1)) as SerializedDecodedCheckpoint;
          job.resolve({ payload });
        } catch (e) {
          job.reject(e instanceof Error ? e : new Error(String(e)));
        }
      } else if (msg && typeof msg === "object") {
        // Error path: { id, error }
        const id = msg.id as number;
        const job = pending.get(id);
        if (!job) return;
        pending.delete(id);
        job.reject(new Error(msg.error ?? "Worker error"));
      }
    };

    worker.onerror = (event) => {
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

        const worker = workers[id % size]!;
        worker.postMessage(
          {
            id,
            seq: seq.toString(),
            compressed,
            balanceCoinTypes: workerBalanceCoinTypes,
          },
          [compressed.buffer], // transfer compressed bytes (zero-copy to worker)
        );
      });
    },

    shutdown(): void {
      for (const worker of workers) {
        worker.terminate();
      }
      workers.length = 0;
      for (const [, job] of pending) {
        job.reject(new Error("Worker pool shut down"));
      }
      pending.clear();
    },
  };
}
