/**
 * jun/checkpoint-decoder-pool — Pool of Bun Workers for parallel archive
 * checkpoint decoding.
 *
 * Workers decode compressed archive checkpoints either into:
 * - zero-copy Rust binary payloads (fast path), or
 * - JSON checkpoint payloads (fallback path).
 *
 * IPC protocol:
 * - binary fast path: Uint8Array with first 4 bytes = job ID (u32 LE)
 * - JSON fallback: string "id\\n{...}"
 * - error path: object { id, error }
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
  binary?: Uint8Array;
  payload?: SerializedDecodedCheckpoint;
}

export interface CheckpointDecoderPoolOptions {
  balanceCoinTypes?: string[] | "*";
  useBinaryPreprocessing?: boolean;
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
  options: CheckpointDecoderPoolOptions = {},
): CheckpointDecoderPool {
  const { balanceCoinTypes, useBinaryPreprocessing = false } = options;
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

      if (msg instanceof Uint8Array || msg instanceof ArrayBuffer) {
        const bytes = msg instanceof Uint8Array ? msg : new Uint8Array(msg);
        if (bytes.byteLength < 4) return;

        const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
        const id = view.getUint32(0, true);
        const job = pending.get(id);
        if (!job) return;
        pending.delete(id);
        job.resolve({ binary: bytes.subarray(4) });
      } else if (typeof msg === "string") {
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
            useBinary: useBinaryPreprocessing,
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
