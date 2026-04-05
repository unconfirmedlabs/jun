/**
 * jun/checkpoint-decoder-pool — Pool of Bun Workers for parallel
 * checkpoint decoding + processing.
 *
 * Workers do ALL heavy work: zstd → protobuf → BCS → processors → JSON.
 * Main thread only JSON.parse + SQL writes.
 *
 * IPC protocol: workers postMessage a single JSON string (zero-copy in
 * Bun 1.2.21+) prefixed with the job ID + newline. Errors are sent as
 * small objects { id, error }. This avoids structured clone overhead
 * for the large payload path.
 */
import type { ProcessedCheckpoint, Checkpoint } from "./pipeline/types.ts";
import type { BalanceChange } from "./balance-processor.ts";
import path from "path";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Serialized checkpoint — all bigints as strings, Dates as ISO strings. */
export interface SerializedProcessedCheckpoint {
  checkpoint: {
    sequenceNumber: string;
    timestamp: string;
    source: string;
    epoch: string;
    digest: string;
    previousDigest: string | null;
    contentDigest: string | null;
    totalNetworkTransactions: string;
    epochRollingGasCostSummary: {
      computationCost: string;
      storageCost: string;
      storageRebate: string;
      nonRefundableStorageFee: string;
    };
  };
  events: Array<Record<string, unknown>>;
  balanceChanges: Array<Record<string, unknown>>;
  transactions: Array<Record<string, unknown>>;
  moveCalls: Array<Record<string, unknown>>;
  objectChanges: Array<Record<string, unknown>>;
  dependencies: Array<Record<string, unknown>>;
  inputs: Array<Record<string, unknown>>;
  commands: Array<Record<string, unknown>>;
  systemTransactions: Array<Record<string, unknown>>;
  unchangedConsensusObjects: Array<Record<string, unknown>>;
}

export interface DecodeResult {
  /** Serialized processed checkpoint (JSON-parsed from worker string). */
  processed: SerializedProcessedCheckpoint;
}

export interface CheckpointDecoderPool {
  /** Decode + process a compressed checkpoint in a worker thread. */
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
  processorNames?: string[],
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
          const processed = JSON.parse(msg.slice(newlineIdx + 1)) as SerializedProcessedCheckpoint;
          job.resolve({ processed });
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
            processorNames,
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
