/**
 * jun/checkpoint-decoder-pool — Pool of Bun Workers for parallel archive
 * checkpoint decoding.
 *
 * Archive backfill path:
 * - workers own fetch + decode for assigned checkpoint ranges
 * - binary fast path when the native Rust decoder is available
 * - JSON fallback when native decode is unavailable
 *
 * Direct decode path:
 * - retained for non-archive callers that still post compressed bytes to a worker
 */
import type { GrpcCheckpointResponse } from "./grpc.ts";
import type { SerializedBalanceChange } from "./checkpoint-response.ts";
import { isNativeCheckpointDecoderAvailable } from "./checkpoint-native-decoder.ts";
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

export interface StreamDecodeResult {
  seq: bigint;
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
  /** Decode a checkpoint from a cached file path (worker reads from disk). */
  decodeCached(seq: bigint, cachePath: string): Promise<DecodeResult>;
  /** Assign cached file ranges to workers — workers own the decode loop. */
  decodeCachedRange(
    from: bigint,
    to: bigint,
    cacheDir: string,
  ): AsyncIterable<StreamDecodeResult>;
  /** Assign checkpoint ranges to workers and stream decoded archive results. */
  stream(
    from: bigint,
    to: bigint,
    archiveUrl: string,
    concurrency: number,
  ): AsyncIterable<StreamDecodeResult>;
  /** Terminate all workers. */
  shutdown(): void;
  /** Number of workers in the pool. */
  readonly size: number;
}

interface PendingJob {
  resolve: (result: DecodeResult) => void;
  reject: (err: Error) => void;
}

interface StreamState {
  queue: StreamDecodeResult[];
  waiters: Array<(result: IteratorResult<StreamDecodeResult>) => void>;
  doneWorkers: number;
  closed: boolean;
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
  const archiveMode = isNativeCheckpointDecoderAvailable() ? "binary" : "json";
  const workerBalanceCoinTypes: string[] | null = balanceCoinTypes === undefined
    ? null
    : balanceCoinTypes === "*"
      ? []
      : balanceCoinTypes;
  const workerUrl = path.join(import.meta.dir, "checkpoint-decoder.ts");

  const workers: Worker[] = [];
  const pending = new Map<number, PendingJob>();
  let activeStream: StreamState | null = null;
  let nextId = 0;
  const MAX_ID = 2_000_000_000;

  function closeStream(state: StreamState): void {
    if (state.closed) return;
    state.closed = true;
    while (state.waiters.length > 0) {
      const resolve = state.waiters.shift()!;
      resolve({ value: undefined, done: true });
    }
    if (activeStream === state) {
      activeStream = null;
    }
  }

  function pushStreamResult(result: StreamDecodeResult): void {
    const state = activeStream;
    if (!state || state.closed) return;
    const waiter = state.waiters.shift();
    if (waiter) {
      waiter({ value: result, done: false });
      return;
    }
    state.queue.push(result);
  }

  function splitRange(from: bigint, to: bigint, parts: number): Array<{ from: bigint; to: bigint }> {
    if (from > to) return Array.from({ length: parts }, () => ({ from: 1n, to: 0n }));

    const total = to - from + 1n;
    const base = total / BigInt(parts);
    const remainder = total % BigInt(parts);
    const ranges: Array<{ from: bigint; to: bigint }> = [];

    let next = from;
    for (let i = 0; i < parts; i++) {
      const chunk = base + (BigInt(i) < remainder ? 1n : 0n);
      if (chunk === 0n) {
        ranges.push({ from: 1n, to: 0n });
        continue;
      }
      const end = next + chunk - 1n;
      ranges.push({ from: next, to: end });
      next = end + 1n;
    }

    return ranges;
  }

  for (let i = 0; i < size; i++) {
    const worker = new Worker(workerUrl);

    worker.onmessage = (event: MessageEvent) => {
      const msg = event.data;

      if (activeStream && (msg instanceof Uint8Array || msg instanceof ArrayBuffer)) {
        const bytes = msg instanceof Uint8Array ? msg : new Uint8Array(msg);
        if (archiveMode === "binary") {
          if (bytes.byteLength < 8) return;
          const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
          const seq = view.getBigUint64(0, true);
          pushStreamResult({ seq, binary: bytes.subarray(8) });
          return;
        }
      }

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
        if (activeStream) {
          const newlineIdx = msg.indexOf("\n");
          const seq = BigInt(msg.slice(0, newlineIdx));
          try {
            const payload = JSON.parse(msg.slice(newlineIdx + 1)) as SerializedDecodedCheckpoint;
            pushStreamResult({ seq, payload });
          } catch (e) {
            console.error("[checkpoint-decoder-pool] failed to parse streamed JSON payload", e);
          }
          return;
        }

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
        if (msg.type === "done" && activeStream) {
          activeStream.doneWorkers += 1;
          if (activeStream.doneWorkers >= workers.length) {
            closeStream(activeStream);
          }
          return;
        }

        if (msg.type === "error" && activeStream) {
          console.error(
            `[checkpoint-decoder-pool] streamed checkpoint ${msg.seq ?? "unknown"} failed: ${msg.error ?? "worker error"}`,
          );
          return;
        }

        const id = msg.id as number;
        const job = pending.get(id);
        if (!job) return;
        pending.delete(id);
        job.reject(new Error(msg.error ?? "Worker error"));
      }
    };

    worker.onerror = (event) => {
      if (activeStream) {
        console.error(`[checkpoint-decoder-pool] worker ${i} crashed during archive stream: ${event.message}`);
        activeStream.doneWorkers += 1;
        if (activeStream.doneWorkers >= workers.length) {
          closeStream(activeStream);
        }
      }

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
      if (activeStream) {
        return Promise.reject(new Error("checkpoint decoder pool is streaming an archive range"));
      }

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

    decodeCached(seq: bigint, cachePath: string): Promise<DecodeResult> {
      return new Promise((resolve, reject) => {
        const id = nextId;
        nextId = nextId >= MAX_ID ? 0 : nextId + 1;
        pending.set(id, { resolve, reject });

        const worker = workers[id % size]!;
        // Simple object fast path — all primitives, no ArrayBuffer transfer needed
        worker.postMessage({
          type: "decode-cached",
          id,
          seq: seq.toString(),
          cachePath,
        });
      });
    },

    decodeCachedRange(from: bigint, to: bigint, cacheDir: string): AsyncIterable<StreamDecodeResult> {
      if (pending.size > 0) {
        throw new Error("checkpoint decoder pool has outstanding decode jobs");
      }
      if (activeStream) {
        throw new Error("checkpoint decoder pool is already streaming");
      }

      const streamState: StreamState = {
        queue: [],
        waiters: [],
        doneWorkers: 0,
        closed: false,
      };
      activeStream = streamState;

      const ranges = splitRange(from, to, workers.length);
      for (let i = 0; i < workers.length; i++) {
        const worker = workers[i]!;
        const range = ranges[i]!;
        worker.postMessage({
          type: "decode-cached-range",
          from: range.from.toString(),
          to: range.to.toString(),
          cacheDir,
          workerIndex: i,
        });
      }

      return {
        [Symbol.asyncIterator](): AsyncIterator<StreamDecodeResult> {
          return {
            next(): Promise<IteratorResult<StreamDecodeResult>> {
              if (streamState.queue.length > 0) {
                return Promise.resolve({ value: streamState.queue.shift()!, done: false });
              }
              if (streamState.closed) {
                return Promise.resolve({ value: undefined, done: true });
              }
              return new Promise(resolve => {
                streamState.waiters.push(resolve);
              });
            },
            return(): Promise<IteratorResult<StreamDecodeResult>> {
              closeStream(streamState);
              return Promise.resolve({ value: undefined, done: true });
            },
          };
        },
      };
    },

    stream(from: bigint, to: bigint, archiveUrl: string, concurrency: number): AsyncIterable<StreamDecodeResult> {
      if (pending.size > 0) {
        throw new Error("checkpoint decoder pool has outstanding decode jobs");
      }
      if (activeStream) {
        throw new Error("checkpoint decoder pool is already streaming an archive range");
      }

      const streamState: StreamState = {
        queue: [],
        waiters: [],
        doneWorkers: 0,
        closed: false,
      };
      activeStream = streamState;

      const ranges = splitRange(from, to, workers.length);
      const concurrencyPerWorker = Math.max(1, concurrency);

      for (let i = 0; i < workers.length; i++) {
        const worker = workers[i]!;
        const range = ranges[i]!;
        worker.postMessage({
          type: "assign",
          workerIndex: i,
          from: range.from.toString(),
          to: range.to.toString(),
          archiveUrl,
          concurrency: concurrencyPerWorker,
          mode: archiveMode,
          balanceCoinTypes: workerBalanceCoinTypes,
        });
      }

      const iterable: AsyncIterable<StreamDecodeResult> = {
        [Symbol.asyncIterator](): AsyncIterator<StreamDecodeResult> {
          return {
            next(): Promise<IteratorResult<StreamDecodeResult>> {
              if (streamState.queue.length > 0) {
                return Promise.resolve({ value: streamState.queue.shift()!, done: false });
              }
              if (streamState.closed) {
                return Promise.resolve({ value: undefined, done: true });
              }
              return new Promise(resolve => {
                streamState.waiters.push(resolve);
              });
            },
            return(): Promise<IteratorResult<StreamDecodeResult>> {
              closeStream(streamState);
              return Promise.resolve({ value: undefined, done: true });
            },
          };
        },
      };

      return iterable;
    },

    shutdown(): void {
      if (activeStream) {
        closeStream(activeStream);
      }
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
