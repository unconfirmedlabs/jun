/**
 * Checkpoint decoder worker pool — manages Bun Workers for parallel decode.
 *
 * Assigns checkpoint ranges to workers. Each worker reads cached .binpb.zst
 * files, runs Rust FFI binary decode, and posts results back via postMessage.
 */
import path from "path";
import type { ParsedBinaryCheckpoint } from "./binary-parser.ts";

export interface StreamDecodeResult {
  seq: bigint;
  parsed?: ParsedBinaryCheckpoint;
}

export interface CheckpointDecoderPool {
  /** Assign cached file ranges to workers — workers own the decode loop. */
  decodeCachedRange(
    from: bigint,
    to: bigint,
    cacheDir: string,
    extractMask?: number,
  ): AsyncIterable<StreamDecodeResult>;
  /** Terminate all workers. */
  shutdown(): void;
  /** Number of workers in the pool. */
  readonly size: number;
}

export function defaultWorkerCount(): number {
  const cpus = navigator.hardwareConcurrency ?? 4;
  return Math.max(1, Math.min(4, cpus - 1));
}

export function createCheckpointDecoderPool(size: number): CheckpointDecoderPool {
  const workerUrl = path.join(import.meta.dir, "checkpoint-decoder.ts");
  const workers: Worker[] = [];

  interface StreamState {
    queue: StreamDecodeResult[];
    waiters: Array<(result: IteratorResult<StreamDecodeResult>) => void>;
    doneWorkers: number;
    closed: boolean;
  }

  let activeStream: StreamState | null = null;

  function closeStream(state: StreamState): void {
    if (state.closed) return;
    state.closed = true;
    while (state.waiters.length > 0) {
      const resolve = state.waiters.shift()!;
      resolve({ value: undefined, done: true });
    }
    if (activeStream === state) activeStream = null;
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
      if (chunk === 0n) { ranges.push({ from: 1n, to: 0n }); continue; }
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

      if (msg && typeof msg === "object") {
        // Parsed result: { seq: string, parsed: ParsedBinaryCheckpoint }
        if ("seq" in msg && "parsed" in msg) {
          pushStreamResult({ seq: BigInt(msg.seq), parsed: msg.parsed });
          return;
        }

        if (msg.type === "done" && activeStream) {
          activeStream.doneWorkers += 1;
          if (activeStream.doneWorkers >= workers.length) {
            closeStream(activeStream);
          }
          return;
        }
        if (msg.type === "error" && activeStream) {
          console.error(`[decoder-pool] worker ${i} error: ${msg.error ?? "unknown"}`);
          return;
        }
      }
    };

    worker.onerror = (event) => {
      if (activeStream) {
        console.error(`[decoder-pool] worker ${i} crashed: ${event.message}`);
        activeStream.doneWorkers += 1;
        if (activeStream.doneWorkers >= workers.length) {
          closeStream(activeStream);
        }
      }
    };

    workers.push(worker);
  }

  return {
    get size() { return size; },

    decodeCachedRange(from: bigint, to: bigint, cacheDir: string, extractMask?: number): AsyncIterable<StreamDecodeResult> {
      if (activeStream) {
        throw new Error("decoder pool is already streaming");
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
        workers[i]!.postMessage({
          type: "decode-cached-range",
          from: ranges[i]!.from.toString(),
          to: ranges[i]!.to.toString(),
          cacheDir,
          workerIndex: i,
          extractMask,
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
              return new Promise(resolve => { streamState.waiters.push(resolve); });
            },
            return(): Promise<IteratorResult<StreamDecodeResult>> {
              closeStream(streamState);
              return Promise.resolve({ value: undefined, done: true });
            },
          };
        },
      };
    },

    shutdown(): void {
      if (activeStream) closeStream(activeStream);
      for (const worker of workers) worker.terminate();
      workers.length = 0;
    },
  };
}
