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

export interface WorkerWriteConfig {
  backend: "clickhouse" | "postgres";
  clickhouseUrl?: string;
  clickhouseDatabase?: string;
  clickhouseUsername?: string;
  clickhousePassword?: string;
  postgresUrl?: string;
  batchSize?: number;
  balanceCoinTypes?: string[] | null;
}

export interface CheckpointDecoderPool {
  /** Assign cached file ranges to workers — workers own the decode loop. */
  decodeCachedRange(
    from: bigint,
    to: bigint,
    cacheDir: string,
    extractMask?: number,
  ): AsyncIterable<StreamDecodeResult>;
  /**
   * Assign cached file ranges to workers — workers decode AND write directly
   * to ClickHouse or Postgres. Returns when all workers have finished.
   */
  decodeAndWriteCachedRange(
    from: bigint,
    to: bigint,
    cacheDir: string,
    extractMask: number | undefined,
    writeConfig: WorkerWriteConfig,
    onProgress?: (decoded: number) => void,
  ): Promise<void>;
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

  interface WriteState {
    doneWorkers: number;
    totalDecoded: number;
    resolve: () => void;
    reject: (err: Error) => void;
    onProgress?: (total: number) => void;
  }

  let activeStream: StreamState | null = null;
  let activeWrite: WriteState | null = null;
  /** Per-worker running decoded counts — used to turn worker running-totals into deltas. */
  const workerProgress: number[] = new Array(size).fill(0);

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

        if (msg.type === "write-progress" && activeWrite) {
          // Worker sends its running total; pool tracks per-worker last-seen
          // values and computes the global total.
          const workerTotal = msg.decoded as number;
          const prev = workerProgress[i] ?? 0;
          workerProgress[i] = workerTotal;
          activeWrite.totalDecoded += workerTotal - prev;
          activeWrite.onProgress?.(activeWrite.totalDecoded);
          return;
        }

        if (msg.type === "write-error") {
          console.error(`[decoder-pool] worker ${i} write-error: ${msg.error ?? "unknown"}`);
          return;
        }

        if (msg.type === "done") {
          if (activeWrite) {
            // Worker sends { type: "done", decoded: N } with the final count.
            // Apply the same delta logic as write-progress.
            if (typeof msg.decoded === "number") {
              const prev = workerProgress[i] ?? 0;
              workerProgress[i] = msg.decoded;
              activeWrite.totalDecoded += msg.decoded - prev;
            }
            activeWrite.doneWorkers += 1;
            activeWrite.onProgress?.(activeWrite.totalDecoded);
            if (activeWrite.doneWorkers >= workers.length) {
              const write = activeWrite;
              activeWrite = null;
              write.resolve();
            }
            return;
          }
          if (activeStream) {
            activeStream.doneWorkers += 1;
            if (activeStream.doneWorkers >= workers.length) {
              closeStream(activeStream);
            }
            return;
          }
        }

        if (msg.type === "error" && activeStream) {
          console.error(`[decoder-pool] worker ${i} error: ${msg.error ?? "unknown"}`);
          return;
        }
      }
    };

    worker.onerror = (event) => {
      if (activeWrite) {
        activeWrite.doneWorkers += 1;
        if (activeWrite.doneWorkers >= workers.length) {
          const write = activeWrite;
          activeWrite = null;
          write.reject(new Error(`worker ${i} crashed: ${event.message}`));
        }
        return;
      }
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
      if (activeWrite) {
        throw new Error("decoder pool is currently writing");
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

    decodeAndWriteCachedRange(
      from: bigint,
      to: bigint,
      cacheDir: string,
      extractMask: number | undefined,
      writeConfig: WorkerWriteConfig,
      onProgress?: (decoded: number) => void,
    ): Promise<void> {
      if (activeStream) {
        throw new Error("decoder pool is already streaming");
      }
      if (activeWrite) {
        throw new Error("decoder pool is already writing");
      }

      return new Promise<void>((resolve, reject) => {
        workerProgress.fill(0);
        activeWrite = {
          doneWorkers: 0,
          totalDecoded: 0,
          resolve,
          reject,
          onProgress,
        };

        const ranges = splitRange(from, to, workers.length);
        for (let i = 0; i < workers.length; i++) {
          workers[i]!.postMessage({
            type: "decode-write-range",
            from: ranges[i]!.from.toString(),
            to: ranges[i]!.to.toString(),
            cacheDir,
            workerIndex: i,
            extractMask,
            batchSize: writeConfig.batchSize ?? 1000,
            backend: writeConfig.backend,
            clickhouseUrl: writeConfig.clickhouseUrl,
            clickhouseDatabase: writeConfig.clickhouseDatabase,
            clickhouseUsername: writeConfig.clickhouseUsername,
            clickhousePassword: writeConfig.clickhousePassword,
            postgresUrl: writeConfig.postgresUrl,
            balanceCoinTypes: writeConfig.balanceCoinTypes ?? null,
          });
        }
      });
    },

    shutdown(): void {
      if (activeStream) closeStream(activeStream);
      activeWrite = null;
      for (const worker of workers) worker.terminate();
      workers.length = 0;
    },
  };
}
