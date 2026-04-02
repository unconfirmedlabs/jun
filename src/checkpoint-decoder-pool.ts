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
// Raw output parser (reads Zig binary format on main thread)
// ---------------------------------------------------------------------------

function readStr(buf: Uint8Array, pos: number, len: number): string {
  let s = "";
  for (let i = 0; i < len; i++) s += String.fromCharCode(buf[pos + i]!);
  return s;
}

function parseRawOutput(raw: Uint8Array): DecodeResult {
  const view = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);

  // Header: u32 numBal + u32 numEvents + u64 timestampMs
  const numBal = view.getUint32(0, true);
  const numEvents = view.getUint32(4, true);
  const timestampMs = Number(view.getBigUint64(8, true));
  const timestamp = new Date(timestampMs);
  const seconds = String(Math.floor(timestampMs / 1000));
  const nanos = (timestampMs % 1000) * 1_000_000;

  let pos = 16;

  // Balance changes
  const balanceChanges: BalanceChange[] = new Array(numBal);
  for (let i = 0; i < numBal; i++) {
    const ownerLen = view.getUint16(pos, true); pos += 2;
    const address = readStr(raw, pos, ownerLen); pos += ownerLen;
    const ctLen = view.getUint16(pos, true); pos += 2;
    const coinType = readStr(raw, pos, ctLen); pos += ctLen;
    const amount = view.getBigInt64(pos, true); pos += 8;
    balanceChanges[i] = {
      txDigest: "", checkpointSeq: 0n, address, coinType,
      amount: amount.toString(), timestamp,
    };
  }

  // Events
  const events: any[] = new Array(numEvents);
  for (let i = 0; i < numEvents; i++) {
    const pkgLen = view.getUint16(pos, true); pos += 2;
    const packageId = readStr(raw, pos, pkgLen); pos += pkgLen;
    const modLen = view.getUint16(pos, true); pos += 2;
    const module = readStr(raw, pos, modLen); pos += modLen;
    const sndLen = view.getUint16(pos, true); pos += 2;
    const sender = readStr(raw, pos, sndLen); pos += sndLen;
    const etLen = view.getUint16(pos, true); pos += 2;
    const eventType = readStr(raw, pos, etLen); pos += etLen;
    const contentsLen = view.getUint32(pos, true); pos += 4;
    const value = raw.slice(pos, pos + contentsLen); pos += contentsLen;
    events[i] = { packageId, module, sender, eventType, contents: { name: eventType, value } };
  }

  // Build GrpcCheckpointResponse shape
  const decoded: GrpcCheckpointResponse = {
    cursor: "0",
    checkpoint: {
      sequenceNumber: "0",
      summary: { timestamp: { seconds, nanos } },
      transactions: [{
        digest: "",
        events: events.length > 0 ? { events } : null,
      }],
    },
  };

  return { decoded, balanceChanges: numBal > 0 ? balanceChanges : null };
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
      } else if (msg.raw) {
        // Native path: raw Zig output buffer — parse on main thread
        job.resolve(parseRawOutput(msg.raw));
      } else {
        // Legacy path: pre-parsed JS objects
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
          { id, seq: seq.toString(), compressed, balanceCoinTypes: workerBalanceCoinTypes },
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
