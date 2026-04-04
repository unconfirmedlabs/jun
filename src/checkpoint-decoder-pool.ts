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

const textDecoder = new TextDecoder();
function readStr(buf: Uint8Array, pos: number, len: number): string {
  return textDecoder.decode(buf.subarray(pos, pos + len));
}
function ensureAvailable(raw: Uint8Array, pos: number, len: number, label: string): void {
  if (pos + len > raw.byteLength) {
    throw new Error(`Native checkpoint payload truncated while reading ${label} at ${pos} (${len}B requested, ${raw.byteLength - pos}B remaining)`);
  }
}

function parseRawOutput(raw: Uint8Array, checkpointSeq: bigint): DecodeResult {
  const view = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);

  // Header: u32 numBal + u32 numEvents + u32 numTransactions + u64 timestampMs
  const numBal = view.getUint32(0, true);
  const numEvents = view.getUint32(4, true);
  const numTransactions = view.getUint32(8, true);
  const timestampMs = Number(view.getBigUint64(12, true));
  const timestamp = new Date(timestampMs);
  const seconds = String(Math.floor(timestampMs / 1000));
  const nanos = (timestampMs % 1000) * 1_000_000;

  let pos = 20;

  // Balance changes
  const balanceChanges: BalanceChange[] = new Array(numBal);
  for (let i = 0; i < numBal; i++) {
    ensureAvailable(raw, pos, 2, `balance owner length ${i}`);
    const ownerLen = view.getUint16(pos, true); pos += 2;
    ensureAvailable(raw, pos, ownerLen, `balance owner ${i}`);
    const address = readStr(raw, pos, ownerLen); pos += ownerLen;
    ensureAvailable(raw, pos, 2, `balance coin type length ${i}`);
    const ctLen = view.getUint16(pos, true); pos += 2;
    ensureAvailable(raw, pos, ctLen, `balance coin type ${i}`);
    const coinType = readStr(raw, pos, ctLen); pos += ctLen;
    ensureAvailable(raw, pos, 8, `balance amount ${i}`);
    const amount = view.getBigInt64(pos, true); pos += 8;
    balanceChanges[i] = {
      txDigest: "", checkpointSeq, address, coinType,
      amount: amount.toString(), timestamp,
    };
  }

  // Transactions
  const transactions: GrpcCheckpointResponse["checkpoint"]["transactions"] = new Array(numTransactions);
  const eventCounts = new Array<number>(numTransactions);
  for (let i = 0; i < numTransactions; i++) {
    ensureAvailable(raw, pos, 2, `transaction digest length ${i}`);
    const digestLen = view.getUint16(pos, true); pos += 2;
    ensureAvailable(raw, pos, digestLen, `transaction digest ${i}`);
    const digest = readStr(raw, pos, digestLen); pos += digestLen;
    ensureAvailable(raw, pos, 2, `transaction sender length ${i}`);
    const senderLen = view.getUint16(pos, true); pos += 2;
    ensureAvailable(raw, pos, senderLen, `transaction sender ${i}`);
    const sender = readStr(raw, pos, senderLen); pos += senderLen;
    ensureAvailable(raw, pos, 1 + 8 + 8 + 8 + 2 + 2, `transaction fixed fields ${i}`);
    const success = view.getUint8(pos) === 1; pos += 1;
    const computationCost = view.getBigUint64(pos, true).toString(); pos += 8;
    const storageCost = view.getBigUint64(pos, true).toString(); pos += 8;
    const storageRebate = view.getBigUint64(pos, true).toString(); pos += 8;
    const numTxEvents = view.getUint16(pos, true); pos += 2;
    const numMoveCalls = view.getUint16(pos, true); pos += 2;

    const commands: NonNullable<NonNullable<GrpcCheckpointResponse["checkpoint"]["transactions"][number]["transaction"]>["commands"]> = new Array(numMoveCalls);
    for (let j = 0; j < numMoveCalls; j++) {
      ensureAvailable(raw, pos, 2, `move call package length ${i}:${j}`);
      const pkgLen = view.getUint16(pos, true); pos += 2;
      ensureAvailable(raw, pos, pkgLen, `move call package ${i}:${j}`);
      const pkg = readStr(raw, pos, pkgLen); pos += pkgLen;
      ensureAvailable(raw, pos, 2, `move call module length ${i}:${j}`);
      const modLen = view.getUint16(pos, true); pos += 2;
      ensureAvailable(raw, pos, modLen, `move call module ${i}:${j}`);
      const mod = readStr(raw, pos, modLen); pos += modLen;
      ensureAvailable(raw, pos, 2, `move call function length ${i}:${j}`);
      const fnLen = view.getUint16(pos, true); pos += 2;
      ensureAvailable(raw, pos, fnLen, `move call function ${i}:${j}`);
      const fn = readStr(raw, pos, fnLen); pos += fnLen;
      commands[j] = { moveCall: { package: pkg, module: mod, function: fn } };
    }

    eventCounts[i] = numTxEvents;
    transactions[i] = {
      digest,
      transaction: {
        sender,
        programmableTransaction: commands.length > 0 ? { commands } : undefined,
        commands: commands.length > 0 ? commands : undefined,
      },
      events: null,
      effects: {
        status: { success },
        gasUsed: { computationCost, storageCost, storageRebate },
      },
    };
  }

  // Events
  const events: Array<{ packageId: string; module: string; sender: string; eventType: string; contents: { name: string; value: Uint8Array } }> = new Array(numEvents);
  for (let i = 0; i < numEvents; i++) {
    ensureAvailable(raw, pos, 2, `event package length ${i}`);
    const pkgLen = view.getUint16(pos, true); pos += 2;
    ensureAvailable(raw, pos, pkgLen, `event package ${i}`);
    const packageId = readStr(raw, pos, pkgLen); pos += pkgLen;
    ensureAvailable(raw, pos, 2, `event module length ${i}`);
    const modLen = view.getUint16(pos, true); pos += 2;
    ensureAvailable(raw, pos, modLen, `event module ${i}`);
    const module = readStr(raw, pos, modLen); pos += modLen;
    ensureAvailable(raw, pos, 2, `event sender length ${i}`);
    const sndLen = view.getUint16(pos, true); pos += 2;
    ensureAvailable(raw, pos, sndLen, `event sender ${i}`);
    const sender = readStr(raw, pos, sndLen); pos += sndLen;
    ensureAvailable(raw, pos, 2, `event type length ${i}`);
    const etLen = view.getUint16(pos, true); pos += 2;
    ensureAvailable(raw, pos, etLen, `event type ${i}`);
    const eventType = readStr(raw, pos, etLen); pos += etLen;
    ensureAvailable(raw, pos, 4, `event contents length ${i}`);
    const contentsLen = view.getUint32(pos, true); pos += 4;
    ensureAvailable(raw, pos, contentsLen, `event contents ${i}`);
    const value = raw.slice(pos, pos + contentsLen); pos += contentsLen;
    events[i] = { packageId, module, sender, eventType, contents: { name: eventType, value } };
  }

  let eventOffset = 0;
  for (let i = 0; i < numTransactions; i++) {
    const count = eventCounts[i] ?? 0;
    if (count > 0) {
      transactions[i]!.events = { events: events.slice(eventOffset, eventOffset + count) };
      eventOffset += count;
    }
  }

  // Build GrpcCheckpointResponse shape
  const decoded: GrpcCheckpointResponse = {
    cursor: checkpointSeq.toString(),
    checkpoint: {
      sequenceNumber: checkpointSeq.toString(),
      summary: { timestamp: { seconds, nanos } },
      transactions,
    },
  };

  return { decoded, balanceChanges: numBal > 0 ? balanceChanges : null };
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

/** Default worker count: min(4, cpus - 1), at least 1. */
/** Check if native Zig decoder is available for the current platform */
export function isNativeDecoderAvailable(): boolean {
  const { suffix } = require("bun:ffi");
  const { existsSync } = require("fs");
  const { join } = require("path");
  const arch = process.arch === "arm64" ? "arm64" : "x64";
  const platform = process.platform === "darwin" ? "darwin" : "linux";
  const paths = [
    join(__dirname, "..", "native", "lib", `libcheckpoint_processor.${platform}-${arch}.${suffix}`),
    join(__dirname, "..", "native", `libcheckpoint_processor.${suffix}`),
  ];
  return paths.some((p: string) => existsSync(p));
}

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
      } else if (msg.raw) {
        // Native path: raw Zig output buffer — parse on main thread
        try {
          job.resolve(parseRawOutput(msg.raw, BigInt(msg.seq)));
        } catch (e) {
          job.reject(e instanceof Error ? e : new Error(String(e)));
        }
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
