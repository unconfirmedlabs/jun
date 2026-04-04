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
    const ownerLen = view.getUint16(pos, true); pos += 2;
    const address = readStr(raw, pos, ownerLen); pos += ownerLen;
    const ctLen = view.getUint16(pos, true); pos += 2;
    const coinType = readStr(raw, pos, ctLen); pos += ctLen;
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
    const digestLen = view.getUint16(pos, true); pos += 2;
    const digest = readStr(raw, pos, digestLen); pos += digestLen;
    const senderLen = view.getUint16(pos, true); pos += 2;
    const sender = readStr(raw, pos, senderLen); pos += senderLen;
    const success = view.getUint8(pos) === 1; pos += 1;
    const computationCost = view.getBigUint64(pos, true).toString(); pos += 8;
    const storageCost = view.getBigUint64(pos, true).toString(); pos += 8;
    const storageRebate = view.getBigUint64(pos, true).toString(); pos += 8;
    const numTxEvents = view.getUint16(pos, true); pos += 2;
    const numMoveCalls = view.getUint16(pos, true); pos += 2;

    const commands: NonNullable<NonNullable<GrpcCheckpointResponse["checkpoint"]["transactions"][number]["transaction"]>["commands"]> = new Array(numMoveCalls);
    for (let j = 0; j < numMoveCalls; j++) {
      const pkgLen = view.getUint16(pos, true); pos += 2;
      const pkg = readStr(raw, pos, pkgLen); pos += pkgLen;
      const modLen = view.getUint16(pos, true); pos += 2;
      const mod = readStr(raw, pos, modLen); pos += modLen;
      const fnLen = view.getUint16(pos, true); pos += 2;
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
          const view = new DataView(msg.raw.buffer, msg.raw.byteOffset, msg.raw.byteLength);
          const numBal = view.getUint32(0, true);
          const numEvents = view.getUint32(4, true);
          const numTx = view.getUint32(8, true);
          console.error(`[jun] Zig parse error cp:${msg.seq} (${msg.raw.length}B) bal:${numBal} ev:${numEvents} tx:${numTx}`);

          // Dump first transaction to find format mismatch
          let pos = 20;
          // Skip balance changes
          for (let i = 0; i < numBal; i++) {
            const ownerLen = view.getUint16(pos, true); pos += 2 + ownerLen;
            const ctLen = view.getUint16(pos, true); pos += 2 + ctLen;
            pos += 8; // i64 amount
          }
          // Dump first tx raw fields
          if (numTx > 0 && pos < msg.raw.length - 20) {
            const digestLen = view.getUint16(pos, true);
            const digestStart = pos + 2;
            const digest = new TextDecoder().decode(msg.raw.subarray(digestStart, digestStart + digestLen));
            pos = digestStart + digestLen;
            const senderLen = view.getUint16(pos, true);
            pos += 2 + senderLen;
            const success = view.getUint8(pos); pos += 1;
            const comp = view.getBigUint64(pos, true); pos += 8;
            const stor = view.getBigUint64(pos, true); pos += 8;
            const reb = view.getBigUint64(pos, true); pos += 8;
            const numTxEv = view.getUint16(pos, true); pos += 2;
            const numMC = view.getUint16(pos, true); pos += 2;
            console.error(`[jun]   tx0: digest=${digest.slice(0,20)}.. success=${success} comp=${comp} numEv=${numTxEv} numMC=${numMC}`);
            // Try first move call
            if (numMC > 0 && pos < msg.raw.length - 10) {
              const pkgLen = view.getUint16(pos, true);
              console.error(`[jun]   mc0: pkgLen=${pkgLen} remaining=${msg.raw.length - pos}B`);
              if (pkgLen > 1000) console.error(`[jun]   *** pkgLen looks wrong — likely format mismatch ***`);
            }
          }
          job.reject(e);
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
