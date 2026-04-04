/**
 * WriterChannel — async write interface that decouples decode from storage writes.
 *
 * Main thread pushes serialized batches into a bounded channel.
 * The channel drains to a Storage implementation (worker thread for SQLite,
 * async queue for Postgres). Backpressure propagates when the channel is full.
 *
 * ```
 * Main thread              Channel (bounded=3)        Writer
 * [decode at 15K/s] -> push -> [batch][batch][batch] -> drain -> [SQLite/Postgres]
 *                       ^ backpressure if full              |
 *                       <- ack (cursor, stats) <-----------<-
 * ```
 */
import type {
  ProcessedCheckpoint,
  DecodedEvent,
  BalanceChange,
  TransactionRecord,
  MoveCallRecord,
  Checkpoint,
} from "./types.ts";

// ---------------------------------------------------------------------------
// WriteAck — returned after a batch is successfully written
// ---------------------------------------------------------------------------

export interface WriteAck {
  /** Highest checkpoint sequence number in the batch (as string — bigint not cloneable) */
  cursor: string;
  eventsWritten: number;
  balanceChangesWritten: number;
  transactionsWritten: number;
  moveCallsWritten: number;
  flushDurationMs: number;
}

// ---------------------------------------------------------------------------
// WriterChannel interface
// ---------------------------------------------------------------------------

export interface WriterChannel {
  /** Initialize — create tables, prepare statements. */
  initialize(): Promise<void>;
  /** Send a batch for writing. Blocks (awaits) if channel is full (backpressure). */
  send(batch: SerializedBatch[]): Promise<void>;
  /** Register callback for successful write acknowledgments. */
  onAck(callback: (ack: WriteAck) => void): void;
  /** Wait for all queued batches to be written. */
  drain(): Promise<void>;
  /** Close the channel — runs deferred indexes, materialization, closes DB. */
  close(): Promise<void>;
  /** Name for logging. */
  readonly name: string;
}

// ---------------------------------------------------------------------------
// Serialized types — bigints converted to strings for postMessage
// ---------------------------------------------------------------------------

export interface SerializedEvent {
  handlerName: string;
  checkpointSeq: string;
  txDigest: string;
  eventSeq: number;
  sender: string;
  timestamp: string; // ISO string
  data: Record<string, unknown>;
}

export interface SerializedBalanceChange {
  txDigest: string;
  checkpointSeq: string;
  address: string;
  coinType: string;
  amount: string;
  timestamp: string; // ISO string
}

export interface SerializedTransaction {
  digest: string;
  sender: string;
  success: boolean;
  computationCost: string;
  storageCost: string;
  storageRebate: string;
  checkpointSeq: string;
  timestamp: string; // ISO string
  moveCallCount: number;
}

export interface SerializedMoveCall {
  txDigest: string;
  callIndex: number;
  package: string;
  module: string;
  function: string;
  checkpointSeq: string;
  timestamp: string; // ISO string
}

export interface SerializedCheckpoint {
  sequenceNumber: string;
  timestamp: string; // ISO string
  source: string;
}

export interface SerializedBatch {
  checkpoint: SerializedCheckpoint;
  events: SerializedEvent[];
  balanceChanges: SerializedBalanceChange[];
  transactions: SerializedTransaction[];
  moveCalls: SerializedMoveCall[];
}

// ---------------------------------------------------------------------------
// Serialization helpers
// ---------------------------------------------------------------------------

/**
 * Serialize a batch of ProcessedCheckpoints into a flat SerializedBatch[].
 * Converts bigint -> string and Date -> ISO string for structured clone.
 */
export function serializeBatch(batch: ProcessedCheckpoint[]): SerializedBatch[] {
  return batch.map(cp => ({
    checkpoint: {
      sequenceNumber: cp.checkpoint.sequenceNumber.toString(),
      timestamp: cp.checkpoint.timestamp.toISOString(),
      source: cp.checkpoint.source,
    },
    events: cp.events.map(e => ({
      handlerName: e.handlerName,
      checkpointSeq: e.checkpointSeq.toString(),
      txDigest: e.txDigest,
      eventSeq: e.eventSeq,
      sender: e.sender,
      timestamp: e.timestamp.toISOString(),
      data: e.data,
    })),
    balanceChanges: cp.balanceChanges.map(b => ({
      txDigest: b.txDigest,
      checkpointSeq: b.checkpointSeq.toString(),
      address: b.address,
      coinType: b.coinType,
      amount: b.amount,
      timestamp: b.timestamp.toISOString(),
    })),
    transactions: cp.transactions.map(t => ({
      digest: t.digest,
      sender: t.sender,
      success: t.success,
      computationCost: t.computationCost,
      storageCost: t.storageCost,
      storageRebate: t.storageRebate,
      checkpointSeq: t.checkpointSeq.toString(),
      timestamp: t.timestamp.toISOString(),
      moveCallCount: t.moveCallCount,
    })),
    moveCalls: cp.moveCalls.map(m => ({
      txDigest: m.txDigest,
      callIndex: m.callIndex,
      package: m.package,
      module: m.module,
      function: m.function,
      checkpointSeq: m.checkpointSeq.toString(),
      timestamp: m.timestamp.toISOString(),
    })),
  }));
}

/**
 * Deserialize a SerializedBatch[] back into ProcessedCheckpoint[].
 * Converts string -> bigint and ISO string -> Date.
 */
export function deserializeBatch(batches: SerializedBatch[]): ProcessedCheckpoint[] {
  return batches.map(sb => ({
    checkpoint: {
      sequenceNumber: BigInt(sb.checkpoint.sequenceNumber),
      timestamp: new Date(sb.checkpoint.timestamp),
      transactions: [], // transactions are tracked separately
      source: sb.checkpoint.source as "live" | "backfill",
    },
    events: sb.events.map(e => ({
      handlerName: e.handlerName,
      checkpointSeq: BigInt(e.checkpointSeq),
      txDigest: e.txDigest,
      eventSeq: e.eventSeq,
      sender: e.sender,
      timestamp: new Date(e.timestamp),
      data: e.data,
    })),
    balanceChanges: sb.balanceChanges.map(b => ({
      txDigest: b.txDigest,
      checkpointSeq: BigInt(b.checkpointSeq),
      address: b.address,
      coinType: b.coinType,
      amount: b.amount,
      timestamp: new Date(b.timestamp),
    })),
    transactions: sb.transactions.map(t => ({
      digest: t.digest,
      sender: t.sender,
      success: t.success,
      computationCost: t.computationCost,
      storageCost: t.storageCost,
      storageRebate: t.storageRebate,
      checkpointSeq: BigInt(t.checkpointSeq),
      timestamp: new Date(t.timestamp),
      moveCallCount: t.moveCallCount,
    })),
    moveCalls: sb.moveCalls.map(m => ({
      txDigest: m.txDigest,
      callIndex: m.callIndex,
      package: m.package,
      module: m.module,
      function: m.function,
      checkpointSeq: BigInt(m.checkpointSeq),
      timestamp: new Date(m.timestamp),
    })),
  }));
}
