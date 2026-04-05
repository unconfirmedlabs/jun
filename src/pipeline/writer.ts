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
} from "./types.ts";
import { emptyProcessed } from "./types.ts";

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
  nonRefundableStorageFee: string | null;
  checkpointSeq: string;
  timestamp: string; // ISO string
  moveCallCount: number;
  epoch: string;
  errorKind: string | null;
  errorDescription: string | null;
  errorCommandIndex: number | null;
  errorAbortCode: string | null;
  errorModule: string | null;
  errorFunction: string | null;
  eventsDigest: string | null;
  lamportVersion: string | null;
  dependencyCount: number;
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

export interface SerializedObjectChange {
  txDigest: string;
  objectId: string;
  changeType: string;
  objectType: string | null;
  inputVersion: string | null;
  inputDigest: string | null;
  inputOwner: string | null;
  inputOwnerKind: string | null;
  outputVersion: string | null;
  outputDigest: string | null;
  outputOwner: string | null;
  outputOwnerKind: string | null;
  isGasObject: boolean;
  checkpointSeq: string;
  timestamp: string;
}

export interface SerializedDependency {
  txDigest: string;
  dependsOnDigest: string;
  checkpointSeq: string;
  timestamp: string;
}

export interface SerializedInput {
  txDigest: string;
  inputIndex: number;
  kind: string;
  objectId: string | null;
  version: string | null;
  digest: string | null;
  mutability: string | null;
  initialSharedVersion: string | null;
  pureBytes: string | null;
  amount: string | null;
  coinType: string | null;
  source: string | null;
  checkpointSeq: string;
  timestamp: string;
}

export interface SerializedCommand {
  txDigest: string;
  commandIndex: number;
  kind: string;
  package: string | null;
  module: string | null;
  function: string | null;
  typeArguments: string | null;
  args: string | null;
  checkpointSeq: string;
  timestamp: string;
}

export interface SerializedSystemTransaction {
  txDigest: string;
  kind: string;
  data: string;
  checkpointSeq: string;
  timestamp: string;
}

export interface SerializedUnchangedConsensusObject {
  txDigest: string;
  objectId: string;
  kind: string;
  version: string | null;
  digest: string | null;
  objectType: string | null;
  checkpointSeq: string;
  timestamp: string;
}

export interface SerializedCheckpoint {
  sequenceNumber: string;
  timestamp: string; // ISO string
  source: string;
  epoch: string;
  digest: string;
  previousDigest: string | null;
  contentDigest: string | null;
  totalNetworkTransactions: string;
  rollingComputationCost: string;
  rollingStorageCost: string;
  rollingStorageRebate: string;
  rollingNonRefundableStorageFee: string;
}

export interface SerializedBatch {
  checkpoint: SerializedCheckpoint;
  events: SerializedEvent[];
  balanceChanges: SerializedBalanceChange[];
  transactions: SerializedTransaction[];
  moveCalls: SerializedMoveCall[];
  objectChanges: SerializedObjectChange[];
  dependencies: SerializedDependency[];
  inputs: SerializedInput[];
  commands: SerializedCommand[];
  systemTransactions: SerializedSystemTransaction[];
  unchangedConsensusObjects: SerializedUnchangedConsensusObject[];
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
      epoch: cp.checkpoint.epoch.toString(),
      digest: cp.checkpoint.digest,
      previousDigest: cp.checkpoint.previousDigest,
      contentDigest: cp.checkpoint.contentDigest,
      totalNetworkTransactions: cp.checkpoint.totalNetworkTransactions.toString(),
      rollingComputationCost: cp.checkpoint.epochRollingGasCostSummary.computationCost,
      rollingStorageCost: cp.checkpoint.epochRollingGasCostSummary.storageCost,
      rollingStorageRebate: cp.checkpoint.epochRollingGasCostSummary.storageRebate,
      rollingNonRefundableStorageFee: cp.checkpoint.epochRollingGasCostSummary.nonRefundableStorageFee,
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
      nonRefundableStorageFee: t.nonRefundableStorageFee,
      checkpointSeq: t.checkpointSeq.toString(),
      timestamp: t.timestamp.toISOString(),
      moveCallCount: t.moveCallCount,
      epoch: t.epoch.toString(),
      errorKind: t.errorKind,
      errorDescription: t.errorDescription,
      errorCommandIndex: t.errorCommandIndex,
      errorAbortCode: t.errorAbortCode,
      errorModule: t.errorModule,
      errorFunction: t.errorFunction,
      eventsDigest: t.eventsDigest,
      lamportVersion: t.lamportVersion,
      dependencyCount: t.dependencyCount,
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
    objectChanges: cp.objectChanges.map(o => ({
      txDigest: o.txDigest,
      objectId: o.objectId,
      changeType: o.changeType,
      objectType: o.objectType,
      inputVersion: o.inputVersion,
      inputDigest: o.inputDigest,
      inputOwner: o.inputOwner,
      inputOwnerKind: o.inputOwnerKind,
      outputVersion: o.outputVersion,
      outputDigest: o.outputDigest,
      outputOwner: o.outputOwner,
      outputOwnerKind: o.outputOwnerKind,
      isGasObject: o.isGasObject,
      checkpointSeq: o.checkpointSeq.toString(),
      timestamp: o.timestamp.toISOString(),
    })),
    dependencies: cp.dependencies.map(d => ({
      txDigest: d.txDigest,
      dependsOnDigest: d.dependsOnDigest,
      checkpointSeq: d.checkpointSeq.toString(),
      timestamp: d.timestamp.toISOString(),
    })),
    inputs: cp.inputs.map(i => ({
      txDigest: i.txDigest,
      inputIndex: i.inputIndex,
      kind: i.kind,
      objectId: i.objectId,
      version: i.version,
      digest: i.digest,
      mutability: i.mutability,
      initialSharedVersion: i.initialSharedVersion,
      pureBytes: i.pureBytes,
      amount: i.amount,
      coinType: i.coinType,
      source: i.source,
      checkpointSeq: i.checkpointSeq.toString(),
      timestamp: i.timestamp.toISOString(),
    })),
    commands: cp.commands.map(c => ({
      txDigest: c.txDigest,
      commandIndex: c.commandIndex,
      kind: c.kind,
      package: c.package,
      module: c.module,
      function: c.function,
      typeArguments: c.typeArguments,
      args: c.args,
      checkpointSeq: c.checkpointSeq.toString(),
      timestamp: c.timestamp.toISOString(),
    })),
    systemTransactions: cp.systemTransactions.map(s => ({
      txDigest: s.txDigest,
      kind: s.kind,
      data: s.data,
      checkpointSeq: s.checkpointSeq.toString(),
      timestamp: s.timestamp.toISOString(),
    })),
    unchangedConsensusObjects: cp.unchangedConsensusObjects.map(u => ({
      txDigest: u.txDigest,
      objectId: u.objectId,
      kind: u.kind,
      version: u.version,
      digest: u.digest,
      objectType: u.objectType,
      checkpointSeq: u.checkpointSeq.toString(),
      timestamp: u.timestamp.toISOString(),
    })),
  }));
}

/**
 * Deserialize a SerializedBatch[] back into ProcessedCheckpoint[].
 * Converts string -> bigint and ISO string -> Date.
 */
export function deserializeBatch(batches: SerializedBatch[]): ProcessedCheckpoint[] {
  return batches.map(sb => {
    // Build the checkpoint object. Transactions array is empty because tx data
    // is carried in the sibling arrays (transactions, moveCalls, etc).
    const base = emptyProcessed({
      sequenceNumber: BigInt(sb.checkpoint.sequenceNumber),
      timestamp: new Date(sb.checkpoint.timestamp),
      transactions: [],
      source: sb.checkpoint.source as "live" | "backfill",
      epoch: BigInt(sb.checkpoint.epoch),
      digest: sb.checkpoint.digest,
      previousDigest: sb.checkpoint.previousDigest,
      contentDigest: sb.checkpoint.contentDigest,
      totalNetworkTransactions: BigInt(sb.checkpoint.totalNetworkTransactions),
      epochRollingGasCostSummary: {
        computationCost: sb.checkpoint.rollingComputationCost,
        storageCost: sb.checkpoint.rollingStorageCost,
        storageRebate: sb.checkpoint.rollingStorageRebate,
        nonRefundableStorageFee: sb.checkpoint.rollingNonRefundableStorageFee,
      },
    });

    base.events = sb.events.map(e => ({
      handlerName: e.handlerName,
      checkpointSeq: BigInt(e.checkpointSeq),
      txDigest: e.txDigest,
      eventSeq: e.eventSeq,
      sender: e.sender,
      timestamp: new Date(e.timestamp),
      data: e.data,
    }));
    base.balanceChanges = sb.balanceChanges.map(b => ({
      txDigest: b.txDigest,
      checkpointSeq: BigInt(b.checkpointSeq),
      address: b.address,
      coinType: b.coinType,
      amount: b.amount,
      timestamp: new Date(b.timestamp),
    }));
    base.transactions = sb.transactions.map(t => ({
      digest: t.digest,
      sender: t.sender,
      success: t.success,
      computationCost: t.computationCost,
      storageCost: t.storageCost,
      storageRebate: t.storageRebate,
      nonRefundableStorageFee: t.nonRefundableStorageFee,
      checkpointSeq: BigInt(t.checkpointSeq),
      timestamp: new Date(t.timestamp),
      moveCallCount: t.moveCallCount,
      epoch: BigInt(t.epoch),
      errorKind: t.errorKind,
      errorDescription: t.errorDescription,
      errorCommandIndex: t.errorCommandIndex,
      errorAbortCode: t.errorAbortCode,
      errorModule: t.errorModule,
      errorFunction: t.errorFunction,
      eventsDigest: t.eventsDigest,
      lamportVersion: t.lamportVersion,
      dependencyCount: t.dependencyCount,
    }));
    base.moveCalls = sb.moveCalls.map(m => ({
      txDigest: m.txDigest,
      callIndex: m.callIndex,
      package: m.package,
      module: m.module,
      function: m.function,
      checkpointSeq: BigInt(m.checkpointSeq),
      timestamp: new Date(m.timestamp),
    }));
    base.objectChanges = sb.objectChanges.map(o => ({
      txDigest: o.txDigest,
      objectId: o.objectId,
      changeType: o.changeType as any,
      objectType: o.objectType,
      inputVersion: o.inputVersion,
      inputDigest: o.inputDigest,
      inputOwner: o.inputOwner,
      inputOwnerKind: o.inputOwnerKind,
      outputVersion: o.outputVersion,
      outputDigest: o.outputDigest,
      outputOwner: o.outputOwner,
      outputOwnerKind: o.outputOwnerKind,
      isGasObject: o.isGasObject,
      checkpointSeq: BigInt(o.checkpointSeq),
      timestamp: new Date(o.timestamp),
    }));
    base.dependencies = sb.dependencies.map(d => ({
      txDigest: d.txDigest,
      dependsOnDigest: d.dependsOnDigest,
      checkpointSeq: BigInt(d.checkpointSeq),
      timestamp: new Date(d.timestamp),
    }));
    base.inputs = sb.inputs.map(i => ({
      txDigest: i.txDigest,
      inputIndex: i.inputIndex,
      kind: i.kind,
      objectId: i.objectId,
      version: i.version,
      digest: i.digest,
      mutability: i.mutability,
      initialSharedVersion: i.initialSharedVersion,
      pureBytes: i.pureBytes,
      amount: i.amount,
      coinType: i.coinType,
      source: i.source,
      checkpointSeq: BigInt(i.checkpointSeq),
      timestamp: new Date(i.timestamp),
    }));
    base.commands = sb.commands.map(c => ({
      txDigest: c.txDigest,
      commandIndex: c.commandIndex,
      kind: c.kind,
      package: c.package,
      module: c.module,
      function: c.function,
      typeArguments: c.typeArguments,
      args: c.args,
      checkpointSeq: BigInt(c.checkpointSeq),
      timestamp: new Date(c.timestamp),
    }));
    base.systemTransactions = sb.systemTransactions.map(s => ({
      txDigest: s.txDigest,
      kind: s.kind,
      data: s.data,
      checkpointSeq: BigInt(s.checkpointSeq),
      timestamp: new Date(s.timestamp),
    }));
    base.unchangedConsensusObjects = sb.unchangedConsensusObjects.map(u => ({
      txDigest: u.txDigest,
      objectId: u.objectId,
      kind: u.kind,
      version: u.version,
      digest: u.digest,
      objectType: u.objectType,
      checkpointSeq: BigInt(u.checkpointSeq),
      timestamp: new Date(u.timestamp),
    }));

    return base;
  });
}
