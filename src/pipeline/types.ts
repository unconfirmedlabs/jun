/**
 * Core pipeline types: Source → Processor → Destination.
 *
 * These interfaces define the contract between pipeline components.
 * Each component is independent and composable.
 */
import type { GrpcTransaction } from "../grpc.ts";

// ---------------------------------------------------------------------------
// Checkpoint — the universal data unit flowing through the pipeline
// ---------------------------------------------------------------------------

/** A checkpoint as it flows through the pipeline. */
export interface Checkpoint {
  /** Checkpoint sequence number */
  sequenceNumber: bigint;
  /** Checkpoint timestamp */
  timestamp: Date;
  /** Transactions in this checkpoint */
  transactions: GrpcTransaction[];
  /** Source that produced this checkpoint */
  source: "live" | "backfill";
  /** Raw protobuf (available from archive source, needed for balance computation) */
  rawProto?: any;
  /** Pre-computed balance changes from archive worker (bypasses balance tracker) */
  precomputedBalanceChanges?: BalanceChange[];

  // ─── Checkpoint summary fields ────────────────────────────────────────────
  /** Epoch this checkpoint belongs to */
  epoch: bigint;
  /** Checkpoint digest (canonical base58 string) */
  digest: string;
  /** Previous checkpoint digest (null for checkpoint 0) */
  previousDigest: string | null;
  /** Content digest — hash of CheckpointContents */
  contentDigest: string | null;
  /** Cumulative transaction count across the entire network */
  totalNetworkTransactions: bigint;
  /** Epoch-to-date rolling gas cost summary */
  epochRollingGasCostSummary: {
    computationCost: string;
    storageCost: string;
    storageRebate: string;
    nonRefundableStorageFee: string;
  };
}

// ---------------------------------------------------------------------------
// Decoded data — output of processors
// ---------------------------------------------------------------------------

/** A decoded event from an event processor. */
export interface DecodedEvent {
  handlerName: string;
  checkpointSeq: bigint;
  txDigest: string;
  eventSeq: number;
  sender: string;
  timestamp: Date;
  data: Record<string, unknown>;
}

/** A balance change from a balance processor. */
export interface BalanceChange {
  txDigest: string;
  checkpointSeq: bigint;
  address: string;
  coinType: string;
  amount: string;
  timestamp: Date;
}

/** A transaction record. */
export interface TransactionRecord {
  digest: string;
  sender: string;
  success: boolean;
  computationCost: string;
  storageCost: string;
  storageRebate: string;
  nonRefundableStorageFee: string | null;
  checkpointSeq: bigint;
  timestamp: Date;
  moveCallCount: number;
  epoch: bigint;
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

/** A Move function call within a transaction. */
export interface MoveCallRecord {
  txDigest: string;
  callIndex: number;
  package: string;
  module: string;
  function: string;
  checkpointSeq: bigint;
  timestamp: Date;
}

/** A checkpoint-level record (one row per checkpoint). */
export interface CheckpointRecord {
  sequenceNumber: bigint;
  epoch: bigint;
  digest: string;
  previousDigest: string | null;
  contentDigest: string | null;
  timestamp: Date;
  totalNetworkTransactions: bigint;
  rollingComputationCost: string;
  rollingStorageCost: string;
  rollingStorageRebate: string;
  rollingNonRefundableStorageFee: string;
}

/** An object change from TransactionEffects.changedObjects. */
export interface ObjectChangeRecord {
  txDigest: string;
  objectId: string;
  /** Derived from (inputState, outputState, idOperation) */
  changeType: "CREATED" | "MUTATED" | "DELETED" | "WRAPPED" | "UNWRAPPED" | "PACKAGE_WRITE";
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
  checkpointSeq: bigint;
  timestamp: Date;
}

/** A transaction dependency edge. */
export interface TransactionDependencyRecord {
  txDigest: string;
  dependsOnDigest: string;
  checkpointSeq: bigint;
  timestamp: Date;
}

/** A programmable transaction input. */
export interface TransactionInputRecord {
  txDigest: string;
  inputIndex: number;
  kind: string;
  objectId: string | null;
  version: string | null;
  digest: string | null;
  mutability: string | null;
  initialSharedVersion: string | null;
  /** For PURE inputs: hex-encoded BCS bytes */
  pureBytes: string | null;
  /** For FUNDS_WITHDRAWAL */
  amount: string | null;
  coinType: string | null;
  source: string | null;
  checkpointSeq: bigint;
  timestamp: Date;
}

/** A command within a programmable transaction (superset of move_calls). */
export interface CommandRecord {
  txDigest: string;
  commandIndex: number;
  /** MoveCall | TransferObjects | SplitCoins | MergeCoins | Publish | Upgrade | MakeMoveVector */
  kind: string;
  package: string | null;
  module: string | null;
  function: string | null;
  /** JSON-serialized array of type arguments (MoveCall only) */
  typeArguments: string | null;
  /** JSON blob for command-specific arguments */
  args: string | null;
  checkpointSeq: bigint;
  timestamp: Date;
}

/** A system (non-programmable) transaction record. */
export interface SystemTransactionRecord {
  txDigest: string;
  /** GENESIS | CHANGE_EPOCH | CONSENSUS_COMMIT_PROLOGUE_V1..V4 | AUTHENTICATOR_STATE_UPDATE | END_OF_EPOCH | RANDOMNESS_STATE_UPDATE | PROGRAMMABLE_SYSTEM_TRANSACTION */
  kind: string;
  /** Full system transaction data as JSON */
  data: string;
  checkpointSeq: bigint;
  timestamp: Date;
}

/** A read-only consensus object reference. */
export interface UnchangedConsensusObjectRecord {
  txDigest: string;
  objectId: string;
  kind: string;
  version: string | null;
  digest: string | null;
  objectType: string | null;
  checkpointSeq: bigint;
  timestamp: Date;
}

/** The result of processing a checkpoint. */
export interface ProcessedCheckpoint {
  checkpoint: Checkpoint;
  events: DecodedEvent[];
  balanceChanges: BalanceChange[];
  transactions: TransactionRecord[];
  moveCalls: MoveCallRecord[];
  objectChanges: ObjectChangeRecord[];
  dependencies: TransactionDependencyRecord[];
  inputs: TransactionInputRecord[];
  commands: CommandRecord[];
  systemTransactions: SystemTransactionRecord[];
  unchangedConsensusObjects: UnchangedConsensusObjectRecord[];
}

/** Build an empty ProcessedCheckpoint wrapping the given checkpoint. */
export function emptyProcessed(checkpoint: Checkpoint): ProcessedCheckpoint {
  return {
    checkpoint,
    events: [],
    balanceChanges: [],
    transactions: [],
    moveCalls: [],
    objectChanges: [],
    dependencies: [],
    inputs: [],
    commands: [],
    systemTransactions: [],
    unchangedConsensusObjects: [],
  };
}

/**
 * Default summary fields for a Checkpoint. Use when sources don't (yet) populate
 * the full summary, and in test fixtures. Does NOT include sequenceNumber,
 * timestamp, transactions, or source — those are always required.
 */
export const DEFAULT_CHECKPOINT_SUMMARY: Pick<
  Checkpoint,
  "epoch" | "digest" | "previousDigest" | "contentDigest" | "totalNetworkTransactions" | "epochRollingGasCostSummary"
> = {
  epoch: 0n,
  digest: "",
  previousDigest: null,
  contentDigest: null,
  totalNetworkTransactions: 0n,
  epochRollingGasCostSummary: {
    computationCost: "0",
    storageCost: "0",
    storageRebate: "0",
    nonRefundableStorageFee: "0",
  },
};

// ---------------------------------------------------------------------------
// Source — where checkpoints come from
// ---------------------------------------------------------------------------

export interface Source {
  /** Source name for logging. */
  readonly name: string;
  /** Stream checkpoints as an async iterable. */
  stream(): AsyncIterable<Checkpoint>;
  /** Stop the source gracefully. */
  stop(): Promise<void>;
}

// ---------------------------------------------------------------------------
// Processor — transforms checkpoints into decoded data
// ---------------------------------------------------------------------------

export interface Processor {
  /** Processor name for logging. */
  readonly name: string;
  /** Initialize the processor (e.g., fetch field definitions from chain). */
  initialize?(): Promise<void>;
  /** Process a checkpoint, returning events and/or balance changes. */
  process(checkpoint: Checkpoint): ProcessedCheckpoint;
  /** Reconfigure the processor at runtime (for hot reload). */
  reload?(config: any): void;
}

// ---------------------------------------------------------------------------
// Storage — persistent, queryable destinations (batched, retried, cursor-tracked)
// ---------------------------------------------------------------------------

export interface Storage {
  /** Storage name for logging. */
  readonly name: string;
  /** Initialize (create tables, open connections). */
  initialize(): Promise<void>;
  /** Write a batch of processed checkpoints. Retried on failure. */
  write(batch: ProcessedCheckpoint[]): Promise<void>;
  /** Graceful shutdown (flush remaining data, close connections). */
  shutdown(): Promise<void>;
}

// ---------------------------------------------------------------------------
// Broadcast — ephemeral streaming destinations (fire-and-forget, no retry)
// ---------------------------------------------------------------------------

export interface Broadcast {
  /** Broadcast name for logging. */
  readonly name: string;
  /** Initialize (start server, connect). */
  initialize(): Promise<void>;
  /** Push a single processed checkpoint to listeners. Synchronous, no retry. */
  push(processed: ProcessedCheckpoint): void;
  /** Graceful shutdown (close connections). */
  shutdown(): Promise<void>;
}

// ---------------------------------------------------------------------------
// Pipeline — orchestrates Source → Processor → Storage + Broadcast
// ---------------------------------------------------------------------------

export interface PipelineConfig {
  /** Suppress human-readable output to stdout (default: false) */
  quiet?: boolean;
  /** Enable machine-readable JSON logs to stderr. Set to a level string (debug, info, warn, error) or true for info. */
  log?: boolean | string;
  /** Buffer config for batching writes */
  buffer?: {
    intervalMs?: number;
    maxBatchSize?: number;
    retries?: number;
  };
  /** Adaptive throttle config (for backfill sources) */
  throttle?: {
    initialConcurrency?: number;
    minConcurrency?: number;
    maxConcurrency?: number;
  };
  /** Gap repair config */
  gapRepair?: {
    enabled?: boolean;
    intervalMs?: number;
  };
  /** HTTP server config */
  serve?: {
    port: number;
    hostname?: string;
  };
  /** Remote config URL for hot reload */
  configUrl?: string;
  /** Auto-reload interval in milliseconds. Requires configUrl. */
  configAutoReloadMs?: number;
  /** Database URL (extracted from storage config). Needed for StateManager + HTTP /query. */
  database?: string;
  /** Network name (e.g. "testnet", "mainnet"). Used as cursor key namespace. */
  network?: string;
  /** Total checkpoints expected (for progress reporting in snapshot mode) */
  totalCheckpoints?: bigint;
  /** Progress callback — called periodically with current state */
  onProgress?: (info: {
    source: string;
    checkpoints: number;
    total?: number;
    rate: number;
    elapsedSecs: number;
  }) => void;
  /** Display format for human-readable output */
  display?: string;
}

export interface Pipeline {
  /** Add a source to the pipeline. */
  source(source: Source): Pipeline;
  /** Add a processor to the pipeline. */
  processor(processor: Processor): Pipeline;
  /** Add a storage destination (Postgres, SQLite). Batched, retried, cursor-tracked. */
  storage(storage: Storage): Pipeline;
  /** Add a broadcast destination (SSE, NATS, stdout). Fire-and-forget, low latency. */
  broadcast(broadcast: Broadcast): Pipeline;
  /** Configure pipeline behavior. */
  configure(config: PipelineConfig): Pipeline;
  /** Start the pipeline. */
  run(): Promise<void>;
  /** Stop the pipeline gracefully. */
  stop(): Promise<void>;
}
