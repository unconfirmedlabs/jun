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

/** The result of processing a checkpoint. */
export interface ProcessedCheckpoint {
  checkpoint: Checkpoint;
  events: DecodedEvent[];
  balanceChanges: BalanceChange[];
}

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
