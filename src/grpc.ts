/**
 * gRPC client wrapper for Sui checkpoint streaming and fetching.
 *
 * Uses @grpc/grpc-js with native proto loading for real HTTP/2 streaming.
 * Provides two operations:
 * - subscribeCheckpoints() → async iterable of checkpoint responses (live mode)
 * - getCheckpoint(seq) → single checkpoint response (backfill mode)
 */
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";

const PROTO_DIR = path.join(import.meta.dir, "..", "proto");

// ---------------------------------------------------------------------------
// Proto loading
// ---------------------------------------------------------------------------

const READ_MASK_PATHS = [
  "transactions.events",
  "transactions.digest",
  "summary.timestamp",
];

function loadProto() {
  // Subscription service
  const subDef = protoLoader.loadSync(
    path.join(PROTO_DIR, "sui/rpc/v2/subscription_service.proto"),
    {
      keepCase: false,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
      includeDirs: [PROTO_DIR],
    },
  );
  const subProto = grpc.loadPackageDefinition(subDef) as any;

  // Ledger service (same proto dir, shares types)
  const ledgerDef = protoLoader.loadSync(
    path.join(PROTO_DIR, "sui/rpc/v2/ledger_service.proto"),
    {
      keepCase: false,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
      includeDirs: [PROTO_DIR],
    },
  );
  const ledgerProto = grpc.loadPackageDefinition(ledgerDef) as any;

  return {
    SubscriptionService: subProto.sui.rpc.v2.SubscriptionService,
    LedgerService: ledgerProto.sui.rpc.v2.LedgerService,
  };
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** A single event from a gRPC checkpoint response */
export interface GrpcEvent {
  packageId: string;
  module: string;
  sender: string;
  eventType: string;
  contents: {
    name: string;
    value: Uint8Array;
  };
}

/** A transaction from a gRPC checkpoint response */
export interface GrpcTransaction {
  digest: string;
  events: {
    events: GrpcEvent[];
  } | null;
}

/** A checkpoint response from gRPC */
export interface GrpcCheckpointResponse {
  cursor: string; // checkpoint sequence number as string (u64)
  checkpoint: {
    sequenceNumber: string;
    summary: {
      timestamp: {
        seconds: string;
        nanos: number;
      };
    } | null;
    transactions: GrpcTransaction[];
  };
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

export interface GrpcClientOptions {
  url: string;
  /** Optional read mask paths. Defaults to transactions.events, transactions.digest, summary.timestamp */
  readMask?: string[];
}

export interface GrpcClient {
  /**
   * Subscribe to live checkpoints. Returns an async iterable that yields
   * checkpoint responses as they arrive from the fullnode.
   */
  subscribeCheckpoints(): AsyncIterable<GrpcCheckpointResponse>;

  /**
   * Fetch a single checkpoint by sequence number. Used for backfill.
   */
  getCheckpoint(seq: bigint): Promise<GrpcCheckpointResponse>;

  /** Close the underlying gRPC connections */
  close(): void;
}

export function createGrpcClient(options: GrpcClientOptions): GrpcClient {
  const { SubscriptionService, LedgerService } = loadProto();
  const creds = grpc.credentials.createSsl();
  const readMask = options.readMask ?? READ_MASK_PATHS;

  const subClient = new SubscriptionService(options.url, creds);
  const ledgerClient = new LedgerService(options.url, creds);

  return {
    subscribeCheckpoints(): AsyncIterable<GrpcCheckpointResponse> {
      return {
        [Symbol.asyncIterator]() {
          const call = subClient.SubscribeCheckpoints({
            readMask: { paths: readMask },
          });

          // Buffer incoming data events for the async iterator
          const buffer: GrpcCheckpointResponse[] = [];
          let waiting: {
            resolve: (value: IteratorResult<GrpcCheckpointResponse>) => void;
            reject: (err: Error) => void;
          } | null = null;
          let done = false;

          call.on("data", (response: GrpcCheckpointResponse) => {
            if (waiting) {
              const w = waiting;
              waiting = null;
              w.resolve({ value: response, done: false });
            } else {
              buffer.push(response);
            }
          });

          call.on("error", (err: any) => {
            done = true;
            if (err.code === grpc.status.CANCELLED) {
              if (waiting) {
                const w = waiting;
                waiting = null;
                w.resolve({ value: undefined as any, done: true });
              }
              return;
            }
            if (waiting) {
              const w = waiting;
              waiting = null;
              w.reject(err);
            }
          });

          call.on("end", () => {
            done = true;
            if (waiting) {
              const w = waiting;
              waiting = null;
              w.resolve({ value: undefined as any, done: true });
            }
          });

          return {
            next(): Promise<IteratorResult<GrpcCheckpointResponse>> {
              if (buffer.length > 0) {
                return Promise.resolve({ value: buffer.shift()!, done: false });
              }
              if (done) {
                return Promise.resolve({ value: undefined as any, done: true });
              }
              return new Promise((resolve, reject) => {
                waiting = { resolve, reject };
              });
            },
            return(): Promise<IteratorResult<GrpcCheckpointResponse>> {
              call.cancel();
              done = true;
              return Promise.resolve({ value: undefined as any, done: true });
            },
          };
        },
      };
    },

    getCheckpoint(seq: bigint): Promise<GrpcCheckpointResponse> {
      return new Promise((resolve, reject) => {
        ledgerClient.GetCheckpoint(
          {
            sequenceNumber: seq.toString(),
            readMask: { paths: readMask },
          },
          (err: any, response: any) => {
            if (err) {
              reject(err);
              return;
            }
            // GetCheckpoint returns { checkpoint }, wrap to match subscription shape
            resolve({
              cursor: response.checkpoint?.sequenceNumber ?? seq.toString(),
              checkpoint: response.checkpoint,
            });
          },
        );
      });
    },

    close() {
      subClient.close();
      ledgerClient.close();
    },
  };
}
