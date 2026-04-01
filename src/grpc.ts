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
  "transactions.transaction",
  "transactions.balance_changes",
  "summary.timestamp",
];

const PROTO_LOAD_OPTIONS: protoLoader.Options = {
  keepCase: false,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
  includeDirs: [PROTO_DIR],
};

function loadProto() {
  // Subscription service
  const subDef = protoLoader.loadSync(
    path.join(PROTO_DIR, "sui/rpc/v2/subscription_service.proto"),
    PROTO_LOAD_OPTIONS,
  );
  const subProto = grpc.loadPackageDefinition(subDef) as any;

  // Ledger service (same proto dir, shares types)
  const ledgerDef = protoLoader.loadSync(
    path.join(PROTO_DIR, "sui/rpc/v2/ledger_service.proto"),
    PROTO_LOAD_OPTIONS,
  );
  const ledgerProto = grpc.loadPackageDefinition(ledgerDef) as any;

  // Move package service
  const moveDef = protoLoader.loadSync(
    path.join(PROTO_DIR, "sui/rpc/v2/move_package_service.proto"),
    PROTO_LOAD_OPTIONS,
  );
  const moveProto = grpc.loadPackageDefinition(moveDef) as any;

  return {
    SubscriptionService: subProto.sui.rpc.v2.SubscriptionService,
    LedgerService: ledgerProto.sui.rpc.v2.LedgerService,
    MovePackageService: moveProto.sui.rpc.v2.MovePackageService,
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

/** A balance change from a gRPC checkpoint response */
export interface GrpcBalanceChange {
  address: string;
  coinType: string;
  amount: string;
}

/** A transaction from a gRPC checkpoint response */
export interface GrpcTransaction {
  digest: string;
  events: {
    events: GrpcEvent[];
  } | null;
  balanceChanges?: GrpcBalanceChange[];
}

/** An OpenSignatureBody from a gRPC DatatypeDescriptor field */
export interface GrpcOpenSignatureBody {
  type: string; // enum string: "ADDRESS", "U64", "DATATYPE", "VECTOR", etc.
  typeName: string;
  typeParameterInstantiation: GrpcOpenSignatureBody[];
  typeParameter: number;
  /** proto-loader oneof wrapper — ignore */
  _type?: string;
  /** proto-loader oneof wrapper — ignore */
  _typeName?: string;
}

/** A field descriptor from a gRPC DatatypeDescriptor */
export interface GrpcFieldDescriptor {
  name: string;
  position: number;
  type: GrpcOpenSignatureBody;
}

/** A DatatypeDescriptor from the gRPC MovePackageService */
export interface GrpcDatatypeDescriptor {
  typeName: string;
  definingId: string;
  module: string;
  name: string;
  abilities: string[];
  typeParameters: Array<{ constraints: string[]; isPhantom: boolean }>;
  kind: string; // "STRUCT" | "ENUM" | ...
  fields: GrpcFieldDescriptor[];
  variants: Array<{
    name: string;
    position: number;
    fields: GrpcFieldDescriptor[];
  }>;
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

  /**
   * Fetch a datatype descriptor by package ID, module name, and type name.
   * Uses the MovePackageService.GetDatatype RPC.
   */
  getDatatype(packageId: string, moduleName: string, name: string): Promise<GrpcDatatypeDescriptor>;

  /** Close the underlying gRPC connections */
  close(): void;
}

export function createGrpcClient(options: GrpcClientOptions): GrpcClient {
  const { SubscriptionService, LedgerService, MovePackageService } = loadProto();
  const creds = grpc.credentials.createSsl();
  const readMask = options.readMask ?? READ_MASK_PATHS;

  // Normalize URL for @grpc/grpc-js: strip https://, ensure :443
  let addr = options.url.replace(/^https?:\/\//, "");
  if (!addr.includes(":")) addr += ":443";

  const subClient = new SubscriptionService(addr, creds);
  const ledgerClient = new LedgerService(addr, creds);
  const moveClient = new MovePackageService(addr, creds);

  return {
    subscribeCheckpoints(): AsyncIterable<GrpcCheckpointResponse> {
      return {
        [Symbol.asyncIterator]() {
          const call = subClient.SubscribeCheckpoints({
            readMask: { paths: readMask },
          });

          // Buffer incoming data events for the async iterator
          const MAX_BUFFER = 1024;
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
              if (buffer.length >= MAX_BUFFER) {
                call.pause();
              }
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
                const value = buffer.shift()!;
                if (buffer.length < MAX_BUFFER / 2) {
                  call.resume();
                }
                return Promise.resolve({ value, done: false });
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

    getDatatype(packageId: string, moduleName: string, name: string): Promise<GrpcDatatypeDescriptor> {
      return new Promise((resolve, reject) => {
        moveClient.GetDatatype(
          { packageId, moduleName, name },
          (err: any, response: any) => {
            if (err) {
              reject(err);
              return;
            }
            if (!response?.datatype) {
              reject(new Error(`Datatype not found: ${packageId}::${moduleName}::${name}`));
              return;
            }
            resolve(response.datatype);
          },
        );
      });
    },

    close() {
      subClient.close();
      ledgerClient.close();
      moveClient.close();
    },
  };
}
