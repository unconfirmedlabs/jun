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

export const DEFAULT_READ_MASK_PATHS = [
  "transactions.events",
  "transactions.digest",
  "transactions.transaction",
  "transactions.effects",
  "transactions.balance_changes",
  "summary.timestamp",
  "summary.epoch",
  "summary.digest",
  "summary.previous_digest",
  "summary.content_digest",
  "summary.total_network_transactions",
  "summary.epoch_rolling_gas_cost_summary",
];

export const RAW_CHECKPOINT_READ_MASK_PATHS = [
  "sequence_number",
  "digest",
  "summary.bcs",
  "transactions.digest",
  "transactions.transaction.bcs",
  "transactions.effects.bcs",
  "transactions.events.bcs",
  "transactions.balance_changes",
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
  const subscriptionProto = grpc.loadPackageDefinition(subDef) as any;

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
    SubscriptionService: subscriptionProto.sui.rpc.v2.SubscriptionService,
    LedgerService: ledgerProto.sui.rpc.v2.LedgerService,
    MovePackageService: moveProto.sui.rpc.v2.MovePackageService,
    subscriptionServiceDefinition: subDef["sui.rpc.v2.SubscriptionService"] as any,
    ledgerServiceDefinition: ledgerDef["sui.rpc.v2.LedgerService"] as any,
  };
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** A single event from a gRPC checkpoint response */
export interface CheckpointEvent {
  packageId: string;
  module: string;
  sender: string;
  eventType: string;
  /** Concrete type arguments from the event's StructTag.typeParams, formatted as
   *  fully qualified type strings (e.g., ["0x2::sui::SUI", "0x456::my_share::MY_SHARE"]).
   *  Populated by the archive source. For gRPC live events, not set — the event
   *  processor falls back to parsing from eventType string. */
  typeParams?: string[];
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

/** Owner of a Sui object — flattened representation used across records. */
export interface GrpcOwner {
  kind: "ADDRESS" | "OBJECT" | "SHARED" | "IMMUTABLE" | "CONSENSUS_ADDRESS";
  /** For ADDRESS/OBJECT/CONSENSUS_ADDRESS: the owner address. For SHARED: unused. */
  address?: string;
  /** For SHARED: initial_shared_version. For CONSENSUS_ADDRESS: start_version. */
  version?: string;
}

/** A changed object from TransactionEffects.changed_objects */
export interface GrpcChangedObject {
  objectId: string;
  inputState: "DOES_NOT_EXIST" | "EXISTS";
  inputVersion?: string;
  inputDigest?: string;
  inputOwner?: GrpcOwner;
  outputState: "DOES_NOT_EXIST" | "OBJECT_WRITE" | "PACKAGE_WRITE" | "ACCUMULATOR_WRITE";
  outputVersion?: string;
  outputDigest?: string;
  outputOwner?: GrpcOwner;
  idOperation: "NONE" | "CREATED" | "DELETED";
  objectType?: string;
  /** AccumulatorWriteV1 details when outputState === ACCUMULATOR_WRITE */
  accumulatorWrite?: {
    address: string;
    type: string;
    operation: "MERGE" | "SPLIT";
    value: string;
  };
}

/** An unchanged consensus object (read-only reference) from effects */
export interface GrpcUnchangedConsensusObject {
  kind: "READ_ONLY_ROOT" | "MUTATE_CONSENSUS_STREAM_ENDED" | "READ_CONSENSUS_STREAM_ENDED" | "CANCELED" | "PER_EPOCH_CONFIG";
  objectId: string;
  version?: string;
  digest?: string;
  objectType?: string;
}

/** A transaction input (from programmable transaction inputs[]) */
export interface GrpcInput {
  kind: "PURE" | "IMMUTABLE_OR_OWNED" | "SHARED" | "RECEIVING" | "FUNDS_WITHDRAWAL";
  /** For PURE: BCS-encoded argument bytes */
  pure?: Uint8Array;
  /** For IMMUTABLE_OR_OWNED/SHARED/RECEIVING: object reference */
  objectId?: string;
  version?: string;
  digest?: string;
  mutability?: "IMMUTABLE" | "MUTABLE" | "NON_EXCLUSIVE_WRITE";
  initialSharedVersion?: string;
  /** For FUNDS_WITHDRAWAL */
  amount?: string;
  coinType?: string;
  source?: "SENDER" | "SPONSOR";
}

/** Execution error details for failed transactions */
export interface GrpcExecutionError {
  kind: string;
  description?: string;
  commandIndex?: number;
  abortCode?: string;
  moveLocation?: {
    package: string;
    module: string;
    function: string;
    instruction?: number;
  };
}

/** System transaction kind discriminator. undefined for regular programmable transactions. */
export type GrpcSystemTransactionKind =
  | "GENESIS"
  | "CHANGE_EPOCH"
  | "CONSENSUS_COMMIT_PROLOGUE_V1"
  | "CONSENSUS_COMMIT_PROLOGUE_V2"
  | "CONSENSUS_COMMIT_PROLOGUE_V3"
  | "CONSENSUS_COMMIT_PROLOGUE_V4"
  | "AUTHENTICATOR_STATE_UPDATE"
  | "END_OF_EPOCH"
  | "RANDOMNESS_STATE_UPDATE"
  | "PROGRAMMABLE_SYSTEM_TRANSACTION";

/** A command within a programmable transaction. Exactly one of the fields will be set. */
export interface GrpcCommand {
  moveCall?: {
    package: string;
    module: string;
    function: string;
    typeArguments?: string[];
  };
  transferObjects?: {
    objects: any[];
    address: any;
  };
  splitCoins?: {
    coin: any;
    amounts: any[];
  };
  mergeCoins?: {
    coin: any;
    coins: any[];
  };
  publish?: {
    modules: Uint8Array[];
    dependencies: string[];
  };
  upgrade?: {
    modules: Uint8Array[];
    dependencies: string[];
    package: string;
    ticket: any;
  };
  makeMoveVector?: {
    elementType?: any;
    elements: any[];
  };
}

/** A transaction from a gRPC checkpoint response */
export interface GrpcTransaction {
  digest: string;
  transaction?: {
    sender: string;
    /** Discriminator for non-programmable (system) transactions. Undefined for regular PTBs. */
    systemKind?: GrpcSystemTransactionKind;
    /** Raw system transaction data as JSON (only populated when systemKind is set) */
    systemData?: any;
    programmableTransaction?: {
      commands?: GrpcCommand[];
    };
    // proto-loader may map under `kind` depending on proto version
    kind?: {
      programmableTransaction?: {
        commands?: GrpcCommand[];
      };
    };
    commands?: GrpcCommand[];
    /** Programmable transaction inputs (PURE, IMMUTABLE_OR_OWNED, SHARED, RECEIVING, FUNDS_WITHDRAWAL) */
    inputs?: GrpcInput[];
    gasPayment?: {
      objects: Array<{ objectId: string; version: string; digest: string }>;
      owner: string;
      price: string;
      budget: string;
    };
    expiration?: {
      kind: "NONE" | "EPOCH" | "VALID_DURING";
      epoch?: string;
      minEpoch?: string;
      maxEpoch?: string;
      minTimestamp?: string;
      maxTimestamp?: string;
      chain?: string;
      nonce?: string;
    };
  };
  events: {
    events: CheckpointEvent[];
  } | null;
  effects?: {
    status: { success?: boolean; error?: GrpcExecutionError };
    gasUsed?: {
      computationCost: string;
      storageCost: string;
      storageRebate: string;
      nonRefundableStorageFee?: string;
    };
    epoch?: string;
    dependencies?: string[];
    changedObjects?: GrpcChangedObject[];
    unchangedConsensusObjects?: GrpcUnchangedConsensusObject[];
    eventsDigest?: string;
    lamportVersion?: string;
  };
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

/** Checkpoint summary fields (from CheckpointSummary proto / BCS) */
export interface GrpcCheckpointSummary {
  timestamp: {
    seconds: string;
    nanos: number;
  };
  epoch?: string;
  digest?: string;
  previousDigest?: string;
  contentDigest?: string;
  totalNetworkTransactions?: string;
  epochRollingGasCostSummary?: {
    computationCost: string;
    storageCost: string;
    storageRebate: string;
    nonRefundableStorageFee: string;
  };
}

/** A checkpoint response from gRPC */
export interface GrpcCheckpointResponse {
  cursor: string; // checkpoint sequence number as string (u64)
  checkpoint: {
    sequenceNumber: string;
    summary: GrpcCheckpointSummary | null;
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

/** Service info response from GetServiceInfo */
export interface GrpcServiceInfo {
  chainId?: string;
  chain?: string;
  epoch?: string;
  checkpointHeight?: string;
  lowestAvailableCheckpoint?: string;
}

/** Epoch info response from GetEpoch */
export interface GrpcEpochInfo {
  epoch?: string;
  firstCheckpoint?: string;
  lastCheckpoint?: string;
}

export interface GrpcClient {
  /**
   * Subscribe to live checkpoints. Returns an async iterable that yields
   * checkpoint responses as they arrive from the fullnode.
   */
  subscribeCheckpoints(): AsyncIterable<GrpcCheckpointResponse>;

  /**
   * Subscribe to live checkpoints as raw protobuf response bytes.
   */
  subscribeCheckpointsRaw(readMask?: string[]): AsyncIterable<Uint8Array>;

  /**
   * Fetch a single checkpoint by sequence number. Used for backfill.
   */
  getCheckpoint(seq: bigint): Promise<GrpcCheckpointResponse>;

  /**
   * Fetch a single checkpoint as raw GetCheckpointResponse protobuf bytes.
   */
  getCheckpointRaw(seq: bigint, readMask?: string[]): Promise<Uint8Array>;

  /**
   * Fetch a datatype descriptor by package ID, module name, and type name.
   * Uses the MovePackageService.GetDatatype RPC.
   */
  getDatatype(packageId: string, moduleName: string, name: string): Promise<GrpcDatatypeDescriptor>;

  /**
   * Get service info (current epoch, checkpoint height, etc).
   */
  getServiceInfo(): Promise<GrpcServiceInfo>;

  /**
   * Get epoch info. If no epoch is provided, returns the current epoch.
   */
  getEpoch(epoch?: bigint): Promise<GrpcEpochInfo>;

  /** Close the underlying gRPC connections */
  close(): void;
}

export function createGrpcClient(options: GrpcClientOptions): GrpcClient {
  const {
    SubscriptionService,
    LedgerService,
    MovePackageService,
    subscriptionServiceDefinition,
    ledgerServiceDefinition,
  } = loadProto();
  const creds = grpc.credentials.createSsl();
  const readMask = options.readMask ?? DEFAULT_READ_MASK_PATHS;

  // Normalize URL for @grpc/grpc-js: strip https://, ensure :443
  let addr = options.url.replace(/^https?:\/\//, "");
  if (!addr.includes(":")) addr += ":443";

  const RawSubscriptionService = grpc.makeGenericClientConstructor({
    SubscribeCheckpoints: {
      ...subscriptionServiceDefinition.SubscribeCheckpoints,
      responseDeserialize: (buf: Buffer) =>
        new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength),
    },
  }, "RawSubscriptionService") as any;

  const RawLedgerService = grpc.makeGenericClientConstructor({
    GetCheckpoint: {
      ...ledgerServiceDefinition.GetCheckpoint,
      responseDeserialize: (buf: Buffer) =>
        new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength),
    },
  }, "RawLedgerService") as any;

  const subClient = new SubscriptionService(addr, creds);
  const ledgerClient = new LedgerService(addr, creds);
  const moveClient = new MovePackageService(addr, creds);
  const rawSubClient = new RawSubscriptionService(addr, creds);
  const rawLedgerClient = new RawLedgerService(addr, creds);

  function streamToAsyncIterable<T>(call: any): AsyncIterable<T> {
    return {
      [Symbol.asyncIterator]() {
        const MAX_BUFFER = 1024;
        const buffer: T[] = [];
        let waiting: {
          resolve: (value: IteratorResult<T>) => void;
          reject: (err: Error) => void;
        } | null = null;
        let done = false;

        call.on("data", (response: T) => {
          if (waiting) {
            const waiter = waiting;
            waiting = null;
            waiter.resolve({ value: response, done: false });
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
              const waiter = waiting;
              waiting = null;
              waiter.resolve({ value: undefined!, done: true as const });
            }
            return;
          }
          if (waiting) {
            const waiter = waiting;
            waiting = null;
            waiter.reject(err);
          }
        });

        call.on("end", () => {
          done = true;
          if (waiting) {
            const waiter = waiting;
            waiting = null;
            waiter.resolve({ value: undefined!, done: true as const });
          }
        });

        return {
          next(): Promise<IteratorResult<T>> {
            if (buffer.length > 0) {
              const value = buffer.shift()!;
              if (buffer.length < MAX_BUFFER / 2) {
                call.resume();
              }
              return Promise.resolve({ value, done: false });
            }
            if (done) {
              return Promise.resolve({ value: undefined!, done: true as const });
            }
            return new Promise((resolve, reject) => {
              waiting = { resolve, reject };
            });
          },
          return(): Promise<IteratorResult<T>> {
            call.cancel();
            done = true;
            return Promise.resolve({ value: undefined!, done: true as const });
          },
        };
      },
    };
  }

  return {
    subscribeCheckpoints(): AsyncIterable<GrpcCheckpointResponse> {
      const call = subClient.SubscribeCheckpoints({
        readMask: { paths: readMask },
      });
      return streamToAsyncIterable<GrpcCheckpointResponse>(call);
    },

    subscribeCheckpointsRaw(rawReadMask = RAW_CHECKPOINT_READ_MASK_PATHS): AsyncIterable<Uint8Array> {
      const call = rawSubClient.SubscribeCheckpoints({
        readMask: { paths: rawReadMask },
      });
      return streamToAsyncIterable<Uint8Array>(call);
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

    getCheckpointRaw(seq: bigint, rawReadMask = RAW_CHECKPOINT_READ_MASK_PATHS): Promise<Uint8Array> {
      return new Promise((resolve, reject) => {
        rawLedgerClient.GetCheckpoint(
          {
            sequenceNumber: seq.toString(),
            readMask: { paths: rawReadMask },
          },
          (err: any, response: Uint8Array) => {
            if (err) {
              reject(err);
              return;
            }
            resolve(response);
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

    getServiceInfo(): Promise<GrpcServiceInfo> {
      return new Promise((resolve, reject) => {
        ledgerClient.GetServiceInfo({}, (err: any, response: any) => {
          if (err) {
            reject(err);
            return;
          }
          resolve(response);
        });
      });
    },

    getEpoch(epoch?: bigint): Promise<GrpcEpochInfo> {
      return new Promise((resolve, reject) => {
        const request: any = {
          readMask: { paths: ["epoch", "first_checkpoint", "last_checkpoint"] },
        };
        if (epoch !== undefined) {
          request.epoch = epoch.toString();
        }
        ledgerClient.GetEpoch(request, (err: any, response: any) => {
          if (err) {
            reject(err);
            return;
          }
          if (!response?.epoch) {
            reject(new Error(`Epoch not found${epoch !== undefined ? `: ${epoch}` : ""}`));
            return;
          }
          resolve(response.epoch);
        });
      });
    },

    close() {
      subClient.close();
      ledgerClient.close();
      moveClient.close();
      rawSubClient.close();
      rawLedgerClient.close();
    },
  };
}
