/**
 * Sui checkpoint archive client.
 *
 * Fetches checkpoints from the public archive (https://checkpoints.mainnet.sui.io/)
 * which has every checkpoint from genesis. No pruning, no rate limits.
 *
 * Flow: HTTP GET → zstd decompress → protobufjs decode → @mysten/sui/bcs decode
 * (summary + effects + events). Pure Mysten baseline — no fast paths.
 */
import { zstdDecompressSync } from "zlib";
import { suiBcs, bcs } from "./bcs-provider.ts";
import { verifyCheckpoint as keiVerify, PreparedCommittee } from "@unconfirmed/kei";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import protobuf from "protobufjs";
import path from "path";
import {
  decodeArchiveCheckpointCompressedNative,
  isNativeCheckpointDecoderAvailable,
} from "./checkpoint-native-decoder.ts";
import type {
  GrpcCheckpointResponse,
  CheckpointEvent,
  GrpcTransaction,
  GrpcChangedObject,
  GrpcOwner,
  GrpcUnchangedConsensusObject,
  GrpcInput,
  GrpcCommand,
  GrpcSystemTransactionKind,
  GrpcExecutionError,
} from "./grpc.ts";
import { parseSender, TransactionData } from "./sui-bcs.ts";

// ─── Event parsing via @mysten/sui/bcs ──────────────────────────────────────

const NativeSuiEvent = suiBcs.struct("Event", {
  packageId: suiBcs.Address,
  transactionModule: suiBcs.String,
  sender: suiBcs.Address,
  type: suiBcs.StructTag,
  contents: suiBcs.vector(suiBcs.U8),
});

const NativeTransactionEvents = suiBcs.struct("TransactionEvents", {
  data: suiBcs.vector(NativeSuiEvent),
});

/** Format a TypeTag enum value to a string. */
function formatTypeTag(tag: any): string {
  if (typeof tag === "string") return tag.toLowerCase();
  if (tag.address) {
    const params = (tag.typeParams ?? []).map((tp: any) => formatTypeTag(tp));
    const suffix = params.length > 0 ? `<${params.join(", ")}>` : "";
    return `${tag.address}::${tag.module}::${tag.name}${suffix}`;
  }
  for (const [key, val] of Object.entries(tag)) {
    if (val === true || val === null) return key.toLowerCase();
    return formatTypeTag(val);
  }
  return "unknown";
}

/** Parse events using @mysten/sui/bcs. */
function parseEvents(eventsBcs: Uint8Array): CheckpointEvent[] {
  const decoded = NativeTransactionEvents.parse(eventsBcs);
  return decoded.data.map((ev: any) => {
    const typeParams = (ev.type.typeParams ?? []).map((tp: any) => formatTypeTag(tp));
    const typeSuffix = typeParams.length > 0 ? `<${typeParams.join(", ")}>` : "";
    const eventType = `${ev.type.address}::${ev.type.module}::${ev.type.name}${typeSuffix}`;
    return {
      packageId: ev.packageId,
      module: ev.transactionModule,
      sender: ev.sender,
      eventType,
      typeParams,
      contents: { name: eventType, value: new Uint8Array(ev.contents) },
    };
  });
}

// ─── CheckpointSummary decoding ─────────────────────────────────────────────

const SuiDigest = bcs.vector(bcs.u8());

const GasCostSummary = bcs.struct("GasCostSummary", {
  computationCost: bcs.u64(),
  storageCost: bcs.u64(),
  storageRebate: bcs.u64(),
  nonRefundableStorageFee: bcs.u64(),
});

const CheckpointSummary = bcs.struct("CheckpointSummary", {
  epoch: bcs.u64(),
  sequenceNumber: bcs.u64(),
  networkTotalTransactions: bcs.u64(),
  contentDigest: SuiDigest,
  previousDigest: bcs.option(SuiDigest),
  epochRollingGasCostSummary: GasCostSummary,
  timestampMs: bcs.u64(),
  // Remaining fields (checkpoint_commitments, end_of_epoch_data, version_specific_data)
  // are not needed — BCS parse stops here.
});

// ─── Effects extraction via @mysten/sui/bcs ────────────────────────────────

/** Inferred output type from @mysten/sui/bcs TransactionEffects schema. */
type ParsedEffects = ReturnType<typeof suiBcs.TransactionEffects.parse>;
/** Inferred Owner enum output type. */
type ParsedOwner = ReturnType<typeof suiBcs.Owner.parse>;

/** Convert string|undefined|"" to string|undefined, rejecting empty strings
 *  (Postgres NUMERIC columns reject the empty-string literal). */
function strOrUndef(v: string | number | bigint | null | undefined): string | undefined {
  if (v == null || v === "") return undefined;
  return String(v);
}

/** Flatten a Mysten Owner enum into the GrpcOwner shape. */
function flattenOwner(owner: ParsedOwner | undefined): GrpcOwner | undefined {
  if (!owner) return undefined;
  switch (owner.$kind) {
    case "AddressOwner": return { kind: "ADDRESS", address: owner.AddressOwner };
    case "ObjectOwner": return { kind: "OBJECT", address: owner.ObjectOwner };
    case "Shared": return { kind: "SHARED", version: String(owner.Shared) };
    case "Immutable": return { kind: "IMMUTABLE" };
    case "ConsensusV2": {
      const inner = owner.ConsensusV2;
      return { kind: "CONSENSUS_ADDRESS", address: inner.owner, version: String(inner.startVersion) };
    }
    default: return undefined;
  }
}

/**
 * Extract effects-level data from a TransactionEffects BCS blob.
 * Returns the full effects object for GrpcTransaction.effects, or null on parse failure.
 */
function extractEffects(effectsBcs: Uint8Array): {
  digest: string;
  effects: NonNullable<GrpcTransaction["effects"]>;
} | null {
  try {
    const parsed = suiBcs.TransactionEffects.parse(effectsBcs);
    if (parsed.$kind !== "V2") return null;
    const v2 = parsed.V2;

    // Status + error
    const success = v2.status.$kind === "Success";
    let error: GrpcExecutionError | undefined;
    if (v2.status.$kind === "Failed") {
      const failed = v2.status.Failed;
      const errorEnum = failed.error;
      error = { kind: errorEnum.$kind };
      if (failed.command != null) error.commandIndex = Number(failed.command);
      if (errorEnum.$kind === "MoveAbort") {
        const [location, abortCode] = errorEnum.MoveAbort;
        error.abortCode = String(abortCode);
        error.moveLocation = {
          package: location.module.address,
          module: location.module.name,
          function: location.functionName,
          instruction: location.instruction != null ? Number(location.instruction) : undefined,
        };
      }
    }

    // Changed objects
    const changedObjects: GrpcChangedObject[] = [];
    for (const [objectId, change] of v2.changedObjects) {
      const inputExists = change.inputState.$kind === "Exist";

      // Exist = [[version, digest], Owner]
      let inputVersion: string | undefined;
      let inputDigest: string | undefined;
      let inputOwner: GrpcOwner | undefined;
      if (inputExists) {
        const [[version, digest], owner] = change.inputState.Exist;
        inputVersion = strOrUndef(version);
        inputDigest = strOrUndef(digest);
        inputOwner = flattenOwner(owner);
      }

      let outputState: GrpcChangedObject["outputState"] = "DOES_NOT_EXIST";
      let outputVersion: string | undefined;
      let outputDigest: string | undefined;
      let outputOwner: GrpcOwner | undefined;
      let accumulatorWrite: GrpcChangedObject["accumulatorWrite"];

      switch (change.outputState.$kind) {
        case "ObjectWrite": {
          outputState = "OBJECT_WRITE";
          const [digest, owner] = change.outputState.ObjectWrite;
          outputDigest = strOrUndef(digest);
          outputOwner = flattenOwner(owner);
          break;
        }
        case "PackageWrite": {
          outputState = "PACKAGE_WRITE";
          const [version, digest] = change.outputState.PackageWrite;
          outputVersion = strOrUndef(version);
          outputDigest = strOrUndef(digest);
          break;
        }
        case "AccumulatorWriteV1": {
          outputState = "ACCUMULATOR_WRITE";
          const write = change.outputState.AccumulatorWriteV1;
          if (write.address) {
            const amount = write.value.$kind === "Integer" ? String(write.value.Integer) : "0";
            accumulatorWrite = {
              address: String(write.address.address),
              type: String(write.address.ty),
              operation: write.operation.$kind === "Split" ? "SPLIT" : "MERGE",
              value: amount,
            };
          }
          break;
        }
        // case "NotExist": outputState stays "DOES_NOT_EXIST"
      }

      const idOpKind = change.idOperation.$kind;
      const idOperation: GrpcChangedObject["idOperation"] =
        idOpKind === "Created" ? "CREATED" : idOpKind === "Deleted" ? "DELETED" : "NONE";

      changedObjects.push({
        objectId,
        inputState: inputExists ? "EXISTS" : "DOES_NOT_EXIST",
        inputVersion,
        inputDigest,
        inputOwner,
        outputState,
        outputVersion,
        outputDigest,
        outputOwner,
        idOperation,
        accumulatorWrite,
      });
    }

    // Unchanged consensus objects
    const unchangedConsensusObjects: GrpcUnchangedConsensusObject[] = [];
    for (const [objectId, sharedKind] of v2.unchangedConsensusObjects) {
      let kind: GrpcUnchangedConsensusObject["kind"] = "READ_ONLY_ROOT";
      let version: string | undefined;
      let digest: string | undefined;

      switch (sharedKind.$kind) {
        case "ReadOnlyRoot": {
          kind = "READ_ONLY_ROOT";
          const [v, d] = sharedKind.ReadOnlyRoot;
          version = strOrUndef(v);
          digest = strOrUndef(d);
          break;
        }
        case "MutateConsensusStreamEnded":
          kind = "MUTATE_CONSENSUS_STREAM_ENDED";
          version = strOrUndef(sharedKind.MutateConsensusStreamEnded);
          break;
        case "ReadConsensusStreamEnded":
          kind = "READ_CONSENSUS_STREAM_ENDED";
          version = strOrUndef(sharedKind.ReadConsensusStreamEnded);
          break;
        case "Cancelled":
          kind = "CANCELED";
          version = strOrUndef(sharedKind.Cancelled);
          break;
        case "PerEpochConfig":
          kind = "PER_EPOCH_CONFIG";
          break;
      }
      unchangedConsensusObjects.push({ kind, objectId, version, digest });
    }

    return {
      digest: v2.transactionDigest,
      effects: {
        status: { success, error },
        gasUsed: {
          computationCost: String(v2.gasUsed.computationCost),
          storageCost: String(v2.gasUsed.storageCost),
          storageRebate: String(v2.gasUsed.storageRebate),
          nonRefundableStorageFee: strOrUndef(v2.gasUsed.nonRefundableStorageFee),
        },
        epoch: strOrUndef(v2.executedEpoch),
        dependencies: v2.dependencies.map(String),
        changedObjects,
        unchangedConsensusObjects,
        eventsDigest: strOrUndef(v2.eventsDigest),
        lamportVersion: strOrUndef(v2.lamportVersion),
      },
    };
  } catch {
    return null;
  }
}

// ─── Transaction data extraction via local TransactionData schema ──────────

/** Map a TransactionKind $kind discriminator to our GrpcSystemTransactionKind. */
function mapSystemKind(kind: string): GrpcSystemTransactionKind | undefined {
  switch (kind) {
    case "ProgrammableTransaction": return undefined;
    case "Genesis": return "GENESIS";
    case "ChangeEpoch": return "CHANGE_EPOCH";
    case "ConsensusCommitPrologue": return "CONSENSUS_COMMIT_PROLOGUE_V1";
    case "ConsensusCommitPrologueV2": return "CONSENSUS_COMMIT_PROLOGUE_V2";
    case "ConsensusCommitPrologueV3": return "CONSENSUS_COMMIT_PROLOGUE_V3";
    case "ConsensusCommitPrologueV4": return "CONSENSUS_COMMIT_PROLOGUE_V4";
    case "AuthenticatorStateUpdate": return "AUTHENTICATOR_STATE_UPDATE";
    case "EndOfEpochTransaction": return "END_OF_EPOCH";
    case "RandomnessStateUpdate": return "RANDOMNESS_STATE_UPDATE";
    case "ProgrammableSystemTransaction": return "PROGRAMMABLE_SYSTEM_TRANSACTION";
    default: return undefined;
  }
}

/** Extract a PTB command into the GrpcCommand shape (one of the variants is set). */
function extractCommand(cmd: any): GrpcCommand {
  const kind = cmd?.$kind;
  switch (kind) {
    case "MoveCall": {
      const mc = cmd.MoveCall;
      return {
        moveCall: {
          package: String(mc?.package ?? ""),
          module: String(mc?.module ?? ""),
          function: String(mc?.function ?? ""),
          typeArguments: (mc?.typeArguments ?? []).map((t: any) => String(t)),
        },
      };
    }
    case "TransferObjects":
      return { transferObjects: { objects: cmd.TransferObjects?.[0] ?? [], address: cmd.TransferObjects?.[1] } };
    case "SplitCoins":
      return { splitCoins: { coin: cmd.SplitCoins?.[0], amounts: cmd.SplitCoins?.[1] ?? [] } };
    case "MergeCoins":
      return { mergeCoins: { coin: cmd.MergeCoins?.[0], coins: cmd.MergeCoins?.[1] ?? [] } };
    case "Publish": {
      const p = cmd.Publish;
      return {
        publish: {
          modules: (p?.[0] ?? []).map((m: any) => new Uint8Array(m)),
          dependencies: (p?.[1] ?? []).map((d: any) => String(d)),
        },
      };
    }
    case "Upgrade": {
      const u = cmd.Upgrade;
      return {
        upgrade: {
          modules: (u?.[0] ?? []).map((m: any) => new Uint8Array(m)),
          dependencies: (u?.[1] ?? []).map((d: any) => String(d)),
          package: String(u?.[2] ?? ""),
          ticket: u?.[3],
        },
      };
    }
    case "MakeMoveVec":
    case "MakeMoveVector":
      return { makeMoveVector: { elementType: cmd[kind]?.[0], elements: cmd[kind]?.[1] ?? [] } };
    default:
      return { moveCall: undefined };
  }
}

/** Extract a CallArg (PTB input) into the GrpcInput shape. */
function extractInput(inp: any): GrpcInput {
  const kind = inp?.$kind;
  switch (kind) {
    case "Pure":
      return { kind: "PURE", pure: new Uint8Array(inp.Pure ?? []) };
    case "Object": {
      const objKind = inp.Object?.$kind;
      if (objKind === "ImmOrOwnedObject") {
        const o = inp.Object.ImmOrOwnedObject;
        return {
          kind: "IMMUTABLE_OR_OWNED",
          objectId: String(o?.[0] ?? ""),
          version: String(o?.[1] ?? ""),
          digest: String(o?.[2] ?? ""),
        };
      }
      if (objKind === "SharedObject") {
        const s = inp.Object.SharedObject;
        return {
          kind: "SHARED",
          objectId: String(s?.objectId ?? s?.id ?? ""),
          initialSharedVersion: String(s?.initialSharedVersion ?? ""),
          mutability: s?.mutable ? "MUTABLE" : "IMMUTABLE",
        };
      }
      if (objKind === "Receiving") {
        const r = inp.Object.Receiving;
        return {
          kind: "RECEIVING",
          objectId: String(r?.[0] ?? ""),
          version: String(r?.[1] ?? ""),
          digest: String(r?.[2] ?? ""),
        };
      }
      return { kind: "IMMUTABLE_OR_OWNED" };
    }
    case "BalanceWithdraw":
    case "FundsWithdrawal": {
      const w = inp.BalanceWithdraw ?? inp.FundsWithdrawal;
      return {
        kind: "FUNDS_WITHDRAWAL",
        amount: w?.amount?.Entire != null ? String(w.amount.Entire) : undefined,
        coinType: String(w?.typeParam ?? w?.coinType ?? ""),
        source: w?.reservation?.$kind === "EntireSponsor" ? "SPONSOR" : "SENDER",
      };
    }
    default:
      return { kind: "PURE" };
  }
}

/**
 * Extract the transaction-level part of a GrpcTransaction from TransactionData BCS.
 * Returns sender + commands + inputs + system kind + gas + expiration.
 */
function extractTransaction(txDataBcs: Uint8Array): NonNullable<GrpcTransaction["transaction"]> | null {
  try {
    const parsed: any = TransactionData.parse(txDataBcs);
    const v1 = parsed.V1;
    if (!v1) return null;

    const kindKind = v1.kind?.$kind;
    const systemKind = mapSystemKind(kindKind);

    // Commands + inputs for programmable transactions
    let commands: GrpcCommand[] | undefined;
    let inputs: GrpcInput[] | undefined;
    if (kindKind === "ProgrammableTransaction" || kindKind === "ProgrammableSystemTransaction") {
      const ptb = v1.kind.ProgrammableTransaction ?? v1.kind.ProgrammableSystemTransaction;
      if (ptb) {
        commands = (ptb.commands ?? []).map(extractCommand);
        inputs = (ptb.inputs ?? []).map(extractInput);
      }
    }

    // Gas payment
    const gasData = v1.gasData;
    const gasPayment = gasData
      ? {
          objects: (gasData.payment ?? []).map((p: any) => ({
            objectId: String(p?.[0] ?? ""),
            version: String(p?.[1] ?? ""),
            digest: String(p?.[2] ?? ""),
          })),
          owner: String(gasData.owner ?? ""),
          price: String(gasData.price ?? "0"),
          budget: String(gasData.budget ?? "0"),
        }
      : undefined;

    // Expiration
    const expKind = v1.expiration?.$kind;
    const expiration =
      expKind === "Epoch"
        ? { kind: "EPOCH" as const, epoch: String(v1.expiration.Epoch) }
        : expKind === "None"
          ? { kind: "NONE" as const }
          : undefined;

    return {
      sender: v1.sender,
      systemKind,
      systemData: systemKind ? v1.kind[kindKind] : undefined,
      programmableTransaction: commands ? { commands } : undefined,
      commands,
      inputs,
      gasPayment,
      expiration,
    };
  } catch {
    return null;
  }
}

// ─── Protobuf loader ─────────────────────────────────────────────────────────

let checkpointType: protobuf.Type | null = null;

export async function getCheckpointType(): Promise<protobuf.Type> {
  if (checkpointType) return checkpointType;

  const PROTO_DIR = path.join(import.meta.dir, "..", "proto");
  const root = new protobuf.Root();
  root.resolvePath = (_origin, target) => {
    if (path.isAbsolute(target)) return target;
    return path.resolve(PROTO_DIR, target);
  };
  await root.load("sui/rpc/v2/checkpoint.proto", { keepCase: false });
  checkpointType = root.lookupType("sui.rpc.v2.Checkpoint");
  return checkpointType;
}

// ─── Archive fetch ──────────────────────────────────────────────────────────

/** Fetch compressed checkpoint bytes from the archive. */
export async function fetchCompressed(seq: bigint, archiveUrl: string, signal?: AbortSignal): Promise<Uint8Array> {
  const url = `${archiveUrl}/${seq}.binpb.zst`;
  const resp = await fetch(url, signal ? { signal } : undefined);
  if (!resp.ok) {
    throw new Error(`Archive fetch failed: ${resp.status} ${resp.statusText} for checkpoint ${seq}`);
  }
  return new Uint8Array(await resp.arrayBuffer());
}

// ─── Archive client ──────────────────────────────────────────────────────────

export interface ArchiveClientOptions {
  /** Archive base URL. Default: https://checkpoints.mainnet.sui.io */
  archiveUrl?: string;
  /** Enable cryptographic checkpoint verification via kei. Requires grpcUrl for committee fetching. */
  verify?: boolean;
  /** gRPC URL for fetching validator committees (required when verify=true). */
  grpcUrl?: string;
}

export interface ArchiveClient {
  /**
   * Fetch a single checkpoint from the archive.
   * Returns data in the same shape as the gRPC client for compatibility.
   */
  fetchCheckpoint(seq: bigint): Promise<GrpcCheckpointResponse>;
}

// Cache prepared committees by epoch (committee changes ~once per 24h)
const committeeCache = new Map<string, PreparedCommittee>();

/** Normalize URL for @grpc/grpc-js: strip https://, ensure :443 */
function toNativeGrpcUrl(url: string): string {
  let addr = url.replace(/^https?:\/\//, "");
  if (!addr.includes(":")) addr += ":443";
  return addr;
}

export async function getCommittee(grpcUrl: string, epoch: string): Promise<PreparedCommittee> {
  if (committeeCache.has(epoch)) return committeeCache.get(epoch)!;

  const PROTO_DIR = path.join(import.meta.dir, "..", "proto");
  const def = protoLoader.loadSync(path.join(PROTO_DIR, "sui/rpc/v2/ledger_service.proto"), {
    keepCase: false, longs: String, enums: String, defaults: true, oneofs: true, includeDirs: [PROTO_DIR],
  });
  const proto = grpc.loadPackageDefinition(def) as any;
  const client = new proto.sui.rpc.v2.LedgerService(toNativeGrpcUrl(grpcUrl), grpc.credentials.createSsl());

  const resp = await new Promise<any>((resolve, reject) => {
    client.GetEpoch({ epoch, readMask: { paths: ["committee"] } }, (err: any, res: any) => {
      client.close();
      if (err) reject(err); else resolve(res);
    });
  });

  const prepared = new PreparedCommittee({
    epoch: BigInt(epoch),
    members: resp.epoch.committee.members.map((m: any) => ({
      publicKey: new Uint8Array(m.publicKey),
      weight: BigInt(m.weight),
    })),
  });

  committeeCache.set(epoch, prepared);
  return prepared;
}

export function createArchiveClient(options?: ArchiveClientOptions): ArchiveClient {
  const archiveUrl = (options?.archiveUrl ?? "https://checkpoints.mainnet.sui.io").replace(/\/$/, "");
  const verify = options?.verify ?? false;
  const grpcUrl = options?.grpcUrl;

  if (verify && !grpcUrl) {
    throw new Error("grpcUrl is required when verify=true (needed to fetch validator committees)");
  }

  return {
    async fetchCheckpoint(seq: bigint): Promise<GrpcCheckpointResponse> {
      const compressed = await fetchCompressed(seq, archiveUrl);
      return decodeCompressedCheckpoint(seq, compressed, verify ? { grpcUrl: grpcUrl! } : undefined);
    },
  };
}

// ─── Shared decode + cache-through ──────────────────────────────────────────

/**
 * Decode a compressed checkpoint (.binpb.zst) into a GrpcCheckpointResponse.
 */
export async function decodeCompressedCheckpoint(
  seq: bigint,
  compressed: Uint8Array,
  verifyOpts?: { grpcUrl: string },
): Promise<GrpcCheckpointResponse> {
  if (!verifyOpts && isNativeCheckpointDecoderAvailable()) {
    const decoded = decodeArchiveCheckpointCompressedNative(compressed);
    if (decoded) return decoded;
  }

  const decompressed = zstdDecompressSync(Buffer.from(compressed));
  const Checkpoint = await getCheckpointType();
  const decoded = Checkpoint.decode(decompressed);
  const cp = Checkpoint.toObject(decoded, { longs: String, enums: String, defaults: false }) as any;
  return decodeCheckpointFromProto(seq, cp, verifyOpts);
}

/** Hex-encode a Uint8Array as "0x..." */
function toHex(bytes: Uint8Array | number[] | null | undefined): string {
  if (!bytes) return "";
  const arr = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  let s = "0x";
  for (let i = 0; i < arr.length; i++) {
    s += arr[i]!.toString(16).padStart(2, "0");
  }
  return s;
}

/**
 * Decode a checkpoint from an already-decoded protobuf object.
 * Avoids re-decompression when the protobuf is already available (e.g., in workers).
 */
export async function decodeCheckpointFromProto(
  seq: bigint,
  cp: any,
  verifyOpts?: { grpcUrl: string },
): Promise<GrpcCheckpointResponse> {

  // BCS decode CheckpointSummary for full summary data
  let timestampMs = 0;
  let epochStr = "0";
  let totalTxStr = "0";
  let contentDigest = "";
  let previousDigest: string | null = null;
  let rollingComputation = "0";
  let rollingStorage = "0";
  let rollingRebate = "0";
  let rollingNonRefundable = "0";

  const summaryBcsRaw = cp.summary?.bcs?.value;
  if (summaryBcsRaw) {
    const summaryBcs = new Uint8Array(summaryBcsRaw);
    const summary: any = CheckpointSummary.parse(summaryBcs);
    timestampMs = Number(summary.timestampMs);
    epochStr = String(summary.epoch);
    totalTxStr = String(summary.networkTotalTransactions);
    contentDigest = toHex(summary.contentDigest);
    previousDigest = summary.previousDigest ? toHex(summary.previousDigest) : null;
    const gas = summary.epochRollingGasCostSummary;
    rollingComputation = String(gas?.computationCost ?? "0");
    rollingStorage = String(gas?.storageCost ?? "0");
    rollingRebate = String(gas?.storageRebate ?? "0");
    rollingNonRefundable = String(gas?.nonRefundableStorageFee ?? "0");

    // Cryptographic verification via kei
    if (verifyOpts && cp.signature) {
      const epochBigInt = BigInt(summary.epoch);
      const prepared = await getCommittee(verifyOpts.grpcUrl, summary.epoch.toString());
      keiVerify(summaryBcs, {
        epoch: epochBigInt,
        signature: Uint8Array.from(cp.signature.signature),
        signersMap: Uint8Array.from(cp.signature.bitmap),
      }, prepared);
    }
  }

  // Convert to the same timestamp format as gRPC stream
  const seconds = Math.floor(timestampMs / 1000).toString();
  const nanos = (timestampMs % 1000) * 1_000_000;

  // BCS decode events + extract tx digests from effects
  const transactions: GrpcCheckpointResponse["checkpoint"]["transactions"] = [];

  for (const tx of cp.transactions ?? []) {
    // Decode full effects from BCS
    const effectsBcs = tx.effects?.bcs?.value as Uint8Array | undefined;
    const effectsResult = effectsBcs ? extractEffects(new Uint8Array(effectsBcs)) : null;
    const digest = tx.digest || effectsResult?.digest || "";

    // Decode full transaction data from BCS
    const txDataBcs = tx.transaction?.bcs?.value as Uint8Array | undefined;
    const transactionData = txDataBcs ? extractTransaction(new Uint8Array(txDataBcs)) : null;

    // Decode events from BCS
    const eventsBcs = tx.events?.bcs?.value as Uint8Array | undefined;
    const events: CheckpointEvent[] = eventsBcs && eventsBcs.length > 0
      ? parseEvents(new Uint8Array(eventsBcs))
      : [];

    // Extract balance changes from protobuf (field 8 on ExecutedTransaction)
    const balanceChanges = (tx.balanceChanges ?? [])
      .filter((bc: any) => bc.address && bc.coinType)
      .map((bc: any) => ({
        address: bc.address,
        coinType: bc.coinType,
        amount: bc.amount ?? "0",
      }));

    const txResult: GrpcTransaction = {
      digest,
      transaction: transactionData ?? undefined,
      events: events.length > 0 ? { events } : null,
      effects: effectsResult?.effects,
      balanceChanges: balanceChanges.length > 0 ? balanceChanges : undefined,
    };

    transactions.push(txResult);
  }

  return {
    cursor: seq.toString(),
    checkpoint: {
      sequenceNumber: cp.sequenceNumber ?? seq.toString(),
      summary: {
        timestamp: { seconds, nanos },
        epoch: epochStr,
        digest: undefined, // archive doesn't carry the checkpoint digest directly; computed separately if needed
        previousDigest: previousDigest ?? undefined,
        contentDigest: contentDigest || undefined,
        totalNetworkTransactions: totalTxStr,
        epochRollingGasCostSummary: {
          computationCost: rollingComputation,
          storageCost: rollingStorage,
          storageRebate: rollingRebate,
          nonRefundableStorageFee: rollingNonRefundable,
        },
      },
      transactions,
    },
  };
}


// ─── Raw checkpoint for verification ─────────────────────────────────────────

export interface RawCheckpoint {
  sequenceNumber: string;
  summary: { bcs: { value: Uint8Array } };
  signature: { signature: Uint8Array; bitmap: Uint8Array; epoch: string };
  contents: { bcs: { value: Uint8Array } };
  transactions: Array<{
    digest: string;
    effects: { bcs: { value: Uint8Array } };
    events?: { bcs: { value: Uint8Array } };
  }>;
}

/**
 * Fetch a raw checkpoint from the archive with all BCS fields intact.
 * No event/effects processing — returns the protobuf-decoded structure for verification.
 */
export async function fetchRawCheckpoint(
  seq: bigint,
  archiveUrl = "https://checkpoints.mainnet.sui.io",
): Promise<RawCheckpoint> {
  const Checkpoint = await getCheckpointType();
  const compressed = await fetchCompressed(seq, archiveUrl.replace(/\/$/, ""));
  const decompressed = zstdDecompressSync(Buffer.from(compressed));
  const decoded = Checkpoint.decode(decompressed);
  const cp = Checkpoint.toObject(decoded, { longs: String, enums: String, defaults: false });

  return {
    sequenceNumber: cp.sequenceNumber ?? seq.toString(),
    summary: { bcs: { value: new Uint8Array(cp.summary?.bcs?.value) } },
    signature: {
      signature: new Uint8Array(cp.signature?.signature),
      bitmap: new Uint8Array(cp.signature?.bitmap),
      epoch: cp.signature?.epoch ?? "0",
    },
    contents: { bcs: { value: new Uint8Array(cp.contents?.bcs?.value) } },
    transactions: (cp.transactions ?? []).map((tx: any) => ({
      digest: tx.digest ?? "",
      effects: { bcs: { value: new Uint8Array(tx.effects?.bcs?.value) } },
      events: tx.events?.bcs?.value
        ? { bcs: { value: new Uint8Array(tx.events.bcs.value) } }
        : undefined,
    })),
  };
}
