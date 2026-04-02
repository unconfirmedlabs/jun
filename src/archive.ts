/**
 * Sui checkpoint archive client.
 *
 * Fetches checkpoints from the public archive (https://checkpoints.mainnet.sui.io/)
 * which has every checkpoint from genesis. No pruning, no rate limits.
 *
 * Flow: HTTP GET → zstd decompress → protobuf decode → BCS decode (summary + events)
 */
import { zstdDecompressSync } from "zlib";
import { bcs as suiBcs } from "@mysten/sui/bcs";
import { bcs } from "@mysten/bcs";
import { verifyCheckpoint as keiVerify, PreparedCommittee } from "@unconfirmed/kei";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import protobuf from "protobufjs";
import { cacheGet, cachePut } from "./cache.ts";
import { parseCheckpointProto } from "./proto-parser.ts";
import { parseCheckpointProtoNative } from "./proto-parser-native.ts";
import path from "path";
import type { GrpcCheckpointResponse, GrpcEvent } from "./grpc.ts";
import { parseSender } from "./sui-bcs.ts";

// ─── Configuration ──────────────────────────────────────────────────────────

/** Use native suiBcs parsers instead of custom fast parsers. Set JUN_LEGACY_PARSERS=1 to enable. */
const USE_LEGACY_PARSERS = process.env.JUN_LEGACY_PARSERS === "1";

// ─── Native BCS types (used when JUN_LEGACY_PARSERS=1) ─────────────────────────

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

/** Format a TypeTag enum value to a string (native path). */
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

/** Parse events using native suiBcs (slow but battle-tested). */
function nativeParseEvents(eventsBcs: Uint8Array): GrpcEvent[] {
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
      contents: { name: eventType, value: new Uint8Array(ev.contents) },
    };
  });
}

// ─── Fast BCS types ─────────────────────────────────────────────────────────

// Fast event parser — replaces suiBcs.struct("TransactionEvents") with direct
// byte-level parsing that builds GrpcEvent[] without intermediate JS objects.
//
// BCS layout:
//   TransactionEvents { data: vec<SuiEvent> }
//   SuiEvent { packageId: Address(32), transactionModule: String,
//              sender: Address(32), type: StructTag, contents: vec<u8> }
//   StructTag { address: Address(32), module: String, name: String, typeParams: vec<TypeTag> }

const HEX = "0123456789abcdef";
const TYPE_TAG_NAMES = ["bool","u8","u64","u128","address","signer","vector","struct","u16","u32","u256"];

function evBytesToHex(buf: Uint8Array, offset: number): string {
  let hex = "0x";
  for (let i = 0; i < 32; i++) {
    const b = buf[offset + i]!;
    hex += HEX[b >> 4]! + HEX[b & 0xf]!;
  }
  return hex;
}

function evReadUleb128(buf: Uint8Array, offset: number): [number, number] {
  let value = 0, shift = 0, pos = offset;
  for (;;) {
    const b = buf[pos]!;
    value |= (b & 0x7f) << shift;
    pos++;
    if ((b & 0x80) === 0) break;
    shift += 7;
  }
  return [value, pos - offset];
}

function evReadString(buf: Uint8Array, offset: number): [string, number] {
  const [len, lb] = evReadUleb128(buf, offset);
  offset += lb;
  let s = "";
  for (let i = 0; i < len; i++) s += String.fromCharCode(buf[offset + i]!);
  return [s, offset + len];
}

/** Format a TypeTag directly from BCS bytes → string. Returns [formatted, newOffset]. */
function formatTypeTagFromBcs(buf: Uint8Array, offset: number): [string, number] {
  const tag = buf[offset]!;
  offset++;

  if (tag <= 5 || (tag >= 8 && tag <= 10)) {
    // Primitive: bool(0), u8(1), u64(2), u128(3), address(4), signer(5), u16(8), u32(9), u256(10)
    return [TYPE_TAG_NAMES[tag]!, offset];
  }

  if (tag === 6) {
    // vector<TypeTag>
    let inner: string;
    [inner, offset] = formatTypeTagFromBcs(buf, offset);
    return [`vector<${inner}>`, offset];
  }

  if (tag === 7) {
    // struct(StructTag)
    const addr = evBytesToHex(buf, offset);
    offset += 32;
    let mod: string;
    [mod, offset] = evReadString(buf, offset);
    let name: string;
    [name, offset] = evReadString(buf, offset);

    const [paramCount, pcBytes] = evReadUleb128(buf, offset);
    offset += pcBytes;

    if (paramCount === 0) {
      return [`${addr}::${mod}::${name}`, offset];
    }

    const params: string[] = [];
    for (let i = 0; i < paramCount; i++) {
      let p: string;
      [p, offset] = formatTypeTagFromBcs(buf, offset);
      params.push(p);
    }
    return [`${addr}::${mod}::${name}<${params.join(", ")}>`, offset];
  }

  return ["unknown", offset];
}

/**
 * Fast-parse TransactionEvents BCS → GrpcEvent[].
 * Builds target format directly from bytes, no intermediate SuiEvent/StructTag objects.
 */
function fastParseEvents(buf: Uint8Array): GrpcEvent[] {
  let offset = 0;

  // vec<SuiEvent> count
  const [count, countBytes] = evReadUleb128(buf, offset);
  offset += countBytes;
  if (count === 0) return [];

  const events: GrpcEvent[] = new Array(count);

  for (let i = 0; i < count; i++) {
    // packageId: Address (32 bytes)
    const packageId = evBytesToHex(buf, offset);
    offset += 32;

    // transactionModule: String
    let module: string;
    [module, offset] = evReadString(buf, offset);

    // sender: Address (32 bytes)
    const sender = evBytesToHex(buf, offset);
    offset += 32;

    // type: StructTag → format directly to eventType string
    const typeAddr = evBytesToHex(buf, offset);
    offset += 32;
    let typeMod: string;
    [typeMod, offset] = evReadString(buf, offset);
    let typeName: string;
    [typeName, offset] = evReadString(buf, offset);

    const [paramCount, pcBytes] = evReadUleb128(buf, offset);
    offset += pcBytes;

    let eventType: string;
    if (paramCount === 0) {
      eventType = `${typeAddr}::${typeMod}::${typeName}`;
    } else {
      const params: string[] = [];
      for (let j = 0; j < paramCount; j++) {
        let p: string;
        [p, offset] = formatTypeTagFromBcs(buf, offset);
        params.push(p);
      }
      eventType = `${typeAddr}::${typeMod}::${typeName}<${params.join(", ")}>`;
    }

    // contents: vec<u8>
    const [contentsLen, clBytes] = evReadUleb128(buf, offset);
    offset += clBytes;
    const contentsValue = buf.subarray(offset, offset + contentsLen);
    offset += contentsLen;

    events[i] = {
      packageId,
      module,
      sender,
      eventType,
      contents: { name: eventType, value: contentsValue },
    };
  }

  return events;
}

// CheckpointSummary decoding (uses @mysten/bcs for custom digest type)
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

/** Decoded transaction metadata from effects BCS. */
interface TxMeta {
  digest: string;
  status: string;
  gasComputation: number;
  gasStorage: number;
  gasRebate: number;
}

/** Extract transaction metadata from TransactionEffects BCS using @mysten/sui/bcs. */
function decodeTxEffects(effectsBcs: Uint8Array): TxMeta | null {
  try {
    const effects = suiBcs.TransactionEffects.parse(effectsBcs);
    const v2 = effects.V2;
    if (!v2) return null;

    return {
      digest: v2.transactionDigest,
      status: v2.status.$kind === "Success" ? "success" : "failure",
      gasComputation: Number(v2.gasUsed.computationCost),
      gasStorage: Number(v2.gasUsed.storageCost),
      gasRebate: Number(v2.gasUsed.storageRebate),
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

// ─── Cached archive fetch ────────────────────────────────────────────────────

/** Fetch compressed checkpoint bytes with local cache. */
export async function fetchCompressed(seq: bigint, archiveUrl: string, signal?: AbortSignal): Promise<Uint8Array> {
  const cached = await cacheGet(seq);
  if (cached) return cached;

  const url = `${archiveUrl}/${seq}.binpb.zst`;
  const resp = await fetch(url, signal ? { signal } : undefined);
  if (!resp.ok) {
    throw new Error(`Archive fetch failed: ${resp.status} ${resp.statusText} for checkpoint ${seq}`);
  }
  const compressed = new Uint8Array(await resp.arrayBuffer());
  await cachePut(seq, compressed);
  return compressed;
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
 * Used by the archive client and the cache-through layer.
 */
export async function decodeCompressedCheckpoint(
  seq: bigint,
  compressed: Uint8Array,
  verifyOpts?: { grpcUrl: string },
): Promise<GrpcCheckpointResponse> {
  const decompressed = zstdDecompressSync(Buffer.from(compressed));
  if (USE_LEGACY_PARSERS) {
    const Checkpoint = await getCheckpointType();
    const decoded = Checkpoint.decode(decompressed);
    const cp = Checkpoint.toObject(decoded, { longs: String, enums: String, defaults: false }) as any;
    return decodeCheckpointFromProto(seq, cp, verifyOpts);
  }
  const cp = parseCheckpointProtoNative(new Uint8Array(decompressed)) as any;
  return decodeCheckpointFromProto(seq, cp, verifyOpts);
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

  // 3. BCS decode CheckpointSummary for timestamp + epoch
  let timestampMs = 0;
  const summaryBcsRaw = cp.summary?.bcs?.value;
  if (summaryBcsRaw) {
    const summaryBcs = new Uint8Array(summaryBcsRaw);
    const summary = CheckpointSummary.parse(summaryBcs);
    timestampMs = Number(summary.timestampMs);

    // 3b. Cryptographic verification via kei
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

  // 4. BCS decode events + extract tx digests from effects
  const transactions: GrpcCheckpointResponse["checkpoint"]["transactions"] = [];

  for (const tx of cp.transactions ?? []) {
    // Decode tx metadata from effects BCS (proto fields aren't populated in archive)
    const effectsBcs = tx.effects?.bcs?.value as Uint8Array | undefined;
    const txMeta = effectsBcs ? decodeTxEffects(new Uint8Array(effectsBcs)) : null;
    const digest = tx.digest || txMeta?.digest || "";

    // Decode sender from TransactionData BCS
    const txDataBcs = tx.transaction?.bcs?.value as Uint8Array | undefined;
    const sender = txDataBcs ? parseSender(new Uint8Array(txDataBcs)) : undefined;

    // Decode events from BCS using fast parser
    const eventsBcs = tx.events?.bcs?.value as Uint8Array | undefined;
    let events: GrpcEvent[] = [];

    if (eventsBcs && eventsBcs.length > 0) {
      const raw = new Uint8Array(eventsBcs);
      events = USE_LEGACY_PARSERS ? nativeParseEvents(raw) : fastParseEvents(raw);
    }

    // Extract balance changes from protobuf (field 8 on ExecutedTransaction)
    const balanceChanges = (tx.balanceChanges ?? [])
      .filter((bc: any) => bc.address && bc.coinType)
      .map((bc: any) => ({
        address: bc.address,
        coinType: bc.coinType,
        amount: bc.amount ?? "0",
      }));

    const txResult: any = {
      digest,
      transaction: sender ? { sender } : undefined,
      events: events.length > 0 ? { events } : null,
      balanceChanges: balanceChanges.length > 0 ? balanceChanges : undefined,
    };

    // Attach effects metadata for SQLite writer
    if (txMeta) {
      txResult.effects = {
        status: { success: txMeta.status === "success" },
        gasUsed: {
          computationCost: String(txMeta.gasComputation),
          storageCost: String(txMeta.gasStorage),
          storageRebate: String(txMeta.gasRebate),
        },
      };
    }

    transactions.push(txResult);
  }

  return {
    cursor: seq.toString(),
    checkpoint: {
      sequenceNumber: cp.sequenceNumber ?? seq.toString(),
      summary: { timestamp: { seconds, nanos } },
      transactions,
    },
  };
}

/**
 * Cache-through checkpoint fetch. Checks local cache first, falls back to the
 * provided fetcher (typically a gRPC call). Use for all historical lookups.
 */
export async function cachedGetCheckpoint(
  seq: bigint,
  fallback: () => Promise<GrpcCheckpointResponse>,
): Promise<GrpcCheckpointResponse> {
  const cached = await cacheGet(seq);
  if (cached) return decodeCompressedCheckpoint(seq, cached);
  return fallback();
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
