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
import path from "path";
import type { GrpcCheckpointResponse, GrpcEvent } from "./grpc.ts";
import { parseSender } from "./sui-bcs.ts";

// ─── BCS types ───────────────────────────────────────────────────────────────

// Event decoding (uses @mysten/sui/bcs for StructTag/Address)
const SuiEvent = suiBcs.struct("Event", {
  packageId: suiBcs.Address,
  transactionModule: suiBcs.String,
  sender: suiBcs.Address,
  type: suiBcs.StructTag,
  contents: suiBcs.vector(suiBcs.U8),
});

const TransactionEvents = suiBcs.struct("TransactionEvents", {
  data: suiBcs.vector(SuiEvent),
});

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

async function getCheckpointType(): Promise<protobuf.Type> {
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

async function getCommittee(grpcUrl: string, epoch: string): Promise<PreparedCommittee> {
  if (committeeCache.has(epoch)) return committeeCache.get(epoch)!;

  const PROTO_DIR = path.join(import.meta.dir, "..", "proto");
  const def = protoLoader.loadSync(path.join(PROTO_DIR, "sui/rpc/v2/ledger_service.proto"), {
    keepCase: false, longs: String, enums: String, defaults: true, oneofs: true, includeDirs: [PROTO_DIR],
  });
  const proto = grpc.loadPackageDefinition(def) as any;
  const client = new proto.sui.rpc.v2.LedgerService(grpcUrl, grpc.credentials.createSsl());

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
      const Checkpoint = await getCheckpointType();

      // 1. Fetch compressed checkpoint from archive
      const url = `${archiveUrl}/${seq}.binpb.zst`;
      const resp = await fetch(url);
      if (!resp.ok) {
        throw new Error(`Archive fetch failed: ${resp.status} ${resp.statusText} for checkpoint ${seq}`);
      }

      // 2. Zstd decompress (Bun built-in)
      const compressed = new Uint8Array(await resp.arrayBuffer());
      const decompressed = zstdDecompressSync(Buffer.from(compressed));

      // 3. Protobuf decode outer Checkpoint
      const decoded = Checkpoint.decode(decompressed);
      const cp = Checkpoint.toObject(decoded, { longs: String, enums: String, defaults: false }) as any;

      // 4. BCS decode CheckpointSummary for timestamp + epoch
      let timestampMs = 0;
      const summaryBcsRaw = cp.summary?.bcs?.value;
      if (summaryBcsRaw) {
        const summaryBcs = new Uint8Array(summaryBcsRaw);
        const summary = CheckpointSummary.parse(summaryBcs);
        timestampMs = Number(summary.timestampMs);

        // 4b. Cryptographic verification via kei
        if (verify && cp.signature) {
          const epochBigInt = BigInt(summary.epoch);
          const prepared = await getCommittee(grpcUrl!, summary.epoch.toString());
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

      // 5. BCS decode events + extract tx digests from effects
      const transactions: GrpcCheckpointResponse["checkpoint"]["transactions"] = [];

      for (const tx of cp.transactions ?? []) {
        // Decode tx metadata from effects BCS (proto fields aren't populated in archive)
        const effectsBcs = tx.effects?.bcs?.value as Uint8Array | undefined;
        const txMeta = effectsBcs ? decodeTxEffects(new Uint8Array(effectsBcs)) : null;
        const digest = tx.digest || txMeta?.digest || "";

        // Decode sender from TransactionData BCS
        const txDataBcs = tx.transaction?.bcs?.value as Uint8Array | undefined;
        const sender = txDataBcs ? parseSender(new Uint8Array(txDataBcs)) : undefined;

        // Decode events from BCS
        const eventsBcs = tx.events?.bcs?.value as Uint8Array | undefined;
        let events: GrpcEvent[] = [];

        if (eventsBcs && eventsBcs.length > 0) {
          const decoded = TransactionEvents.parse(new Uint8Array(eventsBcs));
          events = decoded.data.map((ev: any) => {
            const typeParams = (ev.type.typeParams ?? []).map((tp: any) => formatTypeTag(tp));
            const typeSuffix = typeParams.length > 0 ? `<${typeParams.join(",")}>` : "";
            return {
              packageId: ev.packageId,
              module: ev.transactionModule,
              sender: ev.sender,
              eventType: `${ev.type.address}::${ev.type.module}::${ev.type.name}${typeSuffix}`,
              contents: {
                name: `${ev.type.address}::${ev.type.module}::${ev.type.name}${typeSuffix}`,
                value: new Uint8Array(ev.contents),
              },
            };
          });
        }

        const txResult: any = {
          digest,
          transaction: sender ? { sender } : undefined,
          events: events.length > 0 ? { events } : null,
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
    },
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
