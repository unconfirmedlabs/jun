/**
 * Archive checkpoint worker.
 *
 * Archive range mode:
 *   fetch -> Rust binary FFI -> Uint8Array -> postMessage(buffer, [buffer.buffer])
 *
 * Per-checkpoint decode mode is retained for non-archive callers that still
 * post compressed checkpoint bytes directly to the worker.
 */
/// <reference lib="webworker" />
import { computeBalanceChangesFromArchive } from "./archive-balance.ts";
import { decodeCheckpointFromProto, getCheckpointType } from "./archive.ts";
import type { BalanceChange } from "./balance-processor.ts";
import {
  decodeArchiveCheckpointBinary,
  decodeArchiveCheckpointCompressedNative,
  isNativeCheckpointDecoderAvailable,
} from "./checkpoint-native-decoder.ts";
import type { SerializedBalanceChange } from "./checkpoint-response.ts";
import type { GrpcCheckpointResponse } from "./grpc.ts";
import { parseTimestamp } from "./timestamp.ts";
import { zstdDecompressSync } from "zlib";

declare var self: Worker;

type WorkerArchiveMode = "binary" | "json";

interface DecodeRequestMessage {
  id: number;
  seq: string;
  compressed: Uint8Array;
  balanceCoinTypes: string[] | null;
  useBinary: boolean;
}

interface AssignRangeMessage {
  type: "assign";
  workerIndex: number;
  from: string;
  to: string;
  archiveUrl: string;
  concurrency: number;
  mode: WorkerArchiveMode;
  balanceCoinTypes: string[] | null;
}

function serializeBalanceChanges(changes: BalanceChange[]): SerializedBalanceChange[] {
  return changes.map(change => ({
    txDigest: change.txDigest,
    checkpointSeq: change.checkpointSeq.toString(),
    address: change.address,
    coinType: change.coinType,
    amount: change.amount,
    timestamp: change.timestamp.toISOString(),
  }));
}

async function fetchArchiveCheckpoint(seq: bigint, archiveUrl: string): Promise<Uint8Array> {
  const response = await fetch(`${archiveUrl}/${seq}.binpb.zst`);
  if (!response.ok) {
    throw new Error(`Archive fetch failed: ${response.status} for checkpoint ${seq}`);
  }
  return new Uint8Array(await response.arrayBuffer());
}

async function decodeCheckpointJson(
  compressedBytes: Uint8Array,
  sequenceNumber: bigint,
  balanceCoinTypes: string[] | null,
): Promise<{ decoded: GrpcCheckpointResponse; precomputedBalanceChanges?: SerializedBalanceChange[] }> {
  let decoded: GrpcCheckpointResponse | null = null;
  if (isNativeCheckpointDecoderAvailable()) {
    decoded = decodeArchiveCheckpointCompressedNative(compressedBytes);
  }

  let checkpointProto: any | null = null;
  if (!decoded || balanceCoinTypes !== null) {
    const decompressed = zstdDecompressSync(Buffer.from(compressedBytes));
    const Checkpoint = await getCheckpointType();
    const protoDecoded = Checkpoint.decode(decompressed);
    checkpointProto = Checkpoint.toObject(protoDecoded, { longs: String, enums: String, defaults: false });

    if (!decoded) {
      decoded = await decodeCheckpointFromProto(sequenceNumber, checkpointProto);
    }
  }

  if (!decoded) {
    throw new Error(`Checkpoint ${sequenceNumber} could not be decoded`);
  }

  let precomputedBalanceChanges: SerializedBalanceChange[] | undefined;
  if (balanceCoinTypes !== null) {
    if (!checkpointProto) {
      throw new Error("Archive balance computation requires decoded checkpoint proto");
    }

    const summary = decoded.checkpoint.summary;
    const timestamp = parseTimestamp(summary?.timestamp);
    const coinTypeFilter = balanceCoinTypes.length === 0 ? null : new Set(balanceCoinTypes);
    const archiveBalances = computeBalanceChangesFromArchive(
      checkpointProto,
      sequenceNumber,
      timestamp,
      coinTypeFilter,
    );
    precomputedBalanceChanges = serializeBalanceChanges(archiveBalances);
  }

  return { decoded, precomputedBalanceChanges };
}

function postBinaryById(id: number, binary: Uint8Array): void {
  const result = new Uint8Array(4 + binary.length);
  new DataView(result.buffer).setUint32(0, id, true);
  result.set(binary, 4);
  postMessage(result, [result.buffer]);
}

function postBinaryBySeq(seq: bigint, binary: Uint8Array): void {
  const result = new Uint8Array(8 + binary.length);
  const view = new DataView(result.buffer);
  view.setBigUint64(0, seq, true);
  result.set(binary, 8);
  postMessage(result, [result.buffer]);
}

async function handleDecodeRequest(message: DecodeRequestMessage): Promise<void> {
  const { id, seq, compressed, balanceCoinTypes, useBinary } = message;

  try {
    const compressedBytes = new Uint8Array(compressed);
    const sequenceNumber = BigInt(seq);

    if (useBinary && isNativeCheckpointDecoderAvailable()) {
      const binary = decodeArchiveCheckpointBinary(compressedBytes);
      if (binary) {
        postBinaryById(id, binary);
        return;
      }
    }

    const payload = await decodeCheckpointJson(compressedBytes, sequenceNumber, balanceCoinTypes);
    postMessage(`${id}\n${JSON.stringify(payload)}`);
  } catch (err) {
    postMessage({ id, error: err instanceof Error ? err.message : String(err) });
  }
}

async function handleRangeAssignment(message: AssignRangeMessage): Promise<void> {
  const from = BigInt(message.from);
  const to = BigInt(message.to);
  let next = from;

  const startedAt = performance.now();
  let processed = 0;
  let fetchMs = 0;
  let decodeMs = 0;

  async function processOne(): Promise<void> {
    while (next <= to) {
      const seq = next;
      next += 1n;

      try {
        const fetchStart = performance.now();
        const compressed = await fetchArchiveCheckpoint(seq, message.archiveUrl);
        const afterFetch = performance.now();

        if (message.mode === "binary") {
          const binary = decodeArchiveCheckpointBinary(compressed);
          if (!binary) {
            throw new Error(`Native binary decode failed for checkpoint ${seq}`);
          }
          postBinaryBySeq(seq, binary);
        } else {
          const payload = await decodeCheckpointJson(compressed, seq, message.balanceCoinTypes);
          postMessage(`${seq}\n${JSON.stringify(payload)}`);
        }

        const afterDecode = performance.now();
        processed += 1;
        fetchMs += afterFetch - fetchStart;
        decodeMs += afterDecode - afterFetch;

        if (processed > 0 && processed % 5000 === 0) {
          const elapsed = (performance.now() - startedAt) / 1000;
          const rate = Math.round(processed / Math.max(elapsed, 0.001));
          const avgFetch = (fetchMs / processed).toFixed(2);
          const avgDecode = (decodeMs / processed).toFixed(2);
          console.error(
            `[checkpoint-worker ${message.workerIndex}] ${processed} cp ${rate} cp/s avg_fetch=${avgFetch}ms avg_decode=${avgDecode}ms`,
          );
        }
      } catch (err) {
        postMessage({
          type: "error",
          seq: seq.toString(),
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }
  }

  await Promise.all(
    Array.from({ length: Math.max(1, message.concurrency) }, () => processOne()),
  );

  postMessage({ type: "done" });
}

self.onmessage = async (event: MessageEvent) => {
  const msg = event.data as DecodeRequestMessage | AssignRangeMessage;

  if (msg && typeof msg === "object" && "type" in msg && msg.type === "assign") {
    await handleRangeAssignment(msg);
    return;
  }

  await handleDecodeRequest(msg as DecodeRequestMessage);
};
