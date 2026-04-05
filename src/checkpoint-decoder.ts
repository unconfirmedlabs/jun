/**
 * Archive checkpoint worker.
 *
 * Decodes compressed archive checkpoints into the shared GrpcCheckpointResponse
 * shape, and optionally precomputes archive balance changes when that processor
 * is enabled.
 */
/// <reference lib="webworker" />
import { computeBalanceChangesFromArchive } from "./archive-balance.ts";
import { decodeCheckpointFromProto, getCheckpointType } from "./archive.ts";
import type { BalanceChange } from "./balance-processor.ts";
import {
  decodeArchiveCheckpointCompressedNative,
  isNativeCheckpointDecoderAvailable,
} from "./checkpoint-native-decoder.ts";
import type { SerializedBalanceChange } from "./checkpoint-response.ts";
import type { GrpcCheckpointResponse } from "./grpc.ts";
import { parseTimestamp } from "./timestamp.ts";
import { zstdDecompressSync } from "zlib";

declare var self: Worker;

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

self.onmessage = async (event: MessageEvent) => {
  const { id, seq, compressed, balanceCoinTypes } = event.data as {
    id: number;
    seq: string;
    compressed: Uint8Array;
    balanceCoinTypes: string[] | null;
  };

  try {
    const compressedBytes = new Uint8Array(compressed);
    const sequenceNumber = BigInt(seq);

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
      throw new Error(`Checkpoint ${seq} could not be decoded`);
    }

    let precomputedBalanceChanges: SerializedBalanceChange[] | undefined;
    if (balanceCoinTypes !== null) {
      const checkpoint = checkpointProto;
      if (!checkpoint) {
        throw new Error("Archive balance computation requires decoded checkpoint proto");
      }

      const summary = decoded.checkpoint.summary;
      const timestamp = parseTimestamp(summary?.timestamp);
      const coinTypeFilter = balanceCoinTypes.length === 0 ? null : new Set(balanceCoinTypes);
      const archiveBalances = computeBalanceChangesFromArchive(
        checkpoint,
        sequenceNumber,
        timestamp,
        coinTypeFilter,
      );
      precomputedBalanceChanges = serializeBalanceChanges(archiveBalances);
    }

    postMessage(`${id}\n${JSON.stringify({ decoded, precomputedBalanceChanges })}`);
  } catch (err) {
    postMessage({ id, error: err instanceof Error ? err.message : String(err) });
  }
};
