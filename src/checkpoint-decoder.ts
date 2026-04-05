/**
 * Worker thread for checkpoint decoding.
 *
 * Pure Mysten BCS baseline: zstd decompress → protobufjs decode → @mysten/sui/bcs
 * decode of effects/events/etc. No native FFI, no hand-rolled fast paths.
 */
/// <reference lib="webworker" />
import { decodeCheckpointFromProto, getCheckpointType } from "./archive.ts";
import { computeBalanceChangesFromArchive } from "./archive-balance.ts";
import { parseTimestamp } from "./timestamp.ts";
import { zstdDecompressSync } from "zlib";

declare var self: Worker;

self.onmessage = async (event: MessageEvent) => {
  const { id, seq, compressed, balanceCoinTypes } = event.data as {
    id: number;
    seq: string;
    compressed: Uint8Array;
    balanceCoinTypes: string[] | null;
    needsTransactions?: boolean;
  };

  try {
    const compressedBytes = new Uint8Array(compressed);
    const sequenceNumber = BigInt(seq);
    const decompressed = zstdDecompressSync(Buffer.from(compressedBytes));

    const Checkpoint = await getCheckpointType();
    const protoDecoded = Checkpoint.decode(decompressed);
    const checkpointProto = Checkpoint.toObject(protoDecoded, { longs: String, enums: String, defaults: false });

    const decoded = await decodeCheckpointFromProto(sequenceNumber, checkpointProto);

    let balanceChanges: any[] | null = null;
    if (balanceCoinTypes !== null) {
      const coinTypeFilter = balanceCoinTypes.length === 0 ? null : new Set(balanceCoinTypes);
      const timestampDate = parseTimestamp(decoded.checkpoint.summary?.timestamp);
      balanceChanges = computeBalanceChangesFromArchive(
        checkpointProto, sequenceNumber, timestampDate, coinTypeFilter,
      );
    }

    postMessage({ id, decoded, balanceChanges, error: null });
  } catch (err) {
    postMessage({
      id,
      decoded: null,
      balanceChanges: null,
      error: err instanceof Error ? err.message : String(err),
    });
  }
};
