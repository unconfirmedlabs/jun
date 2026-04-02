/**
 * Worker thread for CPU-bound checkpoint decoding.
 *
 * Receives compressed checkpoint bytes from the main thread,
 * runs zstd decompress + protobuf decode + BCS decode, and
 * returns the decoded GrpcCheckpointResponse + balance changes.
 */
/// <reference lib="webworker" />
import { decodeCompressedCheckpoint, getCheckpointType } from "./archive.ts";
import { computeBalanceChangesFromArchive } from "./archive-balance.ts";
import { zstdDecompressSync } from "zlib";

declare var self: Worker;

self.onmessage = async (event: MessageEvent) => {
  const { id, seq, compressed, balanceCoinTypes } = event.data as {
    id: number;
    seq: string;
    compressed: Uint8Array;
    balanceCoinTypes: string[] | null; // null = disabled, "*" not sent (use null for all)
  };

  try {
    const seqBigInt = BigInt(seq);
    const compressedBytes = new Uint8Array(compressed);

    // Decode checkpoint for events
    const decoded = await decodeCompressedCheckpoint(seqBigInt, compressedBytes);

    // Compute balance changes from archive if enabled
    let balanceChanges: any[] | null = null;
    if (balanceCoinTypes !== null) {
      // Re-decode the protobuf to get ObjectSet + effects (decodeCompressedCheckpoint doesn't expose these)
      const Checkpoint = await getCheckpointType();
      const decompressed = zstdDecompressSync(Buffer.from(compressedBytes));
      const protoDecoded = Checkpoint.decode(decompressed);
      const checkpointProto = Checkpoint.toObject(protoDecoded, { longs: String, enums: String, defaults: false });

      const timestamp = decoded.checkpoint.summary?.timestamp;
      const timestampDate = timestamp
        ? new Date(Number(BigInt(timestamp.seconds) * 1000n + BigInt(Math.floor(timestamp.nanos / 1_000_000))))
        : new Date(0);

      const coinTypeFilter = balanceCoinTypes.length === 0 ? null : new Set(balanceCoinTypes);
      balanceChanges = computeBalanceChangesFromArchive(
        checkpointProto,
        seqBigInt,
        timestampDate,
        coinTypeFilter,
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
