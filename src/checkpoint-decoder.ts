/**
 * Worker thread for CPU-bound checkpoint decoding + balance computation.
 *
 * Receives compressed checkpoint bytes, decompresses ONCE, then:
 * 1. Decodes protobuf → BCS for events (GrpcCheckpointResponse)
 * 2. Computes balance changes from ObjectSet + effects (if enabled)
 *
 * Single decompression, single protobuf decode, shared across both paths.
 */
/// <reference lib="webworker" />
import { decodeCheckpointFromProto, getCheckpointType } from "./archive.ts";
import { parseCheckpointProtoNative } from "./proto-parser-native.ts";
import { computeBalanceChangesFromArchive } from "./archive-balance.ts";
import { zstdDecompressSync } from "zlib";

const USE_LEGACY_PARSERS = process.env.JUN_LEGACY_PARSERS === "1";

declare var self: Worker;

self.onmessage = async (event: MessageEvent) => {
  const { id, seq, compressed, balanceCoinTypes } = event.data as {
    id: number;
    seq: string;
    compressed: Uint8Array;
    balanceCoinTypes: string[] | null;
  };

  try {
    const sequenceNumber = BigInt(seq);

    // Decompress ONCE
    const decompressed = zstdDecompressSync(Buffer.from(new Uint8Array(compressed)));

    // Protobuf decode ONCE
    let checkpointProto: any;
    if (USE_LEGACY_PARSERS) {
      const Checkpoint = await getCheckpointType();
      const protoDecoded = Checkpoint.decode(decompressed);
      checkpointProto = Checkpoint.toObject(protoDecoded, { longs: String, enums: String, defaults: false });
    } else {
      checkpointProto = parseCheckpointProtoNative(new Uint8Array(decompressed));
    }

    // 1. BCS decode events from the shared protobuf (no re-decompression)
    const decoded = await decodeCheckpointFromProto(sequenceNumber, checkpointProto);

    // 2. Compute balance changes from the same protobuf (if enabled)
    let balanceChanges: any[] | null = null;
    if (balanceCoinTypes !== null) {
      const timestamp = decoded.checkpoint.summary?.timestamp;
      const timestampDate = timestamp
        ? new Date(Number(BigInt(timestamp.seconds) * 1000n + BigInt(Math.floor(timestamp.nanos / 1_000_000))))
        : new Date(0);

      const coinTypeFilter = balanceCoinTypes.length === 0 ? null : new Set(balanceCoinTypes);
      balanceChanges = computeBalanceChangesFromArchive(
        checkpointProto,
        sequenceNumber,
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
