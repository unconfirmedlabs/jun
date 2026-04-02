/**
 * Worker thread for checkpoint decoding.
 *
 * Primary path (Zig FFI): single native call from compressed bytes →
 * balance changes + decoded events. No JS decompression or BCS parsing.
 *
 * Fallback path (JS): zstd decompress → proto parse → BCS decode.
 * Used when native lib unavailable or JUN_LEGACY_PARSERS=1.
 */
/// <reference lib="webworker" />
import { processCheckpointCompressed, nativeProcessorAvailable } from "./checkpoint-processor-native.ts";
import { decodeCheckpointFromProto, getCheckpointType } from "./archive.ts";
import { parseCheckpointProtoNative } from "./proto-parser-native.ts";
import { computeBalanceChangesFromArchive } from "./archive-balance.ts";
import { zstdDecompressSync } from "zlib";

const USE_LEGACY = process.env.JUN_LEGACY_PARSERS === "1";
const USE_NATIVE = nativeProcessorAvailable && !USE_LEGACY;

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
    const compressedBytes = new Uint8Array(compressed);
    const coinTypeFilter = balanceCoinTypes === null ? null
      : balanceCoinTypes.length === 0 ? null
      : new Set(balanceCoinTypes);

    // --- Native path: single Zig FFI call ---
    if (USE_NATIVE) {
      const result = processCheckpointCompressed(compressedBytes, sequenceNumber, coinTypeFilter);

      if (result) {
        const timestamp = result.timestampMs > 0
          ? { seconds: String(Math.floor(result.timestampMs / 1000)), nanos: (result.timestampMs % 1000) * 1_000_000 }
          : { seconds: "0", nanos: 0 };

        // Build the same response shape as the JS path
        const transactions = [{
          digest: "",
          events: result.events.length > 0 ? { events: result.events } : null,
          effects: undefined,
          balanceChanges: undefined,
        }];

        const decoded = {
          cursor: seq,
          checkpoint: {
            sequenceNumber: seq,
            summary: { timestamp },
            transactions,
          },
        };

        postMessage({
          id,
          decoded,
          balanceChanges: balanceCoinTypes !== null ? result.balanceChanges : null,
          error: null,
        });
        return;
      }
      // Native failed — fall through to JS path
    }

    // --- JS fallback path ---
    const decompressed = zstdDecompressSync(Buffer.from(compressedBytes));

    let checkpointProto: any;
    if (USE_LEGACY) {
      const Checkpoint = await getCheckpointType();
      const protoDecoded = Checkpoint.decode(decompressed);
      checkpointProto = Checkpoint.toObject(protoDecoded, { longs: String, enums: String, defaults: false });
    } else {
      checkpointProto = parseCheckpointProtoNative(new Uint8Array(decompressed));
    }

    const decoded = await decodeCheckpointFromProto(sequenceNumber, checkpointProto);

    let balanceChanges: any[] | null = null;
    if (balanceCoinTypes !== null) {
      const timestamp = decoded.checkpoint.summary?.timestamp;
      const timestampDate = timestamp
        ? new Date(Number(BigInt(timestamp.seconds) * 1000n + BigInt(Math.floor(timestamp.nanos / 1_000_000))))
        : new Date(0);

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
