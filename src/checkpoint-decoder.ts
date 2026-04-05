/**
 * Archive checkpoint worker.
 *
 * When the native Rust decoder is available, the worker does zero JS decode —
 * Rust handles zstd + proto + BCS + balance computation in a single FFI call.
 * The result is JSON-stringified and sent via postMessage (zero-copy in Bun).
 *
 * Falls back to JS decode (protobufjs + @mysten/sui/bcs) when the native
 * decoder is unavailable or returns null.
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

    // --- Fast path: Rust native decode (zstd + proto + BCS + balances in one call) ---
    if (isNativeCheckpointDecoderAvailable()) {
      const decoded = decodeArchiveCheckpointCompressedNative(compressedBytes);
      if (decoded) {
        // Rust already computed balance changes — no JS fallback needed
        postMessage(`${id}\n${JSON.stringify({ decoded })}`);
        return;
      }
    }

    // --- Fallback: JS decode ---
    const decompressed = zstdDecompressSync(Buffer.from(compressedBytes));
    const Checkpoint = await getCheckpointType();
    const protoDecoded = Checkpoint.decode(decompressed);
    const checkpointProto = Checkpoint.toObject(protoDecoded, { longs: String, enums: String, defaults: false });
    const decoded = await decodeCheckpointFromProto(sequenceNumber, checkpointProto);

    let precomputedBalanceChanges: SerializedBalanceChange[] | undefined;
    if (balanceCoinTypes !== null) {
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

    postMessage(`${id}\n${JSON.stringify({ decoded, precomputedBalanceChanges })}`);
  } catch (err) {
    postMessage({ id, error: err instanceof Error ? err.message : String(err) });
  }
};
