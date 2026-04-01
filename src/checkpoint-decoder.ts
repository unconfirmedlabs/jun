/**
 * Worker thread for CPU-bound checkpoint decoding.
 *
 * Receives compressed checkpoint bytes from the main thread,
 * runs zstd decompress + protobuf decode + BCS decode, and
 * returns the decoded GrpcCheckpointResponse.
 */
/// <reference lib="webworker" />
import { decodeCompressedCheckpoint } from "./archive.ts";

declare var self: Worker;

self.onmessage = async (event: MessageEvent) => {
  const { id, seq, compressed } = event.data as {
    id: number;
    seq: string; // BigInt serialized as string
    compressed: Uint8Array;
  };

  try {
    const decoded = await decodeCompressedCheckpoint(
      BigInt(seq),
      new Uint8Array(compressed),
    );
    postMessage({ id, decoded, error: null });
  } catch (err) {
    postMessage({
      id,
      decoded: null,
      error: err instanceof Error ? err.message : String(err),
    });
  }
};
