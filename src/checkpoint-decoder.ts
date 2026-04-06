/**
 * Checkpoint decode worker — Rust FFI binary decode from cached files.
 *
 * Each worker processes a range of checkpoints: readFileSync → Rust FFI → postMessage(binary).
 * Supports selective extraction via extractMask.
 */
/// <reference lib="webworker" />
import {
  decodeArchiveCheckpointBinary,
  decodeArchiveCheckpointBinarySelective,
  isNativeCheckpointDecoderAvailable,
} from "./checkpoint-native-decoder.ts";

declare var self: Worker;

interface DecodeCachedRangeMessage {
  type: "decode-cached-range";
  from: string;
  to: string;
  cacheDir: string;
  workerIndex: number;
  extractMask?: number;
}

function postBinaryBySeq(seq: bigint, binary: Uint8Array): void {
  const result = new Uint8Array(8 + binary.length);
  const view = new DataView(result.buffer);
  view.setBigUint64(0, seq, true);
  result.set(binary, 8);
  postMessage(result, [result.buffer]);
}

async function handleDecodeCachedRange(msg: DecodeCachedRangeMessage): Promise<void> {
  const { readFileSync } = await import("fs");
  const { join } = await import("path");

  const from = BigInt(msg.from);
  const to = BigInt(msg.to);
  const useSelective = msg.extractMask !== undefined && msg.extractMask !== 0x7FF;
  let processed = 0;
  const startedAt = performance.now();

  if (!isNativeCheckpointDecoderAvailable()) {
    postMessage({ type: "error", error: "Native checkpoint decoder not available" });
    postMessage({ type: "done" });
    return;
  }

  for (let seq = from; seq <= to; seq++) {
    const cachePath = join(msg.cacheDir, `${seq}.binpb.zst`);
    try {
      const compressed = new Uint8Array(readFileSync(cachePath));
      const binary = useSelective
        ? decodeArchiveCheckpointBinarySelective(compressed, msg.extractMask!)
        : decodeArchiveCheckpointBinary(compressed);

      if (binary) {
        postBinaryBySeq(seq, binary);
        processed++;
        if (processed % 5000 === 0) {
          const elapsed = (performance.now() - startedAt) / 1000;
          console.error(
            `[decode-worker ${msg.workerIndex}] ${processed} cp ${Math.round(processed / elapsed)} cp/s`,
          );
        }
      }
    } catch (err) {
      postMessage({
        type: "error",
        seq: seq.toString(),
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  postMessage({ type: "done" });
}

self.onmessage = async (event: MessageEvent) => {
  const msg = event.data;
  if (msg && typeof msg === "object" && msg.type === "decode-cached-range") {
    await handleDecodeCachedRange(msg as DecodeCachedRangeMessage);
  }
};
