/**
 * Worker thread for checkpoint decoding.
 *
 * Native path: Zig FFI processes compressed bytes, worker transfers the raw
 * output buffer to main thread (zero structured clone — only flat bytes).
 *
 * Legacy path: JS zstd + proto + BCS decode → structured clone of JS objects.
 */
/// <reference lib="webworker" />
import { dlopen, ptr, suffix, FFIType } from "bun:ffi";
import { existsSync } from "fs";
import { join } from "path";
import { decodeCheckpointFromProto, getCheckpointType } from "./archive.ts";
import { parseCheckpointProtoNative } from "./proto-parser-native.ts";
import { computeBalanceChangesFromArchive } from "./archive-balance.ts";
import { parseTimestamp } from "./timestamp.ts";
import { zstdDecompressSync } from "zlib";

const USE_LEGACY = process.env.JUN_LEGACY_PARSERS === "1";

// Load Zig lib — try platform-specific prebuilt, then generic name
const ARCH = process.arch === "arm64" ? "arm64" : "x64";
const PLATFORM = process.platform === "darwin" ? "darwin" : "linux";
const PLATFORM_LIB = `libcheckpoint_processor.${PLATFORM}-${ARCH}.${suffix}`;
const GENERIC_LIB = `libcheckpoint_processor.${suffix}`;
const LIB_PATHS = [
  join(import.meta.dir, "..", "native", "lib", PLATFORM_LIB),
  join(import.meta.dir, "..", "native", GENERIC_LIB),
  join(import.meta.dir, "..", GENERIC_LIB),
];

let zigProcess: ((cp: number, cl: number, op: number, oc: number, fp: number, fl: number) => number) | null = null;

if (!USE_LEGACY) {
  for (const libPath of LIB_PATHS) {
    if (existsSync(libPath)) {
      try {
        const lib = dlopen(libPath, {
          process_checkpoint_compressed: {
            args: [FFIType.ptr, FFIType.u32, FFIType.ptr, FFIType.u32, FFIType.ptr, FFIType.u32],
            returns: FFIType.u32,
          },
        });
        zigProcess = lib.symbols.process_checkpoint_compressed;
        break;
      } catch {}
    }
  }
}

// Per-worker output buffer (8MB — transactions + move calls + events + balance changes)
const OUTPUT_CAP = 8 * 1024 * 1024;
const outputBuf = new Uint8Array(OUTPUT_CAP);
const outputPtr = ptr(outputBuf);

// Pre-encode filter once per worker
const emptyFilterPtr = ptr(new Uint8Array(1));
let filterBuf: Uint8Array | null = null;
let filterPtr = 0;
let filterLen = 0;

function encodeFilter(balanceCoinTypes: string[] | null): void {
  if (!balanceCoinTypes || balanceCoinTypes.length === 0) {
    filterPtr = emptyFilterPtr;
    filterLen = 0;
    return;
  }
  const encoded = new TextEncoder().encode(balanceCoinTypes.join("\0"));
  filterBuf = encoded;
  filterPtr = ptr(encoded);
  filterLen = encoded.length;
}

declare var self: Worker;

self.onmessage = async (event: MessageEvent) => {
  const { id, seq, compressed, balanceCoinTypes, needsTransactions } = event.data as {
    id: number;
    seq: string;
    compressed: Uint8Array;
    balanceCoinTypes: string[] | null;
    needsTransactions?: boolean;
  };

  try {
    const compressedBytes = new Uint8Array(compressed);

    // --- Native path: Zig processes, transfer raw output bytes ---
    if (zigProcess) {
      encodeFilter(balanceCoinTypes);

      const bytesWritten = zigProcess(
        ptr(compressedBytes), compressedBytes.length,
        outputPtr, OUTPUT_CAP,
        filterPtr, filterLen,
      );

      if (bytesWritten >= 16) {
        // Copy output to a new buffer for transfer (outputBuf is reused)
        const rawOutput = new Uint8Array(bytesWritten);
        rawOutput.set(outputBuf.subarray(0, bytesWritten));

        postMessage(
          { id, seq, raw: rawOutput, error: null },
          [rawOutput.buffer], // transferable — zero-copy to main thread
        );
        return;
      }
      // Zig failed — fall through to JS
    }

    // --- JS fallback path ---
    const sequenceNumber = BigInt(seq);
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
      raw: null,
      decoded: null,
      balanceChanges: null,
      error: err instanceof Error ? err.message : String(err),
    });
  }
};
