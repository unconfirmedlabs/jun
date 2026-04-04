/**
 * Native (Zig FFI) Walrus blob encoder.
 *
 * Calls into a compiled Zig library that performs Red Stuff erasure coding,
 * Merkle tree construction, and blob ID computation. Returns sliver pairs
 * with authentication hashes.
 *
 * Falls back gracefully if the native library isn't available.
 */
import { dlopen, ptr, suffix, FFIType } from "bun:ffi";
import { existsSync } from "fs";
import { join } from "path";

// ---------------------------------------------------------------------------
// Native library loading
// ---------------------------------------------------------------------------

const LIB_NAME = `libwalrus_encoder.${suffix}`;
const LIB_PATHS = [
  join(import.meta.dir, "..", "native", LIB_NAME),
  join(import.meta.dir, "..", LIB_NAME),
];

let nativeLib: {
  walrus_encode: (
    blob_ptr: number,
    blob_len: number,
    n_shards: number,
    output_ptr: number,
    output_capacity: number,
  ) => number;
  walrus_blob_id: (
    blob_ptr: number,
    blob_len: number,
    n_shards: number,
    out_id: number,
  ) => number;
} | null = null;

for (const libPath of LIB_PATHS) {
  if (existsSync(libPath)) {
    try {
      const lib = dlopen(libPath, {
        walrus_encode: {
          args: [FFIType.ptr, FFIType.u32, FFIType.u16, FFIType.ptr, FFIType.u32],
          returns: FFIType.u32,
        },
        walrus_blob_id: {
          args: [FFIType.ptr, FFIType.u32, FFIType.u16, FFIType.ptr],
          returns: FFIType.u32,
        },
      });
      nativeLib = lib.symbols;
      break;
    } catch {}
  }
}

// ---------------------------------------------------------------------------
// Output buffer (pre-allocated, reused across calls)
// ---------------------------------------------------------------------------

// 16 MB should be enough for most blobs. For larger blobs, allocate on demand.
const DEFAULT_CAPACITY = 16 * 1024 * 1024;
let outputBuf = new Uint8Array(DEFAULT_CAPACITY);
let outputPtr = ptr(outputBuf);

// ---------------------------------------------------------------------------
// Header layout (matches walrus_encoder.zig)
// ---------------------------------------------------------------------------
//   [0..32]   blob_id
//   [32..40]  unencoded_length (u64 LE)
//   [40]      encoding_type (u8)
//   [41..73]  blob_hash (32 bytes)
//   [73..75]  n_shards (u16 LE)
//   [75..77]  symbol_size (u16 LE)
//   [77..79]  n_primary_source (u16 LE)
//   [79..81]  n_secondary_source (u16 LE)
//   Then per sliver pair:
//     [32 bytes] primary_merkle_hash
//     [32 bytes] secondary_merkle_hash
//     [primary_sliver_bytes] primary data
//     [secondary_sliver_bytes] secondary data

const HEADER_SIZE = 81;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface SliverPair {
  primaryHash: Uint8Array; // 32 bytes
  secondaryHash: Uint8Array; // 32 bytes
  primary: Uint8Array;
  secondary: Uint8Array;
}

export interface WalrusEncodeResult {
  blobId: Uint8Array; // 32 bytes
  unencodedLength: number;
  encodingType: number;
  blobHash: Uint8Array; // 32 bytes
  nShards: number;
  symbolSize: number;
  nPrimarySource: number;
  nSecondarySource: number;
  sliverPairs: SliverPair[];
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Check if the native Walrus encoder is available.
 */
export function isNativeAvailable(): boolean {
  return nativeLib !== null;
}

/**
 * Encode a blob into Walrus sliver pairs using the native Zig encoder.
 * Returns null if the native library isn't available.
 */
export function encodeBlob(
  blob: Uint8Array,
  nShards: number,
): WalrusEncodeResult | null {
  if (!nativeLib) return null;

  const bytesWritten = nativeLib.walrus_encode(
    ptr(blob),
    blob.length,
    nShards,
    outputPtr,
    outputBuf.length,
  );

  if (bytesWritten === 0) return null;

  const view = new DataView(outputBuf.buffer);

  // Parse header
  const blobId = outputBuf.slice(0, 32);
  const unencodedLength = Number(view.getBigUint64(32, true));
  const encodingType = outputBuf[40]!;
  const blobHash = outputBuf.slice(41, 73);
  const nShardsOut = view.getUint16(73, true);
  const symbolSize = view.getUint16(75, true);
  const nPrimarySource = view.getUint16(77, true);
  const nSecondarySource = view.getUint16(79, true);

  const primarySliverBytes = nSecondarySource * symbolSize;
  const secondarySliverBytes = nPrimarySource * symbolSize;

  // Parse sliver pairs
  const sliverPairs: SliverPair[] = [];
  let pos = HEADER_SIZE;

  for (let i = 0; i < nShardsOut; i++) {
    const primaryHash = outputBuf.slice(pos, pos + 32);
    pos += 32;
    const secondaryHash = outputBuf.slice(pos, pos + 32);
    pos += 32;
    const primary = outputBuf.slice(pos, pos + primarySliverBytes);
    pos += primarySliverBytes;
    const secondary = outputBuf.slice(pos, pos + secondarySliverBytes);
    pos += secondarySliverBytes;

    sliverPairs.push({ primaryHash, secondaryHash, primary, secondary });
  }

  return {
    blobId,
    unencodedLength,
    encodingType,
    blobHash,
    nShards: nShardsOut,
    symbolSize,
    nPrimarySource,
    nSecondarySource,
    sliverPairs,
  };
}

/**
 * Compute only the blob ID without generating full sliver data.
 * Returns null if the native library isn't available.
 */
export function computeBlobId(
  blob: Uint8Array,
  nShards: number,
): Uint8Array | null {
  if (!nativeLib) return null;

  const idBuf = new Uint8Array(32);
  const ok = nativeLib.walrus_blob_id(ptr(blob), blob.length, nShards, ptr(idBuf));

  if (ok === 0) return null;
  return idBuf;
}

/**
 * Convert a 32-byte blob ID to hex string.
 */
export function blobIdToHex(id: Uint8Array): string {
  return Array.from(id)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}
