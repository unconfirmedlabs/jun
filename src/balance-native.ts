/**
 * Native (Zig FFI) balance change computation.
 *
 * Single FFI call: decompressed protobuf bytes → aggregated balance changes.
 * Replaces the entire JS pipeline: proto parse → effects BCS → coin BCS → diff → aggregate.
 *
 * Falls back to JS implementation if native library isn't available or returns error.
 */
import { dlopen, ptr, suffix, FFIType } from "bun:ffi";
import { existsSync } from "fs";
import { join } from "path";
import type { BalanceChange } from "./balance-processor.ts";

// ---------------------------------------------------------------------------
// Native library loading
// ---------------------------------------------------------------------------

const LIB_NAME = `libbalance_computer.${suffix}`;
const LIB_PATHS = [
  join(import.meta.dir, "..", "native", LIB_NAME),
  join(import.meta.dir, "..", LIB_NAME),
];

let nativeLib: {
  compute_balance_changes: (
    input_ptr: number, input_len: number,
    output_ptr: number, output_capacity: number,
    filter_ptr: number, filter_len: number,
  ) => number;
  compute_balance_changes_compressed: (
    compressed_ptr: number, compressed_len: number,
    output_ptr: number, output_capacity: number,
    filter_ptr: number, filter_len: number,
  ) => number;
} | null = null;

const ffiSymbols = {
  compute_balance_changes: {
    args: [FFIType.ptr, FFIType.u32, FFIType.ptr, FFIType.u32, FFIType.ptr, FFIType.u32] as const,
    returns: FFIType.u32 as const,
  },
  compute_balance_changes_compressed: {
    args: [FFIType.ptr, FFIType.u32, FFIType.ptr, FFIType.u32, FFIType.ptr, FFIType.u32] as const,
    returns: FFIType.u32 as const,
  },
};

for (const libPath of LIB_PATHS) {
  if (existsSync(libPath)) {
    try {
      const lib = dlopen(libPath, ffiSymbols);
      nativeLib = lib.symbols;
      break;
    } catch {}
  }
}

// Pre-allocate buffers
const OUTPUT_CAPACITY = 512 * 1024; // 512KB
const outputBuf = new Uint8Array(OUTPUT_CAPACITY);
const outputView = new DataView(outputBuf.buffer);
const outputPtr = ptr(outputBuf);

// Empty filter buffer for "all coins" mode
const emptyFilter = new Uint8Array(0);
const emptyFilterPtr = ptr(new Uint8Array(1)); // need a valid pointer even if len=0

// Cache for encoded filter
let cachedFilterKey = "";
let cachedFilterBuf: Uint8Array | null = null;
let cachedFilterPtr: number = 0;

function getFilterBuffer(coinTypeFilter: Set<string> | null): { ptr: number; len: number } {
  if (!coinTypeFilter || coinTypeFilter.size === 0) {
    return { ptr: emptyFilterPtr, len: 0 };
  }

  // Encode as null-separated ASCII
  const key = [...coinTypeFilter].sort().join("\0");
  if (key !== cachedFilterKey) {
    const encoded = new TextEncoder().encode(key);
    cachedFilterBuf = encoded;
    cachedFilterPtr = ptr(encoded);
    cachedFilterKey = key;
  }

  return { ptr: cachedFilterPtr, len: cachedFilterBuf!.length };
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export const nativeBalanceAvailable = nativeLib !== null;

/**
 * Compute balance changes from decompressed protobuf bytes using native Zig.
 * Returns null if native lib unavailable or parse fails (caller should fall back to JS).
 */
export function computeBalanceChangesNative(
  decompressed: Uint8Array,
  checkpointSeq: bigint,
  timestamp: Date,
  coinTypeFilter: Set<string> | null,
): BalanceChange[] | null {
  if (!nativeLib) return null;

  const filter = getFilterBuffer(coinTypeFilter);

  const bytesWritten = nativeLib.compute_balance_changes(
    ptr(decompressed),
    decompressed.length,
    outputPtr,
    OUTPUT_CAPACITY,
    filter.ptr,
    filter.len,
  );

  if (bytesWritten === 0 || bytesWritten < 4) return null;

  // Read output
  const numChanges = outputView.getUint32(0, true);
  const changes: BalanceChange[] = new Array(numChanges);
  let pos = 4;

  for (let i = 0; i < numChanges; i++) {
    // owner
    const ownerLen = outputView.getUint16(pos, true);
    pos += 2;
    let owner = "";
    for (let j = 0; j < ownerLen; j++) owner += String.fromCharCode(outputBuf[pos + j]!);
    pos += ownerLen;

    // coinType
    const ctLen = outputView.getUint16(pos, true);
    pos += 2;
    let coinType = "";
    for (let j = 0; j < ctLen; j++) coinType += String.fromCharCode(outputBuf[pos + j]!);
    pos += ctLen;

    // amount (i64)
    const amount = outputView.getBigInt64(pos, true);
    pos += 8;

    changes[i] = {
      txDigest: "",
      checkpointSeq,
      address: owner,
      coinType,
      amount: amount.toString(),
      timestamp,
    };
  }

  return changes;
}

/**
 * Compute balance changes directly from compressed (.binpb.zst) bytes.
 * Single FFI call: zstd decompress → proto parse → BCS parse → diff → aggregate.
 * No JS decompression, no Buffer.from() copy.
 */
export function computeBalanceChangesCompressed(
  compressed: Uint8Array,
  checkpointSeq: bigint,
  timestamp: Date,
  coinTypeFilter: Set<string> | null,
): BalanceChange[] | null {
  if (!nativeLib) return null;

  const filter = getFilterBuffer(coinTypeFilter);

  const bytesWritten = nativeLib.compute_balance_changes_compressed(
    ptr(compressed),
    compressed.length,
    outputPtr,
    OUTPUT_CAPACITY,
    filter.ptr,
    filter.len,
  );

  if (bytesWritten === 0 || bytesWritten < 4) return null;

  const numChanges = outputView.getUint32(0, true);
  const changes: BalanceChange[] = new Array(numChanges);
  let pos = 4;

  for (let i = 0; i < numChanges; i++) {
    const ownerLen = outputView.getUint16(pos, true);
    pos += 2;
    let owner = "";
    for (let j = 0; j < ownerLen; j++) owner += String.fromCharCode(outputBuf[pos + j]!);
    pos += ownerLen;

    const ctLen = outputView.getUint16(pos, true);
    pos += 2;
    let coinType = "";
    for (let j = 0; j < ctLen; j++) coinType += String.fromCharCode(outputBuf[pos + j]!);
    pos += ctLen;

    const amount = outputView.getBigInt64(pos, true);
    pos += 8;

    changes[i] = {
      txDigest: "",
      checkpointSeq,
      address: owner,
      coinType,
      amount: amount.toString(),
      timestamp,
    };
  }

  return changes;
}
