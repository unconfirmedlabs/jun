/**
 * Native (Zig FFI) checkpoint processor.
 *
 * Single FFI call from compressed bytes → balance changes + decoded events.
 * Replaces the entire JS worker pipeline: zstd + proto + BCS + diff + aggregate.
 *
 * Falls back to null if native lib unavailable or parse fails.
 */
import { dlopen, ptr, suffix, FFIType } from "bun:ffi";
import { existsSync } from "fs";
import { join } from "path";
import type { BalanceChange } from "./balance-processor.ts";
import type { GrpcEvent } from "./grpc.ts";

const LIB_NAME = `libcheckpoint_processor.${suffix}`;
const LIB_PATHS = [
  join(import.meta.dir, "..", "native", LIB_NAME),
  join(import.meta.dir, "..", LIB_NAME),
];

let nativeLib: {
  process_checkpoint_compressed: (
    compressed_ptr: number, compressed_len: number,
    output_ptr: number, output_capacity: number,
    filter_ptr: number, filter_len: number,
  ) => number;
} | null = null;

for (const libPath of LIB_PATHS) {
  if (existsSync(libPath)) {
    try {
      const lib = dlopen(libPath, {
        process_checkpoint_compressed: {
          args: [FFIType.ptr, FFIType.u32, FFIType.ptr, FFIType.u32, FFIType.ptr, FFIType.u32],
          returns: FFIType.u32,
        },
      });
      nativeLib = lib.symbols;
      break;
    } catch {}
  }
}

// 2MB output buffer (events can be large with contents bytes)
const OUTPUT_CAPACITY = 2 * 1024 * 1024;
const outputBuf = new Uint8Array(OUTPUT_CAPACITY);
const outputView = new DataView(outputBuf.buffer);
const outputPtr = ptr(outputBuf);

const emptyFilterPtr = ptr(new Uint8Array(1));
let cachedFilterKey = "";
let cachedFilterBuf: Uint8Array | null = null;
let cachedFilterPtr: number = 0;

function getFilter(coinTypeFilter: Set<string> | null): { ptr: number; len: number } {
  if (!coinTypeFilter || coinTypeFilter.size === 0) return { ptr: emptyFilterPtr, len: 0 };
  const key = [...coinTypeFilter].sort().join("\0");
  if (key !== cachedFilterKey) {
    cachedFilterBuf = new TextEncoder().encode(key);
    cachedFilterPtr = ptr(cachedFilterBuf);
    cachedFilterKey = key;
  }
  return { ptr: cachedFilterPtr, len: cachedFilterBuf!.length };
}

export const nativeProcessorAvailable = nativeLib !== null;

export interface ProcessedResult {
  balanceChanges: BalanceChange[];
  events: GrpcEvent[];
  timestampMs: number;
}

function readString(pos: number, len: number): string {
  let s = "";
  for (let i = 0; i < len; i++) s += String.fromCharCode(outputBuf[pos + i]!);
  return s;
}

/**
 * Process a compressed checkpoint: zstd decompress → proto → BCS → balance + events.
 * Returns null if native lib unavailable or parse fails.
 */
export function processCheckpointCompressed(
  compressed: Uint8Array,
  checkpointSeq: bigint,
  coinTypeFilter: Set<string> | null,
): ProcessedResult | null {
  if (!nativeLib) return null;

  const filter = getFilter(coinTypeFilter);
  const bytesWritten = nativeLib.process_checkpoint_compressed(
    ptr(compressed), compressed.length,
    outputPtr, OUTPUT_CAPACITY,
    filter.ptr, filter.len,
  );

  if (bytesWritten === 0 || bytesWritten < 16) return null;

  // Read header (16 bytes)
  const numBalances = outputView.getUint32(0, true);
  const numEvents = outputView.getUint32(4, true);
  const timestampMs = Number(outputView.getBigUint64(8, true));
  const timestamp = new Date(timestampMs);

  let pos = 16;

  // Read balance changes
  const balanceChanges: BalanceChange[] = new Array(numBalances);
  for (let i = 0; i < numBalances; i++) {
    const ownerLen = outputView.getUint16(pos, true); pos += 2;
    const address = readString(pos, ownerLen); pos += ownerLen;
    const ctLen = outputView.getUint16(pos, true); pos += 2;
    const coinType = readString(pos, ctLen); pos += ctLen;
    const amount = outputView.getBigInt64(pos, true); pos += 8;

    balanceChanges[i] = {
      txDigest: "",
      checkpointSeq,
      address,
      coinType,
      amount: amount.toString(),
      timestamp,
    };
  }

  // Read events
  const events: GrpcEvent[] = new Array(numEvents);
  for (let i = 0; i < numEvents; i++) {
    const pkgLen = outputView.getUint16(pos, true); pos += 2;
    const packageId = readString(pos, pkgLen); pos += pkgLen;

    const modLen = outputView.getUint16(pos, true); pos += 2;
    const module = readString(pos, modLen); pos += modLen;

    const sndLen = outputView.getUint16(pos, true); pos += 2;
    const sender = readString(pos, sndLen); pos += sndLen;

    const etLen = outputView.getUint16(pos, true); pos += 2;
    const eventType = readString(pos, etLen); pos += etLen;

    const contentsLen = outputView.getUint32(pos, true); pos += 4;
    const value = new Uint8Array(outputBuf.buffer, pos, contentsLen);
    pos += contentsLen;

    events[i] = {
      packageId,
      module,
      sender,
      eventType,
      contents: { name: eventType, value },
    };
  }

  return { balanceChanges, events, timestampMs };
}
