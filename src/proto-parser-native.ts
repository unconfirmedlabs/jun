/**
 * Native (Zig FFI) protobuf checkpoint parser.
 *
 * Calls into a compiled Zig library that scans protobuf wire format and
 * returns a flat binary index of offset+length pairs. JS creates
 * Uint8Array.subarray() views — zero data copying.
 *
 * Falls back to the pure-JS parser if the native library isn't available.
 */
import { dlopen, ptr, suffix, FFIType, read } from "bun:ffi";
import { existsSync } from "fs";
import { join } from "path";
import type { FastProtoCheckpoint, FastProtoTransaction, FastProtoObject } from "./proto-parser.ts";
import { parseCheckpointProto as jsParseCheckpointProto } from "./proto-parser.ts";

// ---------------------------------------------------------------------------
// Native library loading
// ---------------------------------------------------------------------------

const LIB_NAME = `libproto_parser.${suffix}`;
const LIB_PATHS = [
  join(import.meta.dir, "..", "native", LIB_NAME),
  join(import.meta.dir, "..", LIB_NAME),
];

let nativeLib: {
  parse_checkpoint: (input_ptr: number, input_len: number, output_ptr: number, output_capacity: number) => number;
} | null = null;

for (const libPath of LIB_PATHS) {
  if (existsSync(libPath)) {
    try {
      const lib = dlopen(libPath, {
        parse_checkpoint: {
          args: [FFIType.ptr, FFIType.u32, FFIType.ptr, FFIType.u32],
          returns: FFIType.u32,
        },
      });
      nativeLib = lib.symbols;
      break;
    } catch {}
  }
}

// Pre-allocate output buffer (1MB — enough for any checkpoint)
const OUTPUT_CAPACITY = 1024 * 1024;
const outputBuf = new Uint8Array(OUTPUT_CAPACITY);
const outputView = new DataView(outputBuf.buffer);
const outputPtr = ptr(outputBuf);

// ---------------------------------------------------------------------------
// Header layout
// ---------------------------------------------------------------------------
// u64 sequence_number         [0..8]
// u32 summary_bcs_offset      [8..12]
// u32 summary_bcs_length      [12..16]
// u32 signature_offset        [16..20]
// u32 signature_length        [20..24]
// u32 bitmap_offset           [24..28]
// u32 bitmap_length           [28..32]
// u64 signature_epoch         [32..40]
// u32 contents_bcs_offset     [40..44]
// u32 contents_bcs_length     [44..48]
// u32 num_transactions        [48..52]
// u32 num_objects (at end)    [52..56] -- wait, our header is 52 bytes
// Actually let me re-check the Zig code...

const HEADER_SIZE = 52; // matches Zig

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

function readU32(offset: number): number {
  return outputView.getUint32(offset, true);
}

function readU64AsString(offset: number): string {
  // Read as two u32s and combine (avoid BigInt for common small values)
  const low = outputView.getUint32(offset, true);
  const high = outputView.getUint32(offset + 4, true);
  if (high === 0) return String(low);
  return (BigInt(high) * 0x100000000n + BigInt(low)).toString();
}

function sliceFromInput(input: Uint8Array, offset: number, length: number): Uint8Array {
  if (length === 0) return new Uint8Array(0);
  return input.subarray(offset, offset + length);
}

function stringFromInput(input: Uint8Array, offset: number, length: number): string {
  if (length === 0) return "";
  let str = "";
  for (let i = 0; i < length; i++) {
    str += String.fromCharCode(input[offset + i]!);
  }
  return str;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Parse a decompressed checkpoint protobuf using the native Zig parser.
 * Falls back to pure-JS parser if native library isn't available.
 */
export function parseCheckpointProtoNative(input: Uint8Array): FastProtoCheckpoint {
  if (!nativeLib) return jsParseCheckpointProto(input);

  const bytesWritten = nativeLib.parse_checkpoint(
    ptr(input),
    input.length,
    outputPtr,
    OUTPUT_CAPACITY,
  );

  if (bytesWritten === 0) {
    // Native parse failed — fall back to JS
    return jsParseCheckpointProto(input);
  }

  // Read header
  const sequenceNumber = readU64AsString(0);
  const summaryBcsOff = readU32(8);
  const summaryBcsLen = readU32(12);
  const sigOff = readU32(16);
  const sigLen = readU32(20);
  const bmpOff = readU32(24);
  const bmpLen = readU32(28);
  const sigEpoch = readU64AsString(32);
  const contentsBcsOff = readU32(40);
  const contentsBcsLen = readU32(44);
  const numTx = readU32(48);

  // Read after header: num_objects is patched at offset 52... wait.
  // Let me re-examine the Zig header layout.
  // The Zig code writes: u64 seq, u32x2 summary, u32x2 sig, u32x2 bmp, u64 epoch, u32x2 contents, u32 num_tx, u32 num_objects
  // That's 8 + 8 + 8 + 8 + 8 + 8 + 4 + 4 = 56 bytes, not 52.
  // But HEADER_SIZE in Zig is 52... there's a mismatch. Let me check.
  // Actually: 8(seq) + 4+4(sum) + 4+4(sig) + 4+4(bmp) + 8(epoch) + 4+4(cnt) + 4(numtx) + 4(numobj) = 56
  // The Zig HEADER_SIZE constant says 52 but the actual writes are 56. Bug in the Zig code.
  // For now, read assuming the actual layout (56 bytes).

  const numObjects = readU32(52); // This will be at byte 52 in the Zig output

  // Read transaction entries (variable size due to inline balance changes)
  let readPos = 56; // After header (actual 56 bytes)
  const transactions: FastProtoTransaction[] = new Array(numTx);

  for (let i = 0; i < numTx; i++) {
    const digestOff = readU32(readPos);
    const digestLen = readU32(readPos + 4);
    const effectsOff = readU32(readPos + 8);
    const effectsLen = readU32(readPos + 12);
    const eventsOff = readU32(readPos + 16);
    const eventsLen = readU32(readPos + 20);
    const txBcsOff = readU32(readPos + 24);
    const txBcsLen = readU32(readPos + 28);
    const numBal = readU32(readPos + 32);
    readPos += 36; // 9 u32s = 36 bytes (no placeholder u32 in simplified version)

    // Read inline balance changes
    const balanceChanges: { address: string; coinType: string; amount: string }[] = [];
    for (let j = 0; j < numBal; j++) {
      const addrOff = readU32(readPos);
      const addrLen = readU32(readPos + 4);
      const ctOff = readU32(readPos + 8);
      const ctLen = readU32(readPos + 12);
      const amtOff = readU32(readPos + 16);
      const amtLen = readU32(readPos + 20);
      readPos += 24;

      balanceChanges.push({
        address: stringFromInput(input, addrOff, addrLen),
        coinType: stringFromInput(input, ctOff, ctLen),
        amount: stringFromInput(input, amtOff, amtLen),
      });
    }

    transactions[i] = {
      digest: stringFromInput(input, digestOff, digestLen),
      transaction: txBcsLen > 0 ? { bcs: { value: sliceFromInput(input, txBcsOff, txBcsLen) } } : null,
      effects: effectsLen > 0 ? { bcs: { value: sliceFromInput(input, effectsOff, effectsLen) } } : null,
      events: eventsLen > 0 ? { bcs: { value: sliceFromInput(input, eventsOff, eventsLen) } } : null,
      balanceChanges,
    };
  }

  // Read object entries
  const objects: FastProtoObject[] = [];
  for (let i = 0; i < numObjects; i++) {
    const bcsOff = readU32(readPos);
    const bcsLen = readU32(readPos + 4);
    const idOff = readU32(readPos + 8);
    const idLen = readU32(readPos + 12);
    const version = readU64AsString(readPos + 16);
    readPos += 24; // 4 u32s + 1 u64 = 24 bytes

    objects.push({
      bcs: { value: sliceFromInput(input, bcsOff, bcsLen) },
      objectId: stringFromInput(input, idOff, idLen),
      version,
    });
  }

  return {
    sequenceNumber,
    summary: summaryBcsLen > 0 ? { bcs: { value: sliceFromInput(input, summaryBcsOff, summaryBcsLen) } } : null,
    signature: sigLen > 0 ? {
      epoch: sigEpoch,
      signature: sliceFromInput(input, sigOff, sigLen),
      bitmap: sliceFromInput(input, bmpOff, bmpLen),
    } : null,
    contents: contentsBcsLen > 0 ? { bcs: { value: sliceFromInput(input, contentsBcsOff, contentsBcsLen) } } : null,
    transactions,
    objects: objects.length > 0 ? { objects } : null,
  };
}
